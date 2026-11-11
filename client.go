// Package massengine provides the official Go client SDK for the MassEngine
// filtered vector search REST API.
package massengine

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	// Version is the semantic version of this SDK release.
	Version = "1.1.4"

	defaultUserAgent        = "massengine-go/" + Version
	defaultBaseURL          = "http://localhost:8081"
	defaultTimeout          = 30 * time.Second
	defaultMaxResponseBytes = 64 << 20 // 64 MiB
	maxErrorResponseBytes   = 64 << 10 // 64 KiB
	keepAliveDrainBytes     = 4096
)

// Options configures a MassEngine HTTP Client.
type Options struct {
	BaseURL          string
	APIKey           string
	HTTPClient       *http.Client
	Timeout          time.Duration
	MaxResponseBytes int64
}

// Client is the official Go SDK client for the MassEngine HTTP REST API.
type Client struct {
	baseURL          string
	apiKey           string
	hc               *http.Client
	maxResponseBytes int64
	initErr          error
}

// String returns a redacted representation that never exposes credentials.
func (c *Client) String() string {
	if c == nil {
		return "<nil>"
	}
	return fmt.Sprintf("massengine.Client{BaseURL: %q}", c.baseURL)
}

// GoString returns a redacted Go-syntax representation that never exposes credentials.
func (c *Client) GoString() string {
	return c.String()
}

// NewClient constructs and validates a MassEngine client, returning an error if
// BaseURL or APIKey is invalid.
func NewClient(opts Options) (*Client, error) {
	base, err := validateBaseURL(opts.BaseURL)
	if err != nil {
		return nil, err
	}
	apiKey, err := validateAPIKey(opts.APIKey)
	if err != nil {
		return nil, err
	}

	maxBytes := opts.MaxResponseBytes
	if maxBytes <= 0 {
		maxBytes = defaultMaxResponseBytes
	}

	var hc *http.Client
	if opts.HTTPClient == nil {
		to := opts.Timeout
		if to <= 0 {
			to = defaultTimeout
		}
		var transport http.RoundTripper
		if dt, ok := http.DefaultTransport.(*http.Transport); ok {
			cloned := dt.Clone()
			if cloned.TLSClientConfig == nil {
				cloned.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
			} else if cloned.TLSClientConfig.MinVersion < tls.VersionTLS12 {
				cloned.TLSClientConfig = cloned.TLSClientConfig.Clone()
				cloned.TLSClientConfig.MinVersion = tls.VersionTLS12
			}
			transport = cloned
		}
		hc = &http.Client{
			Timeout:   to,
			Transport: transport,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
	} else {
		copied := *opts.HTTPClient
		if opts.Timeout > 0 {
			copied.Timeout = opts.Timeout
		}
		if copied.CheckRedirect == nil {
			copied.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			}
		}
		hc = &copied
	}

	return &Client{
		baseURL:          base,
		apiKey:           apiKey,
		hc:               hc,
		maxResponseBytes: maxBytes,
	}, nil
}

// New constructs a MassEngine client. If opts contains an invalid BaseURL or
// APIKey, all subsequent requests on the returned Client fail immediately with
// that validation error without making network calls.
func New(opts Options) *Client {
	cli, err := NewClient(opts)
	if err != nil {
		return &Client{initErr: err}
	}
	return cli
}

func validateBaseURL(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		trimmed = defaultBaseURL
	}
	trimmed = strings.TrimRight(trimmed, "/")
	for i := 0; i < len(trimmed); i++ {
		if trimmed[i] <= 0x20 || trimmed[i] == 0x7f {
			return "", errors.New("massengine: invalid base URL")
		}
	}
	u, err := url.Parse(trimmed)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", errors.New("massengine: base URL must use http or https scheme with a valid host")
	}
	return strings.TrimRight(u.String(), "/"), nil
}

func validateAPIKey(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}
	trimmed := strings.Trim(raw, " \t")
	if trimmed == "" {
		return "", errors.New("massengine: invalid API key format")
	}
	for i := 0; i < len(trimmed); i++ {
		if trimmed[i] < 0x21 || trimmed[i] > 0x7e {
			return "", errors.New("massengine: invalid API key format")
		}
	}
	return trimmed, nil
}

func escapeSegment(s string) (string, error) {
	if s == "" || s == "." || s == ".." {
		return "", errors.New("massengine: invalid URL path segment")
	}
	for i := 0; i < len(s); i++ {
		if s[i] < 0x20 || s[i] == 0x7f {
			return "", errors.New("massengine: invalid control character in URL path segment")
		}
	}
	return url.PathEscape(s), nil
}

func escapeTenant(tenant string) (string, error) {
	if strings.ContainsAny(tenant, `/\`) {
		return "", errors.New("massengine: invalid tenant identifier")
	}
	return escapeSegment(tenant)
}

func escapeDatabase(database string) (string, error) {
	if strings.ContainsAny(database, `/\`) {
		return "", errors.New("massengine: invalid database identifier")
	}
	return escapeSegment(database)
}

func resolveOptionalDatabase(database []string) (string, error) {
	if len(database) > 1 {
		return "", errors.New("massengine: at most one database name may be specified")
	}
	if len(database) == 0 || database[0] == "" {
		return "", nil
	}
	if _, err := escapeDatabase(database[0]); err != nil {
		return "", err
	}
	return database[0], nil
}

func escapeDocID(id string) (string, error) {
	if id == "" {
		return "", errors.New("massengine: document identifier is required")
	}
	if id == "batch-get" {
		return "", errors.New("massengine: reserved document identifier")
	}
	parts := strings.Split(id, "/")
	escaped := make([]string, len(parts))
	for i, part := range parts {
		esc, err := escapeSegment(part)
		if err != nil {
			return "", err
		}
		escaped[i] = esc
	}
	return strings.Join(escaped, "/"), nil
}

func validateDocID(id string) error {
	_, err := escapeDocID(id)
	return err
}

func appendDatabaseQuery(basePath, database string) (string, error) {
	if database == "" {
		return basePath, nil
	}
	if _, err := escapeDatabase(database); err != nil {
		return "", err
	}
	return basePath + "?database=" + url.QueryEscape(database), nil
}

// APIError is returned when the server responds with a non-2xx HTTP status.
type APIError struct {
	StatusCode int
	Message    string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("massengine api (status %d): %s", e.StatusCode, e.Message)
}

// Doc represents a document submitted via Ingest or ImportTenant.
type Doc struct {
	ID       string          `json:"id"`
	Database string          `json:"database,omitempty"`
	Vector   []float32       `json:"vector"`
	Date     uint16          `json:"date,omitempty"`
	Category uint16          `json:"category,omitempty"`
	Metadata json.RawMessage `json:"metadata,omitempty"`
}

// IngestResponse is returned by Ingest and IngestDatabase.
type IngestResponse struct {
	Ingested int    `json:"ingested"`
	Degraded bool   `json:"degraded"`
	Database string `json:"database,omitempty"`
}

// SearchRequest specifies a dense vector query with optional database scope,
// metadata/date/category pre-filters, similarity threshold, and pagination offset.
type SearchRequest struct {
	Tenant         string         `json:"tenant,omitempty"`
	Database       string         `json:"database,omitempty"`
	Vector         []float32      `json:"vector"`
	MetadataFilter map[string]any `json:"metadata_filter,omitempty"`
	K              int            `json:"k"`
	Offset         int            `json:"offset,omitempty"`
	MinScore       *float32       `json:"min_score,omitempty"`
	RerankPool     int            `json:"rerank_pool,omitempty"`
	NProbe         int            `json:"nprobe,omitempty"`
	MinDate        *uint16        `json:"min_date,omitempty"`
	MaxDate        *uint16        `json:"max_date,omitempty"`
	Category       *uint16        `json:"category,omitempty"`
	Categories     []uint16       `json:"categories,omitempty"`
}

// Hit is a single search result returned by Search or BatchSearch.
type Hit struct {
	ID       string          `json:"id"`
	DocID    uint64          `json:"doc_id"`
	Score    float32         `json:"score"`
	Shard    int             `json:"shard"`
	Metadata json.RawMessage `json:"metadata,omitempty"`
}

// SearchResponse is returned by Search.
type SearchResponse struct {
	Hits   []Hit   `json:"hits"`
	TookMs float64 `json:"took_ms,omitempty"`
}

// BatchSearchResponse is returned by BatchSearch.
type BatchSearchResponse struct {
	Results []SearchResponse `json:"results"`
}

// DocResponse is returned by GetDoc, PatchDoc, and BatchGetDocs.
type DocResponse struct {
	ID       string          `json:"id"`
	Tenant   string          `json:"tenant,omitempty"`
	Database string          `json:"database,omitempty"`
	Date     uint16          `json:"date,omitempty"`
	Category uint16          `json:"category,omitempty"`
	Found    bool            `json:"found"`
	Degraded bool            `json:"degraded,omitempty"`
	Metadata json.RawMessage `json:"metadata,omitempty"`
}

// PatchRequest specifies an in-place metadata, date, or category update without
// re-embedding the document vector.
type PatchRequest struct {
	Database    string          `json:"database,omitempty"`
	Date        *uint16         `json:"date,omitempty"`
	Category    *uint16         `json:"category,omitempty"`
	Metadata    json.RawMessage `json:"metadata,omitempty"`
	ReplaceMeta bool            `json:"replace_metadata,omitempty"`
}

// DatabaseConfig specifies configuration options when creating or updating a
// named database within a tenant.
type DatabaseConfig struct {
	Database  string `json:"database"`
	NumShards int    `json:"num_shards,omitempty"`
	RF        int    `json:"rf,omitempty"`
	Mode      string `json:"mode,omitempty"`
	DataType  string `json:"data_type,omitempty"`
}

// DatabaseInfo describes a database's routing configuration and storage volume.
type DatabaseInfo struct {
	Tenant         string `json:"tenant"`
	Database       string `json:"database,omitempty"`
	Partition      string `json:"partition,omitempty"`
	Tag            uint32 `json:"tag,omitempty"`
	Mode           string `json:"mode,omitempty"`
	PinnedShard    int    `json:"pinned_shard"`
	NumShards      int    `json:"num_shards,omitempty"`
	RF             int    `json:"rf,omitempty"`
	DataType       string `json:"data_type,omitempty"`
	ExplicitShards bool   `json:"explicit_shards,omitempty"`
	DocCount       uint64 `json:"doc_count"`
	ByteCount      uint64 `json:"byte_count,omitempty"`
}

// TenantSummary describes a tenant's document and storage volume.
type TenantSummary struct {
	Tenant      string `json:"tenant"`
	Database    string `json:"database,omitempty"`
	Partition   string `json:"partition,omitempty"`
	Mode        string `json:"mode,omitempty"`
	PinnedShard int    `json:"pinned_shard,omitempty"`
	NumShards   int    `json:"num_shards,omitempty"`
	RF          int    `json:"rf,omitempty"`
	DataType    string `json:"data_type,omitempty"`
	DocCount    uint64 `json:"doc_count"`
	ByteCount   uint64 `json:"byte_count,omitempty"`
}

// TenantQuota defines a tenant's plan tier, document and byte capacity limits,
// QPS rate limit, and billing suspension state.
type TenantQuota struct {
	Plan      string  `json:"plan,omitempty"`
	MaxDocs   uint64  `json:"max_docs,omitempty"`
	MaxBytes  uint64  `json:"max_bytes,omitempty"`
	MaxQPS    float64 `json:"max_qps,omitempty"`
	Burst     int     `json:"burst,omitempty"`
	Suspended bool    `json:"suspended,omitempty"`
}

// LatencyBucket represents one cumulative histogram bucket in LatencyStats.
type LatencyBucket struct {
	LeMs  string `json:"le_ms"`
	Count uint64 `json:"count"`
}

// LatencyStats summarizes search latency percentiles and histogram buckets.
type LatencyStats struct {
	Count   uint64          `json:"count"`
	AvgMs   float64         `json:"avg_ms"`
	P50Ms   float64         `json:"p50_ms"`
	P95Ms   float64         `json:"p95_ms"`
	P99Ms   float64         `json:"p99_ms"`
	Buckets []LatencyBucket `json:"buckets,omitempty"`
}

// TenantUsage reports cumulative billable read/write counters, logical byte
// footprint, and search latency percentiles for a single tenant.
type TenantUsage struct {
	Tenant         string       `json:"tenant"`
	Plan           string       `json:"plan,omitempty"`
	Suspended      bool         `json:"suspended,omitempty"`
	PartitionCount int          `json:"partition_count"`
	LiveDocs       uint64       `json:"live_docs"`
	LiveBytes      uint64       `json:"live_bytes"`
	MaxDocs        uint64       `json:"max_docs,omitempty"`
	MaxBytes       uint64       `json:"max_bytes,omitempty"`
	MaxQPS         float64      `json:"max_qps,omitempty"`
	ReadRequests   uint64       `json:"read_requests"`
	ReadUnits      uint64       `json:"read_units"`
	WriteRequests  uint64       `json:"write_requests"`
	WriteUnits     uint64       `json:"write_units"`
	RateLimited    uint64       `json:"rate_limited"`
	SearchLatency  LatencyStats `json:"search_latency"`
	UpdatedAt      string       `json:"updated_at,omitempty"`
}

// UsageReport aggregates per-tenant usage across the cluster.
type UsageReport struct {
	Tenants         []TenantUsage `json:"tenants"`
	TotalLiveDocs   uint64        `json:"total_live_docs"`
	TotalLiveBytes  uint64        `json:"total_live_bytes"`
	TotalReadUnits  uint64        `json:"total_read_units"`
	TotalWriteUnits uint64        `json:"total_write_units"`
}

// ReadyStatus is returned by Ready.
type ReadyStatus struct {
	Ready             bool  `json:"ready"`
	UnavailableShards []int `json:"unavailable_shards,omitempty"`
}

func validateDocs(docs []Doc) error {
	for i := range docs {
		if err := validateDocID(docs[i].ID); err != nil {
			return fmt.Errorf("doc %d: %w", i, err)
		}
		if docs[i].Database != "" {
			if _, err := escapeDatabase(docs[i].Database); err != nil {
				return fmt.Errorf("doc %d: %w", i, err)
			}
		}
	}
	return nil
}

// Ingest writes or upserts a batch of documents for tenant (or the tenant
// bound to a tenant-scoped mk_... API key when tenant is empty), with an
// optional target database name.
func (c *Client) Ingest(ctx context.Context, tenant string, docs []Doc, database ...string) (*IngestResponse, error) {
	db, err := resolveOptionalDatabase(database)
	if err != nil {
		return nil, err
	}
	return c.IngestDatabase(ctx, tenant, db, docs)
}

// IngestDatabase writes or upserts a batch of documents into the specified
// database for tenant (or into the bound mk_... key's tenant when tenant is empty).
func (c *Client) IngestDatabase(ctx context.Context, tenant, database string, docs []Doc) (*IngestResponse, error) {
	if c.initErr != nil {
		return nil, c.initErr
	}
	path := "/v1/ingest"
	if tenant != "" {
		escTenant, err := escapeTenant(tenant)
		if err != nil {
			return nil, err
		}
		path = fmt.Sprintf("/v1/tenants/%s/ingest", escTenant)
	}
	if database != "" {
		if _, err := escapeDatabase(database); err != nil {
			return nil, err
		}
	}
	if err := validateDocs(docs); err != nil {
		return nil, err
	}
	body := map[string]any{"docs": docs}
	if database != "" {
		body["database"] = database
	}
	var out IngestResponse
	if err := c.doJSON(ctx, http.MethodPost, path, body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func validateSearchRequest(req SearchRequest) error {
	if len(req.Vector) == 0 {
		return errors.New("massengine: search vector must be non-empty")
	}
	if req.Tenant != "" {
		if _, err := escapeTenant(req.Tenant); err != nil {
			return err
		}
	}
	if req.Database != "" {
		if _, err := escapeDatabase(req.Database); err != nil {
			return err
		}
	}
	return nil
}

// Search executes a single dense vector query with optional metadata, date, and
// category filters.
func (c *Client) Search(ctx context.Context, req SearchRequest) (*SearchResponse, error) {
	if c.initErr != nil {
		return nil, c.initErr
	}
	if err := validateSearchRequest(req); err != nil {
		return nil, err
	}
	var out SearchResponse
	if err := c.doJSON(ctx, http.MethodPost, "/v1/search", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// BatchSearch executes multiple dense vector queries in a single round-trip,
// with an optional batch-level default database name.
func (c *Client) BatchSearch(ctx context.Context, queries []SearchRequest, database ...string) (*BatchSearchResponse, error) {
	if c.initErr != nil {
		return nil, c.initErr
	}
	db, err := resolveOptionalDatabase(database)
	if err != nil {
		return nil, err
	}
	if len(queries) == 0 {
		return nil, errors.New("massengine: batch search queries must be non-empty")
	}
	for i := range queries {
		if err := validateSearchRequest(queries[i]); err != nil {
			return nil, fmt.Errorf("query %d: %w", i, err)
		}
	}
	body := map[string]any{"queries": queries}
	if db != "" {
		body["database"] = db
	}
	var out BatchSearchResponse
	if err := c.doJSON(ctx, http.MethodPost, "/v1/search/batch", body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func docEndpointPath(tenant, id, database string) (string, error) {
	escID, err := escapeDocID(id)
	if err != nil {
		return "", err
	}
	base := "/v1/docs/" + escID
	if tenant != "" {
		escTenant, err := escapeTenant(tenant)
		if err != nil {
			return "", err
		}
		base = fmt.Sprintf("/v1/tenants/%s/docs/%s", escTenant, escID)
	}
	return appendDatabaseQuery(base, database)
}

// GetDoc fetches a document's metadata by ID, optionally scoped to database.
func (c *Client) GetDoc(ctx context.Context, tenant, id string, database ...string) (*DocResponse, error) {
	db, err := resolveOptionalDatabase(database)
	if err != nil {
		return nil, err
	}
	path, err := docEndpointPath(tenant, id, db)
	if err != nil {
		return nil, err
	}
	var out DocResponse
	if err := c.doJSON(ctx, http.MethodGet, path, nil, &out); err != nil {
		return nil, err
	}
	out.Found = true
	return &out, nil
}

// PatchDoc updates an existing document's metadata, date, or category in place
// without re-supplying its vector.
func (c *Client) PatchDoc(ctx context.Context, tenant, id string, req PatchRequest, database ...string) (*DocResponse, error) {
	db, err := resolveOptionalDatabase(database)
	if err != nil {
		return nil, err
	}
	if db != "" && req.Database == "" {
		req.Database = db
	}
	if req.Database != "" {
		if _, err := escapeDatabase(req.Database); err != nil {
			return nil, err
		}
	}
	path, err := docEndpointPath(tenant, id, "")
	if err != nil {
		return nil, err
	}
	var out DocResponse
	if err := c.doJSON(ctx, http.MethodPatch, path, req, &out); err != nil {
		return nil, err
	}
	out.Found = true
	return &out, nil
}

// ImportTenant streams a slice of documents as NDJSON into tenant (or the bound
// mk_... key's tenant when tenant is empty), optionally scoped to database.
func (c *Client) ImportTenant(ctx context.Context, tenant string, docs []Doc, database ...string) (int, error) {
	if c.initErr != nil {
		return 0, c.initErr
	}
	db, err := resolveOptionalDatabase(database)
	if err != nil {
		return 0, err
	}
	basePath := "/v1/import"
	if tenant != "" {
		escTenant, err := escapeTenant(tenant)
		if err != nil {
			return 0, err
		}
		basePath = fmt.Sprintf("/v1/tenants/%s/import", escTenant)
	}
	path, err := appendDatabaseQuery(basePath, db)
	if err != nil {
		return 0, err
	}
	if err := validateDocs(docs); err != nil {
		return 0, err
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for i := range docs {
		if err := enc.Encode(&docs[i]); err != nil {
			return 0, err
		}
	}
	req, err := c.newRequest(ctx, http.MethodPost, path, &buf, "application/x-ndjson")
	if err != nil {
		return 0, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return 0, err
	}
	defer drainAndClose(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return 0, decodeAPIError(resp)
	}
	raw, err := readBounded(resp.Body, c.maxResponseBytes)
	if err != nil {
		return 0, err
	}
	var out struct {
		Imported int `json:"imported"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return 0, err
	}
	return out.Imported, nil
}

// ExportTenant streams all live documents belonging to tenant (or the bound
// mk_... key's tenant when tenant is empty) via NDJSON, optionally filtered to
// a single database.
func (c *Client) ExportTenant(ctx context.Context, tenant string, database ...string) ([]Doc, error) {
	if c.initErr != nil {
		return nil, c.initErr
	}
	db, err := resolveOptionalDatabase(database)
	if err != nil {
		return nil, err
	}
	basePath := "/v1/export"
	if tenant != "" {
		escTenant, err := escapeTenant(tenant)
		if err != nil {
			return nil, err
		}
		basePath = fmt.Sprintf("/v1/tenants/%s/export", escTenant)
	}
	path, err := appendDatabaseQuery(basePath, db)
	if err != nil {
		return nil, err
	}
	req, err := c.newRequest(ctx, http.MethodGet, path, nil, "")
	if err != nil {
		return nil, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer drainAndClose(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, decodeAPIError(resp)
	}
	cr := &countingReader{r: io.LimitReader(resp.Body, c.maxResponseBytes+1)}
	var docs []Doc
	sc := bufio.NewScanner(cr)
	maxLine := 4 << 20
	if c.maxResponseBytes < int64(maxLine) {
		maxLine = int(c.maxResponseBytes) + 1
	}
	initBuf := 64 << 10
	if maxLine < initBuf {
		initBuf = maxLine
	}
	sc.Buffer(make([]byte, initBuf), maxLine)
	for sc.Scan() {
		if cr.n > c.maxResponseBytes {
			return nil, fmt.Errorf("massengine: response body exceeds maximum size (%d bytes)", c.maxResponseBytes)
		}
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var d Doc
		if err := json.Unmarshal([]byte(line), &d); err != nil {
			return nil, err
		}
		docs = append(docs, d)
	}
	if cr.n > c.maxResponseBytes {
		return nil, fmt.Errorf("massengine: response body exceeds maximum size (%d bytes)", c.maxResponseBytes)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return docs, nil
}

// ListDatabases lists all databases configured under tenant (or under the bound
// mk_... key's tenant when tenant is empty).
func (c *Client) ListDatabases(ctx context.Context, tenant string) ([]DatabaseInfo, error) {
	path := "/v1/databases"
	if tenant != "" {
		escTenant, err := escapeTenant(tenant)
		if err != nil {
			return nil, err
		}
		path = fmt.Sprintf("/v1/tenants/%s/databases", escTenant)
	}
	var resp struct {
		Databases  []DatabaseInfo `json:"databases"`
		Partitions []DatabaseInfo `json:"partitions"`
	}
	if err := c.doJSON(ctx, http.MethodGet, path, nil, &resp); err != nil {
		return nil, err
	}
	if len(resp.Databases) > 0 {
		return resp.Databases, nil
	}
	return resp.Partitions, nil
}

// CreateDatabase creates or updates a named database under tenant (or under the
// bound mk_... key's tenant when tenant is empty).
func (c *Client) CreateDatabase(ctx context.Context, tenant string, cfg DatabaseConfig) (*DatabaseInfo, error) {
	if _, err := escapeDatabase(cfg.Database); err != nil {
		return nil, err
	}
	path := "/v1/databases"
	if tenant != "" {
		escTenant, err := escapeTenant(tenant)
		if err != nil {
			return nil, err
		}
		path = fmt.Sprintf("/v1/tenants/%s/databases", escTenant)
	}
	var resp struct {
		Database  DatabaseInfo `json:"database"`
		Partition string       `json:"partition"`
	}
	if err := c.doJSON(ctx, http.MethodPost, path, cfg, &resp); err != nil {
		return nil, err
	}
	if resp.Database.Database == "" {
		if resp.Partition != "" {
			resp.Database.Database = resp.Partition
		} else {
			resp.Database.Database = cfg.Database
		}
	}
	return &resp.Database, nil
}

// GetDatabase fetches metadata and volume statistics for a single named database.
func (c *Client) GetDatabase(ctx context.Context, tenant, database string) (*DatabaseInfo, error) {
	if c.initErr != nil {
		return nil, c.initErr
	}
	escDB, err := escapeDatabase(database)
	if err != nil {
		return nil, err
	}
	path := "/v1/databases/" + escDB
	if tenant != "" {
		escTenant, err := escapeTenant(tenant)
		if err != nil {
			return nil, err
		}
		path = fmt.Sprintf("/v1/tenants/%s/databases/%s", escTenant, escDB)
	}

	req, err := c.newRequest(ctx, http.MethodGet, path, nil, "")
	if err != nil {
		return nil, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusMethodNotAllowed {
		drainAndClose(resp.Body)
		dbs, listErr := c.ListDatabases(ctx, tenant)
		if listErr != nil {
			return nil, listErr
		}
		for i := range dbs {
			if dbs[i].Database == database || dbs[i].Partition == database {
				return &dbs[i], nil
			}
		}
		return nil, &APIError{StatusCode: http.StatusNotFound, Message: "database not found"}
	}
	defer drainAndClose(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, decodeAPIError(resp)
	}
	raw, err := readBounded(resp.Body, c.maxResponseBytes)
	if err != nil {
		return nil, err
	}
	var probe struct {
		Database json.RawMessage `json:"database"`
	}
	var out DatabaseInfo
	if json.Unmarshal(raw, &probe) == nil && len(bytes.TrimSpace(probe.Database)) > 0 && bytes.TrimSpace(probe.Database)[0] == '{' {
		if err := json.Unmarshal(probe.Database, &out); err != nil {
			return nil, err
		}
	} else if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	if out.Database == "" {
		out.Database = database
	}
	return &out, nil
}

// DeleteDatabase purges all documents and routing state for a named database.
func (c *Client) DeleteDatabase(ctx context.Context, tenant, database string) (int, error) {
	escDB, err := escapeDatabase(database)
	if err != nil {
		return 0, err
	}
	path := "/v1/databases/" + escDB
	if tenant != "" {
		escTenant, err := escapeTenant(tenant)
		if err != nil {
			return 0, err
		}
		path = fmt.Sprintf("/v1/tenants/%s/databases/%s", escTenant, escDB)
	}
	var resp struct {
		DocsDeleted int `json:"docs_deleted"`
	}
	if err := c.doJSON(ctx, http.MethodDelete, path, nil, &resp); err != nil {
		return 0, err
	}
	return resp.DocsDeleted, nil
}

// CreateTenantKey registers (or generates if key is empty) a tenant-scoped
// bearer API key. Requires the admin API key.
func (c *Client) CreateTenantKey(ctx context.Context, tenant, key string) (string, error) {
	escTenant, err := escapeTenant(tenant)
	if err != nil {
		return "", err
	}
	path := fmt.Sprintf("/v1/tenants/%s/keys", escTenant)
	body := map[string]string{"key": key}
	var out struct {
		Key string `json:"key"`
	}
	if err := c.doJSON(ctx, http.MethodPost, path, body, &out); err != nil {
		return "", err
	}
	return out.Key, nil
}

// RevokeTenantKey revokes a tenant-scoped bearer API key. Requires the admin API key.
func (c *Client) RevokeTenantKey(ctx context.Context, tenant, key string) error {
	escTenant, err := escapeTenant(tenant)
	if err != nil {
		return err
	}
	path := fmt.Sprintf("/v1/tenants/%s/keys", escTenant)
	body := map[string]string{"key": key}
	return c.doJSON(ctx, http.MethodDelete, path, body, nil)
}

// SetTenantQuota configures a tenant's plan tier, MaxDocs/MaxBytes caps,
// MaxQPS limit, and suspension state. Requires the admin API key.
func (c *Client) SetTenantQuota(ctx context.Context, tenant string, quota TenantQuota) (*TenantQuota, error) {
	escTenant, err := escapeTenant(tenant)
	if err != nil {
		return nil, err
	}
	path := fmt.Sprintf("/v1/tenants/%s/quota", escTenant)
	var resp struct {
		Quota TenantQuota `json:"quota"`
	}
	if err := c.doJSON(ctx, http.MethodPut, path, quota, &resp); err != nil {
		return nil, err
	}
	return &resp.Quota, nil
}

// GetTenantQuota retrieves the active quota configuration for tenant.
func (c *Client) GetTenantQuota(ctx context.Context, tenant string) (*TenantQuota, error) {
	escTenant, err := escapeTenant(tenant)
	if err != nil {
		return nil, err
	}
	path := fmt.Sprintf("/v1/tenants/%s/quota", escTenant)
	var resp struct {
		Quota TenantQuota `json:"quota"`
	}
	if err := c.doJSON(ctx, http.MethodGet, path, nil, &resp); err != nil {
		return nil, err
	}
	return &resp.Quota, nil
}

// GetTenantUsage returns cumulative billable read/write counters, byte footprint,
// and search latency percentiles for tenant.
func (c *Client) GetTenantUsage(ctx context.Context, tenant string) (*TenantUsage, error) {
	escTenant, err := escapeTenant(tenant)
	if err != nil {
		return nil, err
	}
	path := fmt.Sprintf("/v1/tenants/%s/usage", escTenant)
	var out TenantUsage
	if err := c.doJSON(ctx, http.MethodGet, path, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ListUsage returns the cluster-wide usage report across all tenants. Requires
// the admin API key.
func (c *Client) ListUsage(ctx context.Context) (*UsageReport, error) {
	var out UsageReport
	if err := c.doJSON(ctx, http.MethodGet, "/v1/usage", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// BatchGetDocs fetches multiple documents by ID in a single request, optionally
// scoped to database.
func (c *Client) BatchGetDocs(ctx context.Context, tenant string, ids []string, database ...string) ([]DocResponse, error) {
	if c.initErr != nil {
		return nil, c.initErr
	}
	db, err := resolveOptionalDatabase(database)
	if err != nil {
		return nil, err
	}
	path := "/v1/docs/batch-get"
	if tenant != "" {
		escTenant, err := escapeTenant(tenant)
		if err != nil {
			return nil, err
		}
		path = fmt.Sprintf("/v1/tenants/%s/docs/batch-get", escTenant)
	}
	for i, id := range ids {
		if err := validateDocID(id); err != nil {
			return nil, fmt.Errorf("id %d: %w", i, err)
		}
	}
	body := map[string]any{"ids": ids}
	if db != "" {
		body["database"] = db
	}
	var resp struct {
		Docs []DocResponse `json:"docs"`
	}
	if err := c.doJSON(ctx, http.MethodPost, path, body, &resp); err != nil {
		return nil, err
	}
	for i := range resp.Docs {
		resp.Docs[i].Found = true
	}
	return resp.Docs, nil
}

// DeleteDoc deletes a document by ID, optionally scoped to database.
func (c *Client) DeleteDoc(ctx context.Context, tenant, id string, database ...string) error {
	db, err := resolveOptionalDatabase(database)
	if err != nil {
		return err
	}
	path, err := docEndpointPath(tenant, id, db)
	if err != nil {
		return err
	}
	return c.doJSON(ctx, http.MethodDelete, path, nil, nil)
}

// ListTenants lists all known tenants.
func (c *Client) ListTenants(ctx context.Context) ([]TenantSummary, error) {
	var resp struct {
		Tenants []TenantSummary `json:"tenants"`
	}
	if err := c.doJSON(ctx, http.MethodGet, "/v1/tenants", nil, &resp); err != nil {
		return nil, err
	}
	return resp.Tenants, nil
}

// GetTenant returns volume metadata for a single tenant (or database when
// optional database is specified).
func (c *Client) GetTenant(ctx context.Context, tenant string, database ...string) (*TenantSummary, error) {
	db, err := resolveOptionalDatabase(database)
	if err != nil {
		return nil, err
	}
	escTenant, err := escapeTenant(tenant)
	if err != nil {
		return nil, err
	}
	path, err := appendDatabaseQuery(fmt.Sprintf("/v1/tenants/%s", escTenant), db)
	if err != nil {
		return nil, err
	}
	var out TenantSummary
	if err := c.doJSON(ctx, http.MethodGet, path, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// DeleteTenant deletes all documents and state for tenant.
func (c *Client) DeleteTenant(ctx context.Context, tenant string) (int, error) {
	escTenant, err := escapeTenant(tenant)
	if err != nil {
		return 0, err
	}
	path := fmt.Sprintf("/v1/tenants/%s", escTenant)
	var resp struct {
		DocsDeleted int `json:"docs_deleted"`
	}
	if err := c.doJSON(ctx, http.MethodDelete, path, nil, &resp); err != nil {
		return 0, err
	}
	return resp.DocsDeleted, nil
}

// Flush persists buffered writes to durable storage.
func (c *Client) Flush(ctx context.Context) error {
	return c.doJSON(ctx, http.MethodPost, "/v1/flush", nil, nil)
}

// Compact optimizes storage segments.
func (c *Client) Compact(ctx context.Context) error {
	return c.doJSON(ctx, http.MethodPost, "/v1/compact", nil, nil)
}

// Health checks gateway liveness via GET /v1/health.
func (c *Client) Health(ctx context.Context) (bool, error) {
	var resp struct {
		Status string `json:"status"`
		OK     bool   `json:"ok"`
	}
	if err := c.doJSON(ctx, http.MethodGet, "/v1/health", nil, &resp); err != nil {
		return false, err
	}
	s := strings.ToLower(strings.TrimSpace(resp.Status))
	return resp.OK || s == "ok" || s == "healthy", nil
}

// Ready checks service readiness. Both HTTP 200 and HTTP 503 JSON readiness
// payloads return a populated ReadyStatus without an error.
func (c *Client) Ready(ctx context.Context) (*ReadyStatus, error) {
	if c.initErr != nil {
		return nil, c.initErr
	}
	req, err := c.newRequest(ctx, http.MethodGet, "/v1/ready", nil, "")
	if err != nil {
		return nil, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer drainAndClose(resp.Body)
	if (resp.StatusCode >= 200 && resp.StatusCode < 300) || resp.StatusCode == http.StatusServiceUnavailable {
		raw, err := readBounded(resp.Body, c.maxResponseBytes)
		if err != nil {
			return nil, err
		}
		var probe struct {
			Ready             *bool `json:"ready"`
			UnavailableShards []int `json:"unavailable_shards"`
		}
		if json.Unmarshal(raw, &probe) != nil || probe.Ready == nil {
			return nil, decodeAPIErrorBytes(resp.StatusCode, raw)
		}
		return &ReadyStatus{
			Ready:             *probe.Ready,
			UnavailableShards: probe.UnavailableShards,
		}, nil
	}
	return nil, decodeAPIError(resp)
}

func (c *Client) newRequest(ctx context.Context, method, path string, body io.Reader, contentType string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", defaultUserAgent)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	return req, nil
}

func (c *Client) doJSON(ctx context.Context, method, path string, reqBody any, respOut any) error {
	if c.initErr != nil {
		return c.initErr
	}
	var (
		bodyReader  io.Reader
		contentType string
	)
	if reqBody != nil {
		raw, err := json.Marshal(reqBody)
		if err != nil {
			return err
		}
		bodyReader = bytes.NewReader(raw)
		contentType = "application/json"
	}
	req, err := c.newRequest(ctx, method, path, bodyReader, contentType)
	if err != nil {
		return err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer drainAndClose(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return decodeAPIError(resp)
	}
	raw, err := readBounded(resp.Body, c.maxResponseBytes)
	if err != nil {
		return err
	}
	if respOut != nil && len(raw) > 0 {
		return json.Unmarshal(raw, respOut)
	}
	return nil
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

func readBounded(r io.Reader, maxBytes int64) ([]byte, error) {
	raw, err := io.ReadAll(io.LimitReader(r, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > maxBytes {
		return nil, fmt.Errorf("massengine: response body exceeds maximum size (%d bytes)", maxBytes)
	}
	return raw, nil
}

func drainAndClose(rc io.ReadCloser) {
	_, _ = io.Copy(io.Discard, io.LimitReader(rc, keepAliveDrainBytes))
	_ = rc.Close()
}

func decodeAPIErrorBytes(statusCode int, raw []byte) error {
	if len(raw) > maxErrorResponseBytes {
		raw = raw[:maxErrorResponseBytes]
	}
	var errObj struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(raw, &errObj) == nil && errObj.Error != "" {
		return &APIError{StatusCode: statusCode, Message: errObj.Error}
	}
	return &APIError{StatusCode: statusCode, Message: strings.TrimSpace(string(raw))}
}

func decodeAPIError(resp *http.Response) error {
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorResponseBytes))
	return decodeAPIErrorBytes(resp.StatusCode, raw)
}
