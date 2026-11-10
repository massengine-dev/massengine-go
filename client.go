// Package massengine provides the official Go client SDK for the MassEngine
// vector and hybrid search REST API.
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
	Version = "1.1.2"

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

func escapeDocID(id string) (string, error) {
	if id == "batch-get" {
		return "", errors.New("massengine: reserved document identifier")
	}
	return escapeSegment(id)
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
	Vector   []float32       `json:"vector"`
	Date     uint16          `json:"date,omitempty"`
	Category uint16          `json:"category,omitempty"`
	Metadata json.RawMessage `json:"metadata,omitempty"`
}

// IngestResponse is returned by Ingest.
type IngestResponse struct {
	Ingested int  `json:"ingested"`
	Degraded bool `json:"degraded"`
}

// SearchRequest specifies a vector, lexical, or hybrid query with optional
// pre-filters, similarity threshold, and pagination offset.
type SearchRequest struct {
	Tenant         string         `json:"tenant"`
	Vector         []float32      `json:"vector,omitempty"`
	QueryText      string         `json:"query_text,omitempty"`
	HybridAlpha    float32        `json:"hybrid_alpha,omitempty"`
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
	Date     uint16          `json:"date,omitempty"`
	Category uint16          `json:"category,omitempty"`
	Found    bool            `json:"found"`
	Degraded bool            `json:"degraded,omitempty"`
	Metadata json.RawMessage `json:"metadata,omitempty"`
}

// PatchRequest specifies an in-place metadata, date, or category update without
// re-embedding the document vector.
type PatchRequest struct {
	Date        *uint16         `json:"date,omitempty"`
	Category    *uint16         `json:"category,omitempty"`
	Metadata    json.RawMessage `json:"metadata,omitempty"`
	ReplaceMeta bool            `json:"replace_metadata,omitempty"`
}

// TenantSummary describes a tenant's document and storage volume.
type TenantSummary struct {
	Tenant    string `json:"tenant"`
	DocCount  uint64 `json:"doc_count"`
	ByteCount uint64 `json:"byte_count,omitempty"`
}

// TenantQuota defines a tenant's plan tier, document capacity limit, QPS rate
// limit, and billing suspension state.
type TenantQuota struct {
	Plan      string  `json:"plan,omitempty"`
	MaxDocs   uint64  `json:"max_docs,omitempty"`
	MaxQPS    float64 `json:"max_qps,omitempty"`
	Burst     int     `json:"burst,omitempty"`
	Suspended bool    `json:"suspended,omitempty"`
}

// TenantUsage reports cumulative billable read/write counters and capacity
// utilization for a single tenant.
type TenantUsage struct {
	Tenant        string  `json:"tenant"`
	Plan          string  `json:"plan,omitempty"`
	Suspended     bool    `json:"suspended,omitempty"`
	LiveDocs      uint64  `json:"live_docs"`
	MaxDocs       uint64  `json:"max_docs,omitempty"`
	MaxQPS        float64 `json:"max_qps,omitempty"`
	ReadRequests  uint64  `json:"read_requests"`
	ReadUnits     uint64  `json:"read_units"`
	WriteRequests uint64  `json:"write_requests"`
	WriteUnits    uint64  `json:"write_units"`
	RateLimited   uint64  `json:"rate_limited"`
	UpdatedAt     string  `json:"updated_at,omitempty"`
}

// UsageReport aggregates per-tenant usage across the cluster.
type UsageReport struct {
	Tenants         []TenantUsage `json:"tenants"`
	TotalLiveDocs   uint64        `json:"total_live_docs"`
	TotalReadUnits  uint64        `json:"total_read_units"`
	TotalWriteUnits uint64        `json:"total_write_units"`
}

// ReadyStatus is returned by Ready.
type ReadyStatus struct {
	Ready             bool  `json:"ready"`
	UnavailableShards []int `json:"unavailable_shards,omitempty"`
}

// Ingest writes or upserts a batch of documents for tenant.
func (c *Client) Ingest(ctx context.Context, tenant string, docs []Doc) (*IngestResponse, error) {
	escTenant, err := escapeTenant(tenant)
	if err != nil {
		return nil, err
	}
	body := map[string]any{"docs": docs}
	var out IngestResponse
	path := fmt.Sprintf("/v1/tenants/%s/ingest", escTenant)
	if err := c.doJSON(ctx, http.MethodPost, path, body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Search executes a single vector, lexical, or hybrid query.
func (c *Client) Search(ctx context.Context, req SearchRequest) (*SearchResponse, error) {
	var out SearchResponse
	if err := c.doJSON(ctx, http.MethodPost, "/v1/search", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// BatchSearch executes multiple queries in a single round-trip.
func (c *Client) BatchSearch(ctx context.Context, queries []SearchRequest) (*BatchSearchResponse, error) {
	body := map[string]any{"queries": queries}
	var out BatchSearchResponse
	if err := c.doJSON(ctx, http.MethodPost, "/v1/search/batch", body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetDoc fetches a document's metadata by ID.
func (c *Client) GetDoc(ctx context.Context, tenant, id string) (*DocResponse, error) {
	escTenant, err := escapeTenant(tenant)
	if err != nil {
		return nil, err
	}
	escID, err := escapeDocID(id)
	if err != nil {
		return nil, err
	}
	path := fmt.Sprintf("/v1/tenants/%s/docs/%s", escTenant, escID)
	var out DocResponse
	if err := c.doJSON(ctx, http.MethodGet, path, nil, &out); err != nil {
		return nil, err
	}
	out.Found = true
	return &out, nil
}

// PatchDoc updates an existing document's metadata, date, or category in place
// without re-supplying its vector.
func (c *Client) PatchDoc(ctx context.Context, tenant, id string, req PatchRequest) (*DocResponse, error) {
	escTenant, err := escapeTenant(tenant)
	if err != nil {
		return nil, err
	}
	escID, err := escapeDocID(id)
	if err != nil {
		return nil, err
	}
	path := fmt.Sprintf("/v1/tenants/%s/docs/%s", escTenant, escID)
	var out DocResponse
	if err := c.doJSON(ctx, http.MethodPatch, path, req, &out); err != nil {
		return nil, err
	}
	out.Found = true
	return &out, nil
}

// ImportTenant streams a slice of documents as NDJSON into tenant.
func (c *Client) ImportTenant(ctx context.Context, tenant string, docs []Doc) (int, error) {
	if c.initErr != nil {
		return 0, c.initErr
	}
	escTenant, err := escapeTenant(tenant)
	if err != nil {
		return 0, err
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for i := range docs {
		if err := enc.Encode(&docs[i]); err != nil {
			return 0, err
		}
	}
	path := fmt.Sprintf("/v1/tenants/%s/import", escTenant)
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

// ExportTenant streams all live documents belonging to tenant via NDJSON.
func (c *Client) ExportTenant(ctx context.Context, tenant string) ([]Doc, error) {
	if c.initErr != nil {
		return nil, c.initErr
	}
	escTenant, err := escapeTenant(tenant)
	if err != nil {
		return nil, err
	}
	path := fmt.Sprintf("/v1/tenants/%s/export", escTenant)
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

// SetTenantQuota configures a tenant's plan tier, MaxDocs cap, MaxQPS limit,
// and suspension state. Requires the admin API key.
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

// GetTenantUsage returns cumulative billable read/write counters and capacity
// utilization for tenant.
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

// BatchGetDocs fetches multiple documents by ID in a single request.
func (c *Client) BatchGetDocs(ctx context.Context, tenant string, ids []string) ([]DocResponse, error) {
	escTenant, err := escapeTenant(tenant)
	if err != nil {
		return nil, err
	}
	path := fmt.Sprintf("/v1/tenants/%s/docs/batch-get", escTenant)
	body := map[string]any{"ids": ids}
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

// DeleteDoc deletes a document by ID.
func (c *Client) DeleteDoc(ctx context.Context, tenant, id string) error {
	escTenant, err := escapeTenant(tenant)
	if err != nil {
		return err
	}
	escID, err := escapeDocID(id)
	if err != nil {
		return err
	}
	path := fmt.Sprintf("/v1/tenants/%s/docs/%s", escTenant, escID)
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

// GetTenant returns volume metadata for a single tenant.
func (c *Client) GetTenant(ctx context.Context, tenant string) (*TenantSummary, error) {
	escTenant, err := escapeTenant(tenant)
	if err != nil {
		return nil, err
	}
	path := fmt.Sprintf("/v1/tenants/%s", escTenant)
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

// Ready checks service readiness.
func (c *Client) Ready(ctx context.Context) (*ReadyStatus, error) {
	var out ReadyStatus
	if err := c.doJSON(ctx, http.MethodGet, "/v1/ready", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
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

func decodeAPIError(resp *http.Response) error {
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorResponseBytes))
	var errObj struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(raw, &errObj) == nil && errObj.Error != "" {
		return &APIError{StatusCode: resp.StatusCode, Message: errObj.Error}
	}
	return &APIError{StatusCode: resp.StatusCode, Message: strings.TrimSpace(string(raw))}
}
