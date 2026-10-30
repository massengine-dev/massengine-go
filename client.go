// Package massengine provides the official Go client SDK for the MassEngine
// vector and hybrid search REST API.
package massengine

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Options configures a MassEngine HTTP Client.
type Options struct {
	BaseURL    string
	APIKey     string
	HTTPClient *http.Client
	Timeout    time.Duration
}

// Client is the official Go SDK client for the MassEngine HTTP REST API.
type Client struct {
	baseURL string
	apiKey  string
	hc      *http.Client
}

// New constructs a MassEngine client.
func New(opts Options) *Client {
	base := strings.TrimRight(opts.BaseURL, "/")
	if base == "" {
		base = "http://localhost:8081"
	}
	hc := opts.HTTPClient
	if hc == nil {
		to := opts.Timeout
		if to <= 0 {
			to = 30 * time.Second
		}
		hc = &http.Client{Timeout: to}
	}
	return &Client{
		baseURL: base,
		apiKey:  opts.APIKey,
		hc:      hc,
	}
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

// SearchStats reports query execution telemetry.
type SearchStats struct {
	TotalBlocks      int `json:"TotalBlocks"`
	SkippedByTenant  int `json:"SkippedByTenant"`
	SkippedByDate    int `json:"SkippedByDate"`
	SkippedByCat     int `json:"SkippedByCat"`
	ScannedBlocks    int `json:"ScannedBlocks"`
	ScannedVectors   int `json:"ScannedVectors"`
	RerankedVectors  int `json:"RerankedVectors"`
}

// SearchResponse is returned by Search.
type SearchResponse struct {
	Hits  []Hit       `json:"hits"`
	Stats SearchStats `json:"stats"`
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

// TenantSummary describes a tenant's routing tier and volume.
type TenantSummary struct {
	Tenant      string `json:"tenant"`
	Tag         uint32 `json:"tag"`
	Mode        string `json:"mode"`
	PinnedShard int    `json:"pinned_shard"`
	DocCount    uint64 `json:"doc_count"`
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
	body := map[string]any{"docs": docs}
	var out IngestResponse
	path := fmt.Sprintf("/v1/tenants/%s/ingest", url.PathEscape(tenant))
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

// GetDoc fetches a document's metadata by external ID.
func (c *Client) GetDoc(ctx context.Context, tenant, id string) (*DocResponse, error) {
	path := fmt.Sprintf("/v1/tenants/%s/docs/%s", url.PathEscape(tenant), url.PathEscape(id))
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
	path := fmt.Sprintf("/v1/tenants/%s/docs/%s", url.PathEscape(tenant), url.PathEscape(id))
	var out DocResponse
	if err := c.doJSON(ctx, http.MethodPatch, path, req, &out); err != nil {
		return nil, err
	}
	out.Found = true
	return &out, nil
}

// ImportTenant streams a slice of documents as NDJSON into tenant.
func (c *Client) ImportTenant(ctx context.Context, tenant string, docs []Doc) (int, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for i := range docs {
		if err := enc.Encode(&docs[i]); err != nil {
			return 0, err
		}
	}
	path := fmt.Sprintf("/v1/tenants/%s/import", url.PathEscape(tenant))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, &buf)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return 0, decodeAPIError(resp)
	}
	var out struct {
		Imported int `json:"imported"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, err
	}
	return out.Imported, nil
}

// ExportTenant streams all live documents belonging to tenant via NDJSON.
func (c *Client) ExportTenant(ctx context.Context, tenant string) ([]Doc, error) {
	path := fmt.Sprintf("/v1/tenants/%s/export", url.PathEscape(tenant))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return nil, err
	}
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return nil, decodeAPIError(resp)
	}
	var docs []Doc
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 64<<10), 4<<20)
	for sc.Scan() {
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
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return docs, nil
}

// CreateTenantKey registers (or generates if key is empty) a tenant-scoped
// bearer API key. Requires the admin API key.
func (c *Client) CreateTenantKey(ctx context.Context, tenant, key string) (string, error) {
	path := fmt.Sprintf("/v1/tenants/%s/keys", url.PathEscape(tenant))
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
	path := fmt.Sprintf("/v1/tenants/%s/keys", url.PathEscape(tenant))
	body := map[string]string{"key": key}
	return c.doJSON(ctx, http.MethodDelete, path, body, nil)
}

// SetTenantQuota configures a tenant's plan tier, MaxDocs cap, MaxQPS limit,
// and suspension state. Requires the admin API key.
func (c *Client) SetTenantQuota(ctx context.Context, tenant string, quota TenantQuota) (*TenantQuota, error) {
	path := fmt.Sprintf("/v1/tenants/%s/quota", url.PathEscape(tenant))
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
	path := fmt.Sprintf("/v1/tenants/%s/quota", url.PathEscape(tenant))
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
	path := fmt.Sprintf("/v1/tenants/%s/usage", url.PathEscape(tenant))
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

// BatchGetDocs fetches multiple documents by external ID in a single request.
func (c *Client) BatchGetDocs(ctx context.Context, tenant string, ids []string) ([]DocResponse, error) {
	path := fmt.Sprintf("/v1/tenants/%s/docs/batch-get", url.PathEscape(tenant))
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

// DeleteDoc tombstones a document by external ID.
func (c *Client) DeleteDoc(ctx context.Context, tenant, id string) error {
	path := fmt.Sprintf("/v1/tenants/%s/docs/%s", url.PathEscape(tenant), url.PathEscape(id))
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

// GetTenant returns routing and volume metadata for a single tenant.
func (c *Client) GetTenant(ctx context.Context, tenant string) (*TenantSummary, error) {
	path := fmt.Sprintf("/v1/tenants/%s", url.PathEscape(tenant))
	var out TenantSummary
	if err := c.doJSON(ctx, http.MethodGet, path, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// DeleteTenant purges all documents and routing state for tenant across the cluster.
func (c *Client) DeleteTenant(ctx context.Context, tenant string) (int, error) {
	path := fmt.Sprintf("/v1/tenants/%s", url.PathEscape(tenant))
	var resp struct {
		DocsDeleted int `json:"docs_deleted"`
	}
	if err := c.doJSON(ctx, http.MethodDelete, path, nil, &resp); err != nil {
		return 0, err
	}
	return resp.DocsDeleted, nil
}

// Flush flushes all mutable shard arenas into immutable segments.
func (c *Client) Flush(ctx context.Context) error {
	return c.doJSON(ctx, http.MethodPost, "/v1/flush", nil, nil)
}

// Compact triggers lock-free segment compaction across all cluster shards.
func (c *Client) Compact(ctx context.Context) error {
	return c.doJSON(ctx, http.MethodPost, "/v1/compact", nil, nil)
}

// Ready checks cluster readiness.
func (c *Client) Ready(ctx context.Context) (*ReadyStatus, error) {
	var out ReadyStatus
	if err := c.doJSON(ctx, http.MethodGet, "/v1/ready", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) doJSON(ctx context.Context, method, path string, reqBody any, respOut any) error {
	var bodyReader io.Reader
	if reqBody != nil {
		raw, err := json.Marshal(reqBody)
		if err != nil {
			return err
		}
		bodyReader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bodyReader)
	if err != nil {
		return err
	}
	if reqBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return decodeAPIError(resp)
	}
	if respOut != nil {
		return json.NewDecoder(resp.Body).Decode(respOut)
	}
	return nil
}

func decodeAPIError(resp *http.Response) error {
	raw, _ := io.ReadAll(resp.Body)
	var errObj struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(raw, &errObj) == nil && errObj.Error != "" {
		return &APIError{StatusCode: resp.StatusCode, Message: errObj.Error}
	}
	return &APIError{StatusCode: resp.StatusCode, Message: strings.TrimSpace(string(raw))}
}
