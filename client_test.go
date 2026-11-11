package massengine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestClientSDKLifecycle(t *testing.T) {
	var seenUserAgent atomic.Value

	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/health", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})
	mux.HandleFunc("PUT /v1/tenants/{tenant}/quota", func(w http.ResponseWriter, r *http.Request) {
		seenUserAgent.Store(r.Header.Get("User-Agent"))
		var q TenantQuota
		_ = json.NewDecoder(r.Body).Decode(&q)
		_ = json.NewEncoder(w).Encode(map[string]any{"tenant": r.PathValue("tenant"), "quota": q})
	})
	mux.HandleFunc("POST /v1/tenants/{tenant}/keys", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"tenant": r.PathValue("tenant"), "key": "mk_go_test_key"})
	})
	mux.HandleFunc("POST /v1/tenants/{tenant}/ingest", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Docs []Doc `json:"docs"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		_ = json.NewEncoder(w).Encode(IngestResponse{Ingested: len(body.Docs)})
	})
	mux.HandleFunc("POST /v1/search", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(SearchResponse{
			Hits:   []Hit{{ID: "go-1", DocID: 1, Score: 0.98}},
			TookMs: 1.25,
		})
	})
	mux.HandleFunc("GET /v1/tenants/{tenant}", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(TenantSummary{
			Tenant:    r.PathValue("tenant"),
			DocCount:  2,
			ByteCount: 4096,
		})
	})
	mux.HandleFunc("GET /v1/tenants/{tenant}/usage", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(TenantUsage{
			Tenant:         r.PathValue("tenant"),
			Plan:           "pro",
			PartitionCount: 2,
			LiveDocs:       2,
			LiveBytes:      8192,
			MaxDocs:        100,
			MaxBytes:       1073741824,
			ReadUnits:      1,
			WriteUnits:     2,
			SearchLatency: LatencyStats{
				Count: 5,
				AvgMs: 0.85,
				P50Ms: 0.70,
				P95Ms: 1.40,
				P99Ms: 1.90,
				Buckets: []LatencyBucket{
					{LeMs: "1", Count: 4},
					{LeMs: "2.5", Count: 5},
				},
			},
		})
	})
	mux.HandleFunc("GET /v1/usage", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(UsageReport{
			Tenants: []TenantUsage{
				{Tenant: "acme", LiveDocs: 2, LiveBytes: 8192},
			},
			TotalLiveDocs:   2,
			TotalLiveBytes:  8192,
			TotalReadUnits:  1,
			TotalWriteUnits: 2,
		})
	})
	mux.HandleFunc("GET /v1/tenants/{tenant}/docs/{id...}", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.Header.Get("Authorization"), "revoked") {
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "unauthorized"})
			return
		}
		_ = json.NewEncoder(w).Encode(DocResponse{ID: r.PathValue("id"), Tenant: r.PathValue("tenant")})
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	ctx := context.Background()
	admin := New(Options{BaseURL: ts.URL, APIKey: "  admin-key  "})

	healthy, err := admin.Health(ctx)
	if err != nil || !healthy {
		t.Fatalf("Health: healthy=%v err=%v", healthy, err)
	}

	q, err := admin.SetTenantQuota(ctx, "acme", TenantQuota{
		Plan:     "pro",
		MaxDocs:  100,
		MaxBytes: 1073741824,
	})
	if err != nil || q.Plan != "pro" || q.MaxDocs != 100 || q.MaxBytes != 1073741824 {
		t.Fatalf("SetTenantQuota: %+v, err=%v", q, err)
	}
	if gotUA, _ := seenUserAgent.Load().(string); gotUA != "massengine-go/1.1.4" {
		t.Fatalf("expected User-Agent massengine-go/1.1.4, got %q", gotUA)
	}

	tKey, err := admin.CreateTenantKey(ctx, "acme", "")
	if err != nil || !strings.HasPrefix(tKey, "mk_") {
		t.Fatalf("CreateTenantKey: %q, err=%v", tKey, err)
	}

	cli := New(Options{BaseURL: ts.URL, APIKey: tKey})
	ing, err := cli.Ingest(ctx, "acme", []Doc{
		{ID: "go-1", Vector: []float32{1, 0}},
		{ID: "go-2", Vector: []float32{0, 1}},
	})
	if err != nil || ing.Ingested != 2 {
		t.Fatalf("Ingest: %+v, err=%v", ing, err)
	}

	sr, err := cli.Search(ctx, SearchRequest{Tenant: "acme", Vector: []float32{1, 0}, K: 5})
	if err != nil || len(sr.Hits) != 1 || sr.Hits[0].ID != "go-1" || sr.TookMs != 1.25 {
		t.Fatalf("Search: %+v, err=%v", sr, err)
	}

	tsum, err := cli.GetTenant(ctx, "acme")
	if err != nil || tsum.Tenant != "acme" || tsum.DocCount != 2 || tsum.ByteCount != 4096 {
		t.Fatalf("GetTenant: %+v, err=%v", tsum, err)
	}

	u, err := cli.GetTenantUsage(ctx, "acme")
	if err != nil || u.Plan != "pro" || u.LiveDocs != 2 || u.LiveBytes != 8192 || u.MaxBytes != 1073741824 || u.PartitionCount != 2 || u.SearchLatency.P95Ms != 1.40 {
		t.Fatalf("GetTenantUsage: %+v, err=%v", u, err)
	}

	rep, err := admin.ListUsage(ctx)
	if err != nil || rep.TotalLiveBytes != 8192 || rep.TotalLiveDocs != 2 {
		t.Fatalf("ListUsage: %+v, err=%v", rep, err)
	}

	revoked := New(Options{BaseURL: ts.URL, APIKey: "revoked"})
	_, err = revoked.GetDoc(ctx, "acme", "go-1")
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 APIError, got: %v", err)
	}
}

func TestSearchRequestRemovesQueryTextAndHybridAlpha(t *testing.T) {
	rt := reflect.TypeOf(SearchRequest{})
	for _, removed := range []string{"QueryText", "HybridAlpha"} {
		if _, exists := rt.FieldByName(removed); exists {
			t.Fatalf("SearchRequest must not expose %s", removed)
		}
	}
}

func TestPathSegmentValidation(t *testing.T) {
	var hitServer atomic.Bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hitServer.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	cli := New(Options{BaseURL: ts.URL, APIKey: "mk_valid"})
	ctx := context.Background()

	badAdminTenants := []string{"", ".", "..", "acme/evil", "acme\\evil", "acme\x00", "acme\n", "acme\x7f"}
	for _, bt := range badAdminTenants {
		if _, err := cli.GetTenant(ctx, bt); err == nil {
			t.Fatalf("expected error for bad tenant %q", bt)
		}
		if _, err := cli.DeleteTenant(ctx, bt); err == nil {
			t.Fatalf("expected error for bad tenant %q in DeleteTenant", bt)
		}
	}

	badScopedTenants := []string{".", "..", "acme/evil", "acme\\evil", "acme\x00", "acme\n", "acme\x7f"}
	for _, bt := range badScopedTenants {
		if _, err := cli.GetDoc(ctx, bt, "doc-1"); err == nil {
			t.Fatalf("expected error for bad tenant %q in GetDoc", bt)
		}
	}

	badDocIDs := []string{"", ".", "..", "batch-get", "doc\x00", "doc\r\n", "doc\x7f", "a/../b", "a/./b", "/lead", "trail/", "a//b"}
	for _, bid := range badDocIDs {
		if _, err := cli.Ingest(ctx, "acme", []Doc{{ID: bid, Vector: []float32{1, 0}}}); err == nil {
			t.Fatalf("expected error for bad doc ID %q in Ingest", bid)
		}
		if _, err := cli.ImportTenant(ctx, "acme", []Doc{{ID: bid, Vector: []float32{1, 0}}}); err == nil {
			t.Fatalf("expected error for bad doc ID %q in ImportTenant", bid)
		}
		if _, err := cli.GetDoc(ctx, "acme", bid); err == nil {
			t.Fatalf("expected error for bad doc ID %q in GetDoc", bid)
		}
		if _, err := cli.PatchDoc(ctx, "acme", bid, PatchRequest{ReplaceMeta: true}); err == nil {
			t.Fatalf("expected error for bad doc ID %q in PatchDoc", bid)
		}
		if _, err := cli.BatchGetDocs(ctx, "acme", []string{bid}); err == nil {
			t.Fatalf("expected error for bad doc ID %q in BatchGetDocs", bid)
		}
		if err := cli.DeleteDoc(ctx, "acme", bid); err == nil {
			t.Fatalf("expected error for bad doc ID %q in DeleteDoc", bid)
		}
	}

	badDatabases := []string{".", "..", "db/evil", "db\\evil", "db\x00", "db\n"}
	for _, bdb := range badDatabases {
		if _, err := cli.GetDoc(ctx, "acme", "doc-1", bdb); err == nil {
			t.Fatalf("expected error for bad database %q in GetDoc", bdb)
		}
		if _, err := cli.GetDatabase(ctx, "acme", bdb); err == nil {
			t.Fatalf("expected error for bad database %q in GetDatabase", bdb)
		}
	}

	if hitServer.Load() {
		t.Fatal("server should never be contacted when path validation fails")
	}
}

func TestRedirectBlockedNoAuthLeak(t *testing.T) {
	var targetHit atomic.Bool
	var leakedAuth atomic.Value

	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetHit.Store(true)
		leakedAuth.Store(r.Header.Get("Authorization"))
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/steal", http.StatusFound)
	}))
	defer origin.Close()

	cli := New(Options{BaseURL: origin.URL, APIKey: "mk_super_secret_token"})
	_, err := cli.Ready(context.Background())
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusFound {
		t.Fatalf("expected 302 APIError, got: %v", err)
	}
	if targetHit.Load() {
		t.Fatalf("redirect target was contacted! leaked Authorization=%v", leakedAuth.Load())
	}
}

func TestInvalidBaseURLAndAPIKeyNoKeyLeak(t *testing.T) {
	for _, badURL := range []string{"ftp://example.com", "file:///etc/passwd", "javascript:alert(1)", "http://user:pass@example.com"} {
		if _, err := NewClient(Options{BaseURL: badURL}); err == nil {
			t.Fatalf("expected error for invalid BaseURL %q", badURL)
		}
	}

	secret := "mk_ultra_sensitive_secret_value"
	badKeys := []string{
		secret + "\r\nX-Injected: 1",
		secret + "\n",
		secret + "\x00",
		secret + " trailing-space-inside",
		"   ",
	}
	for _, bk := range badKeys {
		_, err := NewClient(Options{BaseURL: "https://massengine.com", APIKey: bk})
		if err == nil {
			t.Fatalf("expected error for invalid APIKey")
		}
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("API key leaked in constructor error: %v", err)
		}

		deferred := New(Options{BaseURL: "https://massengine.com", APIKey: bk})
		if strings.Contains(fmt.Sprintf("%v %#v", deferred, deferred), secret) {
			t.Fatalf("API key leaked in Client formatting")
		}
		_, reqErr := deferred.Ready(context.Background())
		if reqErr == nil || strings.Contains(reqErr.Error(), secret) {
			t.Fatalf("expected redacted deferred error, got: %v", reqErr)
		}
	}
}

func TestTimeoutAndMaxResponseBytes(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/ready", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ready":true,"padding":"` + strings.Repeat("a", 2048) + `"}`))
	})
	mux.HandleFunc("GET /v1/tenants/acme/export", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		for i := 0; i < 20; i++ {
			_, _ = fmt.Fprintf(w, "{\"id\":\"d-%d\",\"vector\":[1,2,3,4]}\n", i)
		}
	})
	mux.HandleFunc("POST /v1/flush", func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(150 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	smallCli := New(Options{BaseURL: ts.URL, MaxResponseBytes: 256})
	if _, err := smallCli.Ready(context.Background()); err == nil || !strings.Contains(err.Error(), "exceeds maximum size") {
		t.Fatalf("expected max response bytes error on Ready, got: %v", err)
	}
	if _, err := smallCli.ExportTenant(context.Background(), "acme"); err == nil || !strings.Contains(err.Error(), "exceeds maximum size") {
		t.Fatalf("expected max response bytes error on ExportTenant, got: %v", err)
	}

	fastTimeoutCli := New(Options{BaseURL: ts.URL, Timeout: 30 * time.Millisecond})
	if err := fastTimeoutCli.Flush(context.Background()); err == nil {
		t.Fatal("expected timeout error on slow Flush")
	}
}

func TestReady503ReturnsStatusWithoutAPIError(t *testing.T) {
	mode := atomic.Int32{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		switch mode.Load() {
		case 0:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"ready":false,"unavailable_shards":[1,3]}`))
		case 1:
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("503 Bad Gateway"))
		case 2:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"cluster upstream not configured"}`))
		default:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"internal failure"}`))
		}
	}))
	defer ts.Close()

	cli := New(Options{BaseURL: ts.URL, APIKey: "mk_test"})
	st, err := cli.Ready(context.Background())
	if err != nil {
		t.Fatalf("expected nil error on 503 JSON ready status, got: %v", err)
	}
	if st.Ready || len(st.UnavailableShards) != 2 || st.UnavailableShards[0] != 1 || st.UnavailableShards[1] != 3 {
		t.Fatalf("unexpected ReadyStatus on 503: %+v", st)
	}

	for _, errMode := range []int32{1, 2, 3} {
		mode.Store(errMode)
		_, err = cli.Ready(context.Background())
		var apiErr *APIError
		if !errors.As(err, &apiErr) {
			t.Fatalf("mode %d: expected APIError, got: %v", errMode, err)
		}
	}
}

func TestEmptySearchVectorRejectedClientSide(t *testing.T) {
	var contacted atomic.Bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		contacted.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	cli := New(Options{BaseURL: ts.URL, APIKey: "mk_test"})
	ctx := context.Background()

	if _, err := cli.Search(ctx, SearchRequest{Tenant: "acme", K: 5}); err == nil {
		t.Fatal("expected validation error when Search vector is empty")
	}
	if _, err := cli.BatchSearch(ctx, nil); err == nil {
		t.Fatal("expected validation error when BatchSearch queries slice is empty")
	}
	if _, err := cli.BatchSearch(ctx, []SearchRequest{{Tenant: "acme", Vector: []float32{1, 0}, K: 5}, {Tenant: "acme", K: 5}}); err == nil {
		t.Fatal("expected validation error when BatchSearch contains empty vector")
	}
	if contacted.Load() {
		t.Fatal("server must not be contacted when search vector is empty")
	}
}

func TestSlashDocIDsAndServeMuxCompatibility(t *testing.T) {
	var seenRawPath atomic.Value
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/tenants/{tenant}/docs/batch-get", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			IDs []string `json:"ids"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		docs := make([]DocResponse, len(req.IDs))
		for i, id := range req.IDs {
			docs[i] = DocResponse{ID: id, Tenant: r.PathValue("tenant")}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"docs": docs})
	})
	mux.HandleFunc("GET /v1/tenants/{tenant}/docs/{id...}", func(w http.ResponseWriter, r *http.Request) {
		seenRawPath.Store(r.URL.EscapedPath())
		_ = json.NewEncoder(w).Encode(DocResponse{ID: r.PathValue("id"), Tenant: r.PathValue("tenant")})
	})
	mux.HandleFunc("PATCH /v1/tenants/{tenant}/docs/{id...}", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(DocResponse{ID: r.PathValue("id"), Tenant: r.PathValue("tenant"), Degraded: false})
	})
	mux.HandleFunc("DELETE /v1/tenants/{tenant}/docs/{id...}", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"deleted": true, "id": r.PathValue("id")})
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	cli := New(Options{BaseURL: ts.URL, APIKey: "mk_test"})
	ctx := context.Background()

	slashID := "docs/2026/q1 report.pdf#0"
	doc, err := cli.GetDoc(ctx, "acme", slashID)
	if err != nil || doc.ID != slashID {
		t.Fatalf("GetDoc with slash ID failed: doc=%+v err=%v", doc, err)
	}
	if gotPath, _ := seenRawPath.Load().(string); gotPath != "/v1/tenants/acme/docs/docs/2026/q1%20report.pdf%230" {
		t.Fatalf("unexpected escaped path: %q", gotPath)
	}
	if _, err := cli.PatchDoc(ctx, "acme", slashID, PatchRequest{ReplaceMeta: true}); err != nil {
		t.Fatalf("PatchDoc with slash ID failed: %v", err)
	}
	batchDocs, err := cli.BatchGetDocs(ctx, "acme", []string{slashID})
	if err != nil || len(batchDocs) != 1 || batchDocs[0].ID != slashID {
		t.Fatalf("BatchGetDocs with slash ID failed: %+v err=%v", batchDocs, err)
	}
	if err := cli.DeleteDoc(ctx, "acme", slashID); err != nil {
		t.Fatalf("DeleteDoc with slash ID failed: %v", err)
	}
}

func TestMultiDatabaseCRUDAndScopedDataPlane(t *testing.T) {
	var mu sync.Mutex
	var requests []string
	var bodies []string
	recordReq := func(r *http.Request, body []byte) {
		mu.Lock()
		defer mu.Unlock()
		u := r.URL.Path
		if r.URL.RawQuery != "" {
			u += "?" + r.URL.RawQuery
		}
		requests = append(requests, r.Method+" "+u)
		bodies = append(bodies, strings.TrimSpace(string(body)))
	}

	var return405OnGetDB atomic.Bool

	mux := http.NewServeMux()
	handleDBList := func(w http.ResponseWriter, r *http.Request) {
		recordReq(r, nil)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"tenant": "acme",
			"databases": []DatabaseInfo{
				{Tenant: "acme/analytics", Database: "analytics", NumShards: 2, RF: 2, DataType: "int8", Mode: "multi", DocCount: 10},
			},
		})
	}
	handleDBCreate := func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		recordReq(r, b)
		var cfg DatabaseConfig
		_ = json.Unmarshal(b, &cfg)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok": true,
			"database": DatabaseInfo{
				Tenant:    "acme/" + cfg.Database,
				Database:  cfg.Database,
				NumShards: cfg.NumShards,
				RF:        cfg.RF,
				DataType:  cfg.DataType,
				Mode:      cfg.Mode,
			},
			"partition": cfg.Database,
		})
	}
	handleDBGet := func(w http.ResponseWriter, r *http.Request) {
		recordReq(r, nil)
		if return405OnGetDB.Load() {
			w.WriteHeader(http.StatusMethodNotAllowed)
			_, _ = w.Write([]byte("Method Not Allowed"))
			return
		}
		db := r.PathValue("database")
		_ = json.NewEncoder(w).Encode(DatabaseInfo{
			Tenant:    "acme/" + db,
			Database:  db,
			NumShards: 2,
			RF:        2,
			DataType:  "int8",
			DocCount:  10,
		})
	}
	handleDBDelete := func(w http.ResponseWriter, r *http.Request) {
		recordReq(r, nil)
		_ = json.NewEncoder(w).Encode(map[string]any{"deleted": true, "docs_deleted": 7})
	}

	mux.HandleFunc("GET /v1/tenants/{tenant}/databases", handleDBList)
	mux.HandleFunc("POST /v1/tenants/{tenant}/databases", handleDBCreate)
	mux.HandleFunc("GET /v1/tenants/{tenant}/databases/{database}", handleDBGet)
	mux.HandleFunc("DELETE /v1/tenants/{tenant}/databases/{database}", handleDBDelete)

	mux.HandleFunc("GET /v1/databases", handleDBList)
	mux.HandleFunc("POST /v1/databases", handleDBCreate)
	mux.HandleFunc("GET /v1/databases/{database}", handleDBGet)
	mux.HandleFunc("DELETE /v1/databases/{database}", handleDBDelete)

	handleIngest := func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		recordReq(r, b)
		_ = json.NewEncoder(w).Encode(IngestResponse{Ingested: 1, Database: "analytics"})
	}
	mux.HandleFunc("POST /v1/tenants/{tenant}/ingest", handleIngest)
	mux.HandleFunc("POST /v1/ingest", handleIngest)

	mux.HandleFunc("POST /v1/search", func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		recordReq(r, b)
		_ = json.NewEncoder(w).Encode(SearchResponse{Hits: []Hit{{ID: "d1", Score: 0.9}}})
	})
	mux.HandleFunc("POST /v1/search/batch", func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		recordReq(r, b)
		_ = json.NewEncoder(w).Encode(BatchSearchResponse{Results: []SearchResponse{{Hits: []Hit{{ID: "d1"}}}}})
	})

	handleDoc := func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		recordReq(r, b)
		_ = json.NewEncoder(w).Encode(DocResponse{ID: r.PathValue("id"), Database: r.URL.Query().Get("database")})
	}
	mux.HandleFunc("GET /v1/tenants/{tenant}/docs/{id...}", handleDoc)
	mux.HandleFunc("PATCH /v1/tenants/{tenant}/docs/{id...}", handleDoc)
	mux.HandleFunc("DELETE /v1/tenants/{tenant}/docs/{id...}", handleDoc)
	mux.HandleFunc("GET /v1/docs/{id...}", handleDoc)
	mux.HandleFunc("PATCH /v1/docs/{id...}", handleDoc)
	mux.HandleFunc("DELETE /v1/docs/{id...}", handleDoc)

	handleBatchGet := func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		recordReq(r, b)
		_ = json.NewEncoder(w).Encode(map[string]any{"docs": []DocResponse{{ID: "d1", Database: "analytics"}}})
	}
	mux.HandleFunc("POST /v1/tenants/{tenant}/docs/batch-get", handleBatchGet)
	mux.HandleFunc("POST /v1/docs/batch-get", handleBatchGet)

	handleImport := func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		recordReq(r, b)
		_ = json.NewEncoder(w).Encode(map[string]any{"imported": 1})
	}
	mux.HandleFunc("POST /v1/tenants/{tenant}/import", handleImport)
	mux.HandleFunc("POST /v1/import", handleImport)

	handleExport := func(w http.ResponseWriter, r *http.Request) {
		recordReq(r, nil)
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write([]byte("{\"id\":\"d1\",\"database\":\"analytics\",\"vector\":[1,0]}\n"))
	}
	mux.HandleFunc("GET /v1/tenants/{tenant}/export", handleExport)
	mux.HandleFunc("GET /v1/export", handleExport)

	ts := httptest.NewServer(mux)
	defer ts.Close()

	ctx := context.Background()
	cli := New(Options{BaseURL: ts.URL, APIKey: "mk_scoped"})

	// 1. Database CRUD (both explicit tenant and tenant == "")
	for _, tenant := range []string{"acme", ""} {
		created, err := cli.CreateDatabase(ctx, tenant, DatabaseConfig{
			Database:  "analytics",
			NumShards: 2,
			RF:        2,
			Mode:      "multi",
			DataType:  "int8",
		})
		if err != nil || created.Database != "analytics" || created.NumShards != 2 {
			t.Fatalf("CreateDatabase(%q): %+v err=%v", tenant, created, err)
		}

		dbs, err := cli.ListDatabases(ctx, tenant)
		if err != nil || len(dbs) != 1 || dbs[0].Database != "analytics" {
			t.Fatalf("ListDatabases(%q): %+v err=%v", tenant, dbs, err)
		}

		return405OnGetDB.Store(false)
		gotDB, err := cli.GetDatabase(ctx, tenant, "analytics")
		if err != nil || gotDB.Database != "analytics" || gotDB.DocCount != 10 {
			t.Fatalf("GetDatabase(%q) direct: %+v err=%v", tenant, gotDB, err)
		}

		return405OnGetDB.Store(true)
		gotFallback, err := cli.GetDatabase(ctx, tenant, "analytics")
		if err != nil || gotFallback.Database != "analytics" {
			t.Fatalf("GetDatabase(%q) 405 fallback: %+v err=%v", tenant, gotFallback, err)
		}

		deletedCount, err := cli.DeleteDatabase(ctx, tenant, "analytics")
		if err != nil || deletedCount != 7 {
			t.Fatalf("DeleteDatabase(%q): %d err=%v", tenant, deletedCount, err)
		}
	}

	// 2. Data-plane database scoping with both explicit tenant and tenant == ""
	for _, tenant := range []string{"acme", ""} {
		if _, err := cli.IngestDatabase(ctx, tenant, "analytics", []Doc{{ID: "d1", Vector: []float32{1, 0}}}); err != nil {
			t.Fatalf("IngestDatabase(%q): %v", tenant, err)
		}
		if _, err := cli.Ingest(ctx, tenant, []Doc{{ID: "d1", Vector: []float32{1, 0}}}, "analytics"); err != nil {
			t.Fatalf("Ingest(%q, ..., database): %v", tenant, err)
		}
		if _, err := cli.Search(ctx, SearchRequest{Tenant: tenant, Database: "analytics", Vector: []float32{1, 0}, K: 5}); err != nil {
			t.Fatalf("Search(%q): %v", tenant, err)
		}
		if _, err := cli.BatchSearch(ctx, []SearchRequest{{Tenant: tenant, Vector: []float32{1, 0}, K: 5}}, "analytics"); err != nil {
			t.Fatalf("BatchSearch(%q): %v", tenant, err)
		}
		if _, err := cli.GetDoc(ctx, tenant, "d1", "analytics"); err != nil {
			t.Fatalf("GetDoc(%q): %v", tenant, err)
		}
		if _, err := cli.PatchDoc(ctx, tenant, "d1", PatchRequest{Database: "analytics", ReplaceMeta: true}); err != nil {
			t.Fatalf("PatchDoc(%q): %v", tenant, err)
		}
		if _, err := cli.BatchGetDocs(ctx, tenant, []string{"d1"}, "analytics"); err != nil {
			t.Fatalf("BatchGetDocs(%q): %v", tenant, err)
		}
		if err := cli.DeleteDoc(ctx, tenant, "d1", "analytics"); err != nil {
			t.Fatalf("DeleteDoc(%q): %v", tenant, err)
		}
		if n, err := cli.ImportTenant(ctx, tenant, []Doc{{ID: "d1", Vector: []float32{1, 0}}}, "analytics"); err != nil || n != 1 {
			t.Fatalf("ImportTenant(%q): n=%d err=%v", tenant, n, err)
		}
		if exported, err := cli.ExportTenant(ctx, tenant, "analytics"); err != nil || len(exported) != 1 || exported[0].Database != "analytics" {
			t.Fatalf("ExportTenant(%q): %+v err=%v", tenant, exported, err)
		}
	}

	// Ensure zero double-slash URLs were ever generated
	mu.Lock()
	defer mu.Unlock()
	for _, r := range requests {
		if strings.Contains(r, "//") {
			t.Fatalf("double-slash URL generated: %q", r)
		}
	}
	// Verify ?database=analytics was sent on ImportTenant & ExportTenant
	hasImportDB := false
	hasExportDB := false
	for _, r := range requests {
		if r == "POST /v1/import?database=analytics" {
			hasImportDB = true
		}
		if r == "GET /v1/export?database=analytics" {
			hasExportDB = true
		}
	}
	if !hasImportDB || !hasExportDB {
		t.Fatalf("expected ?database=analytics on /v1/import and /v1/export, got requests: %v", requests)
	}
	_ = bodies
}
