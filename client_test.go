package massengine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestClientSDKLifecycle(t *testing.T) {
	var seenUserAgent atomic.Value

	mux := http.NewServeMux()
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
			Tenant:     r.PathValue("tenant"),
			Plan:       "pro",
			LiveDocs:   2,
			ReadUnits:  1,
			WriteUnits: 2,
		})
	})
	mux.HandleFunc("GET /v1/tenants/{tenant}/docs/{id}", func(w http.ResponseWriter, r *http.Request) {
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

	q, err := admin.SetTenantQuota(ctx, "acme", TenantQuota{Plan: "pro", MaxDocs: 100})
	if err != nil || q.Plan != "pro" || q.MaxDocs != 100 {
		t.Fatalf("SetTenantQuota: %+v, err=%v", q, err)
	}
	if gotUA, _ := seenUserAgent.Load().(string); gotUA != "massengine-go/1.1.2" {
		t.Fatalf("expected User-Agent massengine-go/1.1.2, got %q", gotUA)
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
	if err != nil || u.Plan != "pro" || u.LiveDocs != 2 {
		t.Fatalf("GetTenantUsage: %+v, err=%v", u, err)
	}

	revoked := New(Options{BaseURL: ts.URL, APIKey: "revoked"})
	_, err = revoked.GetDoc(ctx, "acme", "go-1")
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 APIError, got: %v", err)
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

	badTenants := []string{"", ".", "..", "acme/evil", "acme\\evil", "acme\x00", "acme\n", "acme\x7f"}
	for _, bt := range badTenants {
		if _, err := cli.GetTenant(ctx, bt); err == nil {
			t.Fatalf("expected error for bad tenant %q", bt)
		}
		if _, err := cli.GetDoc(ctx, bt, "doc-1"); err == nil {
			t.Fatalf("expected error for bad tenant %q in GetDoc", bt)
		}
	}

	badDocIDs := []string{"", ".", "..", "batch-get", "doc\x00", "doc\r\n", "doc\x7f"}
	for _, bid := range badDocIDs {
		if _, err := cli.GetDoc(ctx, "acme", bid); err == nil {
			t.Fatalf("expected error for bad doc ID %q", bid)
		}
		if err := cli.DeleteDoc(ctx, "acme", bid); err == nil {
			t.Fatalf("expected error for bad doc ID %q in DeleteDoc", bid)
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
		_, err := NewClient(Options{BaseURL: "https://api.massengine.com", APIKey: bk})
		if err == nil {
			t.Fatalf("expected error for invalid APIKey")
		}
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("API key leaked in constructor error: %v", err)
		}

		deferred := New(Options{BaseURL: "https://api.massengine.com", APIKey: bk})
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
