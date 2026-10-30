package massengine

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestClientSDKLifecycle(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /v1/tenants/{tenant}/quota", func(w http.ResponseWriter, r *http.Request) {
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
			Hits: []Hit{{ID: "go-1", DocID: 1, Score: 0.98}},
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
	admin := New(Options{BaseURL: ts.URL, APIKey: "admin-key"})

	q, err := admin.SetTenantQuota(ctx, "acme", TenantQuota{Plan: "pro", MaxDocs: 100})
	if err != nil || q.Plan != "pro" || q.MaxDocs != 100 {
		t.Fatalf("SetTenantQuota: %+v, err=%v", q, err)
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
	if err != nil || len(sr.Hits) != 1 || sr.Hits[0].ID != "go-1" {
		t.Fatalf("Search: %+v, err=%v", sr, err)
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
