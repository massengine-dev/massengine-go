# MassEngine Go SDK (`massengine-go`)

Official zero-dependency Go client for [MassEngine](https://massengine.com) — the sub-5ms filtered vector search database.

## Installation

```bash
go get github.com/massengine-dev/massengine-go@v1.1.4
```

## Quickstart

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"

	massengine "github.com/massengine-dev/massengine-go"
)

func main() {
	ctx := context.Background()
	cli := massengine.New(massengine.Options{
		BaseURL: "https://massengine.com",
		APIKey:  "mk_your_tenant_api_key",
	})

	// 1. Ingest dense vectors + structured metadata
	_, _ = cli.Ingest(ctx, "acme-prod", []massengine.Doc{
		{
			ID:       "docs/2026/policy-1",
			Vector:   []float32{0.12, -0.44, 0.88, 0.05},
			Category: 1,
			Metadata: json.RawMessage(`{"department":"security","clearance":3,"tags":["soc2","policy"]}`),
		},
	})

	// 2. Dense vector search with rich metadata pre-filters
	res, _ := cli.Search(ctx, massengine.SearchRequest{
		Tenant: "acme-prod",
		Vector: []float32{0.12, -0.44, 0.88, 0.05},
		K:      10,
		MetadataFilter: map[string]any{
			"department": map[string]any{"$in": []string{"security", "legal"}},
			"clearance":  map[string]any{"$gte": 2},
			"tags":       map[string]any{"$contains": "soc2"},
		},
	})
	fmt.Printf("Hits: %+v\n", res.Hits)
}
```

## Multi-Database Scoping

Create isolated databases within a tenant and scope reads/writes to a specific database (pass `tenant = ""` when authenticating with a tenant-scoped `mk_...` key):

```go
_, _ = cli.CreateDatabase(ctx, "", massengine.DatabaseConfig{
	Database: "analytics",
	DataType: "int8",
})

_, _ = cli.IngestDatabase(ctx, "", "analytics", []massengine.Doc{
	{ID: "event-1", Vector: []float32{0.1, 0.2, 0.3, 0.4}},
})

res, _ := cli.Search(ctx, massengine.SearchRequest{
	Database: "analytics",
	Vector:   []float32{0.1, 0.2, 0.3, 0.4},
	K:        5,
})
```

## License

Apache License 2.0
