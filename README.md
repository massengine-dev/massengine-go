# MassEngine Go SDK (`massengine-go`)

Official zero-dependency Go client for [MassEngine](https://massengine.com) — the sub-5ms filtered and hybrid vector search database.

## Installation

```bash
go get github.com/massengine-dev/massengine-go@v1.1.2
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
		BaseURL: "https://api.massengine.com",
		APIKey:  "mk_your_tenant_api_key",
	})

	_, _ = cli.Ingest(ctx, "acme-prod", []massengine.Doc{
		{
			ID:       "doc-1",
			Vector:   []float32{0.12, -0.44, 0.88, 0.05},
			Category: 1,
			Metadata: json.RawMessage(`{"department":"security","clearance":3}`),
		},
	})

	res, _ := cli.Search(ctx, massengine.SearchRequest{
		Tenant:    "acme-prod",
		Vector:    []float32{0.12, -0.44, 0.88, 0.05},
		QueryText: "security compliance",
		K:         10,
		MetadataFilter: map[string]any{
			"department": map[string]any{"$in": []string{"security", "legal"}},
			"clearance":  map[string]any{"$gte": 2},
		},
	})
	fmt.Printf("Hits: %+v\n", res.Hits)
}
```

## License

Apache License 2.0
