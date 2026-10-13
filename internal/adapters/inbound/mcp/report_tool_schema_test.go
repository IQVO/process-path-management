package mcp

import (
	"context"
	"encoding/json"
	"slices"
	"sort"
	"testing"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

// stubReports is a package-internal ReportsClient double for the wire test.
type stubReports struct{ last CatalogueGrowthQuery }

func (s *stubReports) GetCatalogueGrowth(_ context.Context, q CatalogueGrowthQuery) (CatalogueGrowthReportView, error) {
	s.last = q
	return CatalogueGrowthReportView{}, nil
}

func (s *stubReports) GetFreshness(context.Context) (FreshnessView, error) {
	return FreshnessView{}, nil
}

// TestReportTool_GranularityIsOptionalOnTheWire pins gap fix D16 at the
// protocol level: the advertised input schema of
// get_catalogue_growth_report requires only from and to, and a call that
// leaves granularity out reaches the handler (which defaults to day)
// instead of being rejected by the SDK's schema validation.
func TestReportTool_GranularityIsOptionalOnTheWire(t *testing.T) {
	stub := &stubReports{}
	deps := newHarness(t).deps
	deps.Reports = stub
	server := NewServer(deps)

	client := sdk.NewClient(&sdk.Implementation{Name: "d16", Version: "0"}, nil)
	ct, st := sdk.NewInMemoryTransports()
	ctx := context.Background()
	ss, err := server.Connect(ctx, st, nil)
	if err != nil {
		t.Fatalf("server connect: %v", err)
	}
	t.Cleanup(func() { _ = ss.Close() })
	cs, err := client.Connect(ctx, ct, nil)
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })

	list, err := cs.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("list tools: %v", err)
	}
	var tool *sdk.Tool
	for _, tl := range list.Tools {
		if tl.Name == "get_catalogue_growth_report" {
			tool = tl
		}
	}
	if tool == nil {
		t.Fatal("get_catalogue_growth_report not advertised with a reports client configured")
	}
	raw, err := json.Marshal(tool.InputSchema)
	if err != nil {
		t.Fatalf("marshal input schema: %v", err)
	}
	var schema struct {
		Required []string `json:"required"`
	}
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatalf("decode input schema: %v", err)
	}
	sort.Strings(schema.Required)
	if !slices.Equal(schema.Required, []string{"from", "to"}) {
		t.Fatalf("required = %v, want [from to] (granularity is optional)", schema.Required)
	}

	res, err := cs.CallTool(ctx, &sdk.CallToolParams{
		Name:      "get_catalogue_growth_report",
		Arguments: map[string]any{"from": "2026-03-01T00:00:00Z", "to": "2026-03-08T00:00:00Z"},
	})
	if err != nil {
		t.Fatalf("call without granularity: %v", err)
	}
	if res.IsError {
		t.Fatalf("call without granularity returned a tool error: %+v", res.Content)
	}
	if stub.last.From != "2026-03-01T00:00:00Z" || stub.last.To != "2026-03-08T00:00:00Z" || stub.last.Granularity != "" {
		t.Fatalf("forwarded query = %+v", stub.last)
	}
}
