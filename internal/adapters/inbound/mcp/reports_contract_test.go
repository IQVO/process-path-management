package mcp

// Consumer contract tests for the INTRA-REPO reports edge: cmd/mcp's
// ReportsRESTClient calls the pathmgmt-reports binary (REPORTS_BASE_URL) at
// GET /reports/catalogue-growth and GET /reports/catalogue-growth/freshness.
// The contract validates against the PINNED COPY of this repo's own
// apis/openapi-reports.yaml — a self-pin in contracts/pinned.json: producer
// and consumer live in the same module, so the pin guards against editing
// the reports spec without updating the MCP reports client. Fixtures come
// from the pinned spec's own property examples. kin-openapi is test-only.
// NOTE: this repo is banned from sibling REST/MCP calls by design; the only
// REST contract it may pin is this intra-repo one.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
)

const pinnedReportsSpec = "testdata/process-path-management.openapi-reports.yaml"

func loadPinnedReportsSpec(t *testing.T) *openapi3.T {
	t.Helper()
	doc, err := openapi3.NewLoader().LoadFromFile(pinnedReportsSpec)
	if err != nil {
		t.Fatalf("load pinned reports spec: %v", err)
	}
	return doc
}

// R1: the reports client's two call sites are declared by the pinned spec
// with the required/optional query parameters the client sends (shared
// parameters may sit at the path-item level, so both levels are checked).
func TestPinnedSpecCoversTheCallSites(t *testing.T) {
	doc := loadPinnedReportsSpec(t)

	growth := doc.Paths.Find("/reports/catalogue-growth")
	if growth == nil || growth.Get == nil || growth.Get.OperationID != "getCatalogueGrowthReport" {
		t.Fatal("the pinned reports spec no longer declares GET /reports/catalogue-growth (getCatalogueGrowthReport)")
	}
	assertQueryParam(t, growth, "from", true)
	assertQueryParam(t, growth, "to", true)
	assertQueryParam(t, growth, "granularity", false)

	freshness := doc.Paths.Find("/reports/catalogue-growth/freshness")
	if freshness == nil || freshness.Get == nil || freshness.Get.OperationID != "getCatalogueGrowthFreshness" {
		t.Fatal("the pinned reports spec no longer declares GET /reports/catalogue-growth/freshness")
	}
}

// assertQueryParam checks a route's query parameter at BOTH the path-item
// and operation levels, required or optional per want.
func assertQueryParam(t *testing.T, path *openapi3.PathItem, name string, wantRequired bool) {
	t.Helper()
	for _, p := range append(path.Parameters, path.Get.Parameters...) {
		if p.Value == nil || p.Value.Name != name || p.Value.In != "query" {
			continue
		}
		if p.Value.Required != wantRequired {
			t.Fatalf("query parameter %q: required=%v, want %v", name, p.Value.Required, wantRequired)
		}
		return
	}
	t.Fatalf("the route no longer declares the query parameter %q", name)
}

// specRow builds one report row from the pinned schema's own property
// examples, validated against the schema before the client decodes it.
func specRowFixture(t *testing.T) map[string]any {
	t.Helper()
	row := loadPinnedReportsSpec(t).Components.Schemas["CatalogueGrowthRow"].Value
	fixture := map[string]any{}
	for name, prop := range row.Properties {
		if prop.Value == nil {
			continue
		}
		if ex, ok := prop.Value.Example.(float64); ok {
			fixture[name] = int(ex)
		} else if ex := prop.Value.Example; ex != nil {
			fixture[name] = ex
		}
	}
	if err := row.VisitJSON(fixture); err != nil {
		t.Fatalf("the derived row fixture violates CatalogueGrowthRow: %v", err)
	}
	return fixture
}

// R3: the REAL client decodes the spec-shaped fixtures.
func TestClientDecodesTheSpecFixtures(t *testing.T) {
	row := specRowFixture(t)

	var lastPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lastPath = r.URL.RequestURI()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"rows": []any{row}})
	}))
	t.Cleanup(srv.Close)
	c := NewReportsRESTClient(srv.URL, nil)

	got, err := c.GetCatalogueGrowth(context.Background(), CatalogueGrowthQuery{From: "2026-09-01T00:00:00Z", To: "2026-09-08T00:00:00Z"})
	if err != nil {
		t.Fatalf("get catalogue growth: %v", err)
	}
	if !strings.Contains(lastPath, "from=2026-09-01") || !strings.Contains(lastPath, "to=2026-09-08") {
		t.Errorf("request %q does not carry the required from/to query parameters", lastPath)
	}
	if len(got.Rows) != 1 {
		t.Fatalf("decoded %d rows, the fixture carries 1", len(got.Rows))
	}
	if got.Rows[0].PathsDefined != 2 || got.Rows[0].PathsRevised != 1 {
		t.Errorf("row %+v does not carry the fixture's own facts", got.Rows[0])
	}

	freshSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"lagSeconds":1234.5}`))
	}))
	t.Cleanup(freshSrv.Close)
	lag, err := NewReportsRESTClient(freshSrv.URL, nil).GetFreshness(context.Background())
	if err != nil {
		t.Fatalf("get freshness: %v", err)
	}
	if lag.LagSeconds != 1234.5 {
		t.Errorf("lagSeconds = %v, the fixture says 1234.5", lag.LagSeconds)
	}
}

// R4: every response field the client decodes is declared by the pinned
// schema (the client's read set is the contract).
func TestFieldsTheClientReadsAreSpecified(t *testing.T) {
	doc := loadPinnedReportsSpec(t)
	row := doc.Components.Schemas["CatalogueGrowthRow"].Value
	typ := reflect.TypeOf(CatalogueGrowthRowView{})
	for i := range typ.NumField() {
		tag := strings.Split(typ.Field(i).Tag.Get("json"), ",")[0]
		if tag == "" || tag == "-" {
			continue
		}
		if _, declared := row.Properties[tag]; !declared {
			t.Errorf("%s reads %q, which the pinned reports spec does not declare", typ.Name(), tag)
		}
	}
	for _, required := range row.Required {
		if !viewHasTag(typ, required) {
			t.Errorf("the pinned spec requires %q, which CatalogueGrowthRowView does not read (additive producer obligation — verify tolerance)", required)
		}
	}
	if _, ok := doc.Components.Schemas["Freshness"].Value.Properties["lagSeconds"]; !ok {
		t.Error("FreshnessView reads lagSeconds, which the pinned Freshness schema does not declare")
	}
}

func viewHasTag(typ reflect.Type, tag string) bool {
	for i := range typ.NumField() {
		if strings.Split(typ.Field(i).Tag.Get("json"), ",")[0] == tag {
			return true
		}
	}
	return false
}
