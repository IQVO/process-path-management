package http_test

import (
	"encoding/json"
	"errors"
	stdhttp "net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"gopkg.in/yaml.v3"

	"github.com/claudioed/process-path-management/internal/adapters/inbound/http"
	"github.com/claudioed/process-path-management/internal/analytics/report"
)

// reportsSpecPath is the contract of the cmd/pathmgmt-reports binary (ADR
// 0031, gap fix D18).
var reportsSpecPath = filepath.Join("..", "..", "..", "..", "apis", "openapi-reports.yaml")

// specDoc is the subset of an OpenAPI 3.0 document these contract tests read.
type specDoc struct {
	Paths      map[string]map[string]specOperation `yaml:"paths"`
	Components struct {
		Schemas map[string]specSchema `yaml:"schemas"`
	} `yaml:"components"`
}

type specOperation struct {
	OperationID string                  `yaml:"operationId"`
	Responses   map[string]specResponse `yaml:"responses"`
}

type specResponse struct {
	Content map[string]struct {
		Schema specSchema `yaml:"schema"`
	} `yaml:"content"`
}

type specSchema struct {
	Ref        string                `yaml:"$ref"`
	Required   []string              `yaml:"required"`
	Properties map[string]specSchema `yaml:"properties"`
	Items      *specSchema           `yaml:"items"`
}

func loadReportsSpec(t *testing.T) specDoc {
	t.Helper()
	raw, err := os.ReadFile(reportsSpecPath)
	if err != nil {
		t.Fatalf("read %s: %v (every route of cmd/pathmgmt-reports must be specified there)", reportsSpecPath, err)
	}
	var doc specDoc
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse %s: %v", reportsSpecPath, err)
	}
	return doc
}

// resolve follows a local "#/components/schemas/<Name>" reference.
func (d specDoc) resolve(t *testing.T, s specSchema) specSchema {
	t.Helper()
	if s.Ref == "" {
		return s
	}
	name := strings.TrimPrefix(s.Ref, "#/components/schemas/")
	resolved, ok := d.Components.Schemas[name]
	if !ok {
		t.Fatalf("unresolvable $ref %q", s.Ref)
	}
	return resolved
}

// TestReportsContract_RoutesMatchSpec fails when cmd/pathmgmt-reports serves
// a route apis/openapi-reports.yaml does not document, or the spec documents
// a route the router does not serve.
func TestReportsContract_RoutesMatchSpec(t *testing.T) {
	doc := loadReportsSpec(t)
	router := http.NewReportsRouter(&http.ReportsHandlers{Store: &fakeReportStore{}}, nil)

	var served []string
	walk := func(method, route string, _ stdhttp.Handler, _ ...func(stdhttp.Handler) stdhttp.Handler) error {
		served = append(served, strings.ToLower(method)+" "+route)
		return nil
	}
	if err := chi.Walk(router, walk); err != nil {
		t.Fatalf("walk: %v", err)
	}

	var specified []string
	for path, ops := range doc.Paths {
		for method := range ops {
			specified = append(specified, method+" "+path)
		}
	}
	sort.Strings(served)
	sort.Strings(specified)
	if strings.Join(served, ",") != strings.Join(specified, ",") {
		t.Fatalf("routes served %v != routes in %s %v", served, reportsSpecPath, specified)
	}
}

// reportsCase is one request against the reports router and the spec
// operation it must conform to.
type reportsCase struct {
	name   string
	target string
	path   string
	store  *fakeReportStore
	status int
	// rowsKey names the array property whose first item is also checked.
	rowsKey string
}

func reportsCases() []reportsCase {
	bucket := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	full := &fakeReportStore{
		report: report.CatalogueReport{Rows: []report.Row{{Key: report.RowKey{DayBucket: bucket}, PathsDefined: 2, PathsRevised: 1}}},
		lag:    3 * time.Second,
	}
	failing := &fakeReportStore{queryErr: errors.New("db down"), freshErr: errors.New("db down")}
	window := "/reports/catalogue-growth?from=2026-09-01T00:00:00Z&to=2026-09-02T00:00:00Z"
	return []reportsCase{
		{name: "catalogue-growth 200", target: window, path: "/reports/catalogue-growth", store: full, status: stdhttp.StatusOK, rowsKey: "rows"},
		{name: "catalogue-growth 400", target: "/reports/catalogue-growth?to=2026-09-02T00:00:00Z", path: "/reports/catalogue-growth", store: full, status: stdhttp.StatusBadRequest},
		{name: "catalogue-growth 500", target: window, path: "/reports/catalogue-growth", store: failing, status: stdhttp.StatusInternalServerError},
		{name: "freshness 200", target: "/reports/catalogue-growth/freshness", path: "/reports/catalogue-growth/freshness", store: full, status: stdhttp.StatusOK},
		{name: "freshness 500", target: "/reports/catalogue-growth/freshness", path: "/reports/catalogue-growth/freshness", store: failing, status: stdhttp.StatusInternalServerError},
		{name: "healthz 200", target: "/healthz", path: "/healthz", store: full, status: stdhttp.StatusOK},
	}
}

// TestReportsContract_ResponsesMatchSpec drives every documented status of
// every operation and checks the body against the declared schema: every
// required property is present and no property is undocumented.
func TestReportsContract_ResponsesMatchSpec(t *testing.T) {
	doc := loadReportsSpec(t)
	for _, tc := range reportsCases() {
		t.Run(tc.name, func(t *testing.T) {
			router := http.NewReportsRouter(&http.ReportsHandlers{Store: tc.store}, nil)
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, httptest.NewRequest(stdhttp.MethodGet, tc.target, nil))
			if rec.Code != tc.status {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, tc.status, rec.Body.String())
			}
			schema := responseSchema(t, doc, tc.path, rec)
			var body map[string]any
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode: %v", err)
			}
			assertObjectMatches(t, tc.name, schema, body)
			if tc.rowsKey != "" {
				assertFirstItemMatches(t, doc, schema, body, tc.rowsKey)
			}
		})
	}
}

// responseSchema returns the schema the spec declares for this status and
// the response's media type, failing when either is undocumented.
func responseSchema(t *testing.T, doc specDoc, path string, rec *httptest.ResponseRecorder) specSchema {
	t.Helper()
	op, ok := doc.Paths[path]["get"]
	if !ok {
		t.Fatalf("GET %s is not in %s", path, reportsSpecPath)
	}
	resp, ok := op.Responses[strconv.Itoa(rec.Code)]
	if !ok {
		t.Fatalf("GET %s answered %d, which %s does not declare", path, rec.Code, reportsSpecPath)
	}
	mediaType := strings.TrimSpace(strings.Split(rec.Header().Get("Content-Type"), ";")[0])
	content, ok := resp.Content[mediaType]
	if !ok {
		t.Fatalf("GET %s %d answered %q, which the spec does not declare", path, rec.Code, mediaType)
	}
	return doc.resolve(t, content.Schema)
}

func assertObjectMatches(t *testing.T, what string, schema specSchema, body map[string]any) {
	t.Helper()
	for _, req := range schema.Required {
		if _, ok := body[req]; !ok {
			t.Errorf("%s: required property %q missing from the response", what, req)
		}
	}
	for key := range body {
		if _, ok := schema.Properties[key]; !ok {
			t.Errorf("%s: response property %q is not documented in the spec", what, key)
		}
	}
}

func assertFirstItemMatches(t *testing.T, doc specDoc, schema specSchema, body map[string]any, key string) {
	t.Helper()
	items, _ := body[key].([]any)
	if len(items) == 0 {
		t.Fatalf("%s: expected at least one item to check", key)
	}
	first, _ := items[0].(map[string]any)
	arr := schema.Properties[key]
	if arr.Items == nil {
		t.Fatalf("%s: the spec declares no items schema", key)
	}
	assertObjectMatches(t, key+"[0]", doc.resolve(t, *arr.Items), first)
}
