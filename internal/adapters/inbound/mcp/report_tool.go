package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// --- reports REST views (tool + client boundary) ------------------------------

// CatalogueGrowthRowView is one row of the catalogue-growth report as the
// MCP tool returns it and the reports REST client decodes it. Field tags
// match the reports service's JSON so the same struct round-trips both
// ways.
type CatalogueGrowthRowView struct {
	DayBucket        string `json:"dayBucket"`
	PathsDefined     int    `json:"pathsDefined"`
	PathsRevised     int    `json:"pathsRevised"`
	PathsDeactivated int    `json:"pathsDeactivated"`
}

// CatalogueGrowthReportView is the catalogue-growth report body.
type CatalogueGrowthReportView struct {
	Rows []CatalogueGrowthRowView `json:"rows"`
}

// FreshnessView is the freshness-lag body.
type FreshnessView struct {
	LagSeconds float64 `json:"lagSeconds"`
}

// CatalogueGrowthQuery is the filter set passed to the reports REST client.
type CatalogueGrowthQuery struct {
	From        string
	To          string
	Granularity string
}

// ReportsClient is the narrow port the MCP report tool depends on: a
// client of the pathmgmt-reports REST service. It is an interface so the
// tool can be unit-tested with a fake, and so the curated tool never
// talks to the analytical database directly — it goes through the
// reports REST surface, preserving the single read path (ADR 0007).
type ReportsClient interface {
	GetCatalogueGrowth(ctx context.Context, q CatalogueGrowthQuery) (CatalogueGrowthReportView, error)
	GetFreshness(ctx context.Context) (FreshnessView, error)
}

// --- reports REST client ------------------------------------------------------

// ReportsRESTClient is the HTTP implementation of ReportsClient. Base URL
// and the *http.Client are injected so the composition root controls the
// target and timeouts, and tests can point it at an httptest server.
type ReportsRESTClient struct {
	baseURL string
	http    *http.Client
}

// NewReportsRESTClient constructs a ReportsRESTClient for the reports
// service at baseURL. A nil httpClient falls back to a client with a sane
// timeout.
func NewReportsRESTClient(baseURL string, httpClient *http.Client) *ReportsRESTClient {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	return &ReportsRESTClient{baseURL: baseURL, http: httpClient}
}

// GetCatalogueGrowth calls GET /reports/catalogue-growth with q as the
// query string.
func (c *ReportsRESTClient) GetCatalogueGrowth(ctx context.Context, q CatalogueGrowthQuery) (CatalogueGrowthReportView, error) {
	vals := url.Values{}
	vals.Set("from", q.From)
	vals.Set("to", q.To)
	if q.Granularity != "" {
		vals.Set("granularity", q.Granularity)
	}
	var out CatalogueGrowthReportView
	if err := c.getJSON(ctx, "/reports/catalogue-growth?"+vals.Encode(), &out); err != nil {
		return CatalogueGrowthReportView{}, err
	}
	return out, nil
}

// GetFreshness calls GET /reports/catalogue-growth/freshness.
func (c *ReportsRESTClient) GetFreshness(ctx context.Context) (FreshnessView, error) {
	var out FreshnessView
	if err := c.getJSON(ctx, "/reports/catalogue-growth/freshness", &out); err != nil {
		return FreshnessView{}, err
	}
	return out, nil
}

// getJSON performs a GET against baseURL+path and decodes a 2xx JSON body
// into out. A non-2xx response is an error.
func (c *ReportsRESTClient) getJSON(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return fmt.Errorf("reports client: build request: %w", err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("reports client: request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("reports client: unexpected status %d", resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("reports client: decode: %w", err)
	}
	return nil
}

// Compile-time assertion that ReportsRESTClient satisfies the port.
var _ ReportsClient = (*ReportsRESTClient)(nil)

// --- get_catalogue_growth_report tool ------------------------------------

// CatalogueGrowthToolInput is the tool's argument set (untrusted, from a
// model).
type CatalogueGrowthToolInput struct {
	From        string `json:"from" jsonschema:"start of the window, inclusive, RFC3339 (required)"`
	To          string `json:"to" jsonschema:"end of the window, exclusive, RFC3339 (required)"`
	Granularity string `json:"granularity,omitempty" jsonschema:"optional time bucket granularity; only 'day' is supported and it is the default"`
}

// getCatalogueGrowthReport is the tool handler: it validates the required
// window, delegates to the reports REST client, and returns the report
// view.
func (d Deps) getCatalogueGrowthReport(ctx context.Context, in CatalogueGrowthToolInput) (CatalogueGrowthReportView, error) {
	return GetCatalogueGrowthReportForTest(ctx, d.Reports, in)
}

// GetCatalogueGrowthReportForTest is the tool's pure logic, factored out
// so it can be unit-tested with a fake ReportsClient independent of the
// MCP server wiring. It validates from/to and forwards the filters.
func GetCatalogueGrowthReportForTest(ctx context.Context, client ReportsClient, in CatalogueGrowthToolInput) (CatalogueGrowthReportView, error) {
	if client == nil {
		return CatalogueGrowthReportView{}, fmt.Errorf("reports client not configured")
	}
	if in.From == "" || in.To == "" {
		return CatalogueGrowthReportView{}, fmt.Errorf("from and to are required (RFC3339)")
	}
	q := CatalogueGrowthQuery{}
	q.From = in.From
	q.To = in.To
	q.Granularity = in.Granularity
	return client.GetCatalogueGrowth(ctx, q)
}

// registerReportTool adds the curated read-only catalogue-growth report
// tool. It is registered only when a reports client is configured
// (Deps.Reports != nil), so an MCP deployment without the reports service
// simply does not expose it.
func (d Deps) registerReportTool(server *mcp.Server) {
	if d.Reports == nil {
		return
	}
	readOnly := true
	addTool(server, &mcp.Tool{
		Name:        "get_catalogue_growth_report",
		Description: "Return the process-path-management 'Process Path Catalogue Growth & Change' report (paths defined/revised/deactivated) for a time window, bucketed by day. Reads via the pathmgmt-reports REST service.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: readOnly},
	}, d.getCatalogueGrowthReport)
}
