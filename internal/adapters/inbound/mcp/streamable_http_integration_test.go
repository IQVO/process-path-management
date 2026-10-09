//go:build integration

// Integration tests for the MCP inbound adapter over the REAL Streamable
// HTTP transport: mcp.Handler(server) mounted on an httptest.Server, driven
// by the SDK's own client (mcp.NewClient + StreamableClientTransport),
// with the REAL Postgres-backed use cases behind it — exactly the
// deployment shape cmd/mcp serves. This proves the wire contract
// (initialize, tools/list, tools/call for every registered tool) and the
// read-model round trip end-to-end, not the tool handlers in isolation.
//
// The catalogue-growth report tool is additionally proven against the REAL
// pathmgmt-reports stack: the real ReportsRESTClient pointed at a real
// ReportsHandlers router over a migrated analytics database, seeded with
// the same catalogue-growth rows a projector run leaves behind.
//
// Postgres comes from testcontainers: one container per package run,
// migrated once into a template database, one private database per test.
// Never an external DATABASE_URL, never t.Skip.
package mcp_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	sdkmcp "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	inboundhttp "github.com/claudioed/process-path-management/internal/adapters/inbound/http"
	mcpadapter "github.com/claudioed/process-path-management/internal/adapters/inbound/mcp"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/process-path-management/internal/application/usecases"
	"github.com/claudioed/process-path-management/internal/domain/cptschedule"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// One Postgres container serves the whole package, migrated once into a
// template database; each test gets a private clone (milliseconds). See
// internal/application/usecases/usecases_wiring_integration_test.go for
// the same pattern's rationale. Never an external DATABASE_URL, never
// t.Skip.
const mcpTemplateDB = "mcp_wiring_template"

var mcpBaseURL string

func TestMain(m *testing.M) {
	os.Exit(mcpRunTests(m))
}

func mcpRunTests(m *testing.M) int {
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("pathmgmt"),
		tcpostgres.WithUsername("pathmgmt"),
		tcpostgres.WithPassword("pathmgmt"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(90*time.Second),
		),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start postgres container: %v\n", err)
		return 1
	}
	defer func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			fmt.Fprintf(os.Stderr, "terminate postgres container: %v\n", err)
		}
	}()

	mcpBaseURL, err = container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintf(os.Stderr, "postgres connection string: %v\n", err)
		return 1
	}

	// Migrate a template database once; every test clones it.
	if err := mcpCreateDatabase(ctx, mcpTemplateDB); err != nil {
		fmt.Fprintf(os.Stderr, "create template database: %v\n", err)
		return 1
	}
	if err := postgres.RunMigrations(mcpWithDB(mcpBaseURL, mcpTemplateDB), mcpMigrationsDir()); err != nil {
		fmt.Fprintf(os.Stderr, "migrate template: %v\n", err)
		return 1
	}

	return m.Run()
}

// mcpMigratedDB hands the test a connection URL to its own private
// database cloned from the migrated template.
func mcpMigratedDB(t *testing.T) string {
	t.Helper()
	name := fmt.Sprintf("mcp_wiring_%d", time.Now().UnixNano())
	conn, err := pgx.Connect(context.Background(), mcpBaseURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	if _, err := conn.Exec(context.Background(), fmt.Sprintf(
		"CREATE DATABASE %q WITH TEMPLATE %q", name, mcpTemplateDB)); err != nil {
		t.Fatalf("clone database: %v", err)
	}
	return mcpWithDB(mcpBaseURL, name)
}

// mcpCreateDatabase creates an empty database inside the shared container.
func mcpCreateDatabase(ctx context.Context, name string) error {
	conn, err := pgx.Connect(ctx, mcpBaseURL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %q", name)); err != nil {
		return fmt.Errorf("create database %s: %w", name, err)
	}
	return nil
}

// mcpWithDB rewrites the path of a connection URL to the named database.
func mcpWithDB(baseURL, name string) string {
	u, err := url.Parse(baseURL)
	if err != nil {
		panic(err)
	}
	u.Path = "/" + name
	return u.String()
}

// mcpMigrationsDir resolves /migrations relative to this test file.
func mcpMigrationsDir() string {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		panic("unable to resolve test file path")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..", "migrations")
}

// mcpAnalyticsMigrationsDir resolves /migrations/analytics.
func mcpAnalyticsMigrationsDir() string {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		panic("unable to resolve test file path")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..", "migrations", "analytics")
}

// mcpClock is the ports.Clock the use cases accept; a fixed now keeps the
// persisted timestamps deterministic.
type mcpClock struct{ now time.Time }

func (c mcpClock) Now() time.Time { return c.now }

// mcpHarness wires the REAL production stack — Postgres repos,
// UnitOfWork, use cases, mcp.NewServer, mcp.Handler — and serves it over
// HTTP. It returns a connected SDK client session; the test drives
// tools/list and tools/call exactly like a model host would.
type mcpHarness struct {
	session *sdkmcp.ClientSession
	pool    *pgxpool.Pool
}

// newMCPHarness seeds the canonical catalogue state (PICK active, REBIN
// deactivated, and a CPT schedule for site sp1 whose cutoff routes to
// PICK) through the real write use cases on a fresh private database, then
// serves the real MCP stack over Streamable HTTP with every tool
// registered — including the catalogue-growth report tool, whose
// ReportsClient is the real REST client pointed at a real reports router
// over a seeded analytics database.
func newMCPHarness(t *testing.T) *mcpHarness {
	t.Helper()
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, mcpMigratedDB(t))
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)

	wall := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	clock := mcpClock{now: wall}
	uow := postgres.NewUnitOfWork(pool)
	pathRepo := postgres.NewProcessPathRepo(pool)
	cptRepo := postgres.NewCPTScheduleRepo(pool)

	// Seed: define two paths, deactivate one, define the schedule — all
	// through the real write use cases, so the read tools answer from
	// genuinely persisted state.
	define := &usecases.DefinePath{
		Repo: pathRepo, Publisher: nopPublisher{}, Clock: clock, UnitOfWork: uow,
	}
	mustDefine := func(id, matchPrefix string, caps ...shared.Capability) {
		t.Helper()
		if _, err := define.Execute(ctx, shared.PathId(id), matchPrefix, true, caps,
			shared.DestinationLocationRoleUnset, 2*time.Hour, shared.Eligibility{}); err != nil {
			t.Fatalf("seed define %s: %v", id, err)
		}
	}
	mustDefine("PICK", "pick", "pick")
	mustDefine("REBIN", "rebin", "rebin")
	deactivate := &usecases.DeactivatePath{
		Repo: pathRepo, Publisher: nopPublisher{}, Clock: clock,
		UnitOfWork: uow, CPTSchedules: cptRepo,
	}
	if err := deactivate.Execute(ctx, "REBIN"); err != nil {
		t.Fatalf("seed deactivate REBIN: %v", err)
	}
	defineSched := &usecases.DefineCPTSchedule{
		Repo: cptRepo, ProcessPathRepo: pathRepo,
		Publisher: nopPublisher{}, Clock: clock, UnitOfWork: uow,
	}
	cutoffs := mustCutoffs(t, "PICK")
	if _, err := defineSched.Execute(ctx, "sp1", "UTC", cutoffs); err != nil {
		t.Fatalf("seed define schedule: %v", err)
	}

	// The report tool gets the REAL REST client pointed at the REAL
	// reports router over a seeded analytics database — the same
	// client/router pair cmd/mcp and cmd/pathmgmt-reports ship.
	reportsURL := newReportsService(t, wall)

	server := mcpadapter.NewServer(mcpadapter.Deps{
		GetPath:        &usecases.GetPath{Repo: pathRepo},
		ListPaths:      &usecases.ListPaths{Repo: pathRepo},
		GetCPTSchedule: &usecases.GetCPTSchedule{Repo: cptRepo},
		Reports:        mcpadapter.NewReportsRESTClient(reportsURL, nil),
	})

	hs := httptest.NewServer(mcpadapter.Handler(server))
	t.Cleanup(hs.Close)

	client := sdkmcp.NewClient(&sdkmcp.Implementation{Name: "itcov-test-host", Version: "1.0.0"}, nil)
	session, err := client.Connect(ctx, &sdkmcp.StreamableClientTransport{
		Endpoint: hs.URL, DisableStandaloneSSE: true,
	}, nil)
	if err != nil {
		t.Fatalf("mcp connect: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })

	return &mcpHarness{session: session, pool: pool}
}

// newReportsService boots the real pathmgmt-reports stack — analytics
// Postgres (testcontainers, analytics migrations), PostgresReport reader,
// ReportsHandlers, chi router — seeds it with the catalogue-growth rows
// the seeded OLTP writes would leave behind (one defined on wall's day,
// one deactivated), and serves it. It returns the base URL the MCP report
// tool's REST client is pointed at.
func newReportsService(t *testing.T, wall time.Time) string {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("pathmgmt"),
		tcpostgres.WithUsername("pathmgmt"),
		tcpostgres.WithPassword("pathmgmt"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(90*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start analytics postgres container: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })

	analyticsURL, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("analytics connection string: %v", err)
	}
	if err := postgres.RunMigrations(analyticsURL, mcpAnalyticsMigrationsDir()); err != nil {
		t.Fatalf("migrate analytics: %v", err)
	}
	analyticsPool, err := analyticsstore.NewPool(ctx, analyticsURL)
	if err != nil {
		t.Fatalf("open analytics pool: %v", err)
	}
	t.Cleanup(analyticsPool.Close)

	// Seed the rollup exactly like the projector does: counters per UTC
	// day bucket. The 2026-10-09 bucket matches the seeded defines; the
	// 2026-10-08 bucket proves the window filter excludes other days.
	bucket := wall.UTC().Truncate(24 * time.Hour)
	seed := []struct {
		day         time.Time
		defined     int
		deactivated int
	}{
		{bucket.AddDate(0, 0, -1), 5, 0},
		{bucket, 2, 1},
	}
	for _, s := range seed {
		if _, err := analyticsPool.Exec(ctx,
			`INSERT INTO catalogue_growth_rollup (day_bucket, paths_defined, paths_revised, paths_deactivated)
			 VALUES ($1, $2, 0, $3)`,
			s.day, s.defined, s.deactivated); err != nil {
			t.Fatalf("seed rollup %s: %v", s.day, err)
		}
	}
	// Freshness reads max(occurred_at) from analytics_processed_events.
	if _, err := analyticsPool.Exec(ctx,
		`INSERT INTO analytics_processed_events (event_id, occurred_at) VALUES ($1, $2)`,
		"seed-freshness-1", wall); err != nil {
		t.Fatalf("seed freshness: %v", err)
	}

	hs := httptest.NewServer(inboundhttp.NewReportsRouter(
		&inboundhttp.ReportsHandlers{Store: analyticsstore.NewPostgresReport(analyticsPool)}, nil))
	t.Cleanup(hs.Close)
	return hs.URL
}

// mustCutoffs builds the sp1 schedule's cutoff set.
func mustCutoffs(t *testing.T, eligible ...shared.PathId) []cptschedule.Cutoff {
	t.Helper()
	c, err := cptschedule.NewCutoff("sp1-1500", "15:00",
		[]cptschedule.Weekday{cptschedule.Monday, cptschedule.Tuesday}, "ground", eligible)
	if err != nil {
		t.Fatalf("cutoff: %v", err)
	}
	return []cptschedule.Cutoff{c}
}

// nopPublisher swallows the seed writes' events; these tests assert
// through the MCP read surface, not the event stream.
type nopPublisher struct{}

func (nopPublisher) Publish(context.Context, shared.DomainEvent) error { return nil }

// stringContent pulls the single text block out of a CallToolResult.
func stringContent(t *testing.T, res *sdkmcp.CallToolResult) string {
	t.Helper()
	if len(res.Content) == 0 {
		t.Fatalf("tool result carries no content: %+v", res)
	}
	text, ok := res.Content[0].(*sdkmcp.TextContent)
	if !ok {
		t.Fatalf("tool result content is not text: %+v", res.Content[0])
	}
	return text.Text
}

func TestMCP_ListToolsExposesTheFullContract(t *testing.T) {
	h := newMCPHarness(t)
	ctx := context.Background()

	list, err := h.session.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("tools/list: %v", err)
	}
	names := map[string]bool{}
	for _, tool := range list.Tools {
		names[tool.Name] = true
		if tool.Annotations == nil || !tool.Annotations.ReadOnlyHint {
			t.Errorf("tool %q must be annotated ReadOnlyHint=true", tool.Name)
		}
	}
	// Every tool this adapter registers must be advertised, including the
	// two conditionally registered ones.
	for _, want := range []string{
		"get_process_path", "list_process_paths",
		"get_cpt_schedule", "get_catalogue_growth_report",
	} {
		if !names[want] {
			t.Fatalf("tools/list must expose %q, got %v", want, names)
		}
	}
}

func TestMCP_CallToolRoundTripThroughPostgres(t *testing.T) {
	h := newMCPHarness(t)
	ctx := context.Background()

	// get_process_path answers the full persisted definition.
	path, err := h.session.CallTool(ctx, &sdkmcp.CallToolParams{
		Name:      "get_process_path",
		Arguments: map[string]any{"pathId": "PICK"},
	})
	if err != nil {
		t.Fatalf("tools/call get_process_path: %v", err)
	}
	if path.IsError {
		t.Fatalf("get_process_path returned a tool error: %s", stringContent(t, path))
	}
	sc, ok := path.StructuredContent.(map[string]any)
	if !ok || sc["matchPrefix"] != "pick" {
		t.Fatalf("get_process_path must answer the persisted matchPrefix, got %+v", path.StructuredContent)
	}

	// list_process_paths honours activeOnly: the default view hides the
	// deactivated REBIN, the audit view keeps it.
	listDefault, err := h.session.CallTool(ctx, &sdkmcp.CallToolParams{
		Name:      "list_process_paths",
		Arguments: map[string]any{},
	})
	if err != nil {
		t.Fatalf("tools/call list_process_paths: %v", err)
	}
	if listDefault.IsError {
		t.Fatalf("list_process_paths returned a tool error: %s", stringContent(t, listDefault))
	}
	pathsOf := func(res *sdkmcp.CallToolResult) []any {
		t.Helper()
		m, ok := res.StructuredContent.(map[string]any)
		if !ok {
			t.Fatalf("no structured content: %+v", res.StructuredContent)
		}
		arr, ok := m["paths"].([]any)
		if !ok {
			t.Fatalf("no paths array: %+v", m)
		}
		return arr
	}
	if got := pathsOf(listDefault); len(got) != 1 {
		t.Fatalf("default view must be active-only (1 path), got %d", len(got))
	}
	listAll, err := h.session.CallTool(ctx, &sdkmcp.CallToolParams{
		Name:      "list_process_paths",
		Arguments: map[string]any{"activeOnly": false},
	})
	if err != nil {
		t.Fatalf("tools/call list_process_paths (audit view): %v", err)
	}
	if listAll.IsError {
		t.Fatalf("list_process_paths (audit view) returned a tool error: %s", stringContent(t, listAll))
	}
	if got := pathsOf(listAll); len(got) != 2 {
		t.Fatalf("audit view must include the deactivated path (2 paths), got %d", len(got))
	}

	// get_cpt_schedule answers the persisted schedule.
	sched, err := h.session.CallTool(ctx, &sdkmcp.CallToolParams{
		Name:      "get_cpt_schedule",
		Arguments: map[string]any{"siteId": "sp1"},
	})
	if err != nil {
		t.Fatalf("tools/call get_cpt_schedule: %v", err)
	}
	if sched.IsError {
		t.Fatalf("get_cpt_schedule returned a tool error: %s", stringContent(t, sched))
	}
	sm, ok := sched.StructuredContent.(map[string]any)
	if !ok || sm["timezone"] != "UTC" {
		t.Fatalf("get_cpt_schedule must answer the persisted schedule, got %+v", sched.StructuredContent)
	}

	// get_catalogue_growth_report answers through the real REST client ->
	// real reports router -> real analytics Postgres, and honours the
	// window: only wall's day bucket is inside [wall, wall+24h).
	report, err := h.session.CallTool(ctx, &sdkmcp.CallToolParams{
		Name: "get_catalogue_growth_report",
		Arguments: map[string]any{
			"from":        wallRFC3339(dayBucketOf(0)),
			"to":          wallRFC3339(dayBucketOf(1)),
			"granularity": "day",
		},
	})
	if err != nil {
		t.Fatalf("tools/call get_catalogue_growth_report: %v", err)
	}
	if report.IsError {
		t.Fatalf("get_catalogue_growth_report returned a tool error: %s", stringContent(t, report))
	}
	rm, ok := report.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("no structured content on report: %+v", report.StructuredContent)
	}
	rows, ok := rm["rows"].([]any)
	if !ok || len(rows) != 1 {
		t.Fatalf("report must return exactly the one in-window day bucket, got %+v", rm["rows"])
	}
	row, _ := rows[0].(map[string]any)
	if row["pathsDefined"] != float64(2) || row["pathsDeactivated"] != float64(1) {
		t.Fatalf("report row must carry the seeded counters, got %+v", row)
	}
}

// wallRFC3339/dayBucketOf keep the report window anchored to the same
// fixed clock the harness seeds with. dayBucketOf(n) returns the seeded
// wall date's UTC day bucket plus n days.
func wallRFC3339(t time.Time) string { return t.Format(time.RFC3339) }

func dayBucketOf(plusDays int) time.Time {
	return time.Date(2026, 10, 9+plusDays, 0, 0, 0, 0, time.UTC)
}

func TestMCP_CallToolSurfacesDomainAndInputRejectionsAsToolErrors(t *testing.T) {
	h := newMCPHarness(t)
	ctx := context.Background()

	// Domain rejection: an unknown path is a not-found TOOL error, not a
	// transport error and not a zero-value success.
	missing, err := h.session.CallTool(ctx, &sdkmcp.CallToolParams{
		Name:      "get_process_path",
		Arguments: map[string]any{"pathId": "MISSING"},
	})
	if err != nil {
		t.Fatalf("tools/call get_process_path (unknown id): %v", err)
	}
	if !missing.IsError {
		t.Fatal("unknown pathId must surface a tool error, not success")
	}

	// Same contract for the schedule tool.
	missingSite, err := h.session.CallTool(ctx, &sdkmcp.CallToolParams{
		Name:      "get_cpt_schedule",
		Arguments: map[string]any{"siteId": "no-such-site"},
	})
	if err != nil {
		t.Fatalf("tools/call get_cpt_schedule (unknown site): %v", err)
	}
	if !missingSite.IsError {
		t.Fatal("unknown siteId must surface a tool error, not success")
	}

	// Invalid input: an empty pathId is rejected as a tool error.
	empty, err := h.session.CallTool(ctx, &sdkmcp.CallToolParams{
		Name:      "get_process_path",
		Arguments: map[string]any{"pathId": ""},
	})
	if err != nil {
		t.Fatalf("tools/call get_process_path (empty id): %v", err)
	}
	if !empty.IsError {
		t.Fatal("empty pathId must surface a tool error")
	}

	// Invalid input: the report tool forwards a malformed window to the
	// reports service, whose 400 comes back as a tool error.
	noWindow, err := h.session.CallTool(ctx, &sdkmcp.CallToolParams{
		Name: "get_catalogue_growth_report",
		Arguments: map[string]any{
			"from":        "not-a-timestamp",
			"to":          wallRFC3339(dayBucketOf(1)),
			"granularity": "day",
		},
	})
	if err != nil {
		t.Fatalf("tools/call get_catalogue_growth_report (bad window): %v", err)
	}
	if !noWindow.IsError {
		t.Fatal("a malformed report window must surface a tool error")
	}
}
