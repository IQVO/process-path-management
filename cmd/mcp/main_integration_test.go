//go:build integration

// Integration test for cmd/mcp over REAL Postgres from testcontainers: it
// exercises the binary's own boot composition — buildRepo (migrations +
// pool + ping, the exact function run() calls; the already-migrated
// database makes the boot-time migration run a no-op) feeding the exact
// Deps run() assembles when REPORTS_BASE_URL is unset — and drives it with
// the SDK's own client over in-memory transports.
//
// The catalogue is seeded the documented way (docs/docs/ddd/use-cases.md):
// through the real write use cases on the real Postgres repos, with the
// real OutboxPublisher cmd/pathmgmt wires (ADR 0003), so the seeded writes
// leave genuine outbox rows. The assertions then pin, per
// docs/docs/mcp/tools.md and the Governance Charter (ADR 0006, ADR 0041):
//
//   - tools/list advertises exactly the three documented read-only L1
//     tools and nothing else — there is no write tool and, without
//     REPORTS_BASE_URL, no report tool;
//   - every documented tool answers from the persisted state, with the
//     documented output fields and the documented not-found tool errors;
//   - GET /healthz on the cmd's own router answers 200 {"status":"ok"};
//   - driving every read leaves the outbox byte-for-byte unchanged (the
//     charter is read-only; a read tool that wrote an event must fail).
//
// Postgres comes from testcontainers: one container per test binary,
// migrated once into a template database, one private clone per test.
// Never an external DATABASE_URL, never t.Skip.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	inboundmcp "github.com/claudioed/process-path-management/internal/adapters/inbound/mcp"
	outboundkafka "github.com/claudioed/process-path-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/process-path-management/internal/application/usecases"
	"github.com/claudioed/process-path-management/internal/domain/cptschedule"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// mcpITBaseURL is the shared container's connection URL; every test clones
// its own database from the migrated mcpITTemplateDB.
var mcpITBaseURL string

const mcpITTemplateDB = "mcp_cmd_template"

func TestMain(m *testing.M) {
	os.Exit(mcpITRun(m))
}

func mcpITRun(m *testing.M) int {
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
	if mcpITBaseURL, err = container.ConnectionString(ctx, "sslmode=disable"); err != nil {
		fmt.Fprintf(os.Stderr, "postgres connection string: %v\n", err)
		return 1
	}
	if err := mcpITCreateDatabase(ctx, mcpITTemplateDB); err != nil {
		fmt.Fprintf(os.Stderr, "create template database: %v\n", err)
		return 1
	}
	if err := postgres.RunMigrations(mcpITWithDB(mcpITBaseURL, mcpITTemplateDB), mcpITMigrationsDir()); err != nil {
		fmt.Fprintf(os.Stderr, "migrate template: %v\n", err)
		return 1
	}
	return m.Run()
}

// mcpITFreshDB hands the test a connection URL to its own private database
// cloned from the migrated template (milliseconds, fully isolated).
func mcpITFreshDB(t *testing.T) string {
	t.Helper()
	name := fmt.Sprintf("mcp_cmd_%d", time.Now().UnixNano())
	conn, err := pgx.Connect(context.Background(), mcpITBaseURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	if _, err := conn.Exec(context.Background(), fmt.Sprintf(
		"CREATE DATABASE %q WITH TEMPLATE %q", name, mcpITTemplateDB)); err != nil {
		t.Fatalf("clone database: %v", err)
	}
	return mcpITWithDB(mcpITBaseURL, name)
}

func mcpITCreateDatabase(ctx context.Context, name string) error {
	conn, err := pgx.Connect(ctx, mcpITBaseURL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %q", name)); err != nil {
		return fmt.Errorf("create database %s: %w", name, err)
	}
	return nil
}

func mcpITWithDB(baseURL, name string) string {
	u, err := url.Parse(baseURL)
	if err != nil {
		panic(err)
	}
	u.Path = "/" + name
	return u.String()
}

// mcpITMigrationsDir resolves the repo's /migrations relative to this file
// (cmd/mcp -> repo root).
func mcpITMigrationsDir() string {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		panic("unable to resolve test file path")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "migrations")
}

// fixedClock is the ports.Clock every write use case accepts; a fixed wall
// keeps the persisted timestamps deterministic.
type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

// mcpITHarness is the booted binary: buildRepo's repos and pool, the
// seeded catalogue, the Deps run() assembles, and a connected SDK client
// session over in-memory transports.
type mcpITHarness struct {
	session *sdk.ClientSession
	server  *sdk.Server
	pool    *pgxpool.Pool
}

var mcpITWall = time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)

func newMCPITHarness(t *testing.T) *mcpITHarness {
	t.Helper()
	ctx := context.Background()
	dsn := mcpITFreshDB(t)

	// The cmd's real boot path: migrations (a no-op on the migrated
	// clone), the pool, the boot ping — buildRepo, exactly as run()
	// calls it.
	repo, pool, closeAdapters, err := buildRepo(ctx, dsn, dsn, mcpITMigrationsDir(), quietLogger())
	if err != nil {
		t.Fatalf("buildRepo: %v", err)
	}
	t.Cleanup(closeAdapters)

	// Seed through the real write use cases with the real OutboxPublisher
	// cmd/pathmgmt wires (ADR 0003), so the seeded writes leave real
	// outbox rows the read-only assertion below is measured against.
	publisher := postgres.NewOutboxPublisher(pool, uuid.NewString,
		outboundkafka.IntegrationEncoder{},
		outboundkafka.NewAnalyticsEncoder(uuid.NewString))
	uow := postgres.NewUnitOfWork(pool)
	cptRepo := postgres.NewCPTScheduleRepo(pool)
	clock := fixedClock{now: mcpITWall}

	define := &usecases.DefinePath{Repo: repo, Publisher: publisher, Clock: clock, UnitOfWork: uow}
	for _, seed := range []struct {
		id, matchPrefix string
		caps            []shared.Capability
	}{
		{"PICK", "pick", []shared.Capability{"pick"}},
		{"REBIN", "rebin", []shared.Capability{"rebin"}},
	} {
		if _, err := define.Execute(ctx, shared.PathId(seed.id), seed.matchPrefix, true, seed.caps,
			shared.DestinationLocationRoleUnset, 2*time.Hour, shared.Eligibility{}, shared.Intake{}); err != nil {
			t.Fatalf("seed define %s: %v", seed.id, err)
		}
	}
	deactivate := &usecases.DeactivatePath{
		Repo: repo, Publisher: publisher, Clock: clock,
		UnitOfWork: uow, CPTSchedules: cptRepo,
	}
	if err := deactivate.Execute(ctx, "REBIN"); err != nil {
		t.Fatalf("seed deactivate REBIN: %v", err)
	}
	cutoff, err := cptschedule.NewCutoff("sp1-1500", "15:00",
		[]cptschedule.Weekday{cptschedule.Monday, cptschedule.Tuesday}, "ground", []shared.PathId{"PICK"})
	if err != nil {
		t.Fatalf("cutoff: %v", err)
	}
	defineSchedule := &usecases.DefineCPTSchedule{
		Repo: cptRepo, ProcessPathRepo: repo,
		Publisher: publisher, Clock: clock, UnitOfWork: uow,
	}
	if _, err := defineSchedule.Execute(ctx, "sp1", "UTC", []cptschedule.Cutoff{cutoff}); err != nil {
		t.Fatalf("seed define schedule: %v", err)
	}

	// Exactly the Deps run() assembles with REPORTS_BASE_URL unset: the
	// three read use cases, one shared pool, no Reports client.
	deps := inboundmcp.Deps{
		GetPath:        &usecases.GetPath{Repo: repo},
		ListPaths:      &usecases.ListPaths{Repo: repo},
		GetCPTSchedule: &usecases.GetCPTSchedule{Repo: cptRepo},
	}
	server := inboundmcp.NewServer(deps)

	ct, st := sdk.NewInMemoryTransports()
	ss, err := server.Connect(ctx, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ss.Close() })
	session, err := sdk.NewClient(&sdk.Implementation{Name: "cmd-mcp-itcov", Version: "1.0.0"}, nil).Connect(ctx, ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })

	return &mcpITHarness{session: session, server: server, pool: pool}
}

// mcpITCountOutbox reads the outbox row count the transactional outbox
// left behind.
func (h *mcpITHarness) mcpITCountOutbox(t *testing.T) int {
	t.Helper()
	var n int
	if err := h.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM outbox_events`).Scan(&n); err != nil {
		t.Fatalf("count outbox_events: %v", err)
	}
	return n
}

// mcpITCall calls one tool and returns its structured content.
func (h *mcpITHarness) mcpITCall(t *testing.T, name string, args map[string]any) map[string]any {
	t.Helper()
	res, err := h.session.CallTool(context.Background(), &sdk.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		t.Fatalf("tools/call %s: %v", name, err)
	}
	if res.IsError {
		raw, _ := json.Marshal(res.Content)
		t.Fatalf("tools/call %s returned a tool error: %s", name, raw)
	}
	m, ok := res.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("tools/call %s: no structured content: %+v", name, res.StructuredContent)
	}
	return m
}

// TestMCPCompositionServesTheDocumentedReadOnlyToolSurface pins the tool
// governance of the booted binary (docs/docs/mcp/tools.md, Governance
// Charter, ADR 0006/0041): exactly the three documented tools, each
// annotated read-only with authority L1, and the documented /healthz probe
// on the cmd's own router.
func TestMCPCompositionServesTheDocumentedReadOnlyToolSurface(t *testing.T) {
	h := newMCPITHarness(t)

	list, err := h.session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatalf("tools/list: %v", err)
	}
	names := make([]string, 0, len(list.Tools))
	for _, tool := range list.Tools {
		names = append(names, tool.Name)
		if tool.Annotations == nil || !tool.Annotations.ReadOnlyHint {
			t.Errorf("tool %q must be annotated ReadOnlyHint=true", tool.Name)
		}
		if got := tool.Meta["com.warehouse/authority"]; got != "L1" {
			t.Errorf("tool %q authority _meta = %v, want L1 (ADR 0041)", tool.Name, got)
		}
	}
	slices.Sort(names)
	want := []string{"get_cpt_schedule", "get_process_path", "list_process_paths"}
	if !slices.Equal(names, want) {
		t.Fatalf("tools/list = %v, want exactly %v: no write tool exists (the charter is read-only) and get_catalogue_growth_report must be absent without REPORTS_BASE_URL", names, want)
	}

	// The cmd's own router exposes the documented probe surface
	// (docs/docs/mcp/tools.md): GET /healthz answers 200 {"status":"ok"}.
	probe := httptest.NewServer(newRouter(inboundmcp.Handler(h.server)))
	t.Cleanup(probe.Close)
	resp, err := http.Get(probe.URL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || string(body) != `{"status":"ok"}` {
		t.Fatalf("GET /healthz = %d %s, want 200 {\"status\":\"ok\"}", resp.StatusCode, body)
	}
}

// TestMCPToolsAnswerTheSeededCatalogueAndNeverWrite proves every
// documented tool answers from genuinely persisted state with the
// documented shapes and errors, and that driving them all leaves the
// transactional outbox unchanged: cmd/mcp is read-only (ADR 0006), so a
// read that enqueued an event is a governance violation.
func TestMCPToolsAnswerTheSeededCatalogueAndNeverWrite(t *testing.T) {
	h := newMCPITHarness(t)

	before := h.mcpITCountOutbox(t)
	if before == 0 {
		t.Fatal("the seeded writes must have left outbox rows for the unchanged assertion to mean anything")
	}

	// get_process_path answers the full persisted definition
	// (docs/docs/mcp/tools.md: pathId, matchPrefix, direct,
	// requiredCapabilities, status/active, cycleTimeP95, intake).
	pick := h.mcpITCall(t, "get_process_path", map[string]any{"pathId": "PICK"})
	for k, want := range map[string]any{
		"pathId": "PICK", "matchPrefix": "pick", "direct": true,
		"status": "ACTIVE", "active": true, "cycleTimeP95": "2h0m0s",
	} {
		if got := pick[k]; got != want {
			t.Errorf("get_process_path PICK %s = %v, want %v", k, got, want)
		}
	}
	for _, k := range []string{"createdAt", "updatedAt"} {
		ts, err := time.Parse(time.RFC3339, fmt.Sprint(pick[k]))
		if err != nil {
			t.Errorf("get_process_path PICK %s = %v, not RFC 3339", k, pick[k])
		} else if !ts.Equal(mcpITWall) {
			t.Errorf("get_process_path PICK %s = %v, want the seeded wall instant %s (offset varies with the server's zone)", k, pick[k], mcpITWall)
		}
	}
	if fmt.Sprint(pick["requiredCapabilities"]) != "[pick]" {
		t.Errorf("get_process_path PICK requiredCapabilities = %v, want [pick]", pick["requiredCapabilities"])
	}
	if _, ok := pick["intake"].([]any); !ok {
		t.Errorf("get_process_path PICK intake = %v, want always an array ([] for no demand source)", pick["intake"])
	}

	// A deactivated path stays readable, in any status (docs: "Returns
	// one path in any status").
	rebin := h.mcpITCall(t, "get_process_path", map[string]any{"pathId": "REBIN"})
	if rebin["status"] != "DEACTIVATED" || rebin["active"] != false {
		t.Errorf("get_process_path REBIN = %v, want status DEACTIVATED and active=false", rebin)
	}

	// An unknown id is a tool error, not an empty success
	// (usecases.ErrPathNotFound).
	ghost, err := h.session.CallTool(context.Background(), &sdk.CallToolParams{
		Name: "get_process_path", Arguments: map[string]any{"pathId": "GHOST"},
	})
	if err != nil {
		t.Fatalf("tools/call get_process_path GHOST: %v", err)
	}
	raw, _ := json.Marshal(ghost.Content)
	if !ghost.IsError || !strings.Contains(string(raw), "process path not found") {
		t.Fatalf("get_process_path GHOST = isError:%v %s, want a tool error naming the not-found path", ghost.IsError, raw)
	}

	// list_process_paths honours activeOnly: the default view hides the
	// deactivated path, the audit view keeps it.
	pathsOf := func(res map[string]any) []any {
		t.Helper()
		arr, ok := res["paths"].([]any)
		if !ok {
			t.Fatalf("list_process_paths: no paths array: %+v", res)
		}
		return arr
	}
	if got := pathsOf(h.mcpITCall(t, "list_process_paths", map[string]any{})); len(got) != 1 {
		t.Errorf("list_process_paths default = %d paths, want 1 (active-only)", len(got))
	}
	if got := pathsOf(h.mcpITCall(t, "list_process_paths", map[string]any{"activeOnly": false})); len(got) != 2 {
		t.Errorf("list_process_paths activeOnly=false = %d paths, want 2 (audit view)", len(got))
	}

	// get_cpt_schedule answers the persisted schedule: timezone and the
	// cutoff's cptId, localTime, daysOfWeek, shipMethod,
	// eligiblePathIds (ADR 0010).
	schedule := h.mcpITCall(t, "get_cpt_schedule", map[string]any{"siteId": "sp1"})
	if schedule["siteId"] != "sp1" || schedule["timezone"] != "UTC" {
		t.Errorf("get_cpt_schedule sp1 = %v, want siteId sp1 timezone UTC", schedule)
	}
	cutoffs, ok := schedule["cutoffs"].([]any)
	if !ok || len(cutoffs) != 1 {
		t.Fatalf("get_cpt_schedule cutoffs = %v, want exactly one", schedule["cutoffs"])
	}
	c0, _ := cutoffs[0].(map[string]any)
	for k, want := range map[string]any{
		"cptId": "sp1-1500", "localTime": "15:00", "shipMethod": "ground",
	} {
		if c0[k] != want {
			t.Errorf("cutoff %s = %v, want %v", k, c0[k], want)
		}
	}
	if fmt.Sprint(c0["daysOfWeek"]) != "[Mon Tue]" || fmt.Sprint(c0["eligiblePathIds"]) != "[PICK]" {
		t.Errorf("cutoff daysOfWeek/eligiblePathIds = %v/%v, want [Mon Tue]/[PICK]", c0["daysOfWeek"], c0["eligiblePathIds"])
	}

	// A site with no schedule is a tool error (usecases.ErrCPTScheduleNotFound).
	unknown, err := h.session.CallTool(context.Background(), &sdk.CallToolParams{
		Name: "get_cpt_schedule", Arguments: map[string]any{"siteId": "no-such-site"},
	})
	if err != nil {
		t.Fatalf("tools/call get_cpt_schedule unknown site: %v", err)
	}
	raw, _ = json.Marshal(unknown.Content)
	if !unknown.IsError || !strings.Contains(string(raw), "cpt schedule not found") {
		t.Fatalf("get_cpt_schedule unknown site = isError:%v %s, want a tool error naming the not-found schedule", unknown.IsError, raw)
	}

	// Governance: every documented tool was driven above; none of it may
	// have written an outbox row.
	if after := h.mcpITCountOutbox(t); after != before {
		t.Fatalf("outbox rows %d -> %d after driving every read tool: a read-only tool wrote an event (ADR 0006 charter violation)", before, after)
	}
}
