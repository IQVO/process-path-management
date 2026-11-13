//go:build integration

package main

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/claudioed/process-path-management/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/postgres"
)

// The READER binary's boot-level proof (ADR 0007, ADR 0031): the real run()
// boots on an analytically-migrated EMPTY database the test seeded through
// the repo's OWN analytics writer (the projector's PostgresProjection),
// answers the documented liveness probe, and serves the documented
// catalogue-growth report and its freshness over the real HTTP surface —
// the curated report cmd/mcp only proxies via REPORTS_BASE_URL, left as a
// gap by that binary's integration test. A SIGTERM drains it cleanly.

// reportsContainerStart bounds a testcontainers start.
const reportsContainerStart = 120 * time.Second

// reportsBootDeadline bounds how long the test waits for the listener.
const reportsBootDeadline = 60 * time.Second

// startReportsPostgres boots an EMPTY analytical Postgres and migrates it
// with the repo's own analytics migrations: the reports binary is the
// reader and never migrates (the projector owns the schema, ADR 0007), so
// the test stands in for the writer's schema the way a deployment's
// projector has left it.
func startReportsPostgres(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), reportsContainerStart)
	defer cancel()
	c, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("pathmgmt_reports_it"),
		tcpostgres.WithUsername("it"), tcpostgres.WithPassword("it"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).WithStartupTimeout(60*time.Second)))
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(c) })
	url, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres dsn: %v", err)
	}
	if err := postgres.RunMigrations(url, "../../migrations/analytics"); err != nil {
		t.Fatalf("run analytics migrations: %v", err)
	}
	return url
}

// freeReportsAddr reserves a loopback port for the reports listener.
func freeReportsAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

// getReports performs a GET and decodes the JSON body, returning the status
// (-1 when the connection fails) and the raw body string.
func getReports(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Get(url)
	if err != nil {
		return -1, err.Error()
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(raw)
}

// growthRow is the documented wire shape of one catalogue-growth row
// (dayBucket RFC3339 + the three counters).
type growthRow struct {
	DayBucket        string `json:"dayBucket"`
	PathsDefined     int    `json:"pathsDefined"`
	PathsRevised     int    `json:"pathsRevised"`
	PathsDeactivated int    `json:"pathsDeactivated"`
}

type growthReport struct {
	Rows []growthRow `json:"rows"`
}

// sigtermDrain stops the signal-driven run() the only way it can be stopped
// inside a test process — a SIGTERM to self — retrying until run() returns,
// and requires a clean nil. The test's own handler is registered BEFORE
// run() boots and is deliberately NEVER unregistered (no signal.Stop): by
// the time run() returns its own signal.Notify has already drained, so
// unregistering the last handler would restore Go's default SIGTERM
// disposition while one of the retry signals can still be in flight, and
// the test binary itself would die with "signal: terminated".
func sigtermDrain(t *testing.T, done <-chan error) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("a SIGTERMed reports server must drain cleanly, got %v", err)
			}
			return
		case <-time.After(200 * time.Millisecond):
			// run() has not registered its handler yet (or is still
			// draining); signal again.
		}
		if time.Now().After(deadline) {
			t.Fatal("run() did not return after SIGTERM")
		}
	}
}

func TestReportsServeCatalogueGrowthAndDrainCleanly(t *testing.T) {
	dsn := startReportsPostgres(t)

	// Seed the read model the way only the projector may: the repo's OWN
	// analytics writer. Two definitions and one revision on day 1, one
	// deactivation on day 2, and a re-apply of the first event under the
	// SAME id — the documented per-eventId idempotency that makes it a
	// no-op.
	day1 := time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)
	day2 := time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC)
	ctx := context.Background()
	writer, err := analyticsstore.NewPool(ctx, dsn)
	if err != nil {
		t.Fatalf("open analytics writer pool: %v", err)
	}
	t.Cleanup(writer.Close)
	projection := analyticsstore.NewPostgresProjection(writer)
	seed := func(eventId string, at time.Time, apply func(context.Context, string, time.Time) error) {
		t.Helper()
		if err := apply(ctx, eventId, at); err != nil {
			t.Fatalf("seed %s: %v", eventId, err)
		}
	}
	seed("reports-it-e1", day1.Add(10*time.Hour), projection.ApplyProcessPathCreated)
	seed("reports-it-e2", day1.Add(11*time.Hour), projection.ApplyProcessPathCreated)
	seed("reports-it-e3", day1.Add(12*time.Hour), projection.ApplyProcessPathUpdated)
	seed("reports-it-e4", day2.Add(9*time.Hour), projection.ApplyProcessPathDeactivated)
	seed("reports-it-e1", day1.Add(10*time.Hour), projection.ApplyProcessPathCreated) // idempotent re-apply

	// Nothing but the environment a deployment sets (ADR 0037).
	addr := freeReportsAddr(t)
	t.Setenv("ANALYTICS_DATABASE_URL", dsn)
	t.Setenv("HTTP_ADDR", addr)
	t.Setenv("LOG_LEVEL", "warn")

	// The test's own SIGTERM handler, registered BEFORE run() boots and
	// never unregistered (see sigtermDrain).
	testSignals := make(chan os.Signal, 8)
	signal.Notify(testSignals, syscall.SIGINT, syscall.SIGTERM)
	done := make(chan error, 1)
	go func() { done <- run() }()

	// The documented liveness probe of this reader (ADR 0029).
	var status int
	var body string
	for deadline := time.Now().Add(reportsBootDeadline); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		select {
		case err := <-done:
			t.Fatalf("run() returned while booting: %v", err)
		default:
		}
		if status, body = getReports(t, "http://"+addr+"/healthz"); status == http.StatusOK {
			break
		}
	}
	if status != http.StatusOK {
		t.Fatalf("/healthz = %d %s, want 200", status, body)
	}

	// The documented report (docs/docs/api-reference/rest-reports): one row
	// per UTC day bucket whose start lies in the half-open window
	// [from, to), ordered by day, from the SEEDED facts.
	base := "http://" + addr + "/reports/catalogue-growth"
	status, body = getReports(t, base+"?from=2026-11-10T00:00:00Z&to=2026-11-12T00:00:00Z")
	if status != http.StatusOK {
		t.Fatalf("catalogue-growth = %d %s, want 200", status, body)
	}
	var report growthReport
	if err := json.Unmarshal([]byte(body), &report); err != nil {
		t.Fatalf("catalogue-growth body %s: %v", body, err)
	}
	want := []growthRow{
		{DayBucket: "2026-11-10T00:00:00Z", PathsDefined: 2, PathsRevised: 1, PathsDeactivated: 0},
		{DayBucket: "2026-11-11T00:00:00Z", PathsDefined: 0, PathsRevised: 0, PathsDeactivated: 1},
	}
	if len(report.Rows) != len(want) {
		t.Fatalf("catalogue-growth rows = %+v, want %+v", report.Rows, want)
	}
	for i, w := range want {
		if report.Rows[i] != w {
			t.Fatalf("catalogue-growth row %d = %+v, want %+v (day 1: 2 defined + 1 revised; day 2: 1 deactivated; the re-applied event id counted once)", i, report.Rows[i], w)
		}
	}

	// The window is half-open: [from, to) excludes day 1's bucket.
	status, body = getReports(t, base+"?from=2026-11-11T00:00:00Z&to=2026-11-12T00:00:00Z")
	if status != http.StatusOK {
		t.Fatalf("catalogue-growth half-open window = %d %s, want 200", status, body)
	}
	report = growthReport{}
	if err := json.Unmarshal([]byte(body), &report); err != nil {
		t.Fatalf("catalogue-growth body %s: %v", body, err)
	}
	if len(report.Rows) != 1 || report.Rows[0] != want[1] {
		t.Fatalf("half-open window rows = %+v, want only %+v", report.Rows, want[1])
	}

	// An empty window is a 200 with an EMPTY rows array, never null and
	// never a 404 (the endpoint's documented contract).
	status, body = getReports(t, base+"?from=2026-11-20T00:00:00Z&to=2026-11-21T00:00:00Z")
	if status != http.StatusOK {
		t.Fatalf("catalogue-growth empty window = %d %s, want 200", status, body)
	}
	report = growthReport{}
	if err := json.Unmarshal([]byte(body), &report); err != nil {
		t.Fatalf("catalogue-growth body %s: %v", body, err)
	}
	if report.Rows == nil || len(report.Rows) != 0 {
		t.Fatalf("empty window rows = %s, want a present empty array", body)
	}

	// A missing required parameter is the documented RFC 7807 400.
	status, body = getReports(t, base+"?to=2026-11-12T00:00:00Z")
	if status != http.StatusBadRequest || !json.Valid([]byte(body)) {
		t.Fatalf("catalogue-growth without from = %d %s, want a 400 problem document", status, body)
	}

	// The documented freshness: now minus the newest projected event time —
	// positive, because the seeded facts are in the past.
	status, body = getReports(t, "http://"+addr+"/reports/catalogue-growth/freshness")
	if status != http.StatusOK {
		t.Fatalf("freshness = %d %s, want 200", status, body)
	}
	var freshness struct {
		LagSeconds float64 `json:"lagSeconds"`
	}
	if err := json.Unmarshal([]byte(body), &freshness); err != nil {
		t.Fatalf("freshness body %s: %v", body, err)
	}
	if freshness.LagSeconds <= 0 {
		t.Fatalf("freshness lagSeconds = %f, want > 0 (the newest seeded event is in the past)", freshness.LagSeconds)
	}

	sigtermDrain(t, done)
}
