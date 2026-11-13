//go:build integration

// End-to-end proof of the REAL entry point of cmd/pathmgmt-reports on
// testcontainers: the analytical database is migrated with the repo's own
// runner and seeded with the repo's own analytics writer (the projection
// cmd/pathmgmt-projector owns), the binary boots with nothing but its
// documented environment, the documented probe answers, the documented
// catalogue-growth report and its freshness answer the seeded facts, and
// SIGTERM drains cleanly.
//
// Doc sources: docs/docs/adr/0007-analytical-data-product.md (the reports
// binary is the read-only reader of the analytical database;
// ANALYTICS_DATABASE_URL, HTTP_ADDR :8092),
// apis/openapi-reports.yaml and docs/docs/api-reference/rest-reports/
// get-catalogue-growth-report.api.mdx + get-catalogue-growth-freshness.api.mdx
// (from/to required RFC 3339, granularity=day; rows[].day_bucket/
// paths_defined/paths_revised/paths_deactivated; freshness lagSeconds).
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/claudioed/process-path-management/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/postgres"
)

// itMigrationsPathValue resolves the repo's ANALYTICAL migrations directory
// (migrations/analytics) relative to this file.
var itMigrationsPathValue = func() string {
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "migrations", "analytics")
}()

// TestMain holds the package-level SIGTERM registration for the whole test
// run: the real run() installs (and on return RESTORES) its own signal
// handler, so a SIGTERM arriving outside a live run() would otherwise kill
// the test binary itself.
func TestMain(m *testing.M) {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	os.Exit(m.Run())
}

// itPostgres starts a throwaway analytical Postgres and returns its DSN.
func itPostgres(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("pathmgmt_reports_itest"),
		tcpostgres.WithUsername("pathmgmt"),
		tcpostgres.WithPassword("pathmgmt"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Logf("terminate postgres container: %v", err)
		}
	})
	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("resolve connection string: %v", err)
	}
	return dsn
}

// service is one booted instance of the real run().
type service struct {
	base     string
	done     <-chan error
	stopOnce sync.Once
	stopErr  error
}

// itBootReports sets the environment a deployment sets, starts the REAL
// run() in-process and waits for the documented probe /healthz.
func itBootReports(t *testing.T, env map[string]string) *service {
	t.Helper()
	for k, v := range env {
		t.Setenv(k, v)
	}
	addr := freeAddr(t)
	t.Setenv("HTTP_ADDR", addr)
	done := make(chan error, 1)
	go func() { done <- run() }()
	s := &service{base: "http://" + addr, done: done}
	t.Cleanup(func() { itAssertCleanDrain(t, s.stop()) })
	waitUntil(t, "the documented probe /healthz to answer 200", func() bool {
		code, _ := getJSON(t, s.base+"/healthz")
		return code == http.StatusOK
	})
	return s
}

// stop drives the binary's real shutdown path (SIGTERM — run() blocks on
// its own signal.Notify); safe because TestMain keeps a signal sink.
func (s *service) stop() error {
	s.stopOnce.Do(func() {
		select {
		case s.stopErr = <-s.done: // already stopped on its own
		default:
		}
		if s.stopErr == nil {
			if killErr := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); killErr != nil {
				s.stopErr = killErr
				return
			}
			select {
			case s.stopErr = <-s.done:
			case <-time.After(60 * time.Second):
				s.stopErr = fmt.Errorf("the reports server did not stop")
			}
		}
	})
	return s.stopErr
}

// itAssertCleanDrain checks run()'s shutdown outcome: a SIGTERM-driven stop
// is a clean shutdown and run() returns nil.
func itAssertCleanDrain(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("the reports server must drain cleanly on SIGTERM, got %v", err)
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(90 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

func getJSON(t *testing.T, url string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		return 0, nil
	}
	defer func() { _ = resp.Body.Close() }()
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	return resp.StatusCode, body
}

// TestReportsServeTheProjectionAndStopCleanly proves the shipped reader's
// contract: the analytical migrations + the repo's own writer seed the
// day rollup (distinct, non-zero counts), then env only -> /healthz -> the
// documented catalogue-growth row and freshness answer the seeded facts ->
// SIGTERM drains cleanly.
func TestReportsServeTheProjectionAndStopCleanly(t *testing.T) {
	dsn := itPostgres(t)
	if err := postgres.RunMigrations(dsn, itMigrationsPathValue); err != nil {
		t.Fatalf("run analytics migrations: %v", err)
	}
	ctx := context.Background()
	writer, err := analyticsstore.NewPool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	projection := analyticsstore.NewPostgresProjection(writer)

	day := time.Now().UTC().Truncate(24 * time.Hour).Add(-24 * time.Hour)
	steps := []struct {
		apply func(context.Context, string, time.Time) error
		id    string
		at    time.Time
	}{
		{projection.ApplyProcessPathCreated, "rep-e1", day.Add(8 * time.Hour)},
		{projection.ApplyProcessPathCreated, "rep-e2", day.Add(9 * time.Hour)},
		{projection.ApplyProcessPathUpdated, "rep-e3", day.Add(10 * time.Hour)},
		{projection.ApplyProcessPathDeactivated, "rep-e4", day.Add(11 * time.Hour)},
	}
	for _, st := range steps {
		if err := st.apply(ctx, st.id, st.at); err != nil {
			t.Fatalf("seed %s: %v", st.id, err)
		}
	}

	s := itBootReports(t, map[string]string{
		"ANALYTICS_DATABASE_URL": dsn,
		"LOG_LEVEL":              "error",
	})

	from, to := day.Format(time.RFC3339), day.Add(24*time.Hour).Format(time.RFC3339)
	code, body := getJSON(t, s.base+"/reports/catalogue-growth?from="+from+"&to="+to+"&granularity=day")
	if code != http.StatusOK {
		t.Fatalf("catalogue-growth = %d %v", code, body)
	}
	rows, _ := body["rows"].([]any)
	if len(rows) != 1 {
		t.Fatalf("catalogue-growth rows = %v", body)
	}
	row, _ := rows[0].(map[string]any)
	if row["dayBucket"] != from {
		t.Fatalf("dayBucket = %v (want %s)", row["dayBucket"], from)
	}
	if row["pathsDefined"] != float64(2) || row["pathsRevised"] != float64(1) || row["pathsDeactivated"] != float64(1) {
		t.Fatalf("catalogue row = %v", row)
	}

	code, body = getJSON(t, s.base+"/reports/catalogue-growth/freshness")
	if code != http.StatusOK || body["lagSeconds"] == nil {
		t.Fatalf("freshness = %d %v", code, body)
	}
	if lag, _ := body["lagSeconds"].(float64); lag < 0 {
		t.Fatalf("lagSeconds = %v (never negative)", body)
	}

	itAssertCleanDrain(t, s.stop())
}
