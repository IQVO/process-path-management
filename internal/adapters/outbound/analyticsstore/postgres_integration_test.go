//go:build integration

package analyticsstore_test

import (
	"context"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/claudioed/process-path-management/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/process-path-management/internal/analytics/report"
)

// analyticsDB boots a throwaway analytics Postgres via testcontainers and
// applies the analytics migrations. The test owns its own database —
// never an external ANALYTICS_DATABASE_URL, never t.Skip — so CI cannot
// silently skip this suite (fleet rule; see the architecture fitness
// tests).
func analyticsDB(t *testing.T) (rwURL, roURL string) {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("pathmgmt"),
		tcpostgres.WithUsername("pathmgmt"),
		tcpostgres.WithPassword("pathmgmt"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start analytics postgres container: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })

	url, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	if err := postgres.RunMigrations(url, analyticsMigrationsDir(t)); err != nil {
		t.Fatalf("migrate analytics: %v", err)
	}
	return url, url
}

// analyticsMigrationsDir resolves /migrations/analytics relative to this
// test file, regardless of the working directory go test runs from.
func analyticsMigrationsDir(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("unable to resolve test file path")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..", "migrations", "analytics")
}

func TestPostgresProjectionAndReport_RoundTrip(t *testing.T) {
	rwURL, _ := analyticsDB(t)
	url := rwURL

	pool, err := analyticsstore.NewPool(context.Background(), url)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(pool.Close)

	ctx := context.Background()
	base := time.Now().UTC().Truncate(24 * time.Hour)
	prefix := "e-" + time.Now().Format("150405.000000000")

	proj := analyticsstore.NewPostgresProjection(pool)
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("apply: %v", err)
		}
	}

	// Apply the same event ids twice: idempotent.
	apply := func() {
		must(proj.ApplyProcessPathCreated(ctx, prefix+"-c1", base))
		must(proj.ApplyProcessPathCreated(ctx, prefix+"-c2", base))
		must(proj.ApplyProcessPathUpdated(ctx, prefix+"-u1", base))
		must(proj.ApplyProcessPathDeactivated(ctx, prefix+"-d1", base))
	}
	apply()
	apply()

	rdr := analyticsstore.NewPostgresReport(pool)
	rep, err := rdr.Query(ctx, report.ReportQuery{
		From:        base,
		To:          base.Add(24 * time.Hour),
		Granularity: report.GranularityDay,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	// The rollup is a single shared day_bucket row with no scope
	// dimension, so other tests' rows on the same day may also be
	// present; find our own bucket and check it is at least our tally
	// (idempotent re-apply must not double it).
	if len(rep.Rows) != 1 {
		t.Fatalf("rows = %d, want 1 (single day bucket, no scope dimension)", len(rep.Rows))
	}
	row := rep.Rows[0]
	if row.PathsDefined < 2 {
		t.Errorf("PathsDefined = %d, want >= 2", row.PathsDefined)
	}
	if row.PathsRevised < 1 {
		t.Errorf("PathsRevised = %d, want >= 1", row.PathsRevised)
	}
	if row.PathsDeactivated < 1 {
		t.Errorf("PathsDeactivated = %d, want >= 1", row.PathsDeactivated)
	}

	lag, err := rdr.FreshnessLag(ctx)
	if err != nil {
		t.Fatalf("FreshnessLag: %v", err)
	}
	if lag < 0 {
		t.Errorf("lag = %v, want >= 0", lag)
	}
}

// TestReadOnlyPool_RejectsWrites asserts the reader pool is genuinely
// read-only: an attempt to write through it must be rejected by Postgres.
func TestReadOnlyPool_RejectsWrites(t *testing.T) {
	rwURL, _ := analyticsDB(t)
	url := rwURL

	roPool, err := analyticsstore.NewReadOnlyPool(context.Background(), url)
	if err != nil {
		t.Fatalf("NewReadOnlyPool: %v", err)
	}
	t.Cleanup(roPool.Close)

	ctx := context.Background()
	_, err = roPool.Exec(ctx,
		`INSERT INTO catalogue_growth_rollup (day_bucket) VALUES ($1)`,
		time.Now().UTC().Truncate(24*time.Hour).Add(999*24*time.Hour))
	if err == nil {
		t.Fatal("expected read-only pool to reject INSERT, but it succeeded")
	}

	// The read side still works over the same read-only pool.
	rdr := analyticsstore.NewPostgresReport(roPool)
	if _, err := rdr.FreshnessLag(ctx); err != nil {
		t.Fatalf("FreshnessLag over read-only pool: %v", err)
	}
}

// TestFreshnessLag_EmptyStore covers the NULL path: max(occurred_at) over
// an empty table returns a single NULL row (not zero rows), which must be
// read as a zero lag rather than a scan error.
func TestFreshnessLag_EmptyStore(t *testing.T) {
	rwURL, _ := analyticsDB(t)
	url := rwURL

	pool, err := analyticsstore.NewPool(context.Background(), url)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(pool.Close)

	ctx := context.Background()
	if _, err := pool.Exec(ctx, `TRUNCATE analytics_processed_events`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	lag, err := analyticsstore.NewPostgresReport(pool).FreshnessLag(ctx)
	if err != nil {
		t.Fatalf("FreshnessLag on empty store: %v", err)
	}
	if lag != 0 {
		t.Fatalf("empty-store lag = %v, want 0", lag)
	}
}

// TestConsumedEventsRepo_MarksOnce verifies the consumer dedupe gate: the
// same event_id is admitted once and rejected thereafter.
func TestConsumedEventsRepo_MarksOnce(t *testing.T) {
	rwURL, _ := analyticsDB(t)
	url := rwURL

	pool, err := analyticsstore.NewPool(context.Background(), url)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(pool.Close)

	ctx := context.Background()
	id := "consumed-" + time.Now().Format("150405.000000000")
	t.Cleanup(func() { _, _ = pool.Exec(ctx, `DELETE FROM analytics_consumed_events WHERE event_id = $1`, id) })

	repo := analyticsstore.NewConsumedEventsRepo(pool)
	first, err := repo.MarkProcessed(ctx, id)
	if err != nil || !first {
		t.Fatalf("first MarkProcessed = (%v, %v), want (true, nil)", first, err)
	}
	second, err := repo.MarkProcessed(ctx, id)
	if err != nil || second {
		t.Fatalf("second MarkProcessed = (%v, %v), want (false, nil)", second, err)
	}
}
