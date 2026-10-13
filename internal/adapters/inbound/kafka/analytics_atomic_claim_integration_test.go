//go:build integration

package kafka_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	// Shares the package's testcontainers broker (startBroker, defined in
	// analytics_consumer_integration_test.go); the blank import keeps
	// TestKafkaIntegrationTestsUseTestcontainers' per-file check satisfied.
	_ "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	inboundkafka "github.com/claudioed/process-path-management/internal/adapters/inbound/kafka"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/process-path-management/internal/analytics/report"
)

// crashOnFirstApplyProjection wraps the REAL PostgresProjection. The first
// ApplyProcessPathCreated simulates the projector process being stopped
// right after the dedupe claim was written and before the projection write
// committed: it cancels the consumer's run context (SIGTERM / pod eviction)
// and reports that cancellation, exactly like a real store whose in-flight
// statement is interrupted. Every later call is delegated.
type crashOnFirstApplyProjection struct {
	report.ProjectionStore
	crash   context.CancelFunc
	once    sync.Once
	crashed chan struct{}
}

func (p *crashOnFirstApplyProjection) ApplyProcessPathCreated(ctx context.Context, eventId string, at time.Time) error {
	first := false
	p.once.Do(func() { first = true })
	if first {
		p.crash()
		close(p.crashed)
		<-ctx.Done()
		return ctx.Err()
	}
	return p.ProjectionStore.ApplyProcessPathCreated(ctx, eventId, at)
}

// TestAnalyticsConsumer_CrashBetweenClaimAndApply_RedeliveryAppliesExactlyOnce
// pins ADR 0032: the consumer's dedupe claim (analytics_consumed_events)
// and the projection write commit in ONE analytics transaction. A process
// stop after the claim and before the apply commits must leave NO claim
// behind, so the redelivered message (same consumer group, uncommitted
// offset) is applied — exactly once. Before the fix the claim committed on
// its own, the redelivery saw isNew=false and the projection update was
// lost forever.
func TestAnalyticsConsumer_CrashBetweenClaimAndApply_RedeliveryAppliesExactlyOnce(t *testing.T) {
	brokerList := startBroker(t)
	pool := startAtomicClaimPostgres(t)
	ctx := context.Background()

	topic := uniqueTopic(t)
	createTopic(t, brokerList, topic)
	createTopic(t, brokerList, topic+".dlq")
	group := fmt.Sprintf("itest-atomic-group-%d", time.Now().UnixNano())

	eventID := fmt.Sprintf("evt-atomic-%d", time.Now().UnixNano())
	at := time.Date(2026, 10, 7, 8, 0, 0, 0, time.UTC)
	publish(t, brokerList, topic, envelopeMsg(t, "PICK", eventID, "ProcessPathCreated", at))

	// --- first delivery: the process stops between claim and apply ---
	runCtx, stop := context.WithCancel(ctx)
	crashing := &crashOnFirstApplyProjection{
		ProjectionStore: analyticsstore.NewPostgresProjection(pool),
		crash:           stop,
		crashed:         make(chan struct{}),
	}
	first := newAtomicClaimConsumer(brokerList, topic, group, crashing, pool)
	runErr := make(chan error, 1)
	go func() { runErr <- first.Run(runCtx) }()
	select {
	case <-crashing.crashed:
	case <-time.After(projectionDeadline):
		t.Fatal("the first consumer never reached the projection apply")
	}
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run after a mid-apply stop = %v, want nil (clean shutdown)", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the first consumer did not stop")
	}
	_ = first.Close()

	if n := countEventRows(t, pool, "analytics_consumed_events", eventID); n != 0 {
		t.Errorf("after a stop between claim and apply, analytics_consumed_events holds %d row(s) for %s, want 0: the claim must roll back with the unfinished apply", n, eventID)
	}

	// --- redelivery: a fresh process in the SAME group re-reads the
	// uncommitted offset and must apply the event exactly once ---
	second := newAtomicClaimConsumer(brokerList, topic, group, analyticsstore.NewPostgresProjection(pool), pool)
	secondCtx, stopSecond := context.WithCancel(ctx)
	secondErr := make(chan error, 1)
	go func() { secondErr <- second.Run(secondCtx) }()
	defer func() {
		stopSecond()
		select {
		case <-secondErr:
		case <-time.After(15 * time.Second):
			t.Error("the second consumer did not stop")
		}
		_ = second.Close()
	}()

	deadline := time.Now().Add(projectionDeadline)
	for pathsDefinedOn(t, pool, at) < 1 {
		if time.Now().After(deadline) {
			t.Fatalf("redelivered %s was never applied: catalogue_growth_rollup.paths_defined for %s stayed 0 (lost projection update)", eventID, at.Format("2006-01-02"))
		}
		time.Sleep(200 * time.Millisecond)
	}
	// Give a stray double-apply a moment to show up.
	time.Sleep(2 * time.Second)

	if got := pathsDefinedOn(t, pool, at); got != 1 {
		t.Errorf("paths_defined = %d, want exactly 1", got)
	}
	if n := countEventRows(t, pool, "analytics_consumed_events", eventID); n != 1 {
		t.Errorf("analytics_consumed_events rows for %s = %d, want 1", eventID, n)
	}
	if n := countEventRows(t, pool, "analytics_processed_events", eventID); n != 1 {
		t.Errorf("analytics_processed_events rows for %s = %d, want 1", eventID, n)
	}
}

func newAtomicClaimConsumer(brokerList []string, topic, group string, projection report.ProjectionStore, pool *pgxpool.Pool) *inboundkafka.AnalyticsConsumer {
	return inboundkafka.NewAnalyticsConsumerInGroup(brokerList, topic, group,
		projection, analyticsstore.NewConsumedEventsRepo(pool),
		analyticsstore.NewUnitOfWork(pool), nil)
}

func startAtomicClaimPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	pgC, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("pathmgmt"),
		tcpostgres.WithUsername("pathmgmt"),
		tcpostgres.WithPassword("pathmgmt"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start analytics postgres container: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(pgC) })
	url, err := pgC.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	_, thisFile, _, _ := runtime.Caller(0)
	migrations := filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..", "migrations", "analytics")
	if err := postgres.RunMigrations(url, migrations); err != nil {
		t.Fatalf("migrate analytics: %v", err)
	}
	pool, err := analyticsstore.NewPool(ctx, url)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func countEventRows(t *testing.T, pool *pgxpool.Pool, table, eventID string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM "+table+" WHERE event_id = $1", eventID).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

func pathsDefinedOn(t *testing.T, pool *pgxpool.Pool, day time.Time) int {
	t.Helper()
	var n int
	err := pool.QueryRow(context.Background(),
		`SELECT paths_defined FROM catalogue_growth_rollup WHERE day_bucket = $1`,
		day.UTC().Truncate(24*time.Hour)).Scan(&n)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0
	}
	if err != nil {
		t.Fatalf("read catalogue_growth_rollup: %v", err)
	}
	return n
}
