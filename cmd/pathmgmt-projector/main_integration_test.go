//go:build integration

package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/claudioed/process-path-management/internal/adapters/outbound/analyticsstore"
	outboundkafka "github.com/claudioed/process-path-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// The WRITER binary's boot-level proof (ADR 0007, ADR 0029): the real run()
// boots on nothing but the environment a deployment sets — an EMPTY
// analytical Postgres the binary migrates itself (migrations/analytics) and
// a real Kafka broker — replays this service's OWN analytics topic
// (warehouse.process-path-management.analytics) from the earliest offset,
// projects the events the repo's own AnalyticsPublisher encodes into the
// documented day-bucketed catalogue-growth rollup exactly once per
// CloudEvents id, and a SIGTERM drains it cleanly (ADR 0012's shutdown
// order: readiness flips first, consumer awaited, pool closed last).

// projectorContainerStart bounds a testcontainers start.
const projectorContainerStart = 120 * time.Second

// projectorProjectionDeadline bounds how long the test waits for the
// consumer group to join and the projection to answer. The work itself is
// sub-second; the budget is dominated by the consumer-group join (kafka-go
// retries a not-yet-available group coordinator every 5s).
const projectorProjectionDeadline = 90 * time.Second

// startProjectorPostgres boots an EMPTY analytical Postgres: the projector
// owns the schema and runs migrations/analytics itself at boot.
func startProjectorPostgres(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), projectorContainerStart)
	defer cancel()
	c, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("pathmgmt_projector_it"),
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
	return url
}

// startProjectorKafka boots a real Kafka broker, the projector's only input.
func startProjectorKafka(t *testing.T) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), projectorContainerStart)
	defer cancel()
	c, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID(fmt.Sprintf("pathmgmt-projector-it-%d", time.Now().UnixNano())))
	if err != nil {
		t.Fatalf("start kafka: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(c) })
	brokers, err := c.Brokers(ctx)
	if err != nil {
		t.Fatalf("kafka brokers: %v", err)
	}
	return brokers
}

// createProjectorTopic creates the one-partition input topic and waits for
// its leader, the way the cluster's broker would have it (the projector
// never creates topics itself).
func createProjectorTopic(t *testing.T, brokers []string, topic string) {
	t.Helper()
	conn, err := kafkago.Dial("tcp", brokers[0])
	if err != nil {
		t.Fatalf("dial broker: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}); err != nil {
		t.Fatalf("create topic %s: %v", topic, err)
	}
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		if parts, err := conn.ReadPartitions(topic); err == nil && len(parts) == 1 && parts[0].Leader.ID != 0 {
			return
		}
	}
	t.Fatalf("topic %s never got a leader", topic)
}

// projectorWaitUntil polls cond until it holds or the deadline passes.
func projectorWaitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(projectorProjectionDeadline); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

// freeProjectorAddr reserves a loopback port for the admin listener.
func freeProjectorAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

// sigtermDrain stops the signal-driven run() the only way it can be stopped
// inside a test process — a SIGTERM to self — retrying until run() returns,
// and requires a clean nil. The test's own handler is registered BEFORE
// run() boots and is deliberately NEVER unregistered (no signal.Stop): by
// the time run() returns, its own signal.NotifyContext stop has already
// run, so unregistering the last handler would restore Go's default
// SIGTERM disposition while one of the retry signals can still be in
// flight, and the test binary itself would die with "signal: terminated".
func sigtermDrain(t *testing.T, done <-chan error) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("a SIGTERMed projector must drain cleanly, got %v", err)
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

// projectorRollup reads one day bucket's counters from the analytical
// database the projector migrated and wrote.
func projectorRollup(t *testing.T, pool *pgxpool.Pool, bucket time.Time) (int, int, int, bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var defined, revised, deactivated int
	err := pool.QueryRow(ctx,
		`SELECT paths_defined, paths_revised, paths_deactivated FROM catalogue_growth_rollup WHERE day_bucket = $1`,
		bucket).Scan(&defined, &revised, &deactivated)
	return defined, revised, deactivated, err == nil
}

func TestProjectorProjectsOwnAnalyticsEventsAndDrainsCleanly(t *testing.T) {
	dsn := startProjectorPostgres(t)
	brokers := startProjectorKafka(t)
	// The input topic exists before the projector boots, as in the cluster.
	createProjectorTopic(t, brokers, outboundkafka.AnalyticsTopic)

	// The events the projector will consume: real domain events encoded by
	// the repo's OWN AnalyticsPublisher onto its OWN analytics topic. Two
	// definitions and one revision on day 1, one deactivation on day 2, and
	// a redelivery of the first creation under the SAME CloudEvents id —
	// the documented at-least-once delivery the exactly-once projection
	// must absorb as a no-op.
	day1 := time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)
	day2 := time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC)
	created1 := shared.ProcessPathCreated{
		PathId: "PICK-IT-1", MatchPrefix: "pick-it-a", Direct: true,
		RequiredCapabilities: []shared.Capability{"pick"}, CycleTimeP95: 2 * time.Hour,
		At: day1.Add(10 * time.Hour),
	}
	created2 := shared.ProcessPathCreated{
		PathId: "PACK-IT-1", MatchPrefix: "pack-it-a", Direct: false,
		RequiredCapabilities: []shared.Capability{"pack"}, CycleTimeP95: time.Hour,
		At: day1.Add(11 * time.Hour),
	}
	revised1 := shared.ProcessPathUpdated{
		PathId: "PICK-IT-1", MatchPrefix: "pick-it-b", Direct: true,
		RequiredCapabilities: []shared.Capability{"pick", "hazmat"}, CycleTimeP95: 3 * time.Hour,
		At: day1.Add(12 * time.Hour),
	}
	deactivated2 := shared.ProcessPathDeactivated{PathId: "PACK-IT-1", At: day2.Add(9 * time.Hour)}

	publisher := outboundkafka.NewAnalyticsDirectPublisher(brokers, func() string { return "" })
	t.Cleanup(func() { _ = publisher.Close() })
	publish := func(id string, event shared.DomainEvent) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if err := publisher.PublishWithId(ctx, event, id); err != nil {
			t.Fatalf("publish %s: %v", id, err)
		}
	}
	publish("projector-it-e1", created1)
	publish("projector-it-e2", created2)
	publish("projector-it-e3", revised1)
	publish("projector-it-e4", deactivated2)
	publish("projector-it-e1", created1) // Kafka's at-least-once redelivery

	// Nothing but the environment a deployment sets (ADR 0037): the
	// migrations path points at the repo's own analytics migrations because
	// the test runs from this package's directory.
	addr := freeProjectorAddr(t)
	t.Setenv("ANALYTICS_DATABASE_URL", dsn)
	t.Setenv("KAFKA_BROKERS", strings.Join(brokers, ","))
	t.Setenv("ADMIN_ADDR", addr)
	t.Setenv("ANALYTICS_MIGRATIONS_PATH", "../../migrations/analytics")
	t.Setenv("LOG_LEVEL", "warn")

	// The test's own SIGTERM handler, registered BEFORE run() boots and
	// never unregistered (see sigtermDrain).
	testSignals := make(chan os.Signal, 8)
	signal.Notify(testSignals, syscall.SIGINT, syscall.SIGTERM)
	done := make(chan error, 1)
	go func() { done <- run() }()

	projectorWaitUntil(t, "the projector's /readyz to answer 200", func() bool {
		select {
		case err := <-done:
			t.Fatalf("run() returned while booting: %v", err)
			return false
		default:
		}
		resp, err := http.Get("http://" + addr + "/readyz")
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})

	// The documented projection (ADR 0007): one row per UTC day bucket,
	// counters per event kind, exactly once per CloudEvents id despite the
	// redelivery.
	pool, err := analyticsstore.NewPool(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open analytics pool: %v", err)
	}
	t.Cleanup(pool.Close)
	projectorWaitUntil(t, "day 1 to be projected as 2 defined + 1 revised", func() bool {
		d, r, da, ok := projectorRollup(t, pool, day1)
		return ok && d == 2 && r == 1 && da == 0
	})
	projectorWaitUntil(t, "day 2 to be projected as 1 deactivated", func() bool {
		d, r, da, ok := projectorRollup(t, pool, day2)
		return ok && d == 0 && r == 0 && da == 1
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var processed, consumed int
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM analytics_processed_events),
		(SELECT count(*) FROM analytics_consumed_events)`).Scan(&processed, &consumed); err != nil {
		t.Fatalf("count idempotency rows: %v", err)
	}
	if processed != 4 || consumed != 4 {
		t.Fatalf("processed=%d consumed=%d, want 4 and 4: the redelivered CloudEvents id must be a no-op", processed, consumed)
	}

	sigtermDrain(t, done)
}
