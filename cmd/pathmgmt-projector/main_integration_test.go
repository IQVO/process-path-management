//go:build integration

// End-to-end proof of the REAL entry point of cmd/pathmgmt-projector on
// testcontainers (the container recipe of the repo's own
// cmd/pathmgmt/e2e_integration_test.go): an EMPTY analytical Postgres (the
// binary migrates it itself), a real Kafka (the binary consumes the
// analytics topic), the documented readiness probe answers, the projector
// folds the repo's own encoder events into the catalogue-growth rollup (a
// replay of the same CloudEvents id dedupes), and SIGTERM drains cleanly.
//
// Doc sources: docs/docs/adr/0007-analytical-data-product.md (the projector
// is the single writer of the analytical database; ANALYTICS_DATABASE_URL,
// KAFKA_BROKERS, ANALYTICS_MIGRATIONS_PATH default migrations/analytics,
// ADMIN_ADDR :8091), docs/docs/adr/0012-kafka-dlq-and-graceful-shutdown.md
// (/readyz flips to not-ready first; the consumer's Run goroutine is
// awaited on shutdown), docs/docs/adr/0037-postgres-and-kafka-are-the-only-
// runtime.md (the mandatory environment), migrations/analytics/
// 0001_report.up.sql (the rollup asserted below).
//
// The consumer group is the CONSTANT AnalyticsConsumerGroup
// (internal/adapters/inbound/kafka): this repo pins it (ADR 0007 — the
// projector consumes only its OWN analytics topic), so the unique-per-run
// group trick is deliberately NOT used; a fresh throwaway testcontainers
// broker guarantees no group collision instead.
package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	outboundkafka "github.com/claudioed/process-path-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// itMigrationsPathValue resolves the repo's ANALYTICAL migrations directory
// (migrations/analytics) relative to this file: the binary's default
// ANALYTICS_MIGRATIONS_PATH is CWD-relative, which a go test run cannot
// rely on.
var itMigrationsPathValue = func() string {
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "migrations", "analytics")
}()

// One shared Kafka container per package; Postgres is per-test so every
// test starts from a database state it owns.
var (
	itBrokers   []string
	itKafkaOnce sync.Once
	itKafkaErr  error
	itKafkaCtr  *tckafka.KafkaContainer
)

// TestMain holds the package-level SIGTERM registration for the whole test
// run: the real run() installs (and on return RESTORES) its own signal
// handler, so a SIGTERM arriving outside a live run() would otherwise kill
// the test binary itself. Registering a sink here makes signal-driven
// shutdown of run() always safe. It then terminates the shared Kafka
// container after m.Run().
func TestMain(m *testing.M) {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	code := m.Run()
	if itKafkaCtr != nil {
		if err := testcontainers.TerminateContainer(itKafkaCtr); err != nil {
			fmt.Fprintf(os.Stderr, "terminate kafka container: %v\n", err)
		}
	}
	os.Exit(code)
}

// itKafka starts the shared broker once and returns its address.
func itKafka(t *testing.T) []string {
	t.Helper()
	itKafkaOnce.Do(func() {
		ctx := context.Background()
		container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
			tckafka.WithClusterID(fmt.Sprintf("pathmgmt-projector-itest-%d", time.Now().UnixNano())),
		)
		if err != nil {
			itKafkaErr = err
			return
		}
		itKafkaCtr = container
		itBrokers, err = container.Brokers(ctx)
		if err != nil {
			itKafkaErr = err
		}
	})
	if itKafkaErr != nil {
		t.Fatalf("start kafka container: %v", itKafkaErr)
	}
	return itBrokers
}

// itPostgres starts a throwaway EMPTY analytical Postgres and returns its
// DSN: the projector's boot step owns the migrations.
func itPostgres(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("pathmgmt_analytics_itest"),
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

// itEnsureTopic creates topic if the broker has auto-create disabled.
func itEnsureTopic(t *testing.T, brokers []string, topic string) {
	t.Helper()
	conn, err := kafkago.Dial("tcp", brokers[0])
	if err != nil {
		t.Fatalf("dial broker: %v", err)
	}
	defer func() { _ = conn.Close() }()
	controller, err := conn.Controller()
	if err != nil {
		t.Fatalf("resolve controller: %v", err)
	}
	ctrlConn, err := kafkago.Dial("tcp", net.JoinHostPort(controller.Host, fmt.Sprint(controller.Port)))
	if err != nil {
		t.Fatalf("dial controller: %v", err)
	}
	defer func() { _ = ctrlConn.Close() }()
	if err := ctrlConn.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}); err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			t.Fatalf("create topic %s: %v", topic, err)
		}
	}
}

// itProduce writes already-encoded messages onto the broker.
func itProduce(t *testing.T, brokers []string, msgs []outboundkafka.Encoded) {
	t.Helper()
	w := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Balancer: &kafkago.Hash{}}
	defer func() { _ = w.Close() }()
	kmsgs := make([]kafkago.Message, 0, len(msgs))
	for _, m := range msgs {
		kmsgs = append(kmsgs, kafkago.Message{Topic: m.Topic, Key: []byte(m.Key), Value: m.Value})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := w.WriteMessages(ctx, kmsgs...); err != nil {
		t.Fatalf("produce %d messages: %v", len(kmsgs), err)
	}
}

// service is one booted instance of the real run().
type service struct {
	addr     string
	done     <-chan error
	stopOnce sync.Once
	stopErr  error
}

// itBootProjector sets the environment a deployment sets, starts the REAL
// run() in-process and waits for the documented readiness probe /readyz
// (ADR 0012).
func itBootProjector(t *testing.T, env map[string]string) *service {
	t.Helper()
	for k, v := range env {
		t.Setenv(k, v)
	}
	addr := freeAddr(t)
	t.Setenv("ADMIN_ADDR", addr)
	done := make(chan error, 1)
	go func() { done <- run() }()
	s := &service{addr: addr, done: done}
	t.Cleanup(func() { itAssertCleanDrain(t, s.stop()) })
	waitUntil(t, "/readyz 200", func() bool {
		resp, err := http.Get("http://" + addr + "/readyz")
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})
	return s
}

// stop drives the binary's real shutdown path. run() takes no context from
// its caller — it blocks on signal.Notify(SIGINT, SIGTERM) itself, which is
// exactly the kubelet's path; the SIGTERM is safe because TestMain keeps a
// package-level signal sink registered.
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
				s.stopErr = fmt.Errorf("the projector did not stop")
			}
		}
	})
	return s.stopErr
}

// itAssertCleanDrain checks run()'s shutdown outcome: a SIGTERM-driven stop
// runs the graceful shutdown sequence of ADR 0012 and run() returns nil.
func itAssertCleanDrain(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("the projector must drain cleanly on SIGTERM, got %v", err)
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

// TestProjectorBootsMigratesProjectsAndStopsCleanly proves the shipped
// binary's contract on real infrastructure: env only -> the analytical
// database is migrated by the binary itself -> /readyz answers -> the
// catalogue-change events (encoded by the repo's own AnalyticsEncoder) are
// folded into the day rollup -> a replay of the same CloudEvents id does
// not double count -> SIGTERM drains cleanly.
func TestProjectorBootsMigratesProjectsAndStopsCleanly(t *testing.T) {
	dsn := itPostgres(t) // empty: the projector owns the analytical migrations
	brokers := itKafka(t)
	itEnsureTopic(t, brokers, outboundkafka.AnalyticsTopic)

	s := itBootProjector(t, map[string]string{
		"ANALYTICS_DATABASE_URL":    dsn,
		"ANALYTICS_MIGRATIONS_PATH": itMigrationsPathValue,
		"KAFKA_BROKERS":             strings.Join(brokers, ","),
		"LOG_LEVEL":                 "error",
	})

	// The repo's real encoder builds the wire form of the three documented
	// catalogue-change events (ADR 0007 / internal/domain/shared/events.go);
	// the CloudEvents id is passed explicitly so a replay is a true
	// redelivery.
	at := time.Now().UTC().Truncate(24 * time.Hour).Add(-24 * time.Hour)
	encoder := outboundkafka.NewAnalyticsEncoder(func() string { return "" })
	created1, err := encoder.Encode(shared.ProcessPathCreated{
		PathId: "pth-itest-1", MatchPrefix: "it1/", Direct: true, At: at.Add(8 * time.Hour),
	}, "proj-e1")
	if err != nil {
		t.Fatal(err)
	}
	created2, err := encoder.Encode(shared.ProcessPathCreated{
		PathId: "pth-itest-2", MatchPrefix: "it2/", Direct: false, At: at.Add(9 * time.Hour),
	}, "proj-e2")
	if err != nil {
		t.Fatal(err)
	}
	updated, err := encoder.Encode(shared.ProcessPathUpdated{
		PathId: "pth-itest-1", MatchPrefix: "it1b/", Direct: true, At: at.Add(10 * time.Hour),
	}, "proj-e3")
	if err != nil {
		t.Fatal(err)
	}
	deactivated, err := encoder.Encode(shared.ProcessPathDeactivated{
		PathId: "pth-itest-2", At: at.Add(11 * time.Hour),
	}, "proj-e4")
	if err != nil {
		t.Fatal(err)
	}
	itProduce(t, brokers, []outboundkafka.Encoded{created1, created2, updated, deactivated})
	itProduce(t, brokers, []outboundkafka.Encoded{created1}) // replay of proj-e1

	// The rollup lands for the UTC day of the events, and the replayed
	// ProcessPathCreated did not double count (the id claim and the rollup
	// upsert are one transaction).
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open analytics pool: %v", err)
	}
	t.Cleanup(pool.Close)
	waitUntil(t, "the catalogue-growth rollup projected", func() bool {
		var defined, revised, deactivatedN, processed int
		err := pool.QueryRow(context.Background(), `SELECT
			(SELECT paths_defined FROM catalogue_growth_rollup WHERE day_bucket = $1),
			(SELECT paths_revised FROM catalogue_growth_rollup WHERE day_bucket = $1),
			(SELECT paths_deactivated FROM catalogue_growth_rollup WHERE day_bucket = $1),
			(SELECT count(*) FROM analytics_processed_events)`, at).Scan(&defined, &revised, &deactivatedN, &processed)
		return err == nil && defined == 2 && revised == 1 && deactivatedN == 1 && processed == 4
	})

	// The binary migrated the empty analytical database itself.
	var applied int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM schema_migrations").Scan(&applied); err != nil || applied == 0 {
		t.Fatalf("schema_migrations after boot: %d rows (err %v): the projector must migrate the empty database itself", applied, err)
	}

	itAssertCleanDrain(t, s.stop())
}
