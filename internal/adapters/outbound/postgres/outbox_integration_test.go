//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/claudioed/process-path-management/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/process-path-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/process-path-management/internal/application/usecases"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// outboxDB boots a throwaway Postgres (testcontainers — the test owns its
// own database, never an external DATABASE_URL) and runs migrations.
func outboxDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("pathmgmt"),
		tcpostgres.WithUsername("pathmgmt"),
		tcpostgres.WithPassword("pathmgmt"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })

	url, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	if err := postgres.RunMigrations(url, migrationsDir(t)); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	pool, err := postgres.NewPool(ctx, url)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

type recordingSink struct {
	sent    []outboundkafka.Encoded
	failOn  string // EventId to fail on, "" for never
	failErr error
}

func (s *recordingSink) Send(_ context.Context, enc outboundkafka.Encoded) error {
	if s.failOn != "" && enc.EventId == s.failOn {
		return s.failErr
	}
	s.sent = append(s.sent, enc)
	return nil
}

func countOutbox(t *testing.T, pool *pgxpool.Pool, where string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM outbox_events WHERE "+where).Scan(&n); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	return n
}

type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time { return c.t }

func TestOutbox_DefinePath_CommitsAggregateAndEventTogether(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	uc := &usecases.DefinePath{
		Repo:       postgres.NewProcessPathRepo(pool),
		Publisher:  postgres.NewOutboxPublisher(pool, uuid.NewString),
		Clock:      fixedClock{t: time.Now().UTC().Truncate(time.Microsecond)},
		UnitOfWork: postgres.NewUnitOfWork(pool),
	}

	if _, err := uc.Execute(ctx, "PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, 2*time.Hour, shared.Eligibility{}); err != nil {
		t.Fatalf("define: %v", err)
	}
	if got := countOutbox(t, pool, "published_at IS NULL AND event_type = '"+cloudevents.TypeProcessPathCreated+"' AND aggregate_id = 'PICK'"); got != 1 {
		t.Fatalf("expected 1 unpublished ProcessPathCreated row for PICK, got %d", got)
	}
	found, err := postgres.NewProcessPathRepo(pool).FindByID(ctx, "PICK")
	if err != nil || found == nil {
		t.Fatalf("expected PICK persisted, got %v err=%v", found, err)
	}
}

// The whole point of the outbox: if the event cannot be enqueued the
// aggregate change must not survive either. We provoke the publish
// failure by handing the OutboxPublisher an id generator that returns a
// value violating outbox_events.event_id's UUID type.
func TestOutbox_PublishFailure_RollsBackAggregate(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	uc := &usecases.DefinePath{
		Repo:       postgres.NewProcessPathRepo(pool),
		Publisher:  postgres.NewOutboxPublisher(pool, func() string { return "not-a-uuid" }),
		Clock:      fixedClock{t: time.Now().UTC()},
		UnitOfWork: postgres.NewUnitOfWork(pool),
	}

	if _, err := uc.Execute(ctx, "PACK", "pack", true, []shared.Capability{"pack"}, shared.DestinationLocationRoleUnset, 2*time.Hour, shared.Eligibility{}); err == nil {
		t.Fatal("expected the invalid event_id to fail the publish")
	}
	found, err := postgres.NewProcessPathRepo(pool).FindByID(ctx, "PACK")
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	if found != nil {
		t.Fatal("aggregate row survived a failed publish: the unit of work did not roll back")
	}
	if got := countOutbox(t, pool, "aggregate_id = 'PACK'"); got != 0 {
		t.Fatalf("expected no outbox rows for PACK, got %d", got)
	}
}

func TestOutboxRelay_PublishesInOrderAndMarksRows(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	repo := postgres.NewProcessPathRepo(pool)
	pub := postgres.NewOutboxPublisher(pool, uuid.NewString)
	uow := postgres.NewUnitOfWork(pool)

	define := &usecases.DefinePath{Repo: repo, Publisher: pub, Clock: fixedClock{t: now}, UnitOfWork: uow}
	revise := &usecases.RevisePath{Repo: repo, Publisher: pub, Clock: fixedClock{t: now.Add(time.Second)}, UnitOfWork: uow}
	deactivate := &usecases.DeactivatePath{Repo: repo, Publisher: pub, Clock: fixedClock{t: now.Add(2 * time.Second)}, UnitOfWork: uow}

	if _, err := define.Execute(ctx, "SLAM", "slam", true, []shared.Capability{"slam"}, shared.DestinationLocationRoleUnset, 2*time.Hour, shared.Eligibility{}); err != nil {
		t.Fatalf("define: %v", err)
	}
	if _, err := revise.Execute(ctx, "SLAM", "slam-lane", []shared.Capability{"slam"}, 3*time.Hour, shared.Eligibility{}); err != nil {
		t.Fatalf("revise: %v", err)
	}
	if err := deactivate.Execute(ctx, "SLAM"); err != nil {
		t.Fatalf("deactivate: %v", err)
	}

	sink := &recordingSink{}
	relay := postgres.NewOutboxRelay(pool, sink, slog.Default())
	n, err := relay.RelayOnce(ctx)
	if err != nil {
		t.Fatalf("relay: %v", err)
	}
	if n != 3 || len(sink.sent) != 3 {
		t.Fatalf("expected 3 published, got n=%d sent=%d", n, len(sink.sent))
	}
	want := []string{cloudevents.TypeProcessPathCreated, cloudevents.TypeProcessPathUpdated, cloudevents.TypeProcessPathDeactivated}
	for i, w := range want {
		if sink.sent[i].EventType != w || sink.sent[i].Key != "SLAM" {
			t.Fatalf("event %d: want %s keyed SLAM, got %s keyed %s", i, w, sink.sent[i].EventType, sink.sent[i].Key)
		}
		// The relayed value is the CloudEvent minted at enqueue time: its
		// `id` is exactly the outbox row's persisted event_id (ADR 0016).
		e, err := cloudevents.Decode(sink.sent[i].Value)
		if err != nil {
			t.Fatalf("event %d: relayed value is not a CloudEvent: %v", i, err)
		}
		if e.ID() != sink.sent[i].EventId || e.Type() != w || e.Subject() != "SLAM" {
			t.Fatalf("event %d: id/type/subject = %s/%s/%s, want %s/%s/SLAM", i, e.ID(), e.Type(), e.Subject(), sink.sent[i].EventId, w)
		}
	}
	if got := countOutbox(t, pool, "published_at IS NULL"); got != 0 {
		t.Fatalf("expected every row marked published, %d still pending", got)
	}
	// A second pass finds nothing and republishes nothing.
	n, err = relay.RelayOnce(ctx)
	if err != nil || n != 0 || len(sink.sent) != 3 {
		t.Fatalf("second pass should be a no-op, got n=%d err=%v sent=%d", n, err, len(sink.sent))
	}
}

func TestOutboxRelay_SinkFailure_StopsAtFailedRowAndRetriesLater(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	repo := postgres.NewProcessPathRepo(pool)
	pub := postgres.NewOutboxPublisher(pool, uuid.NewString)
	uow := postgres.NewUnitOfWork(pool)
	define := &usecases.DefinePath{Repo: repo, Publisher: pub, Clock: fixedClock{t: now}, UnitOfWork: uow}
	for _, id := range []shared.PathId{"A1", "B2", "C3"} {
		if _, err := define.Execute(ctx, id, strings.ToLower(string(id)), true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, 2*time.Hour, shared.Eligibility{}); err != nil {
			t.Fatalf("define %s: %v", id, err)
		}
	}
	var secondId string
	if err := pool.QueryRow(ctx, "SELECT event_id::text FROM outbox_events WHERE aggregate_id = 'B2'").Scan(&secondId); err != nil {
		t.Fatalf("lookup B2 event id: %v", err)
	}

	sink := &recordingSink{failOn: secondId, failErr: errors.New("broker down")}
	relay := postgres.NewOutboxRelay(pool, sink, slog.Default())
	n, err := relay.RelayOnce(ctx)
	if err == nil {
		t.Fatal("expected the failing row to surface an error")
	}
	if n != 1 || len(sink.sent) != 1 || sink.sent[0].Key != "A1" {
		t.Fatalf("expected only A1 published before the failure, got n=%d sent=%v", n, sink.sent)
	}
	if got := countOutbox(t, pool, "published_at IS NULL"); got != 2 {
		t.Fatalf("expected B2 and C3 still pending (ordering preserved), got %d pending", got)
	}
	var attempts int
	var lastErr string
	if err := pool.QueryRow(ctx, "SELECT attempts, coalesce(last_error,'') FROM outbox_events WHERE aggregate_id = 'B2'").Scan(&attempts, &lastErr); err != nil {
		t.Fatalf("read B2: %v", err)
	}
	if attempts != 1 || lastErr == "" {
		t.Fatalf("expected B2 to record the failed attempt, got attempts=%d last_error=%q", attempts, lastErr)
	}

	// Broker recovers: the next pass drains the rest, in order.
	sink.failOn = ""
	n, err = relay.RelayOnce(ctx)
	if err != nil || n != 2 {
		t.Fatalf("recovery pass: n=%d err=%v", n, err)
	}
	if sink.sent[1].Key != "B2" || sink.sent[2].Key != "C3" {
		t.Fatalf("expected B2 then C3 after recovery, got %v", sink.sent)
	}
	if got := countOutbox(t, pool, "published_at IS NULL"); got != 0 {
		t.Fatalf("expected outbox drained, %d pending", got)
	}
}
