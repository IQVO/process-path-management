//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	outboundkafka "github.com/claudioed/process-path-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/process-path-management/internal/application/usecases"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// TestOutbox_MultiEncoder_EnqueuesOnBothTopics proves ADR 0007's core
// guarantee: a single domain event, published through an OutboxPublisher
// configured with BOTH the integration encoder and the analytics encoder,
// lands as two rows in the SAME transaction — one per topic — so the
// integration stream and the new analytics stream can never diverge from
// what actually happened to the aggregate.
func TestOutbox_MultiEncoder_EnqueuesOnBothTopics(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	newId := uuid.NewString
	pub := postgres.NewOutboxPublisher(pool, newId,
		outboundkafka.IntegrationEncoder{},
		outboundkafka.NewAnalyticsEncoder(newId),
	)
	uc := &usecases.DefinePath{
		Repo:       postgres.NewProcessPathRepo(pool),
		Publisher:  pub,
		Clock:      fixedClock{t: time.Now().UTC().Truncate(time.Microsecond)},
		UnitOfWork: postgres.NewUnitOfWork(pool),
	}

	if _, err := uc.Execute(ctx, "ZIP", "zip", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, 2*time.Hour, shared.Eligibility{}, shared.Intake{}); err != nil {
		t.Fatalf("define: %v", err)
	}

	if got := countOutbox(t, pool, "aggregate_id = 'ZIP' AND topic = '"+outboundkafka.Topic+"'"); got != 1 {
		t.Fatalf("expected 1 integration-topic row for ZIP, got %d", got)
	}
	if got := countOutbox(t, pool, "aggregate_id = 'ZIP' AND topic = '"+outboundkafka.AnalyticsTopic+"'"); got != 1 {
		t.Fatalf("expected 1 analytics-topic row for ZIP, got %d", got)
	}

	// Both rows share the same event_id: they are the SAME event, encoded
	// twice for two topics, not two different events.
	var integrationEventId, analyticsEventId string
	if err := pool.QueryRow(ctx, "SELECT event_id::text FROM outbox_events WHERE aggregate_id = 'ZIP' AND topic = $1", outboundkafka.Topic).Scan(&integrationEventId); err != nil {
		t.Fatalf("lookup integration event id: %v", err)
	}
	if err := pool.QueryRow(ctx, "SELECT event_id::text FROM outbox_events WHERE aggregate_id = 'ZIP' AND topic = $1", outboundkafka.AnalyticsTopic).Scan(&analyticsEventId); err != nil {
		t.Fatalf("lookup analytics event id: %v", err)
	}
	if integrationEventId != analyticsEventId {
		t.Fatalf("expected the same event_id on both topics, got integration=%s analytics=%s", integrationEventId, analyticsEventId)
	}
}

// TestOutbox_MultiEncoder_RollsBackBothRowsOnFailure proves the whole
// point of doing this inside one transaction: if either encoder's row
// fails to enqueue, neither topic's row — nor the aggregate write —
// survives.
func TestOutbox_MultiEncoder_RollsBackBothRowsOnFailure(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	// An id generator returning a non-UUID value violates
	// outbox_events.event_id's UUID type on the FIRST insert (the
	// integration encoder's row), so the analytics row is never even
	// attempted and the whole transaction rolls back.
	badId := func() string { return "not-a-uuid" }
	pub := postgres.NewOutboxPublisher(pool, badId,
		outboundkafka.IntegrationEncoder{},
		outboundkafka.NewAnalyticsEncoder(badId),
	)
	uc := &usecases.DefinePath{
		Repo:       postgres.NewProcessPathRepo(pool),
		Publisher:  pub,
		Clock:      fixedClock{t: time.Now().UTC()},
		UnitOfWork: postgres.NewUnitOfWork(pool),
	}

	if _, err := uc.Execute(ctx, "BAD1", "bad1", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, 2*time.Hour, shared.Eligibility{}, shared.Intake{}); err == nil {
		t.Fatal("expected the invalid event_id to fail the publish")
	}
	found, err := postgres.NewProcessPathRepo(pool).FindByID(ctx, "BAD1")
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	if found != nil {
		t.Fatal("aggregate row survived a failed multi-topic publish: the unit of work did not roll back")
	}
	if got := countOutbox(t, pool, "aggregate_id = 'BAD1'"); got != 0 {
		t.Fatalf("expected no outbox rows for BAD1 on either topic, got %d", got)
	}
}
