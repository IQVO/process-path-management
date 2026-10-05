//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/claudioed/process-path-management/internal/adapters/outbound/postgres"
)

// Sweeper tests (ADR 0018) against a real testcontainers Postgres: expired
// idempotency_keys and PUBLISHED outbox_events rows are deleted; rows that
// are not yet eligible, and UNPUBLISHED outbox rows, are never touched;
// a TTL of 0 disables that half entirely.

func TestSweeper_DeletesExpiredRowsAndKeepsTheRest(t *testing.T) {
	db := outboxDB(t)
	ctx := context.Background()

	// Seed: two idempotency keys (30h old = past the 24h TTL, and 1h old
	// = fresh), one PUBLISHED outbox row 8d old (past the 7d retention),
	// and one UNPUBLISHED outbox row equally old — which must survive:
	// unpublished rows are never swept, however old (they are pending
	// events, not forensics).
	_, err := db.Exec(ctx, `INSERT INTO idempotency_keys (key, method, path, request_hash, created_at)
		VALUES ('stale-key', 'POST', '/process-paths', 'x', now() - interval '30 hours')`)
	if err != nil {
		t.Fatalf("seed stale key: %v", err)
	}
	_, err = db.Exec(ctx, `INSERT INTO idempotency_keys (key, method, path, request_hash, created_at)
		VALUES ('fresh-key', 'POST', '/process-paths', 'x', now() - interval '1 hour')`)
	if err != nil {
		t.Fatalf("seed fresh key: %v", err)
	}
	_, err = db.Exec(ctx, `INSERT INTO outbox_events (event_id, aggregate_id, event_type, payload, occurred_at, created_at, published_at, topic)
		VALUES (gen_random_uuid(), 'OLD', 'ProcessPathCreated', '{}'::jsonb, now() - interval '8 days', now() - interval '8 days', now() - interval '8 days', 'warehouse.process-path-management.events')`)
	if err != nil {
		t.Fatalf("seed published outbox row: %v", err)
	}
	_, err = db.Exec(ctx, `INSERT INTO outbox_events (event_id, aggregate_id, event_type, payload, occurred_at, created_at, topic)
		VALUES (gen_random_uuid(), 'PEND', 'ProcessPathCreated', '{}'::jsonb, now() - interval '8 days', now() - interval '8 days', 'warehouse.process-path-management.events')`)
	if err != nil {
		t.Fatalf("seed unpublished outbox row: %v", err)
	}

	res, err := postgres.NewSweeper(db).SweepOnce(ctx)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if res.IdempotencyKeys != 1 {
		t.Fatalf("expected 1 idempotency key deleted, got %d", res.IdempotencyKeys)
	}
	if res.OutboxEvents != 1 {
		t.Fatalf("expected 1 published outbox row deleted, got %d", res.OutboxEvents)
	}

	count := func(table, where string) int {
		var n int
		if err := db.QueryRow(ctx, "SELECT count(*) FROM "+table+" WHERE "+where).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		return n
	}
	if n := count("idempotency_keys", "key = 'fresh-key'"); n != 1 {
		t.Fatalf("fresh idempotency key must survive, got %d rows", n)
	}
	if n := count("outbox_events", "aggregate_id = 'PEND'"); n != 1 {
		t.Fatalf("unpublished outbox row must never be swept, got %d rows", n)
	}
	if n := count("outbox_events", "aggregate_id = 'OLD'"); n != 0 {
		t.Fatalf("expired published outbox row must be gone, got %d rows", n)
	}
}

func TestSweeper_DisabledHalvesDeleteNothing(t *testing.T) {
	db := outboxDB(t)
	ctx := context.Background()

	_, err := db.Exec(ctx, `INSERT INTO idempotency_keys (key, method, path, request_hash, created_at)
		VALUES ('stale-key-2', 'POST', '/process-paths', 'x', now() - interval '30 hours')`)
	if err != nil {
		t.Fatalf("seed stale key: %v", err)
	}
	_, err = db.Exec(ctx, `INSERT INTO outbox_events (event_id, aggregate_id, event_type, payload, occurred_at, created_at, published_at, topic)
		VALUES (gen_random_uuid(), 'OLD2', 'ProcessPathCreated', '{}'::jsonb, now() - interval '8 days', now() - interval '8 days', now() - interval '8 days', 'warehouse.process-path-management.events')`)
	if err != nil {
		t.Fatalf("seed published outbox row: %v", err)
	}

	// Both halves disabled (<=0): nothing is deleted — the pre-ADR-0018
	// behaviour, available as an explicit escape hatch.
	res, err := postgres.NewSweeper(db,
		postgres.WithIdempotencyKeyTTL(0),
		postgres.WithOutboxRetention(0),
	).SweepOnce(ctx)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if res.IdempotencyKeys != 0 || res.OutboxEvents != 0 {
		t.Fatalf("expected zero deletions with both halves disabled, got %+v", res)
	}

	var n int
	if err := db.QueryRow(ctx, "SELECT count(*) FROM idempotency_keys").Scan(&n); err != nil {
		t.Fatalf("count keys: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected the stale key to be kept when disabled, got %d rows", n)
	}
}

// TestOutboxLagGauge_QuerySemantics pins the gauge's SQL semantics against
// a real database: 0 when fully drained, the age of the oldest unpublished
// row when one exists.
func TestOutboxLagGauge_QuerySemantics(t *testing.T) {
	db := outboxDB(t)
	ctx := context.Background()

	// Drained: no unpublished rows at all.
	lag, err := postgres.OldestUnpublishedLagSecondsForTest(ctx, db)
	if err != nil {
		t.Fatalf("lag on drained outbox: %v", err)
	}
	if lag != 0 {
		t.Fatalf("drained outbox lag = %v, want 0", lag)
	}

	_, err = db.Exec(ctx, `INSERT INTO outbox_events (event_id, aggregate_id, event_type, payload, occurred_at, created_at, topic)
		VALUES (gen_random_uuid(), 'LAG', 'ProcessPathCreated', '{}'::jsonb, now() - interval '90 seconds', now() - interval '90 seconds', 'warehouse.process-path-management.events')`)
	if err != nil {
		t.Fatalf("seed lagging row: %v", err)
	}
	lag, err = postgres.OldestUnpublishedLagSecondsForTest(ctx, db)
	if err != nil {
		t.Fatalf("lag with pending row: %v", err)
	}
	if lag < 89 || lag > 120 {
		t.Fatalf("expected ~90s lag, got %v", lag)
	}
}
