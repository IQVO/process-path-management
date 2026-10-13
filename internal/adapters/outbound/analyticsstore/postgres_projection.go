package analyticsstore

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/process-path-management/internal/analytics/report"
)

// PostgresProjection is the WRITER implementation of report.ProjectionStore,
// backed by a pgxpool over the analytical database. Every Apply* runs in a
// transaction that first claims the event id in analytics_processed_events
// (ON CONFLICT DO NOTHING); it only mutates the rollup when the claim is
// new, making each apply idempotent per eventId under Kafka's
// at-least-once delivery. It is the only writer of the analytical
// database.
type PostgresProjection struct {
	pool *pgxpool.Pool
}

// NewPostgresProjection constructs a PostgresProjection over pool.
func NewPostgresProjection(pool *pgxpool.Pool) *PostgresProjection {
	return &PostgresProjection{pool: pool}
}

// claim inserts eventId into analytics_processed_events, returning true iff
// this call newly recorded it (so the caller should apply the effect). It
// runs inside tx so the claim and the effect commit atomically.
func claim(ctx context.Context, tx pgx.Tx, eventId string, occurredAt time.Time) (bool, error) {
	tag, err := tx.Exec(ctx,
		`INSERT INTO analytics_processed_events (event_id, occurred_at)
		 VALUES ($1, $2) ON CONFLICT (event_id) DO NOTHING`,
		eventId, occurredAt)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

// inTx runs fn in a transaction, committing on success and rolling back on
// error. When ctx already carries the analytics UnitOfWork's transaction
// (ADR 0032) fn joins it instead, and the UnitOfWork owns commit/rollback,
// so the consumer's dedupe claim and this write commit together.
func (p *PostgresProjection) inTx(ctx context.Context, fn func(pgx.Tx) error) error {
	if tx, ok := txFrom(ctx); ok {
		return fn(tx)
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}

// rollupDelta is the set of counter increments a single event contributes
// to a catalogue-growth row.
type rollupDelta struct {
	pathsDefined     int
	pathsRevised     int
	pathsDeactivated int
}

// apply claims eventId and, when the claim is new, upserts delta into the
// day_bucket row. It is the shared body of every Apply* method.
func (p *PostgresProjection) apply(ctx context.Context, eventId string, at time.Time, delta rollupDelta) error {
	return p.inTx(ctx, func(tx pgx.Tx) error {
		isNew, err := claim(ctx, tx, eventId, at)
		if err != nil {
			return fmt.Errorf("analyticsstore: claim event: %w", err)
		}
		if !isNew {
			return nil
		}
		return upsertRollup(ctx, tx, at, delta)
	})
}

// ApplyProcessPathCreated records a path definition. Idempotent on eventId.
func (p *PostgresProjection) ApplyProcessPathCreated(ctx context.Context, eventId string, at time.Time) error {
	return p.apply(ctx, eventId, at, rollupDelta{pathsDefined: 1})
}

// ApplyProcessPathUpdated records a path revision. Idempotent on eventId.
func (p *PostgresProjection) ApplyProcessPathUpdated(ctx context.Context, eventId string, at time.Time) error {
	return p.apply(ctx, eventId, at, rollupDelta{pathsRevised: 1})
}

// ApplyProcessPathDeactivated records a path deactivation. Idempotent on
// eventId.
func (p *PostgresProjection) ApplyProcessPathDeactivated(ctx context.Context, eventId string, at time.Time) error {
	return p.apply(ctx, eventId, at, rollupDelta{pathsDeactivated: 1})
}

// upsertRollup adds delta into the day_bucket row, inserting it if absent.
// day_bucket is derived by truncating at to the UTC day.
func upsertRollup(ctx context.Context, tx pgx.Tx, at time.Time, delta rollupDelta) error {
	bucket := at.UTC().Truncate(24 * time.Hour)
	_, err := tx.Exec(ctx,
		`INSERT INTO catalogue_growth_rollup (
			day_bucket, paths_defined, paths_revised, paths_deactivated)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (day_bucket) DO UPDATE SET
			paths_defined     = catalogue_growth_rollup.paths_defined + EXCLUDED.paths_defined,
			paths_revised     = catalogue_growth_rollup.paths_revised + EXCLUDED.paths_revised,
			paths_deactivated = catalogue_growth_rollup.paths_deactivated + EXCLUDED.paths_deactivated`,
		bucket, delta.pathsDefined, delta.pathsRevised, delta.pathsDeactivated)
	if err != nil {
		return fmt.Errorf("analyticsstore: upsert rollup: %w", err)
	}
	return nil
}

// Compile-time assertion that PostgresProjection satisfies the write port.
var _ report.ProjectionStore = (*PostgresProjection)(nil)
