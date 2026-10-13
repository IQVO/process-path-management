package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	outboundkafka "github.com/claudioed/process-path-management/internal/adapters/outbound/kafka"
)

// Sink is where OutboxRelay forwards drained events — in production the
// Kafka Publisher's Send; in tests a recorder.
type Sink interface {
	Send(ctx context.Context, enc outboundkafka.Encoded) error
}

// OutboxRelay drains unpublished outbox_events rows onto a Sink, oldest
// first, marking each row published as it goes (ADR 0003).
//
// Delivery is at-least-once: a crash between a successful Send and the
// row's UPDATE republishes that event on the next pass. Consumers already
// treat the topic as replayable (they rebuild their catalogue cache from
// offset zero on every boot) and key their caches by PathId, so a
// duplicate is an idempotent overwrite for them. Ordering per path is
// preserved because rows are drained in id order within one relay and
// FOR UPDATE SKIP LOCKED keeps two relays (a rolling deploy's overlapping
// old and new pod) from claiming the same row.
//
// A row that keeps failing is capped by WithMaxAttempts (ADR 0030): once it
// has failed that many times it is excluded from future claims, so one
// poison row cannot stop the whole relay forever. Such a row stays in
// outbox_events with its attempts and last_error for inspection; later rows
// with the same key overtake it (the documented price of the cap).
type OutboxRelay struct {
	pool        *pgxpool.Pool
	sink        Sink
	logger      *slog.Logger
	interval    time.Duration
	batchSize   int
	maxAttempts int
}

// RelayOption customises an OutboxRelay.
type RelayOption func(*OutboxRelay)

// WithInterval sets how long the relay sleeps between passes when the
// last pass found nothing to publish. Default 1s.
func WithInterval(d time.Duration) RelayOption {
	return func(r *OutboxRelay) { r.interval = d }
}

// WithBatchSize caps how many rows one pass claims. Default 100.
func WithBatchSize(n int) RelayOption {
	return func(r *OutboxRelay) { r.batchSize = n }
}

// WithMaxAttempts caps how many failed sends a row may accumulate before
// the relay stops claiming it. The default 0 means unlimited retries (the
// pre-cap behaviour). A row that reaches the cap is left unpublished in
// outbox_events with attempts and last_error, logged once at ERROR with its
// id, topic, event type and key, and re-armed by an operator with
// `UPDATE outbox_events SET attempts = 0 WHERE id = ...` (ADR 0030).
func WithMaxAttempts(n int) RelayOption {
	return func(r *OutboxRelay) { r.maxAttempts = n }
}

// NewOutboxRelay constructs a relay draining pool into sink.
func NewOutboxRelay(pool *pgxpool.Pool, sink Sink, logger *slog.Logger, opts ...RelayOption) *OutboxRelay {
	if logger == nil {
		logger = slog.Default()
	}
	r := &OutboxRelay{pool: pool, sink: sink, logger: logger, interval: time.Second, batchSize: 100}
	for _, o := range opts {
		o(r)
	}
	return r
}

// Run drains the outbox until ctx is cancelled. A pass that publishes a
// full batch is followed immediately by another pass (there is probably
// more waiting); an empty pass sleeps for the configured interval. A
// failing pass is logged and retried after the interval — the rows stay
// unpublished, so nothing is lost.
func (r *OutboxRelay) Run(ctx context.Context) error {
	for {
		n, err := r.RelayOnce(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			r.logger.ErrorContext(ctx, "outbox relay pass failed", "error", err)
		}
		if n == r.batchSize && err == nil {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(r.interval):
		}
	}
}

// RelayOnce performs a single pass: claim up to batchSize unpublished rows
// under a row lock, send each in order, and mark it published. It returns
// how many rows were published. On the first Send failure the pass stops
// (preserving per-path ordering — a later event for the same path must
// not overtake a failed earlier one), records the error on that row, and
// returns it; rows already sent in this pass stay marked published. With
// WithMaxAttempts, a row that already failed that many times is no longer
// claimed (ADR 0030): from then on later rows overtake it.
func (r *OutboxRelay) RelayOnce(ctx context.Context) (int, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("postgres: begin relay pass: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `
		SELECT id, event_id, event_type, aggregate_id, payload, topic, COALESCE(traceparent, ''), COALESCE(tracestate, '')
		FROM outbox_events
		WHERE published_at IS NULL
		  AND ($2::int <= 0 OR attempts < $2::int)
		ORDER BY id
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`, r.batchSize, r.maxAttempts)
	if err != nil {
		return 0, fmt.Errorf("postgres: claim outbox rows: %w", err)
	}
	type pending struct {
		id  int64
		enc outboundkafka.Encoded
	}
	var batch []pending
	for rows.Next() {
		var p pending
		if err := rows.Scan(&p.id, &p.enc.EventId, &p.enc.EventType, &p.enc.Key, &p.enc.Value, &p.enc.Topic, &p.enc.Trace.Traceparent, &p.enc.Trace.Tracestate); err != nil {
			rows.Close()
			return 0, fmt.Errorf("postgres: scan outbox row: %w", err)
		}
		batch = append(batch, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("postgres: read outbox rows: %w", err)
	}

	published := 0
	for _, p := range batch {
		if err := r.sink.Send(ctx, p.enc); err != nil {
			return published, r.recordSendFailure(ctx, tx, p.id, p.enc, err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE outbox_events SET published_at = now(), attempts = attempts + 1, last_error = NULL WHERE id = $1
		`, p.id); err != nil {
			return published, fmt.Errorf("postgres: mark outbox row %d published: %w", p.id, err)
		}
		published++
	}
	if err := tx.Commit(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		return published, fmt.Errorf("postgres: commit relay pass: %w", err)
	}
	if published > 0 {
		r.logger.DebugContext(ctx, "outbox relay published events", "count", published)
	}
	return published, nil
}

// recordSendFailure records the send error on the failed row and commits
// the pass so earlier rows keep their published marks, wrapping every
// bookkeeping failure around the original send error. When this failure
// brings the row to the attempt cap (ADR 0030), the row is reported once at
// ERROR: from the next pass on it is no longer claimed.
func (r *OutboxRelay) recordSendFailure(ctx context.Context, tx pgx.Tx, id int64, enc outboundkafka.Encoded, err error) error {
	var attempts int
	if uerr := tx.QueryRow(ctx, `
		UPDATE outbox_events SET attempts = attempts + 1, last_error = $2 WHERE id = $1 RETURNING attempts
	`, id, err.Error()).Scan(&attempts); uerr != nil {
		err = errors.Join(err, fmt.Errorf("postgres: record outbox failure: %w", uerr))
	}
	if cerr := tx.Commit(ctx); cerr != nil {
		err = errors.Join(err, fmt.Errorf("postgres: commit relay pass: %w", cerr))
	} else if r.maxAttempts > 0 && attempts >= r.maxAttempts {
		r.logger.ErrorContext(ctx, "outbox relay: row reached the attempt cap and is no longer claimed",
			"outbox_id", id, "topic", enc.Topic, "event_type", enc.EventType, "key", enc.Key,
			"attempts", attempts, "max_attempts", r.maxAttempts, "error", err,
			"rearm", "UPDATE outbox_events SET attempts = 0 WHERE id = <outbox_id>")
	}
	return fmt.Errorf("outbox relay: send %s (row %d): %w", enc.EventType, id, err)
}
