package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// outboxMeterName is this adapter's OpenTelemetry instrumentation scope.
const outboxMeterName = "github.com/claudioed/process-path-management/internal/adapters/outbound/postgres"

// outboxLagQuery finds the age, in seconds, of the OLDEST unpublished
// outbox_events row -- the one the relay would drain next. created_at is
// set once at INSERT time and never touched again, so now() - created_at
// for the oldest row IS how long the relay has been behind for at least
// that long (ADR 0003's "an outbox-lag metric is a follow-up" note,
// closed by ADR 0018). It says nothing about why a row is lagging (relay
// idle vs genuinely stuck retrying); both cases raise this gauge, which
// is the point: "how long has SOMETHING been waiting," not "why."
const outboxLagQuery = `
	SELECT EXTRACT(EPOCH FROM (now() - created_at))
	FROM outbox_events
	WHERE published_at IS NULL
	ORDER BY id
	LIMIT 1
`

// oldestUnpublishedLagSeconds reports the age in seconds of the oldest
// unpublished outbox_events row via q, or 0 when the outbox is fully
// drained (no unpublished rows at all -- pgx.ErrNoRows is not a failure
// here, it is the "caught up" answer).
func oldestUnpublishedLagSeconds(ctx context.Context, q querier) (float64, error) {
	var lag float64
	if err := q.QueryRow(ctx, outboxLagQuery).Scan(&lag); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("postgres: query outbox lag: %w", err)
	}
	return lag, nil
}

// OldestUnpublishedLagSecondsForTest exposes the lag query for
// integration tests, which pin the gauge's SQL semantics (0 when
// drained, the oldest pending row's age otherwise) against a real
// Postgres — the async OTel callback itself cannot be observed
// deterministically.
func OldestUnpublishedLagSecondsForTest(ctx context.Context, pool *pgxpool.Pool) (float64, error) {
	return oldestUnpublishedLagSeconds(ctx, pool)
}

// RegisterOutboxLagGauge installs an asynchronous gauge, sampled once per
// collection, reporting process_path_management.outbox.lag_seconds: the
// age of the oldest unpublished outbox_events row (0 when fully drained).
// It is created against the global MeterProvider so package init ordering
// versus telemetry.Setup does not matter (mirrors workforce-management's
// own outbox gauge, the fleet reference for this pattern).
//
// Callers should keep the returned metric.Registration and call
// Unregister on shutdown to stop the callback from touching the pool
// after the pool is closed; the database is never queried at
// registration time.
func RegisterOutboxLagGauge(pool *pgxpool.Pool) (metric.Registration, error) {
	meter := otel.Meter(outboxMeterName)
	gauge, err := meter.Float64ObservableGauge(
		"process_path_management.outbox.lag_seconds",
		metric.WithDescription("Age in seconds of the oldest unpublished outbox_events row; 0 when the outbox is fully drained."),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, fmt.Errorf("postgres: create outbox lag gauge: %w", err)
	}
	reg, err := meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		lag, err := oldestUnpublishedLagSeconds(ctx, pool)
		if err != nil {
			return err
		}
		o.ObserveFloat64(gauge, lag)
		return nil
	}, gauge)
	if err != nil {
		return nil, fmt.Errorf("postgres: register outbox lag callback: %w", err)
	}
	return reg, nil
}
