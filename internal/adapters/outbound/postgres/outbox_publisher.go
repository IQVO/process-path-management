package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	outboundkafka "github.com/claudioed/process-path-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// OutboxPublisher implements ports.EventPublisher by writing each
// encoder's Kafka wire form of event into outbox_events instead of the
// broker (ADR 0003, extended by ADR 0007 to fan one event out to several
// topics). When called inside UnitOfWork.Execute the insert joins the use
// case's transaction, so the aggregate change and every one of its
// outbox rows commit together or not at all. OutboxRelay later drains
// the table onto Kafka.
//
// One outbox row is written per (event x encoder), so this service's
// existing single integration topic and its new analytics topic
// (warehouse.process-path-management.analytics) can never diverge: both
// are enqueued in the exact same transaction as the aggregate write.
type OutboxPublisher struct {
	pool     *pgxpool.Pool
	newId    func() string
	encoders []outboundkafka.Encoder
}

// NewOutboxPublisher constructs an OutboxPublisher over pool that fans
// each event through every encoder given, in order. newId mints the
// envelope event_id shared by a given event's row across every encoder
// invocation, so a redelivery carries the same id on every topic it was
// enqueued for.
//
// Passing outboundkafka.IntegrationEncoder{} alone preserves this
// service's original single-topic behaviour; adding
// outboundkafka.NewAnalyticsEncoder(newId) alongside it additionally
// enqueues the same event onto the analytics topic.
func NewOutboxPublisher(pool *pgxpool.Pool, newId func() string, encoders ...outboundkafka.Encoder) *OutboxPublisher {
	if len(encoders) == 0 {
		encoders = []outboundkafka.Encoder{outboundkafka.IntegrationEncoder{}}
	}
	return &OutboxPublisher{pool: pool, newId: newId, encoders: encoders}
}

// Publish stores event's encoded message for every configured encoder in
// the outbox. It never touches Kafka.
func (p *OutboxPublisher) Publish(ctx context.Context, event shared.DomainEvent) error {
	eventId := p.newId()
	trace := outboundkafka.TraceContextFrom(ctx)
	q := querierFrom(ctx, p.pool)
	for _, enc := range p.encoders {
		encoded, err := enc.Encode(event, eventId)
		if err != nil {
			return err
		}
		// Capture the trace context HERE, while the request span is still
		// in ctx: the relay runs on a background loop later (ADR 0027).
		// Empty strings are stored as NULL so untraced rows stay clean.
		encoded.Trace = trace
		_, err = q.Exec(ctx, `
			INSERT INTO outbox_events (event_id, event_type, aggregate_id, payload, occurred_at, topic, traceparent, tracestate)
			VALUES ($1, $2, $3, $4, $5, $6, NULLIF($7, ''), NULLIF($8, ''))
			ON CONFLICT (event_id, topic) DO NOTHING
		`, encoded.EventId, encoded.EventType, encoded.Key, encoded.Value, event.OccurredAt(), encoded.Topic,
			encoded.Trace.Traceparent, encoded.Trace.Tracestate)
		if err != nil {
			return fmt.Errorf("postgres: enqueue outbox event %s for %s: %w", encoded.EventType, encoded.Topic, err)
		}
	}
	return nil
}
