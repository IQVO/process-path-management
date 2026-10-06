package kafka

import (
	"context"
	"fmt"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/process-path-management/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// AnalyticsTopic is the dedicated topic the analytics data product
// consumes. It is separate from the integration topic (Topic) so the
// OLTP integration contract and the analytical read-model stream evolve
// independently (ADR 0007), mirroring facility-layout's ADR-0010 pattern.
const AnalyticsTopic = "warehouse.process-path-management.analytics"

// AnalyticsEncoder turns this service's own domain events into their
// analytics wire form on AnalyticsTopic: the SAME CloudEvent the
// integration Encode produces (same `type`, `subject`, `time`, `data`
// payload) except for dataschema
// urn:warehouse:process-path-management:analytics:<EventName>:v1 (ADR
// 0016 — this replaces the retired analytics schema version field).
type AnalyticsEncoder struct {
	// NewId mints the CloudEvents `id` on the direct (no-outbox) path. It
	// is the projector's idempotency key, so it must be unique per
	// occurrence.
	NewId func() string
}

// NewAnalyticsEncoder constructs an AnalyticsEncoder. newId mints each
// event's CloudEvents `id` on the direct path.
func NewAnalyticsEncoder(newId func() string) *AnalyticsEncoder {
	return &AnalyticsEncoder{NewId: newId}
}

// Encode maps event to its analytics message. eventId is supplied by the
// caller (matching the Encoder contract every other encoder in this
// package follows) so the outbox can persist the same id it will later
// publish under.
func (e *AnalyticsEncoder) Encode(event shared.DomainEvent, eventId string) (Encoded, error) {
	return encodeFor(event, eventId, AnalyticsTopic, cloudevents.StreamAnalytics)
}

// Compile-time assertion that AnalyticsEncoder satisfies the outbox's
// multi-topic Encoder port.
var _ Encoder = (*AnalyticsEncoder)(nil)

// AnalyticsPublisher publishes process-path-management domain events onto
// AnalyticsTopic directly (no outbox), for the EVENT_PUBLISHER=kafka +
// no-Postgres dev/test path where there is no transaction to bind the
// integration and analytics rows to. In the cluster (Postgres configured)
// the AnalyticsEncoder above is used instead, via
// postgres.NewOutboxPublisher.
type AnalyticsPublisher struct {
	encoder *AnalyticsEncoder
	writer  Writer
}

// SetNewId overrides the id-minting function used by the standalone
// Publish path.
func (p *AnalyticsPublisher) SetNewId(newId func() string) { p.encoder.NewId = newId }

// PublishWithId publishes event onto AnalyticsTopic under a caller-minted
// CloudEvents id — the form FanOutPublisher uses so ONE occurrence gets
// ONE id on every topic (ADR 0016's no-Postgres fan-out edge). It
// implements IdAwareSender.
func (p *AnalyticsPublisher) PublishWithId(ctx context.Context, event shared.DomainEvent, eventId string) error {
	enc, err := p.encoder.Encode(event, eventId)
	if err != nil {
		return err
	}
	enc.Trace = TraceContextFrom(ctx)
	msg := kafkago.Message{
		Key:     []byte(enc.Key),
		Value:   enc.Value,
		Headers: enc.messageHeaders(),
	}
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("kafka: publish %s analytics event: %w", enc.EventType, err)
	}
	return nil
}

// NewAnalyticsDirectPublisher constructs an AnalyticsPublisher writing to
// AnalyticsTopic on brokers. newId mints each event's CloudEvents `id`.
//
// Balancer is kafkago.Hash, matching Publisher's NewPublisher choice (see
// its doc comment): this publisher already keys every message by the
// integration Encode's Key (PathId/SiteId, via AnalyticsEncoder.Encode
// below), but LeastBytes would silently discard that key for partition
// routing — Hash is what actually turns it into a same-aggregate-
// same-partition guarantee now that AnalyticsTopic has more than one
// partition (warehouse-infra PR #42).
func NewAnalyticsDirectPublisher(brokers []string, newId func() string) *AnalyticsPublisher {
	return &AnalyticsPublisher{
		encoder: NewAnalyticsEncoder(newId),
		writer: &kafkago.Writer{
			BatchTimeout:           syncWriterBatchTimeout,
			RequiredAcks:           syncWriterRequiredAcks,
			Addr:                   kafkago.TCP(brokers...),
			Topic:                  AnalyticsTopic,
			Balancer:               &kafkago.Hash{},
			AllowAutoTopicCreation: true,
		},
	}
}

// Publish encodes event as an analytics CloudEvent and writes it to
// AnalyticsTopic directly.
func (p *AnalyticsPublisher) Publish(ctx context.Context, event shared.DomainEvent) error {
	enc, err := p.encoder.Encode(event, p.encoder.NewId())
	if err != nil {
		return err
	}
	enc.Trace = TraceContextFrom(ctx)
	msg := kafkago.Message{
		Key:     []byte(enc.Key),
		Value:   enc.Value,
		Headers: enc.messageHeaders(),
	}
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("kafka: publish %s analytics event: %w", enc.EventType, err)
	}
	return nil
}

// Close releases the underlying Kafka writer.
func (p *AnalyticsPublisher) Close() error {
	if w, ok := p.writer.(*kafkago.Writer); ok {
		return w.Close()
	}
	return nil
}

// Compile-time assertion that AnalyticsPublisher satisfies the fan-out
// Sender interface.
var _ Sender = (*AnalyticsPublisher)(nil)
