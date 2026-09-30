package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// AnalyticsTopic is the dedicated topic the analytics data product
// consumes. It is separate from the integration topic (Topic) so the
// OLTP integration contract and the analytical read-model stream evolve
// independently (ADR 0007), mirroring facility-layout's ADR-0010 pattern.
const AnalyticsTopic = "warehouse.process-path-management.analytics"

// analyticsSchemaVersion is the schema version stamped onto every
// analytics envelope this encoder emits.
const analyticsSchemaVersion = 1

// AnalyticsEnvelope is the Envelope v1 wrapper for the analytics stream.
// Like the integration Envelope it carries the domain event's own JSON
// as its data field: this service's ProcessPathData payload already
// serializes itself to its wire shape, so no per-event marshalling
// switch beyond the existing one is needed. The only additions over the
// integration Envelope are the CloudEvents-style schema_version and the
// snake_case field naming the estate's analytics contract fixes.
type AnalyticsEnvelope struct {
	EventId       string          `json:"event_id"`
	EventType     string          `json:"event_type"`
	OccurredAt    time.Time       `json:"occurred_at"`
	Source        string          `json:"source"`
	SchemaVersion int             `json:"schema_version"`
	Data          json.RawMessage `json:"data"`
}

// AnalyticsEncoder turns this service's own domain events into their
// analytics wire form on AnalyticsTopic. It reuses the SAME event->payload
// mapping the integration Encode function already has (ProcessPathCreated/
// Updated/Deactivated), so the analytics contract and the integration
// contract never disagree on what a given event's fields mean — only the
// envelope and the topic differ.
type AnalyticsEncoder struct {
	// NewId mints the envelope's event_id. It is the projector's
	// idempotency key, so it must be unique per encoded message.
	NewId func() string
}

// NewAnalyticsEncoder constructs an AnalyticsEncoder. newId mints each
// envelope's event_id.
func NewAnalyticsEncoder(newId func() string) *AnalyticsEncoder {
	return &AnalyticsEncoder{NewId: newId}
}

// Encode maps event to its analytics message. eventId is supplied by the
// caller (matching the Encoder contract every other encoder in this
// package follows) so the outbox can persist the same id it will later
// publish under.
func (e *AnalyticsEncoder) Encode(event shared.DomainEvent, eventId string) (Encoded, error) {
	// Reuse the integration Encode's event->ProcessPathData mapping and
	// event-type constants by encoding once against the integration
	// envelope shape, then re-wrapping its already-derived pathId/typ/data
	// into the analytics envelope. This keeps a single switch statement as
	// the one place a new event type must be taught to this publisher.
	integration, err := Encode(event, eventId)
	if err != nil {
		return Encoded{}, err
	}

	var integrationEnv Envelope
	if err := json.Unmarshal(integration.Value, &integrationEnv); err != nil {
		return Encoded{}, fmt.Errorf("kafka: decode intermediate integration envelope: %w", err)
	}
	data, err := json.Marshal(integrationEnv.Data)
	if err != nil {
		return Encoded{}, fmt.Errorf("kafka: marshal analytics event data: %w", err)
	}

	env := AnalyticsEnvelope{
		EventId:       eventId,
		EventType:     integration.EventType,
		OccurredAt:    event.OccurredAt(),
		Source:        Source,
		SchemaVersion: analyticsSchemaVersion,
		Data:          data,
	}
	payload, err := json.Marshal(env)
	if err != nil {
		return Encoded{}, fmt.Errorf("kafka: marshal analytics envelope: %w", err)
	}

	return Encoded{
		Topic:     AnalyticsTopic,
		EventId:   eventId,
		EventType: integration.EventType,
		Key:       integration.Key,
		Value:     payload,
	}, nil
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

// NewAnalyticsDirectPublisher constructs an AnalyticsPublisher writing to
// AnalyticsTopic on brokers. newId mints each envelope's event_id.
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
			Addr:                   kafkago.TCP(brokers...),
			Topic:                  AnalyticsTopic,
			Balancer:               &kafkago.Hash{},
			AllowAutoTopicCreation: true,
		},
	}
}

// Publish encodes event onto the analytics envelope and writes it to
// AnalyticsTopic directly.
func (p *AnalyticsPublisher) Publish(ctx context.Context, event shared.DomainEvent) error {
	enc, err := p.encoder.Encode(event, p.encoder.NewId())
	if err != nil {
		return err
	}
	msg := kafkago.Message{Key: []byte(enc.Key), Value: enc.Value}
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
