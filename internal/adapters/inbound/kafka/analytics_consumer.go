// Package kafka contains process-path-management's inbound Kafka adapters.
// Today that is the analytics consumer only: this service otherwise has no
// inbound Kafka consumer (it is the SOURCE of the process-path published
// language, never a consumer of anyone else's topic) — the analytics
// consumer here is different: it consumes THIS SERVICE'S OWN analytics
// topic, replaying its own past-tense events into the analytical read
// model (ADR 0007), mirroring facility-layout's ADR-0010 pattern.
//
// Consistent with the rest of the analytics pipeline, this consumer is
// trace-free: it opens no spans and reads no trace headers.
//
// ADR 0012 adds a dead-letter queue (mirroring order-management's ADR
// 0025 §DLQ, adapted for this consumer's DIFFERENT correctness shape):
// unlike RepromiseConsumer, whose dedupe check and mutation are ONE
// atomic use case (RepromiseOrder.Execute, wrapped in a single
// UnitOfWork transaction — a failed attempt rolls back the dedupe
// marking too, so retrying the whole handler is safe), this consumer's
// idempotency gate (ProcessedEvents.MarkProcessed, a separate Postgres
// table/transaction) and its projection apply (PostgresProjection,
// ANOTHER separate transaction) are two independent stores. Retrying
// the WHOLE of HandleMessage naively would be unsafe: a first attempt
// that marks the event processed and then fails to apply would make a
// second attempt's MarkProcessed report isNew=false and silently skip
// the apply forever, masking a real failure as success. See
// handleFetchedMessage's doc comment for the fix: MarkProcessed is
// called exactly once per message, never retried after a successful
// mark; only the projection apply itself (each attempt fully atomic via
// PostgresProjection's own inTx wrapping) is retried.
package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/cenkalti/backoff/v4"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/process-path-management/internal/analytics/report"
)

// AnalyticsConsumerGroup is the Kafka consumer group the analytics
// projector reads under.
const AnalyticsConsumerGroup = "process-path-management-analytics"

// analyticsDlqTopicSuffix names the dead-letter topic this consumer
// publishes a poison message to, relative to its OWN source topic
// (never a fixed constant) — mirrors RepromiseConsumer's dlqTopicSuffix
// convention exactly, letting an isolated test topic get its own
// isolated DLQ topic for free.
const analyticsDlqTopicSuffix = ".dlq"

// maxAnalyticsHandlerAttempts bounds the in-process retry of the
// projection-apply step (ADR-0012 §DLQ) before a message is
// dead-lettered: 1 initial attempt plus up to 2 retries, matching the
// fleet's "up to 3" bound (see RepromiseConsumer.maxHandlerAttempts).
const maxAnalyticsHandlerAttempts = 3

const (
	analyticsRetryInitialInterval = 100 * time.Millisecond
	analyticsRetryMaxInterval     = 2 * time.Second
)

// ProcessedEvents is the consumer's idempotency gate: MarkProcessed
// records an event id if it has not been seen and reports whether this
// call was the first to record it. It is declared here (rather than in
// application/ports) because it is an analytics-only concern the OLTP
// layers never touch; the analyticsstore ConsumedEventsRepo implements
// it.
type ProcessedEvents interface {
	MarkProcessed(ctx context.Context, eventId string) (bool, error)
}

// analyticsEnvelope is the inbound decode shape of the Envelope v1 wrapper
// on the analytics topic. Declared here (rather than imported from the
// outbound publisher) so this inbound adapter does not depend on an
// outbound adapter (arch-go enforced).
type analyticsEnvelope struct {
	EventId       string          `json:"event_id"`
	EventType     string          `json:"event_type"`
	OccurredAt    time.Time       `json:"occurred_at"`
	Source        string          `json:"source"`
	SchemaVersion int             `json:"schema_version"`
	Data          json.RawMessage `json:"data"`
}

// AnalyticsConsumer reads analytics events off the analytics topic and
// applies each to the catalogue-growth ProjectionStore, exactly once per
// event_id despite Kafka's at-least-once delivery.
type AnalyticsConsumer struct {
	Reader     *kafkago.Reader
	Projection report.ProjectionStore
	Processed  ProcessedEvents
	Logger     *slog.Logger
	// dlqWriter publishes a poison message (ADR-0012 §DLQ) to
	// topic+analyticsDlqTopicSuffix after maxAnalyticsHandlerAttempts
	// in-process retries of the projection apply step all fail with a
	// genuine infrastructure error. nil in the zero-value struct the
	// existing unit tests build directly (they call HandleMessage,
	// which never reaches this field) — dlqPublish itself guards
	// against a nil writer so those tests keep compiling unchanged.
	dlqWriter *kafkago.Writer
}

// NewAnalyticsConsumer constructs an AnalyticsConsumer reading topic from
// brokers under AnalyticsConsumerGroup. The dead-letter topic is always
// derived as topic+analyticsDlqTopicSuffix, so an isolated test topic
// gets its own isolated DLQ topic for free.
func NewAnalyticsConsumer(brokers []string, topic string, projection report.ProjectionStore, processed ProcessedEvents, logger *slog.Logger) *AnalyticsConsumer {
	if logger == nil {
		logger = slog.Default()
	}
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: AnalyticsConsumerGroup,
		// Start a brand-new consumer group at the EARLIEST offset. The
		// analytics projection must see the full history of the topic
		// (it is a replayable read model, not a live integration
		// reaction), so a fresh projector reads from the beginning
		// rather than kafka-go's default of the latest offset. Once the
		// group has committed offsets, those take precedence and this
		// only affects the first join.
		StartOffset: kafkago.FirstOffset,
	})
	return &AnalyticsConsumer{
		Reader:     reader,
		Projection: projection,
		Processed:  processed,
		Logger:     logger,
		dlqWriter: &kafkago.Writer{
			Addr:  kafkago.TCP(brokers...),
			Topic: topic + analyticsDlqTopicSuffix,
		},
	}
}

// Run reads and handles messages until ctx is cancelled or the reader
// returns a fatal error. handleFetchedMessage always commits the
// offset (on success, on a malformed/non-projecting message, or after
// exhausting retries and publishing to the dead-letter topic), so one
// bad message cannot wedge the projector. Only a commit failure or a
// DLQ publish failure aborts the loop — a genuine infrastructure
// problem this process cannot route around by itself.
func (c *AnalyticsConsumer) Run(ctx context.Context) error {
	for {
		msg, err := c.Reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		if err := c.handleFetchedMessage(ctx, msg); err != nil {
			return err
		}
	}
}

// Close releases the underlying Kafka reader and, if configured, the DLQ
// writer.
func (c *AnalyticsConsumer) Close() error {
	readerErr := c.Reader.Close()
	if c.dlqWriter == nil {
		return readerErr
	}
	return errors.Join(readerErr, c.dlqWriter.Close())
}

// HandleMessage decodes raw as an analyticsEnvelope and applies the
// matching projection method for its event_type. Event types outside the
// projection contract are ignored (and not marked processed). For a
// projecting event it dedupes on event_id via ProcessedEvents before
// applying, so a redelivery is a no-op. It is exported separately from Run
// so tests can feed raw envelopes without a live broker.
//
// This method does NOT retry and is not used by Run's own per-message
// flow (see handleFetchedMessage) — it exists for the existing direct
// unit-test surface and for any caller that wants single-shot,
// no-DLQ semantics.
func (c *AnalyticsConsumer) HandleMessage(ctx context.Context, raw []byte) error {
	env, err := decodeAnalyticsEnvelope(raw)
	if err != nil {
		return err
	}
	if !isProjectingEventType(env.EventType) {
		return nil
	}

	isNew, err := c.Processed.MarkProcessed(ctx, env.EventId)
	if err != nil {
		return fmt.Errorf("analytics: mark processed: %w", err)
	}
	if !isNew {
		return nil
	}

	return c.applyProjection(ctx, env)
}

// handleFetchedMessage processes one fetched message with a
// retry-then-dead-letter shape (ADR-0012 §DLQ) that is SAFE for this
// consumer's two-separate-transaction correctness shape (see this
// file's package doc comment for why HandleMessage's naive
// mark-then-apply cannot simply be retried as a whole):
//
//  1. Decode. A malformed message is logged and committed — there is
//     nothing a retry could fix.
//  2. Filter by event type. A non-projecting event is committed as a
//     no-op, exactly HandleMessage's existing behaviour.
//  3. Call ProcessedEvents.MarkProcessed EXACTLY ONCE. If it errors
//     (a genuine infrastructure failure — MarkProcessed itself has not
//     yet had any observable effect on THIS call), the mark call is
//     retried up to maxAnalyticsHandlerAttempts times; note MarkProcessed
//     is idempotent by its own contract, so retrying the marking call
//     itself (as opposed to retrying it AFTER a successful mark) is
//     safe. If already marked (isNew=false — a redelivery of an event
//     an earlier run already fully processed), commit and stop: no
//     apply, matching HandleMessage's existing dedupe semantics.
//  4. If newly marked, retry ONLY the projection apply call up to
//     maxAnalyticsHandlerAttempts times. Each attempt is a single
//     atomic Postgres transaction (PostgresProjection's own inTx
//     wrapping), so a failed attempt fully rolls back and a retry can
//     never double-apply.
//  5. After either retry loop exhausts its budget, the raw message and
//     error context are published to <topic>.dlq and the offset is
//     committed anyway — one poison message must never permanently
//     block every other event behind it on this partition.
func (c *AnalyticsConsumer) handleFetchedMessage(ctx context.Context, msg kafkago.Message) error {
	env, decodeErr := decodeAnalyticsEnvelope(msg.Value)
	if decodeErr != nil {
		c.Logger.ErrorContext(ctx, "analytics: skipping unparseable kafka message", "error", decodeErr)
		return c.commit(ctx, msg)
	}
	if !isProjectingEventType(env.EventType) {
		return c.commit(ctx, msg)
	}

	isNew, err := c.markProcessedWithRetry(ctx, env.EventId)
	if err != nil {
		return c.deadLetterAndCommit(ctx, msg, env, fmt.Errorf("mark processed: %w", err))
	}
	if !isNew {
		return c.commit(ctx, msg)
	}

	if err := c.applyProjectionWithRetry(ctx, env); err != nil {
		return c.deadLetterAndCommit(ctx, msg, env, fmt.Errorf("apply projection: %w", err))
	}
	return c.commit(ctx, msg)
}

// markProcessedWithRetry retries ProcessedEvents.MarkProcessed up to
// maxAnalyticsHandlerAttempts times with jittered exponential backoff,
// bounded by ctx. Safe to retry: MarkProcessed's own contract is
// idempotent (recording the same event_id twice is a documented no-op
// reporting isNew=false the second time), and no apply has happened yet
// at the point this is called.
func (c *AnalyticsConsumer) markProcessedWithRetry(ctx context.Context, eventId string) (bool, error) {
	var isNew bool
	err := c.retryBounded(ctx, func() error {
		var innerErr error
		isNew, innerErr = c.Processed.MarkProcessed(ctx, eventId)
		return innerErr
	})
	return isNew, err
}

// applyProjectionWithRetry retries the projection apply call up to
// maxAnalyticsHandlerAttempts times with jittered exponential backoff,
// bounded by ctx. Safe to retry because each attempt is a single atomic
// Postgres transaction (see PostgresProjection.apply's inTx wrapping) —
// a failed attempt rolls back cleanly, so a retry can never
// double-count the same event.
func (c *AnalyticsConsumer) applyProjectionWithRetry(ctx context.Context, env analyticsEnvelope) error {
	return c.retryBounded(ctx, func() error {
		return c.applyProjection(ctx, env)
	})
}

// retryBounded runs fn with jittered exponential backoff, up to
// maxAnalyticsHandlerAttempts total attempts, bounded by ctx's own
// deadline/cancellation.
func (c *AnalyticsConsumer) retryBounded(ctx context.Context, fn func() error) error {
	policy := backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(analyticsRetryInitialInterval),
		backoff.WithMaxInterval(analyticsRetryMaxInterval),
	)
	bounded := backoff.WithContext(backoff.WithMaxRetries(policy, maxAnalyticsHandlerAttempts-1), ctx)
	return backoff.Retry(fn, bounded)
}

// deadLetterAndCommit logs, publishes msg (raw, unmodified) plus error
// context to the dead-letter topic, and commits the offset regardless —
// one poison message must never block every other event behind it.
func (c *AnalyticsConsumer) deadLetterAndCommit(ctx context.Context, msg kafkago.Message, env analyticsEnvelope, cause error) error {
	c.Logger.ErrorContext(ctx, "analytics: exhausted retries, sending to dead-letter topic",
		"dlq_topic", c.Reader.Config().Topic+analyticsDlqTopicSuffix,
		"event_id", env.EventId, "event_type", env.EventType, "attempts", maxAnalyticsHandlerAttempts, "error", cause)
	if dlqErr := c.dlqPublish(ctx, msg, cause); dlqErr != nil {
		return fmt.Errorf("analytics: publish to dead-letter topic: %w", dlqErr)
	}
	return c.commit(ctx, msg)
}

// dlqPublish writes the raw, unmodified message payload plus error
// context (as headers, so the raw body stays byte-identical for a
// manual replay tool) to the dead-letter topic. A nil dlqWriter (the
// zero-value AnalyticsConsumer some unit tests construct directly,
// which never exercises this path) is a documented no-op rather than a
// nil-pointer panic.
func (c *AnalyticsConsumer) dlqPublish(ctx context.Context, msg kafkago.Message, cause error) error {
	if c.dlqWriter == nil {
		return nil
	}
	headers := append([]kafkago.Header{}, msg.Headers...)
	headers = append(headers,
		kafkago.Header{Key: "x-dlq-source-topic", Value: []byte(c.Reader.Config().Topic)},
		kafkago.Header{Key: "x-dlq-error", Value: []byte(cause.Error())},
		kafkago.Header{Key: "x-dlq-failed-at", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
	)
	return c.dlqWriter.WriteMessages(ctx, kafkago.Message{
		Key:     msg.Key,
		Value:   msg.Value,
		Headers: headers,
	})
}

// commit acknowledges msg so it is never redelivered. Only a commit
// failure itself aborts the consume loop.
func (c *AnalyticsConsumer) commit(ctx context.Context, msg kafkago.Message) error {
	return c.Reader.CommitMessages(ctx, msg)
}

// decodeAnalyticsEnvelope decodes raw as an analyticsEnvelope.
func decodeAnalyticsEnvelope(raw []byte) (analyticsEnvelope, error) {
	var env analyticsEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return analyticsEnvelope{}, fmt.Errorf("analytics: decode envelope: %w", err)
	}
	return env, nil
}

// isProjectingEventType reports whether eventType is one this consumer
// applies to the projection — every other event type on this topic is
// silently skipped (and never marked processed), exactly the
// pre-existing filter's behaviour.
func isProjectingEventType(eventType string) bool {
	switch eventType {
	case "ProcessPathCreated", "ProcessPathUpdated", "ProcessPathDeactivated":
		return true
	default:
		return false
	}
}

// applyProjection dispatches env to the matching Projection method.
// Callers must have already confirmed isProjectingEventType(env.EventType).
func (c *AnalyticsConsumer) applyProjection(ctx context.Context, env analyticsEnvelope) error {
	switch env.EventType {
	case "ProcessPathCreated":
		return c.Projection.ApplyProcessPathCreated(ctx, env.EventId, env.OccurredAt)
	case "ProcessPathUpdated":
		return c.Projection.ApplyProcessPathUpdated(ctx, env.EventId, env.OccurredAt)
	case "ProcessPathDeactivated":
		return c.Projection.ApplyProcessPathDeactivated(ctx, env.EventId, env.OccurredAt)
	default:
		return nil
	}
}
