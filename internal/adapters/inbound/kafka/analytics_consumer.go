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
// 0025 §DLQ). ADR 0032 makes the consumer's idempotency gate
// (ProcessedEvents.MarkProcessed, analytics_consumed_events) and the
// projection apply (PostgresProjection) commit in ONE analytics transaction
// (UnitOfWork), exactly like RepromiseConsumer's single-UnitOfWork use case:
// a failed or interrupted attempt rolls the claim back with the write, so
// the whole claim+apply step is retried as a unit and a process stop
// between the two can no longer lose the projection update. See
// handleFetchedMessage.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/cenkalti/backoff/v4"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/process-path-management/internal/adapters/kafka/cloudevents"
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
// claim+apply step (ADR-0012 §DLQ, ADR 0032) before a message is
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

// UnitOfWork runs fn in ONE analytics transaction that both
// ProcessedEvents.MarkProcessed and the ProjectionStore join through ctx
// (ADR 0032): the claim and the projection write commit together or not at
// all. Declared here for the same reason as ProcessedEvents; the
// analyticsstore UnitOfWork implements it.
type UnitOfWork interface {
	Execute(ctx context.Context, fn func(ctx context.Context) error) error
}

// passthroughUnitOfWork runs fn directly. It is only the fallback for a
// consumer built without a UnitOfWork (the in-memory stores and fakes tests
// use, which have no transaction to share); production wiring always
// passes analyticsstore.NewUnitOfWork.
type passthroughUnitOfWork struct{}

func (passthroughUnitOfWork) Execute(ctx context.Context, fn func(ctx context.Context) error) error {
	return fn(ctx)
}

// analyticsEvent is the projection-relevant view of one inbound
// CloudEvents 1.0 event (ADR 0016): its `id` (the dedupe key), full `type`
// (the dispatch key) and `time` (the domain occurred-at). The projection
// needs no payload field, so `data` is not decoded here.
type analyticsEvent struct {
	Id   string
	Type string
	Time time.Time
}

// AnalyticsConsumer reads analytics events off the analytics topic and
// applies each to the catalogue-growth ProjectionStore, exactly once per
// CloudEvents `id` despite Kafka's at-least-once delivery.
type AnalyticsConsumer struct {
	Reader     *kafkago.Reader
	Projection report.ProjectionStore
	Processed  ProcessedEvents
	// Atomic brackets MarkProcessed + the projection apply in one
	// transaction (ADR 0032). nil falls back to passthroughUnitOfWork
	// (in-memory stores and test fakes only).
	Atomic UnitOfWork
	Logger *slog.Logger
	// dlqWriter publishes a poison message (ADR-0012 §DLQ) to
	// topic+analyticsDlqTopicSuffix after maxAnalyticsHandlerAttempts
	// in-process retries of the claim+apply step all fail with a
	// genuine infrastructure error. nil in the zero-value struct the
	// existing unit tests build directly (they call HandleMessage,
	// which never reaches this field) — dlqPublish itself guards
	// against a nil writer so those tests keep compiling unchanged.
	dlqWriter *kafkago.Writer
}

// NewAnalyticsConsumer constructs an AnalyticsConsumer reading topic from
// brokers under AnalyticsConsumerGroup. The dead-letter topic is always
// derived as topic+analyticsDlqTopicSuffix, so an isolated test topic
// gets its own isolated DLQ topic for free. atomic is the analytics
// UnitOfWork both processed and projection join (ADR 0032).
func NewAnalyticsConsumer(brokers []string, topic string, projection report.ProjectionStore, processed ProcessedEvents, atomic UnitOfWork, logger *slog.Logger) *AnalyticsConsumer {
	return NewAnalyticsConsumerInGroup(brokers, topic, AnalyticsConsumerGroup, projection, processed, atomic, logger)
}

// NewAnalyticsConsumerInGroup is NewAnalyticsConsumer with an explicit
// consumer group. Production always uses AnalyticsConsumerGroup (via
// NewAnalyticsConsumer); the explicit form exists so integration tests that
// share ONE broker can give every test its own group instead of all of
// them joining (and rebalancing) the same group through one broker's
// lifetime — membership is group-wide state, not per-topic state, so a
// previous test's leaving member could otherwise hold the next test's join
// behind a rebalance.
func NewAnalyticsConsumerInGroup(brokers []string, topic, groupID string, projection report.ProjectionStore, processed ProcessedEvents, atomic UnitOfWork, logger *slog.Logger) *AnalyticsConsumer {
	if logger == nil {
		logger = slog.Default()
	}
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,
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
		Atomic:     atomic,
		Logger:     logger,
		dlqWriter:  newDLQWriter(brokers, topic+analyticsDlqTopicSuffix),
	}
}

// newDLQWriter builds the dead-letter writer for dlqTopic. It sets
// AllowAutoTopicCreation, the fleet convention for every writer
// (warehouse-infra/terraform/kafka.tf leaves topic creation to the
// producing writer): "<topic>.dlq" is only ever written on the rare
// poison-message path, so it usually does not exist yet when it is first
// needed. Without the flag that first dead-letter write fails with
// "[3] Unknown Topic Or Partition", the offset is (correctly) not
// committed, and Run aborts, stopping the projector on the very message
// the DLQ exists to route around.
func newDLQWriter(brokers []string, dlqTopic string) *kafkago.Writer {
	return &kafkago.Writer{
		Addr:                   kafkago.TCP(brokers...),
		Topic:                  dlqTopic,
		AllowAutoTopicCreation: true,
		// BatchTimeout: a DLQ write is a synchronous single message; with
		// kafka-go's 1s default the writer holds every write for a full second
		// waiting to fill a batch, capping dead-lettering at ~1 msg/s/partition
		// (observed live: a backlog of legacy messages took hours to drain while
		// the consumer processed nothing else).
		BatchTimeout: dlqBatchTimeout,
	}
}

// Run reads and handles messages until ctx is cancelled or the reader
// returns a fatal error. handleFetchedMessage always commits the
// offset (on success, on a non-projecting message, after dead-lettering a
// non-CloudEvents message, or after
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
			if ctx.Err() != nil && errors.Is(err, context.Canceled) {
				// Shutdown landed mid-handle: the offset was not committed,
				// so the message is redelivered on the next start. A clean
				// stop, not a crash.
				return nil
			}
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

// HandleMessage decodes raw as a CloudEvents 1.0 event and claims+applies
// it (claimAndApply: MarkProcessed and the matching projection method in
// ONE analytics transaction, ADR 0032). Types outside the projection
// contract are ignored (and not marked processed); a redelivery of an
// applied event is a no-op. A message that is not a valid CloudEvent
// (including the retired flat envelope) is rejected with an error wrapping
// cloudevents.ErrNotCloudEvent. It is exported separately from Run so tests
// can feed raw messages without a live broker.
//
// This method does NOT retry and is not used by Run's own per-message
// flow (see handleFetchedMessage) — it exists for the existing direct
// unit-test surface and for any caller that wants single-shot,
// no-DLQ semantics.
func (c *AnalyticsConsumer) HandleMessage(ctx context.Context, raw []byte) error {
	env, err := decodeAnalyticsEvent(raw)
	if err != nil {
		return err
	}
	if !isProjectingEventType(env.Type) {
		return nil
	}
	return c.claimAndApply(ctx, env)
}

// handleFetchedMessage processes one fetched message with a
// retry-then-dead-letter shape (ADR-0012 §DLQ, ADR 0032):
//
//  1. Decode. A message that is not a valid CloudEvents 1.0 event
//     (malformed JSON, the retired flat envelope, missing attributes) is
//     a deterministic poison message: it is published to the dead-letter
//     topic and committed — there is nothing a retry could fix (ADR 0016).
//  2. Filter by event type. A non-projecting event is committed as a
//     no-op, exactly HandleMessage's existing behaviour.
//  3. Claim and apply as ONE unit (claimAndApply): MarkProcessed and the
//     projection write share one analytics transaction, so a failed
//     attempt rolls the claim back together with the write. The whole step
//     is retried up to maxAnalyticsHandlerAttempts times; a redelivery of
//     an already-applied event is a committed no-op.
//  4. After the retry budget is exhausted, the raw message and error
//     context are published to <topic>.dlq and the offset is committed
//     anyway — one poison message must never permanently block every
//     other event behind it on this partition.
//  5. A retry loop that ended only because ctx was cancelled (pod
//     shutdown mid-handle) is NOT poison: ctx.Err() is returned without
//     dead-lettering or committing, so the message is redelivered on the
//     next start — and, because the interrupted transaction left no claim,
//     applied then — while Run treats the cancellation as a clean stop.
func (c *AnalyticsConsumer) handleFetchedMessage(ctx context.Context, msg kafkago.Message) error {
	env, decodeErr := decodeAnalyticsEvent(msg.Value)
	if decodeErr != nil {
		c.Logger.WarnContext(ctx, "analytics: rejecting non-CloudEvents kafka message",
			"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "error", decodeErr)
		return c.deadLetterAndCommit(ctx, msg, analyticsEvent{}, decodeErr)
	}
	if !isProjectingEventType(env.Type) {
		return c.commit(ctx, msg)
	}

	if err := c.retryBounded(ctx, func() error { return c.claimAndApply(ctx, env) }); err != nil {
		if ctx.Err() != nil {
			return ctx.Err() // shutting down: never dead-letter a message only cancellation interrupted
		}
		return c.deadLetterAndCommit(ctx, msg, env, fmt.Errorf("claim and apply: %w", err))
	}
	return c.commit(ctx, msg)
}

// claimAndApply claims env's CloudEvents id and, when the claim is new,
// applies the projection — both inside ONE analytics transaction (ADR
// 0032). Safe to retry as a whole: a failed attempt rolled its claim back.
func (c *AnalyticsConsumer) claimAndApply(ctx context.Context, env analyticsEvent) error {
	return c.unitOfWork().Execute(ctx, func(ctx context.Context) error {
		isNew, err := c.Processed.MarkProcessed(ctx, env.Id)
		if err != nil {
			return fmt.Errorf("analytics: mark processed: %w", err)
		}
		if !isNew {
			return nil
		}
		return c.applyProjection(ctx, env)
	})
}

// unitOfWork returns the configured analytics UnitOfWork, or the
// passthrough fallback for a consumer built without one.
func (c *AnalyticsConsumer) unitOfWork() UnitOfWork {
	if c.Atomic == nil {
		return passthroughUnitOfWork{}
	}
	return c.Atomic
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
func (c *AnalyticsConsumer) deadLetterAndCommit(ctx context.Context, msg kafkago.Message, env analyticsEvent, cause error) error {
	c.Logger.ErrorContext(ctx, "analytics: sending message to dead-letter topic",
		"dlq_topic", c.dlqTopic(),
		"ce_id", env.Id, "ce_type", env.Type, "error", cause)
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
		kafkago.Header{Key: "x-dlq-source-topic", Value: []byte(c.sourceTopic())},
		kafkago.Header{Key: "x-dlq-error", Value: []byte(cause.Error())},
		kafkago.Header{Key: "x-dlq-failed-at", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
	)
	return writeDLQ(ctx, c.dlqWriter, kafkago.Message{
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

// sourceTopic is the topic this consumer reads; empty for the zero-value
// struct unit tests build without a Reader.
func (c *AnalyticsConsumer) sourceTopic() string {
	if c.Reader == nil {
		return ""
	}
	return c.Reader.Config().Topic
}

// dlqTopic is the dead-letter topic derived from sourceTopic.
func (c *AnalyticsConsumer) dlqTopic() string {
	return c.sourceTopic() + analyticsDlqTopicSuffix
}

// decodeAnalyticsEvent decodes and validates raw as a CloudEvents 1.0
// event via the service's single cloudevents helper. Any failure wraps
// cloudevents.ErrNotCloudEvent — the retired flat envelope is never
// parsed as a fallback.
func decodeAnalyticsEvent(raw []byte) (analyticsEvent, error) {
	e, err := cloudevents.Decode(raw)
	if err != nil {
		return analyticsEvent{}, fmt.Errorf("analytics: %w", err)
	}
	return analyticsEvent{Id: e.ID(), Type: e.Type(), Time: e.Time()}, nil
}

// isProjectingEventType reports whether the full CloudEvents `type` is one
// this consumer applies to the projection — every other type on this
// topic (e.g. CPTScheduleChanged) is silently skipped (and never marked
// processed) for forward compatibility.
func isProjectingEventType(eventType string) bool {
	switch eventType {
	case cloudevents.TypeProcessPathCreated, cloudevents.TypeProcessPathUpdated, cloudevents.TypeProcessPathDeactivated:
		return true
	default:
		return false
	}
}

// applyProjection dispatches env on its full `type` to the matching
// Projection method, passing the CloudEvents `id` and `time`. Callers
// must have already confirmed isProjectingEventType(env.Type).
func (c *AnalyticsConsumer) applyProjection(ctx context.Context, env analyticsEvent) error {
	switch env.Type {
	case cloudevents.TypeProcessPathCreated:
		return c.Projection.ApplyProcessPathCreated(ctx, env.Id, env.Time)
	case cloudevents.TypeProcessPathUpdated:
		return c.Projection.ApplyProcessPathUpdated(ctx, env.Id, env.Time)
	case cloudevents.TypeProcessPathDeactivated:
		return c.Projection.ApplyProcessPathDeactivated(ctx, env.Id, env.Time)
	default:
		return nil
	}
}

// dlqTopicReadyAttempts / dlqTopicReadyBackoff bound how long a DLQ publish
// waits for an auto-created "<topic>.dlq" to become writable.
const (
	dlqTopicReadyAttempts = 40
	dlqTopicReadyBackoff  = 250 * time.Millisecond
)

// dlqMessageWriter is the slice of *kafkago.Writer writeDLQ needs.
type dlqMessageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafkago.Message) error
}

// writeDLQ publishes msg to the dead-letter topic, retrying (bounded) while
// the topic is still being auto-created. AllowAutoTopicCreation alone is not
// enough: the first write races partition leader election and the broker
// answers UnknownTopicOrPartition / LeaderNotAvailable for a few hundred
// milliseconds. Any other error -- or exhausting the budget -- is returned,
// so the caller still refuses to commit the offset (no message loss).
func writeDLQ(ctx context.Context, w dlqMessageWriter, msg kafkago.Message) error {
	var err error
	for attempt := 0; attempt < dlqTopicReadyAttempts; attempt++ {
		if err = w.WriteMessages(ctx, msg); err == nil || !isTopicNotReady(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		case <-time.After(dlqTopicReadyBackoff):
		}
	}
	return err
}

// isTopicNotReady reports whether err only means the (auto-created) topic
// has no leader yet.
func isTopicNotReady(err error) bool {
	var werrs kafkago.WriteErrors
	if errors.As(err, &werrs) {
		for _, e := range werrs {
			if e != nil && !isTopicNotReady(e) {
				return false
			}
		}
		return werrs.Count() > 0
	}
	return errors.Is(err, kafkago.UnknownTopicOrPartition) || errors.Is(err, kafkago.LeaderNotAvailable)
}

// dlqBatchTimeout flushes a dead-letter write almost immediately.
const dlqBatchTimeout = 10 * time.Millisecond
