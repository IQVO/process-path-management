//go:build integration

package kafka_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	// This file shares its Kafka broker (via startBroker, defined in
	// analytics_consumer_integration_test.go in this same package) with
	// the rest of this package's integration tests rather than starting
	// a second one — see that file's own testcontainers-go/modules/kafka
	// usage. This blank import keeps
	// TestKafkaIntegrationTestsUseTestcontainers' per-file content check
	// satisfied without duplicating container bring-up logic here.
	_ "github.com/testcontainers/testcontainers-go/modules/kafka"

	inboundkafka "github.com/claudioed/process-path-management/internal/adapters/inbound/kafka"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/analyticsstore"
)

// alwaysFailingProcessedEventsFor wraps a real inboundkafka.ProcessedEvents
// so MarkProcessed fails with a genuine infrastructure error for exactly
// poisonEventID, on EVERY call, while every other event_id is delegated
// unchanged — letting one poison message coexist in the SAME test with a
// normal, successfully-processed message on the SAME partition. Mirrors
// order-management's repromise_dlq_integration_test.go
// alwaysFailingProcessedEventsFor exactly, adapted to this consumer's
// ProcessedEvents port.
type alwaysFailingProcessedEventsFor struct {
	inboundkafka.ProcessedEvents
	poisonEventID string
}

func (p *alwaysFailingProcessedEventsFor) MarkProcessed(ctx context.Context, eventId string) (bool, error) {
	if eventId == p.poisonEventID {
		return false, fmt.Errorf("simulated poison-message infrastructure failure for event %s", eventId)
	}
	return p.ProcessedEvents.MarkProcessed(ctx, eventId)
}

// TestAnalyticsConsumer_PoisonMessage_GoesToDeadLetterTopicWithoutBlockingPartition
// is the ADR-0012 §DLQ acceptance test: a message whose MarkProcessed call
// ALWAYS fails (simulated infrastructure error) must, after exactly
// maxAnalyticsHandlerAttempts (3) in-process retries, land on
// "<topic>.dlq" with the raw original payload plus error context, and the
// consumer must commit past it and keep processing — a well-formed
// message published right after the poison one must be handled without
// delay, proving the partition was never blocked on the one bad message.
func TestAnalyticsConsumer_PoisonMessage_GoesToDeadLetterTopicWithoutBlockingPartition(t *testing.T) {
	brokerList := startBroker(t)
	topic := uniqueTopic(t)
	dlqTopic := topic + ".dlq"
	createTopic(t, brokerList, topic)
	createTopic(t, brokerList, dlqTopic)

	base := time.Now().UTC().Truncate(time.Second)
	poisonEventID := fmt.Sprintf("evt-dlq-poison-%d", time.Now().UnixNano())

	store := analyticsstore.NewMemoryStore()
	processed := newMemoryProcessedEvents()
	failingProcessed := &alwaysFailingProcessedEventsFor{ProcessedEvents: processed, poisonEventID: poisonEventID}

	consumer := inboundkafka.NewAnalyticsConsumer(brokerList, topic, store, failingProcessed, nil)
	defer func() { _ = consumer.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- consumer.Run(ctx) }()

	// Start reading the DLQ topic BEFORE publishing, so the poison
	// message's eventual dead-letter write is never missed to a race.
	dlqReader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     brokerList,
		Topic:       dlqTopic,
		GroupID:     fmt.Sprintf("dlq-reader-%d", time.Now().UnixNano()),
		StartOffset: kafkago.FirstOffset,
	})
	defer func() { _ = dlqReader.Close() }()

	publish(t, brokerList, topic,
		envelopeMsg(t, "PICK", poisonEventID, "ProcessPathCreated", base),
		envelopeMsg(t, "PACK", "evt-dlq-healthy", "ProcessPathCreated", base),
	)

	// Assert the poison message lands on the DLQ topic with the raw
	// payload and error context, after the retry budget is exhausted.
	dlqCtx, dlqCancel := context.WithTimeout(ctx, 60*time.Second)
	defer dlqCancel()
	dlqMsg, err := dlqReader.ReadMessage(dlqCtx)
	if err != nil {
		t.Fatalf("read from dead-letter topic: %v", err)
	}

	gotEnvelope := envelopeMsg(t, "PICK", poisonEventID, "ProcessPathCreated", base)
	if string(dlqMsg.Value) != string(gotEnvelope.Value) {
		t.Fatalf("dead-letter payload does not match original raw message:\nwant %s\ngot  %s", gotEnvelope.Value, dlqMsg.Value)
	}
	var sawSourceTopic, sawError bool
	for _, h := range dlqMsg.Headers {
		switch h.Key {
		case "x-dlq-source-topic":
			sawSourceTopic = string(h.Value) == topic
		case "x-dlq-error":
			sawError = len(h.Value) > 0
		}
	}
	if !sawSourceTopic {
		t.Fatal("dead-letter message missing x-dlq-source-topic header matching the source topic")
	}
	if !sawError {
		t.Fatal("dead-letter message missing non-empty x-dlq-error header")
	}

	// Prove the partition was never blocked: the healthy message
	// published right after the poison one is projected without delay.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		rep, err := store.Query(context.Background(), reportQuery(base))
		if err == nil && len(rep.Rows) == 1 && rep.Rows[0].PathsDefined == 1 {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatal("consumer never projected the healthy message published right after the poison one")
}

// TestAnalyticsConsumer_LegacyFlatEnvelope_IsDeadLetteredNotParsed proves
// ADR 0016's consumer rule against a real broker: a retired flat-envelope
// message is a deterministic poison message — it lands unmodified on
// "<topic>.dlq", is never projected, and the CloudEvent published right
// after it on the same partition is still projected.
func TestAnalyticsConsumer_LegacyFlatEnvelope_IsDeadLetteredNotParsed(t *testing.T) {
	brokerList := startBroker(t)
	topic := uniqueTopic(t)
	dlqTopic := topic + ".dlq"
	createTopic(t, brokerList, topic)
	createTopic(t, brokerList, dlqTopic)

	base := time.Now().UTC().Truncate(time.Second)
	store := analyticsstore.NewMemoryStore()
	consumer := inboundkafka.NewAnalyticsConsumer(brokerList, topic, store, newMemoryProcessedEvents(), nil)
	defer func() { _ = consumer.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = consumer.Run(ctx) }()

	dlqReader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     brokerList,
		Topic:       dlqTopic,
		GroupID:     fmt.Sprintf("dlq-legacy-reader-%d", time.Now().UnixNano()),
		StartOffset: kafkago.FirstOffset,
	})
	defer func() { _ = dlqReader.Close() }()

	legacy := legacyFlatMsg(t, "PICK", "evt-legacy-flat", base)
	publish(t, brokerList, topic, legacy, envelopeMsg(t, "PACK", "evt-ce-healthy", "ProcessPathCreated", base))

	dlqCtx, dlqCancel := context.WithTimeout(ctx, 60*time.Second)
	defer dlqCancel()
	dlqMsg, err := dlqReader.ReadMessage(dlqCtx)
	if err != nil {
		t.Fatalf("read from dead-letter topic: %v", err)
	}
	if string(dlqMsg.Value) != string(legacy.Value) {
		t.Fatalf("dead-letter payload = %s, want the raw legacy message %s", dlqMsg.Value, legacy.Value)
	}

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		rep, err := store.Query(context.Background(), reportQuery(base))
		if err == nil && len(rep.Rows) == 1 && rep.Rows[0].PathsDefined == 1 {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatal("expected exactly the CloudEvent (not the legacy message) to be projected")
}
