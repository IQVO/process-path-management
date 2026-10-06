//go:build integration

package kafka_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	inboundkafka "github.com/claudioed/process-path-management/internal/adapters/inbound/kafka"
	"github.com/claudioed/process-path-management/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/process-path-management/internal/analytics/report"
)

// These tests drive the real analytics consumer against a REAL Kafka broker
// the test itself starts, via testcontainers — never an external
// KAFKA_BROKERS or the shared cluster broker (fleet-wide rule: CI's
// integration job provisions Postgres only, never Kafka).

var (
	sharedBrokers   []string
	sharedContainer testcontainers.Container
)

// projectionDeadline bounds how long a test waits for the consumer to
// project a message. The work itself is sub-second; the budget is dominated
// by the consumer-group join (kafka-go retries a not-yet-available group
// coordinator every 5s) and must absorb a CPU-starved CI runner executing
// several -race packages and containers at once.
const projectionDeadline = 60 * time.Second

func TestMain(m *testing.M) {
	code := m.Run()
	if sharedContainer != nil {
		if err := testcontainers.TerminateContainer(sharedContainer); err != nil {
			fmt.Fprintf(os.Stderr, "terminate kafka container: %v\n", err)
		}
	}
	os.Exit(code)
}

func startBroker(t *testing.T) []string {
	t.Helper()
	if sharedBrokers != nil {
		return sharedBrokers
	}
	ctx := context.Background()
	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID("pathmgmt-analytics-itest"),
	)
	if err != nil {
		t.Fatalf("start kafka container: %v", err)
	}
	sharedContainer = container
	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("resolve kafka brokers: %v", err)
	}
	sharedBrokers = brokers
	return brokers
}

func uniqueTopic(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("warehouse.process-path-management.analytics.itest-%d", time.Now().UnixNano())
}

// startConsumer runs an AnalyticsConsumer over topic in its OWN consumer
// group (never the production AnalyticsConsumerGroup) and tears it down in
// t.Cleanup: cancel, wait for Run to return, then Close so the member
// cleanly leaves its group before the next test starts. A group per test
// keeps group membership (group-wide state on the shared broker) from
// leaking from one test into the next. It returns the consumer's context,
// cancelled on cleanup.
func startConsumer(t *testing.T, brokerList []string, topic string, projection report.ProjectionStore, processed inboundkafka.ProcessedEvents) context.Context {
	t.Helper()
	group := fmt.Sprintf("itest-group-%d", time.Now().UnixNano())
	consumer := inboundkafka.NewAnalyticsConsumerInGroup(brokerList, topic, group, projection, processed, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = consumer.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Log("analytics consumer did not stop within 15s of cancellation")
		}
		_ = consumer.Close()
	})
	return ctx
}

func createTopic(t *testing.T, brokerList []string, topic string) {
	t.Helper()
	// A freshly started broker can still refuse a dial or answer
	// NOT_CONTROLLER / COORDINATOR_NOT_AVAILABLE for a moment after its
	// "started" log line; retry instead of failing the test (and, via the
	// shared broker, every test after it) on the first hiccup.
	deadline := time.Now().Add(60 * time.Second)
	var lastErr error
	for {
		lastErr = tryCreateTopic(brokerList[0], topic)
		if lastErr == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("create topic %s: %v", topic, lastErr)
		}
		time.Sleep(300 * time.Millisecond)
	}

	conn, err := kafkago.Dial("tcp", brokerList[0])
	if err != nil {
		t.Fatalf("dial %s: %v", brokerList[0], err)
	}
	defer func() { _ = conn.Close() }()

	// Readable is not writable: wait until every partition has an elected
	// leader. Without this the first publish raced leader election and
	// spent seconds in its retry loop (LEADER_NOT_AVAILABLE), eating the
	// budget of the test that follows.
	deadline = time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		partitions, err := conn.ReadPartitions(topic)
		if err == nil && len(partitions) > 0 && allHaveLeader(partitions) {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("topic %s never got a partition leader", topic)
}

func tryCreateTopic(broker, topic string) error {
	conn, err := kafkago.Dial("tcp", broker)
	if err != nil {
		return fmt.Errorf("dial %s: %w", broker, err)
	}
	defer func() { _ = conn.Close() }()
	err = conn.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1})
	if err != nil && !errors.Is(err, kafkago.TopicAlreadyExists) {
		return err
	}
	return nil
}

func allHaveLeader(partitions []kafkago.Partition) bool {
	for _, p := range partitions {
		if p.Leader.ID < 0 || p.Leader.Host == "" {
			return false
		}
	}
	return true
}

func publish(t *testing.T, brokerList []string, topic string, msgs ...kafkago.Message) {
	t.Helper()
	w := &kafkago.Writer{
		Addr:     kafkago.TCP(brokerList...),
		Topic:    topic,
		Balancer: &kafkago.LeastBytes{},
	}
	defer func() { _ = w.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var err error
	for attempt := 0; attempt < 20; attempt++ {
		if err = w.WriteMessages(ctx, msgs...); err == nil {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("publish to %s: %v", topic, err)
}

// envelopeMsg builds the structured-mode CloudEvents 1.0 analytics
// message (ADR 0016) the AnalyticsEncoder publishes for eventName, with the
// content-type header the production publisher attaches.
func envelopeMsg(t *testing.T, pathId, eventId, eventName string, at time.Time) kafkago.Message {
	t.Helper()
	value, err := cloudevents.New(cloudevents.Spec{
		ID: eventId, Entity: cloudevents.EntityProcessPath, EventName: eventName,
		Subject: pathId, Time: at, Stream: cloudevents.StreamAnalytics, Version: 1,
		Data: map[string]any{"path_id": pathId},
	})
	if err != nil {
		t.Fatalf("build cloudevent: %v", err)
	}
	return kafkago.Message{Key: []byte(pathId), Value: value, Headers: []kafkago.Header{cloudevents.ContentTypeHeader()}}
}

// legacyFlatMsg is the RETIRED pre-ADR-0016 flat analytics envelope.
func legacyFlatMsg(t *testing.T, pathId, eventId string, at time.Time) kafkago.Message {
	t.Helper()
	value, err := json.Marshal(map[string]any{
		"event_id": eventId, "event_type": "ProcessPathCreated", "occurred_at": at.UTC(),
		"source": "process-path-management", "schema_version": 1, "data": map[string]any{"path_id": pathId},
	})
	if err != nil {
		t.Fatalf("marshal legacy envelope: %v", err)
	}
	return kafkago.Message{Key: []byte(pathId), Value: value}
}

// TestAnalyticsConsumer_ReplaysFromFirstOffsetIntoMemoryStore proves the
// consumer replays a topic's full pre-existing history into the analytics
// read model, starting fresh from FirstOffset.
func TestAnalyticsConsumer_ReplaysFromFirstOffsetIntoMemoryStore(t *testing.T) {
	brokerList := startBroker(t)
	topic := uniqueTopic(t)
	createTopic(t, brokerList, topic)

	base := time.Now().UTC().Truncate(time.Second)
	publish(t, brokerList, topic,
		envelopeMsg(t, "PICK", "e1", "ProcessPathCreated", base),
		envelopeMsg(t, "PICK", "e2", "ProcessPathUpdated", base),
		envelopeMsg(t, "PACK", "e3", "ProcessPathDeactivated", base),
	)

	store := analyticsstore.NewMemoryStore()
	startConsumer(t, brokerList, topic, store, newMemoryProcessedEvents())

	deadline := time.Now().Add(projectionDeadline)
	for time.Now().Before(deadline) {
		rep, err := store.Query(context.Background(), reportQuery(base))
		if err == nil && len(rep.Rows) == 1 && rep.Rows[0].PathsDefined == 1 && rep.Rows[0].PathsRevised == 1 && rep.Rows[0].PathsDeactivated == 1 {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatal("consumer never projected the full pre-existing history")
}

// TestAnalyticsConsumer_IsIdempotentAcrossRedelivery proves a duplicate
// message (same event_id) is applied only once, using the store's own
// idempotency gate — the fleet's at-least-once delivery guarantee.
func TestAnalyticsConsumer_IsIdempotentAcrossRedelivery(t *testing.T) {
	brokerList := startBroker(t)
	topic := uniqueTopic(t)
	createTopic(t, brokerList, topic)

	base := time.Now().UTC().Truncate(time.Second)
	msg := envelopeMsg(t, "PICK", "dup-event", "ProcessPathCreated", base)
	publish(t, brokerList, topic, msg, msg)

	store := analyticsstore.NewMemoryStore()
	startConsumer(t, brokerList, topic, store, newMemoryProcessedEvents())

	deadline := time.Now().Add(projectionDeadline)
	for time.Now().Before(deadline) {
		rep, err := store.Query(context.Background(), reportQuery(base))
		if err == nil && len(rep.Rows) == 1 {
			if rep.Rows[0].PathsDefined != 1 {
				t.Fatalf("expected exactly 1 apply for a duplicate event_id, got %d", rep.Rows[0].PathsDefined)
			}
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatal("consumer never projected the deduped event")
}

// memoryProcessedEvents is a minimal in-memory ProcessedEvents gate for
// this integration test — analyticsstore.MemoryStore already dedupes
// internally via its own Apply* methods, so a real deployment relies on
// analyticsstore.ConsumedEventsRepo (Postgres-backed) for this gate; here
// a trivial in-memory implementation is enough to exercise the consumer's
// routing and replay behaviour end-to-end.
type memoryProcessedEvents struct {
	seen map[string]bool
}

func newMemoryProcessedEvents() *memoryProcessedEvents {
	return &memoryProcessedEvents{seen: map[string]bool{}}
}

func (p *memoryProcessedEvents) MarkProcessed(_ context.Context, eventId string) (bool, error) {
	if p.seen[eventId] {
		return false, nil
	}
	p.seen[eventId] = true
	return true, nil
}

func reportQuery(base time.Time) report.ReportQuery {
	return report.ReportQuery{
		From:        base.Add(-24 * time.Hour),
		To:          base.Add(24 * time.Hour),
		Granularity: report.GranularityDay,
	}
}
