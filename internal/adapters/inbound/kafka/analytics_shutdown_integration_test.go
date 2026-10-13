//go:build integration

package kafka_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	// Shares the package's testcontainers broker (startBroker, defined in
	// analytics_consumer_integration_test.go); the blank import keeps
	// TestKafkaIntegrationTestsUseTestcontainers' per-file check satisfied,
	// exactly like analytics_consumer_dlq_integration_test.go.
	_ "github.com/testcontainers/testcontainers-go/modules/kafka"

	inboundkafka "github.com/claudioed/process-path-management/internal/adapters/inbound/kafka"
)

// blockingProjection parks every apply call until the handler's context is
// cancelled, then reports that cancellation -- exactly what a real
// projection store does when the pod is told to stop mid-write.
type blockingProjection struct {
	entered chan struct{}
}

func (p *blockingProjection) block(ctx context.Context) error {
	select {
	case p.entered <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return ctx.Err()
}

func (p *blockingProjection) ApplyProcessPathCreated(ctx context.Context, _ string, _ time.Time) error {
	return p.block(ctx)
}

func (p *blockingProjection) ApplyProcessPathUpdated(ctx context.Context, _ string, _ time.Time) error {
	return p.block(ctx)
}

func (p *blockingProjection) ApplyProcessPathDeactivated(ctx context.Context, _ string, _ time.Time) error {
	return p.block(ctx)
}

// TestAnalyticsConsumer_ShutdownMidHandle_IsCleanAndDeadLettersNothing pins
// the shutdown contract: cancelling Run's context while a healthy message is
// being handled is a clean stop (Run returns nil), and that message is NOT
// dead-lettered -- its offset stays uncommitted, so it is redelivered on the
// next start. Before the fix the retry wrapper's cancellation error was
// routed into deadLetterAndCommit as if the message were poison, and Run
// surfaced the failed DLQ publish on the cancelled context as an error.
func TestAnalyticsConsumer_ShutdownMidHandle_IsCleanAndDeadLettersNothing(t *testing.T) {
	brokerList := startBroker(t)
	topic := uniqueTopic(t)
	dlqTopic := topic + ".dlq"
	createTopic(t, brokerList, topic)
	createTopic(t, brokerList, dlqTopic)

	projection := &blockingProjection{entered: make(chan struct{}, 1)}
	consumer := inboundkafka.NewAnalyticsConsumerInGroup(brokerList, topic,
		fmt.Sprintf("itest-shutdown-group-%d", time.Now().UnixNano()),
		projection, newMemoryProcessedEvents(), nil)
	defer func() { _ = consumer.Close() }()

	consumeCtx, consumeCancel := context.WithCancel(context.Background())
	defer consumeCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- consumer.Run(consumeCtx) }()

	base := time.Now().UTC().Truncate(time.Second)
	eventID := fmt.Sprintf("evt-shutdown-%d", time.Now().UnixNano())
	publish(t, brokerList, topic, envelopeMsg(t, "PICK", eventID, "ProcessPathCreated", base))

	select {
	case <-projection.entered:
	case <-time.After(60 * time.Second):
		t.Fatal("the consumer never started applying the message")
	}
	consumeCancel()

	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run after its context was cancelled mid-handle = %v, want nil (clean shutdown)", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("consumer did not stop after context cancellation")
	}

	// Close flushes any DLQ batch a cancelled WriteMessages had already
	// enqueued, so a dead-letter that "failed" on the cancelled context but
	// still reached the broker is caught below.
	_ = consumer.Close()

	dlqReader := newDLQReader(t, brokerList, dlqTopic)
	defer func() { _ = dlqReader.Close() }()
	readCtx, readCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer readCancel()
	if msg, err := dlqReader.ReadMessage(readCtx); err == nil {
		t.Fatalf("a healthy message was dead-lettered on shutdown: key=%q headers=%v", string(msg.Key), msg.Headers)
	}
}
