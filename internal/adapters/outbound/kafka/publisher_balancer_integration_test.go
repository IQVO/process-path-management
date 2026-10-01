//go:build integration

package kafka_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	"github.com/claudioed/process-path-management/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/process-path-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// createTopicWithPartitions creates topic with the given partition count
// against a real broker. Used by
// TestPublisherKeysMessagesForSamePathIdOntoTheSamePartition to exercise
// the exact "1->8 partitions" scaleup scenario (warehouse-infra PR #42)
// that exposed the LeastBytes-ignores-Key bug fleet-wide (see
// order-management PR #111 / ADR 0027, the reference fix this test
// mirrors).
func createTopicWithPartitions(t *testing.T, ctx context.Context, brokers []string, topic string, numPartitions int) {
	t.Helper()
	conn, err := kafkago.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		t.Fatalf("dial Kafka broker: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: numPartitions, ReplicationFactor: 1}); err != nil {
		t.Fatalf("create Kafka topic: %v", err)
	}

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		partitions, err := conn.ReadPartitions(topic)
		if err == nil && len(partitions) >= numPartitions {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("Kafka topic %q never became ready with %d partitions", topic, numPartitions)
}

// TestPublisherKeysMessagesForSamePathIdOntoTheSamePartition is the
// real-Kafka-level guarantee behind switching the Writer's Balancer from
// LeastBytes to Hash: on an 8-partition topic (mirroring the Phase 3
// partition scaleup, warehouse-infra PR #42), every event published for
// the SAME PathId must land on the SAME partition, and an event for a
// DIFFERENT PathId is free to land elsewhere. This service's Encode
// already stamped Message.Key with the PathId before this fix — a
// fake-writer unit test asserting Key alone would NOT catch that
// LeastBytes silently ignores it when deciding partition placement, so
// this test runs against a real broker (testcontainers), never a fake
// Writer, following the reference fix's shape (order-management PR #111 /
// ADR 0027, and this repo's own ADR 0013).
func TestPublisherKeysMessagesForSamePathIdOntoTheSamePartition(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID("process-path-management-kafka-itest-partitioning"),
	)
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("terminate Kafka container: %v", err)
		}
	})

	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("resolve Kafka brokers: %v", err)
	}
	const numPartitions = 8
	topic := fmt.Sprintf("warehouse.process-path-management.events.itest-part-%d", time.Now().UnixNano())
	createTopicWithPartitions(t, ctx, brokers, topic, numPartitions)

	publisher := outboundkafka.NewPublisher(brokers, func() string { return fmt.Sprintf("evt-%d", time.Now().UnixNano()) })
	t.Cleanup(func() { _ = publisher.Close() })

	occurredAt := time.Now().UTC().Truncate(time.Second)
	const samePathId = "PICK-itest-same-partition"
	const otherPathId = "PACK-itest-other-partition"

	// Publish 3 events for samePathId (mirrors the real fleet ordering
	// concern: created, then updated, then deactivated) plus 1 for a
	// different path, to prove the key -- not accident -- drives
	// partition placement. Encode is this package's real encoder; only
	// the destination Topic is overridden (via Send) to this test's
	// isolated topic, so the exact production Key derivation is
	// exercised unchanged.
	events := []shared.DomainEvent{
		shared.ProcessPathCreated{PathId: samePathId, MatchPrefix: "pick", Direct: true, At: occurredAt},
		shared.ProcessPathUpdated{PathId: samePathId, MatchPrefix: "pick", Direct: true, At: occurredAt.Add(time.Minute)},
		shared.ProcessPathDeactivated{PathId: samePathId, At: occurredAt.Add(2 * time.Minute)},
		shared.ProcessPathCreated{PathId: otherPathId, MatchPrefix: "pack", Direct: true, At: occurredAt},
	}
	for _, event := range events {
		enc, err := outboundkafka.Encode(event, fmt.Sprintf("evt-%d", time.Now().UnixNano()))
		if err != nil {
			t.Fatalf("encode event for %v: %v", event, err)
		}
		enc.Topic = topic // route this test's encoded messages onto the isolated itest topic
		if err := publisher.Send(ctx, enc); err != nil {
			t.Fatalf("send event: %v", err)
		}
	}

	// Read every message back with its partition, one reader per
	// partition (a single Reader without an explicit Partition only
	// sees whichever partition it's assigned, not all of them).
	partitionOf := map[string]int{}
	for p := 0; p < numPartitions; p++ {
		reader := kafkago.NewReader(kafkago.ReaderConfig{
			Brokers:     brokers,
			Topic:       topic,
			Partition:   p,
			StartOffset: kafkago.FirstOffset,
			MaxWait:     2 * time.Second,
		})
		func() {
			defer func() { _ = reader.Close() }()
			readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
			defer readCancel()
			for {
				msg, err := reader.ReadMessage(readCtx)
				if err != nil {
					return // timeout: no more messages on this partition
				}
				partitionOf[string(msg.Key)] = p
			}
		}()
	}

	if len(partitionOf) != 2 {
		t.Fatalf("observed keys->partition = %v, want exactly 2 distinct keys (samePathId, otherPathId)", partitionOf)
	}
	samePartition, ok := partitionOf[samePathId]
	if !ok {
		t.Fatalf("no message observed with key %q; partitionOf = %v", samePathId, partitionOf)
	}
	if _, ok := partitionOf[otherPathId]; !ok {
		t.Fatalf("no message observed with key %q; partitionOf = %v", otherPathId, partitionOf)
	}

	// Re-verify by reading samePartition alone and counting how many of
	// samePathId's 3 messages landed there -- all 3 must be present,
	// proving the guarantee isn't a one-message coincidence.
	countOnSamePartition := 0
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		Partition:   samePartition,
		StartOffset: kafkago.FirstOffset,
		MaxWait:     2 * time.Second,
	})
	defer func() { _ = reader.Close() }()
	readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
	defer readCancel()
	for {
		msg, err := reader.ReadMessage(readCtx)
		if err != nil {
			break
		}
		if string(msg.Key) == samePathId {
			if _, ceErr := cloudevents.Decode(msg.Value); ceErr != nil {
				t.Fatalf("decode cloudevent: %v", ceErr)
			}
			countOnSamePartition++
		}
	}
	if countOnSamePartition != 3 {
		t.Errorf("found %d of samePathId's 3 messages on partition %d, want 3 (all events for one path must share a partition)", countOnSamePartition, samePartition)
	}
}
