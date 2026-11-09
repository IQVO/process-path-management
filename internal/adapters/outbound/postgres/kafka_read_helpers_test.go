//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// readFromStart reads exactly n messages of topic from the first offset
// (bounded by ctx). The broker is always one a testcontainers helper of this
// package started.
func readFromStart(t *testing.T, ctx context.Context, brokers []string, topic string, n int) []kafkago.Message {
	t.Helper()
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers, Topic: topic, StartOffset: kafkago.FirstOffset, MaxWait: 2 * time.Second,
	})
	defer func() { _ = reader.Close() }()
	out := make([]kafkago.Message, 0, n)
	for len(out) < n {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			t.Fatalf("read %s (%d of %d): %v", topic, len(out), n, err)
		}
		out = append(out, msg)
	}
	return out
}
