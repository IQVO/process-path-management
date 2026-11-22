package kafka

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	kafkago "github.com/segmentio/kafka-go"
)

// TestAnalyticsConsumerLogsNoEventDerivedLineBreaks feeds the analytics
// consumer a payload that is not a CloudEvent (dead-lettered) and dead-letters
// an event whose id, type and apply error carry CR/LF: no log attribute may
// carry the line break.
func TestAnalyticsConsumerLogsNoEventDerivedLineBreaks(t *testing.T) {
	rec := newCRLFRecorder()
	// A reader without a group never dials; its commit just fails, which the
	// test ignores.
	reader := kafkago.NewReader(kafkago.ReaderConfig{Brokers: []string{"127.0.0.1:1"}, Topic: "logsafe.topic"})
	t.Cleanup(func() { _ = reader.Close() })
	c := &AnalyticsConsumer{Reader: reader, Logger: slog.New(rec)}
	ctx := context.Background()
	msg := kafkago.Message{Topic: "logsafe.topic", Key: []byte("key" + crlfForged), Value: []byte("not a CloudEvent" + crlfForged)}
	_ = c.handleFetchedMessage(ctx, msg)
	env := analyticsEvent{Id: "id" + crlfForged, Type: "logsafe.Evil" + crlfForged}
	_ = c.deadLetterAndCommit(ctx, msg, env, errors.New("claim and apply: refused"+crlfForged))
	rec.assertNoLineBreaks(t, 3)
}
