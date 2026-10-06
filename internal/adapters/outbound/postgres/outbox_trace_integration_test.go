//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	outboundkafka "github.com/claudioed/process-path-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

const (
	outboxTraceID     = "4bf92f3577b34da6a3ce929d0e0e4736"
	outboxSpanID      = "00f067aa0ba902b7"
	outboxTraceparent = "00-" + outboxTraceID + "-" + outboxSpanID + "-01"
	outboxTracestate  = "vendor=1"
)

// ADR 0027: the request's W3C trace context is captured when the event is
// enqueued (the relay's context has no relation to the request) and
// travels through outbox_events to the relay's Sink in Encoded.Trace, from
// where Publisher.Send turns it into traceparent/tracestate headers.
func TestOutbox_TraceContextSurvivesTheRelay(t *testing.T) {
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })

	pool := outboxDB(t)
	traceID, _ := trace.TraceIDFromHex(outboxTraceID)
	spanID, _ := trace.SpanIDFromHex(outboxSpanID)
	ts, err := trace.ParseTraceState(outboxTracestate)
	if err != nil {
		t.Fatalf("tracestate: %v", err)
	}
	traced := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled, TraceState: ts,
	}))

	pub := postgres.NewOutboxPublisher(pool, uuid.NewString,
		outboundkafka.IntegrationEncoder{}, outboundkafka.NewAnalyticsEncoder(uuid.NewString))
	now := time.Now().UTC().Truncate(time.Microsecond)
	if err := pub.Publish(traced, shared.ProcessPathCreated{PathId: "TRACED", At: now}); err != nil {
		t.Fatalf("publish traced: %v", err)
	}
	// An event raised with no active span must carry no trace context.
	if err := pub.Publish(context.Background(), shared.ProcessPathCreated{PathId: "UNTRACED", At: now}); err != nil {
		t.Fatalf("publish untraced: %v", err)
	}

	sink := &recordingSink{}
	relay := postgres.NewOutboxRelay(pool, sink, nil)
	// The relay runs under a background context: it must not matter.
	if n, err := relay.RelayOnce(context.Background()); err != nil || n != 4 {
		t.Fatalf("RelayOnce = %d, %v; want 4 rows", n, err)
	}

	want := outboundkafka.TraceContext{Traceparent: outboxTraceparent, Tracestate: outboxTracestate}
	traced4, untraced := 0, 0
	for _, enc := range sink.sent {
		switch enc.Key {
		case "TRACED":
			traced4++
			if enc.Trace != want {
				t.Fatalf("TRACED row (%s): trace = %+v, want %+v", enc.Topic, enc.Trace, want)
			}
		case "UNTRACED":
			untraced++
			if enc.Trace != (outboundkafka.TraceContext{}) {
				t.Fatalf("UNTRACED row (%s): trace = %+v, want zero", enc.Topic, enc.Trace)
			}
		}
	}
	if traced4 != 2 || untraced != 2 {
		t.Fatalf("want 2 traced + 2 untraced rows (integration + analytics topics), got %d + %d", traced4, untraced)
	}
}
