package kafka

import (
	"context"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/claudioed/process-path-management/internal/domain/shared"
)

const (
	testTraceID     = "4bf92f3577b34da6a3ce929d0e0e4736"
	testSpanID      = "00f067aa0ba902b7"
	wantTraceparent = "00-" + testTraceID + "-" + testSpanID + "-01"
	wantTracestate  = "vendor=1"
)

// useW3CPropagator installs the same W3C TraceContext + Baggage composite
// telemetry.Setup installs in production, and restores the previous global
// propagator when the test ends.
func useW3CPropagator(t *testing.T) {
	t.Helper()
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })
}

// tracedContext returns a context carrying a sampled span context, the shape
// otelchi leaves in the request context of every REST call.
func tracedContext(t *testing.T) context.Context {
	t.Helper()
	traceID, err := trace.TraceIDFromHex(testTraceID)
	if err != nil {
		t.Fatalf("trace id: %v", err)
	}
	spanID, err := trace.SpanIDFromHex(testSpanID)
	if err != nil {
		t.Fatalf("span id: %v", err)
	}
	ts, err := trace.ParseTraceState(wantTracestate)
	if err != nil {
		t.Fatalf("tracestate: %v", err)
	}
	sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled, TraceState: ts})
	return trace.ContextWithSpanContext(context.Background(), sc)
}

func headerValue(msg kafkago.Message, key string) (string, bool) {
	for _, h := range msg.Headers {
		if h.Key == key {
			return string(h.Value), true
		}
	}
	return "", false
}

func assertTraceHeaders(t *testing.T, msg kafkago.Message) {
	t.Helper()
	if got, ok := headerValue(msg, HeaderTraceparent); !ok || got != wantTraceparent {
		t.Fatalf("traceparent header = %q (present=%v), want %q", got, ok, wantTraceparent)
	}
	if got, ok := headerValue(msg, HeaderTracestate); !ok || got != wantTracestate {
		t.Fatalf("tracestate header = %q (present=%v), want %q", got, ok, wantTracestate)
	}
	if ct, ok := headerValue(msg, "content-type"); !ok || ct != "application/cloudevents+json; charset=UTF-8" {
		t.Fatalf("content-type header must stay present next to the trace headers, got %q (present=%v)", ct, ok)
	}
}

func assertNoTraceHeaders(t *testing.T, msg kafkago.Message) {
	t.Helper()
	for _, k := range []string{HeaderTraceparent, HeaderTracestate} {
		if v, ok := headerValue(msg, k); ok {
			t.Fatalf("no active span: header %s must be absent, got %q", k, v)
		}
	}
}

// ADR 0027: the integration Publisher's direct path puts the request's W3C
// trace context in the traceparent/tracestate headers, as apis/asyncapi.yaml
// promises.
func TestPublisher_Publish_PropagatesTraceContextHeaders(t *testing.T) {
	useW3CPropagator(t)
	w := &recordingFanOutWriter{}
	p := &Publisher{Writer: w, NewId: func() string { return "evt-1" }}

	if err := p.Publish(tracedContext(t), shared.ProcessPathCreated{PathId: "PICK", At: time.Now()}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	assertTraceHeaders(t, w.msgs[0])
}

func TestPublisher_PublishWithId_PropagatesTraceContextHeaders(t *testing.T) {
	useW3CPropagator(t)
	w := &recordingFanOutWriter{}
	p := &Publisher{Writer: w, NewId: func() string { return "unused" }}

	if err := p.PublishWithId(tracedContext(t), shared.ProcessPathDeactivated{PathId: "PICK", At: time.Now()}, "evt-2"); err != nil {
		t.Fatalf("publish: %v", err)
	}
	assertTraceHeaders(t, w.msgs[0])
}

// The analytics direct publisher has its own writer path and previously set
// only content-type too.
func TestAnalyticsPublisher_PropagatesTraceContextHeaders(t *testing.T) {
	useW3CPropagator(t)
	w := &recordingFanOutWriter{}
	p := &AnalyticsPublisher{encoder: NewAnalyticsEncoder(func() string { return "evt-3" }), writer: w}
	event := shared.ProcessPathCreated{PathId: "PICK", At: time.Now()}

	if err := p.Publish(tracedContext(t), event); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := p.PublishWithId(tracedContext(t), event, "evt-4"); err != nil {
		t.Fatalf("publish with id: %v", err)
	}
	if len(w.msgs) != 2 {
		t.Fatalf("want 2 messages, got %d", len(w.msgs))
	}
	assertTraceHeaders(t, w.msgs[0])
	assertTraceHeaders(t, w.msgs[1])
}

// Both topics of one occurrence under the no-Postgres fan-out carry the same
// trace context.
func TestFanOut_PropagatesTraceContextToBothTopics(t *testing.T) {
	useW3CPropagator(t)
	iw, aw := &recordingFanOutWriter{}, &recordingFanOutWriter{}
	fan := NewSharedIdFanOut(func() string { return "shared" },
		&Publisher{Writer: iw, NewId: func() string { return "x" }},
		&AnalyticsPublisher{encoder: NewAnalyticsEncoder(func() string { return "x" }), writer: aw})

	if err := fan.Publish(tracedContext(t), shared.ProcessPathCreated{PathId: "PICK", At: time.Now()}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	assertTraceHeaders(t, iw.msgs[0])
	assertTraceHeaders(t, aw.msgs[0])
}

// The outbox relay path: the trace context captured at enqueue time travels
// in Encoded.Trace and Send turns it into headers (the relay's own ctx has
// no span).
func TestPublisher_Send_WritesEncodedTraceContextAsHeaders(t *testing.T) {
	useW3CPropagator(t)
	w := &recordingFanOutWriter{}
	p := &Publisher{Writer: w}
	enc, err := Encode(shared.ProcessPathCreated{PathId: "PICK", At: time.Now()}, "evt-5")
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	enc.Trace = TraceContext{Traceparent: wantTraceparent, Tracestate: wantTracestate}

	if err := p.Send(context.Background(), enc); err != nil {
		t.Fatalf("send: %v", err)
	}
	assertTraceHeaders(t, w.msgs[0])
}

// Without an active span nothing is invented: no traceparent/tracestate
// header, on any publish path.
func TestPublishers_WithoutActiveSpan_OmitTraceHeaders(t *testing.T) {
	useW3CPropagator(t)
	iw, aw := &recordingFanOutWriter{}, &recordingFanOutWriter{}
	event := shared.ProcessPathCreated{PathId: "PICK", At: time.Now()}

	if err := (&Publisher{Writer: iw, NewId: func() string { return "e1" }}).Publish(context.Background(), event); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := (&AnalyticsPublisher{encoder: NewAnalyticsEncoder(func() string { return "e2" }), writer: aw}).Publish(context.Background(), event); err != nil {
		t.Fatalf("analytics publish: %v", err)
	}
	assertNoTraceHeaders(t, iw.msgs[0])
	assertNoTraceHeaders(t, aw.msgs[0])
	if _, ok := headerValue(iw.msgs[0], "content-type"); !ok {
		t.Fatal("content-type header must remain")
	}
}

// Baggage is never put on the wire: only traceparent/tracestate are promised.
func TestTraceContextFrom_OnlyTraceparentAndTracestate(t *testing.T) {
	useW3CPropagator(t)
	tc := TraceContextFrom(tracedContext(t))
	if tc.Traceparent != wantTraceparent || tc.Tracestate != wantTracestate {
		t.Fatalf("TraceContextFrom = %+v", tc)
	}
	if got := TraceContextFrom(context.Background()); got != (TraceContext{}) {
		t.Fatalf("no span: want the zero TraceContext, got %+v", got)
	}
}
