package kafka

import (
	"context"

	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/claudioed/process-path-management/internal/adapters/kafka/cloudevents"
)

// W3C trace-context header names. apis/asyncapi.yaml promises these on
// every message (ADR 0016: "W3C trace context stays in the
// traceparent/tracestate headers"), ADR 0027 records how they are produced.
const (
	HeaderTraceparent = "traceparent"
	HeaderTracestate  = "tracestate"
)

// TraceContext is the W3C trace context (traceparent + tracestate) of the
// operation that raised a domain event. The zero value means "no trace
// context": such a message carries no trace headers at all.
type TraceContext struct {
	Traceparent string
	Tracestate  string
}

// TraceContextFrom captures the trace context carried by ctx with the
// global OpenTelemetry propagator (the W3C TraceContext + Baggage composite
// telemetry.Setup installs). Only the traceparent and tracestate fields are
// kept — baggage is deliberately not put on the wire. A ctx with no active
// span yields the zero TraceContext.
//
// It must be called where the request span is still in ctx: for the
// transactional outbox that is the enqueue (OutboxPublisher.Publish), not
// the relay, whose context belongs to a background loop with no relation to
// the request that raised the event.
func TraceContextFrom(ctx context.Context) TraceContext {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return TraceContext{
		Traceparent: carrier.Get(HeaderTraceparent),
		Tracestate:  carrier.Get(HeaderTracestate),
	}
}

// headers returns the Kafka headers for tc: nothing for the zero value, a
// traceparent header when only that is set, plus tracestate when present.
func (tc TraceContext) headers() []kafkago.Header {
	var out []kafkago.Header
	if tc.Traceparent == "" {
		return out
	}
	out = append(out, kafkago.Header{Key: HeaderTraceparent, Value: []byte(tc.Traceparent)})
	if tc.Tracestate != "" {
		out = append(out, kafkago.Header{Key: HeaderTracestate, Value: []byte(tc.Tracestate)})
	}
	return out
}

// messageHeaders is the full Kafka header set of one encoded message: the
// structured-mode CloudEvents content-type (ADR 0016) followed by the W3C
// trace context when the event was raised inside a traced operation.
func (e Encoded) messageHeaders() []kafkago.Header {
	return append([]kafkago.Header{cloudevents.ContentTypeHeader()}, e.Trace.headers()...)
}
