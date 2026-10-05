package telemetry

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// meterName scopes this service's own instruments, keeping them distinct
// from the ones otelchi and the runtime collector register.
const meterName = "github.com/claudioed/process-path-management"

// pathDefinitionCounterName is the business metric: how often an
// operator's attempt to define a new process path actually takes effect.
// A rejection rate climbing means the operator SPA (or a scripted
// caller) is submitting malformed path definitions — a proxy for a
// broken input form, not a transient failure. Matches the standard-
// metrics convention's naming rule (ADR 0019:
// <context>.<aggregate>.<verb>) exactly.
const pathDefinitionCounterName = "process_path_management.paths.defined"

// outcomeKey distinguishes accepted from rejected definition attempts on
// the single counter, per the standard-metrics convention (ADR 0019) of
// one counter with an outcome attribute over two separately-named
// counters for the same event's success/failure split.
const outcomeKey = attribute.Key("outcome")

const (
	outcomeAccepted = "accepted"
	outcomeRejected = "rejected"
)

// PathMetrics implements ports.PathMetrics against the global
// MeterProvider. Until Setup installs a real provider, the global one is a
// no-op, so recording is cheap and safe in tests and local runs.
type PathMetrics struct {
	counter metric.Int64Counter
}

// NewPathMetrics registers the paths-defined counter. It only fails if the
// instrument name is invalid, which is a programming error, not a runtime
// condition — callers that would rather run un-instrumented than not at
// all can ignore the error and pass a nil *PathMetrics instead (its
// methods are nil-safe, see below).
func NewPathMetrics() (*PathMetrics, error) {
	counter, err := otel.Meter(meterName).Int64Counter(
		pathDefinitionCounterName,
		metric.WithDescription("Attempts to define a new process path, by outcome (accepted or rejected)."),
		metric.WithUnit("{path}"),
	)
	if err != nil {
		return nil, err
	}
	return &PathMetrics{counter: counter}, nil
}

// PathDefinitionAccepted records an accepted DefinePath call. Nil-safe:
// a nil *PathMetrics receiver is a documented no-op, matching
// inventory-storage's ReservationMetrics nil-safety convention.
func (m *PathMetrics) PathDefinitionAccepted(ctx context.Context) {
	if m == nil {
		return
	}
	m.record(ctx, outcomeAccepted)
}

// PathDefinitionRejected records a rejected DefinePath call (invalid
// input or a duplicate PathId).
func (m *PathMetrics) PathDefinitionRejected(ctx context.Context) {
	if m == nil {
		return
	}
	m.record(ctx, outcomeRejected)
}

func (m *PathMetrics) record(ctx context.Context, outcome string) {
	m.counter.Add(ctx, 1, metric.WithAttributes(outcomeKey.String(outcome)))
}
