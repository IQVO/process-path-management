package telemetry

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// sumsByAttrs collects the republish counter's data points keyed "kind/outcome".
func republishSums(t *testing.T, reader *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	out := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != catalogueRepublishCounterName {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("%s is %T, want an int64 sum", m.Name, m.Data)
			}
			for _, dp := range sum.DataPoints {
				kind, _ := dp.Attributes.Value(kindKey)
				outcome, _ := dp.Attributes.Value(outcomeKey)
				out[kind.AsString()+"/"+outcome.AsString()] += dp.Value
			}
		}
	}
	return out
}

func TestCatalogueRepublishMetrics_RecordKindAndOutcome(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(prev) })

	m, err := NewPathMetrics()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	m.CatalogueRepublished(ctx, "path", 7)
	m.CatalogueRepublished(ctx, "schedule", 2)
	m.CatalogueRepublishFailed(ctx)

	got := republishSums(t, reader)
	want := map[string]int64{"path/accepted": 7, "schedule/accepted": 2, "/failed": 1}
	if len(got) != len(want) {
		t.Fatalf("data points = %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("data points = %v, want %v", got, want)
		}
	}
}

// A nil *PathMetrics is the documented "not instrumented" value.
func TestCatalogueRepublishMetrics_NilReceiverIsANoOp(t *testing.T) {
	var m *PathMetrics
	m.CatalogueRepublished(context.Background(), "path", 1)
	m.CatalogueRepublishFailed(context.Background())
}
