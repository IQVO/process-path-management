package kafka_test

import (
	"context"
	"testing"
	"time"

	outboundkafka "github.com/claudioed/process-path-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// TestGolden_DispatchPathCreated pins the exact CloudEvent the declared
// DISPATCH path (catalogue/paths.json, ADR 0035) publishes on both streams.
// wes-work-planning and fulfillment-execution read
// match_prefix/direct/required_capabilities from it (the path id becomes the
// fulfillment-execution task type, so it must be DISPATCH); order-management
// reads intake (ADR 0034) and must see STOCK_TRANSFER only, which keeps
// customer order lines off this path.
func TestGolden_DispatchPathCreated(t *testing.T) {
	event := shared.ProcessPathCreated{
		PathId: "DISPATCH", MatchPrefix: "dispatch", Direct: true,
		RequiredCapabilities: []shared.Capability{"dispatch"},
		CycleTimeP95:         2 * time.Hour,
		Intake:               shared.MustIntake(shared.DemandSourceStockTransfer),
		At:                   goldenAt,
	}
	data := `{"path_id":"DISPATCH","match_prefix":"dispatch","direct":true,"required_capabilities":["dispatch"],"cycle_time_p95":"2h0m0s","eligibility":{},"intake":["STOCK_TRANSFER"]}`

	w := &fakeWriter{}
	p := &outboundkafka.Publisher{Writer: w, NewId: func() string { return goldenId }}
	if err := p.Publish(context.Background(), event); err != nil {
		t.Fatalf("publish: %v", err)
	}
	msg := w.messages[0]
	if msg.Topic != outboundkafka.Topic || string(msg.Key) != "DISPATCH" {
		t.Fatalf("topic/key = %s/%s, want %s/DISPATCH", msg.Topic, msg.Key, outboundkafka.Topic)
	}
	if want := goldenJSON("ProcessPathCreated", "events", "DISPATCH", data); string(msg.Value) != want {
		t.Fatalf("integration wire mismatch\n got: %s\nwant: %s", msg.Value, want)
	}

	enc, err := outboundkafka.NewAnalyticsEncoder(func() string { return goldenId }).Encode(event, goldenId)
	if err != nil {
		t.Fatalf("analytics encode: %v", err)
	}
	if want := goldenJSON("ProcessPathCreated", "analytics", "DISPATCH", data); string(enc.Value) != want {
		t.Fatalf("analytics wire mismatch\n got: %s\nwant: %s", enc.Value, want)
	}
}
