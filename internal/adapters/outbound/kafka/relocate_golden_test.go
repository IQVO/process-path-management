package kafka_test

import (
	"context"
	"testing"
	"time"

	outboundkafka "github.com/claudioed/process-path-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// TestGolden_RelocatePathCreated pins the exact CloudEvent the declared
// RELOCATE path (catalogue/paths.json, ADR 0033) publishes on both streams.
// wes-work-planning, fulfillment-execution and workforce-management read
// match_prefix/direct/required_capabilities from it; order-management reads
// intake (ADR 0034) and must see SLOT_RELOCATION only, which keeps customer
// order lines off this path without any fake eligibility attribute.
func TestGolden_RelocatePathCreated(t *testing.T) {
	event := shared.ProcessPathCreated{
		PathId: "RELOCATE", MatchPrefix: "relocate", Direct: true,
		RequiredCapabilities: []shared.Capability{"relocate"},
		CycleTimeP95:         2 * time.Hour,
		Intake:               shared.MustIntake(shared.DemandSourceSlotRelocation),
		At:                   goldenAt,
	}
	data := `{"path_id":"RELOCATE","match_prefix":"relocate","direct":true,"required_capabilities":["relocate"],"cycle_time_p95":"2h0m0s","eligibility":{},"intake":["SLOT_RELOCATION"]}`

	w := &fakeWriter{}
	p := &outboundkafka.Publisher{Writer: w, NewId: func() string { return goldenId }}
	if err := p.Publish(context.Background(), event); err != nil {
		t.Fatalf("publish: %v", err)
	}
	msg := w.messages[0]
	if msg.Topic != outboundkafka.Topic || string(msg.Key) != "RELOCATE" {
		t.Fatalf("topic/key = %s/%s, want %s/RELOCATE", msg.Topic, msg.Key, outboundkafka.Topic)
	}
	if want := goldenJSON("ProcessPathCreated", "events", "RELOCATE", data); string(msg.Value) != want {
		t.Fatalf("integration wire mismatch\n got: %s\nwant: %s", msg.Value, want)
	}

	enc, err := outboundkafka.NewAnalyticsEncoder(func() string { return goldenId }).Encode(event, goldenId)
	if err != nil {
		t.Fatalf("analytics encode: %v", err)
	}
	if want := goldenJSON("ProcessPathCreated", "analytics", "RELOCATE", data); string(enc.Value) != want {
		t.Fatalf("analytics wire mismatch\n got: %s\nwant: %s", enc.Value, want)
	}
}
