package kafka_test

import (
	"context"
	"testing"
	"time"

	outboundkafka "github.com/claudioed/process-path-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// TestGolden_CaseFlowPathsCreated pins the exact CloudEvent each declared
// case-flow path (catalogue/paths.json, ADR 0038) publishes on both
// streams. The consumers read match_prefix/direct/required_capabilities
// (the path id becomes the fulfillment-execution task type for BREAK_CASE),
// order-management reads eligibility and intake to route customer order
// lines, and the new eligibility attributes and the Shipping destination
// role are additive fields of the unchanged v1 payload.
func TestGolden_CaseFlowPathsCreated(t *testing.T) {
	cases := []struct {
		id    string
		event shared.ProcessPathCreated
		data  string
	}{
		{
			id: "FULL_CASE",
			event: shared.ProcessPathCreated{
				PathId: "FULL_CASE", MatchPrefix: "full-case", Direct: true,
				RequiredCapabilities: []shared.Capability{"pick"},
				CycleTimeP95:         90 * time.Minute,
				Eligibility:          shared.NewEligibility(nil, []string{"fullCase"}, nil, false),
				Intake:               shared.MustIntake(shared.DemandSourceCustomerOrder),
				At:                   goldenAt,
			},
			data: `{"path_id":"FULL_CASE","match_prefix":"full-case","direct":true,"required_capabilities":["pick"],"cycle_time_p95":"1h30m0s","eligibility":{"required_product_attributes":["fullCase"]},"intake":["CUSTOMER_ORDER"]}`,
		},
		{
			id: "CROSS_DOCK",
			event: shared.ProcessPathCreated{
				PathId: "CROSS_DOCK", MatchPrefix: "cross-dock", Direct: true,
				RequiredCapabilities:    []shared.Capability{"dispatch"},
				DestinationLocationRole: shared.DestinationLocationRoleShipping,
				CycleTimeP95:            time.Hour,
				Eligibility:             shared.NewEligibility(nil, []string{"crossDock"}, nil, false),
				Intake:                  shared.MustIntake(shared.DemandSourceCustomerOrder, shared.DemandSourceStockTransfer),
				At:                      goldenAt,
			},
			data: `{"path_id":"CROSS_DOCK","match_prefix":"cross-dock","direct":true,"required_capabilities":["dispatch"],"destination_location_role":"Shipping","cycle_time_p95":"1h0m0s","eligibility":{"required_product_attributes":["crossDock"]},"intake":["CUSTOMER_ORDER","STOCK_TRANSFER"]}`,
		},
		{
			id: "BREAK_CASE",
			event: shared.ProcessPathCreated{
				PathId: "BREAK_CASE", MatchPrefix: "break", Direct: true,
				RequiredCapabilities: []shared.Capability{"break"},
				CycleTimeP95:         2 * time.Hour,
				Intake:               shared.MustIntake(),
				At:                   goldenAt,
			},
			data: `{"path_id":"BREAK_CASE","match_prefix":"break","direct":true,"required_capabilities":["break"],"cycle_time_p95":"2h0m0s","eligibility":{},"intake":[]}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.id, func(t *testing.T) {
			w := &fakeWriter{}
			p := &outboundkafka.Publisher{Writer: w, NewId: func() string { return goldenId }}
			if err := p.Publish(context.Background(), tc.event); err != nil {
				t.Fatalf("publish: %v", err)
			}
			msg := w.messages[0]
			if msg.Topic != outboundkafka.Topic || string(msg.Key) != tc.id {
				t.Fatalf("topic/key = %s/%s, want %s/%s", msg.Topic, msg.Key, outboundkafka.Topic, tc.id)
			}
			if want := goldenJSON("ProcessPathCreated", "events", tc.id, tc.data); string(msg.Value) != want {
				t.Fatalf("integration wire mismatch\n got: %s\nwant: %s", msg.Value, want)
			}
			enc, err := outboundkafka.NewAnalyticsEncoder(func() string { return goldenId }).Encode(tc.event, goldenId)
			if err != nil {
				t.Fatalf("analytics encode: %v", err)
			}
			if want := goldenJSON("ProcessPathCreated", "analytics", tc.id, tc.data); string(enc.Value) != want {
				t.Fatalf("analytics wire mismatch\n got: %s\nwant: %s", enc.Value, want)
			}
		})
	}
}
