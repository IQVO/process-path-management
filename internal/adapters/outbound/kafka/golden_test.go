package kafka_test

import (
	"context"
	"testing"
	"time"

	outboundkafka "github.com/claudioed/process-path-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/process-path-management/internal/domain/cptschedule"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// Golden wire tests (ADR 0016): the exact structured-mode CloudEvents JSON
// every published event type produces on BOTH streams, plus the Kafka
// content-type header. Any drift in an attribute, the `type` string (a
// cross-service contract consumed by four sibling services), or the
// payload shape fails here byte-for-byte.

const goldenId = "4f1c2a7e-9d31-4a6b-8f0e-6b2c1d5e7a90"

var goldenAt = time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)

func goldenEvents() []struct {
	name  string
	event shared.DomainEvent
	key   string
	data  string
} {
	maxUnits := 1
	return []struct {
		name  string
		event shared.DomainEvent
		key   string
		data  string
	}{
		{
			name: "ProcessPathCreated",
			event: shared.ProcessPathCreated{
				PathId: "PICK", MatchPrefix: "pick", Direct: true,
				RequiredCapabilities: []shared.Capability{"pick"},
				CycleTimeP95:         2 * time.Hour,
				Eligibility:          shared.NewEligibility(&maxUnits, nil, []string{"hazmat"}, false),
				At:                   goldenAt,
			},
			key:  "PICK",
			data: `{"path_id":"PICK","match_prefix":"pick","direct":true,"required_capabilities":["pick"],"cycle_time_p95":"2h0m0s","eligibility":{"max_units_per_line":1,"excluded_product_attributes":["hazmat"]}}`,
		},
		{
			name: "ProcessPathUpdated",
			event: shared.ProcessPathUpdated{
				PathId: "PACK", MatchPrefix: "pack", Direct: false,
				RequiredCapabilities:    []shared.Capability{"pack"},
				DestinationLocationRole: shared.DestinationLocationRoleDrop,
				At:                      goldenAt,
			},
			key:  "PACK",
			data: `{"path_id":"PACK","match_prefix":"pack","required_capabilities":["pack"],"destination_location_role":"Drop","cycle_time_p95":"0s","eligibility":{}}`,
		},
		{
			name:  "ProcessPathDeactivated",
			event: shared.ProcessPathDeactivated{PathId: "PICK", At: goldenAt},
			key:   "PICK",
			data:  `{"path_id":"PICK"}`,
		},
		{
			name: "CPTScheduleChanged",
			event: cptschedule.CPTScheduleChanged{
				SiteId: "sp1", Timezone: "America/Sao_Paulo",
				Cutoffs: []cptschedule.CutoffSnapshot{{
					CptId: "sp1-1500", LocalTime: "15:00",
					DaysOfWeek:      []cptschedule.Weekday{cptschedule.Monday},
					ShipMethod:      "ground",
					EligiblePathIds: []shared.PathId{"PICK"},
				}},
				At: goldenAt,
			},
			key:  "sp1",
			data: `{"site_id":"sp1","timezone":"America/Sao_Paulo","cutoffs":[{"cpt_id":"sp1-1500","local_time":"15:00","days_of_week":["Mon"],"ship_method":"ground","eligible_path_ids":["PICK"]}]}`,
		},
	}
}

func goldenEntity(name string) string {
	if name == "CPTScheduleChanged" {
		return "cptschedule"
	}
	return "processpath"
}

func goldenJSON(name, stream, subject, data string) string {
	return `{"specversion":"1.0","id":"` + goldenId + `","source":"/warehouse/process-path-management",` +
		`"type":"com.warehouse.wes.process-path-management.` + goldenEntity(name) + `.` + name + `",` +
		`"subject":"` + subject + `","datacontenttype":"application/json",` +
		`"dataschema":"urn:warehouse:process-path-management:` + stream + `:` + name + `:v1",` +
		`"time":"2026-09-30T12:00:00Z","data":` + data + `}`
}

func TestGolden_IntegrationStream_ExactCloudEventsJSON(t *testing.T) {
	for _, tc := range goldenEvents() {
		t.Run(tc.name, func(t *testing.T) {
			w := &fakeWriter{}
			p := &outboundkafka.Publisher{Writer: w, NewId: func() string { return goldenId }}
			if err := p.Publish(context.Background(), tc.event); err != nil {
				t.Fatalf("publish: %v", err)
			}
			msg := w.messages[0]
			if msg.Topic != outboundkafka.Topic || string(msg.Key) != tc.key {
				t.Fatalf("topic/key = %s/%s", msg.Topic, msg.Key)
			}
			if want := goldenJSON(tc.name, "events", tc.key, tc.data); string(msg.Value) != want {
				t.Fatalf("wire mismatch\n got: %s\nwant: %s", msg.Value, want)
			}
			assertContentTypeHeader(t, msg)
		})
	}
}

func TestGolden_AnalyticsStream_ExactCloudEventsJSON(t *testing.T) {
	for _, tc := range goldenEvents() {
		t.Run(tc.name, func(t *testing.T) {
			enc, err := outboundkafka.NewAnalyticsEncoder(func() string { return goldenId }).Encode(tc.event, goldenId)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			if enc.Topic != outboundkafka.AnalyticsTopic || enc.Key != tc.key || enc.EventId != goldenId {
				t.Fatalf("topic/key/id = %s/%s/%s", enc.Topic, enc.Key, enc.EventId)
			}
			if want := goldenJSON(tc.name, "analytics", tc.key, tc.data); string(enc.Value) != want {
				t.Fatalf("wire mismatch\n got: %s\nwant: %s", enc.Value, want)
			}
			// The relay sends outbox rows through Publisher.Send, which
			// must attach the content-type header on this path too.
			w := &fakeWriter{}
			if err := (&outboundkafka.Publisher{Writer: w}).Send(context.Background(), enc); err != nil {
				t.Fatalf("send: %v", err)
			}
			assertContentTypeHeader(t, w.messages[0])
		})
	}
}
