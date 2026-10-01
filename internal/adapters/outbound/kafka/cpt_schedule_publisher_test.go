package kafka_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/claudioed/process-path-management/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/process-path-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/process-path-management/internal/domain/cptschedule"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

func TestPublish_ProcessPathCreated_WithCycleTimeP95AndEligibility_IsOnTheWire(t *testing.T) {
	w := &fakeWriter{}
	p := &outboundkafka.Publisher{Writer: w, NewId: func() string { return "evt-6" }}
	maxUnits := 1
	eligibility := shared.NewEligibility(&maxUnits, []string{"giftWrap"}, []string{"hazmat"}, true)

	err := p.Publish(context.Background(), shared.ProcessPathCreated{
		PathId:               "SINGLES",
		MatchPrefix:          "singles",
		Direct:               true,
		RequiredCapabilities: []shared.Capability{"pick"},
		CycleTimeP95:         90 * time.Minute,
		Eligibility:          eligibility,
		At:                   time.Now(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(w.messages[0].Value, &raw); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	var data map[string]json.RawMessage
	if err := json.Unmarshal(raw["data"], &data); err != nil {
		t.Fatalf("unmarshal data: %v", err)
	}
	var cycleTime string
	if err := json.Unmarshal(data["cycle_time_p95"], &cycleTime); err != nil {
		t.Fatalf("unmarshal cycle_time_p95: %v", err)
	}
	if cycleTime != "1h30m0s" {
		t.Fatalf("want cycle_time_p95 1h30m0s, got %q", cycleTime)
	}

	var elig map[string]json.RawMessage
	if err := json.Unmarshal(data["eligibility"], &elig); err != nil {
		t.Fatalf("unmarshal eligibility: %v", err)
	}
	var maxUnitsGot int
	if err := json.Unmarshal(elig["max_units_per_line"], &maxUnitsGot); err != nil {
		t.Fatalf("unmarshal max_units_per_line: %v", err)
	}
	if maxUnitsGot != 1 {
		t.Fatalf("want max_units_per_line 1, got %d", maxUnitsGot)
	}
	var nonSortable bool
	if err := json.Unmarshal(elig["non_sortable"], &nonSortable); err != nil {
		t.Fatalf("unmarshal non_sortable: %v", err)
	}
	if !nonSortable {
		t.Fatal("want non_sortable true")
	}
}

func TestPublish_ProcessPathCreated_PermissiveEligibility_OmitsSubFields(t *testing.T) {
	w := &fakeWriter{}
	p := &outboundkafka.Publisher{Writer: w, NewId: func() string { return "evt-7" }}

	err := p.Publish(context.Background(), shared.ProcessPathCreated{
		PathId:               "PICK",
		MatchPrefix:          "pick",
		Direct:               true,
		RequiredCapabilities: []shared.Capability{"pick"},
		CycleTimeP95:         2 * time.Hour,
		Eligibility:          shared.Eligibility{},
		At:                   time.Now(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(w.messages[0].Value, &raw); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	var data map[string]json.RawMessage
	if err := json.Unmarshal(raw["data"], &data); err != nil {
		t.Fatalf("unmarshal data: %v", err)
	}
	var elig map[string]json.RawMessage
	if err := json.Unmarshal(data["eligibility"], &elig); err != nil {
		t.Fatalf("unmarshal eligibility: %v", err)
	}
	if len(elig) != 0 {
		t.Fatalf("want an empty eligibility object on the wire for a permissive value, got %+v", elig)
	}
}

func TestPublish_ProcessPathUpdated_CarriesCycleTimeP95(t *testing.T) {
	w := &fakeWriter{}
	p := &outboundkafka.Publisher{Writer: w, NewId: func() string { return "evt-8" }}

	err := p.Publish(context.Background(), shared.ProcessPathUpdated{
		PathId:               "PICK",
		MatchPrefix:          "pick-zone-a",
		Direct:               true,
		RequiredCapabilities: []shared.Capability{"pick", "hazmat"},
		CycleTimeP95:         3 * time.Hour,
		Eligibility:          shared.Eligibility{},
		At:                   time.Now(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(w.messages[0].Value, &raw); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	var data map[string]json.RawMessage
	if err := json.Unmarshal(raw["data"], &data); err != nil {
		t.Fatalf("unmarshal data: %v", err)
	}
	var cycleTime string
	if err := json.Unmarshal(data["cycle_time_p95"], &cycleTime); err != nil {
		t.Fatalf("unmarshal cycle_time_p95: %v", err)
	}
	if cycleTime != "3h0m0s" {
		t.Fatalf("want cycle_time_p95 3h0m0s, got %q", cycleTime)
	}
}

func TestPublish_CPTScheduleChanged_WritesFullSnapshotEnvelope(t *testing.T) {
	w := &fakeWriter{}
	p := &outboundkafka.Publisher{Writer: w, NewId: func() string { return "evt-9" }}

	err := p.Publish(context.Background(), cptschedule.CPTScheduleChanged{
		SiteId:   "sp1",
		Timezone: "America/Sao_Paulo",
		Cutoffs: []cptschedule.CutoffSnapshot{
			{
				CptId:           "sp1-1500",
				LocalTime:       "15:00",
				DaysOfWeek:      []cptschedule.Weekday{cptschedule.Monday, cptschedule.Tuesday},
				ShipMethod:      "ground",
				EligiblePathIds: []shared.PathId{"PICK", "PACK"},
			},
		},
		At: time.Now(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(w.messages) != 1 {
		t.Fatalf("want 1 message, got %d", len(w.messages))
	}
	if string(w.messages[0].Key) != "sp1" {
		t.Fatalf("want message key sp1, got %s", w.messages[0].Key)
	}

	e, err := cloudevents.Decode(w.messages[0].Value)
	if err != nil {
		t.Fatalf("decode cloudevent: %v", err)
	}
	if e.Type() != cloudevents.TypeCPTScheduleChanged {
		t.Fatalf("want type %s, got %s", cloudevents.TypeCPTScheduleChanged, e.Type())
	}
	if e.Subject() != "sp1" {
		t.Fatalf("want subject sp1, got %s", e.Subject())
	}
	assertContentTypeHeader(t, w.messages[0])

	var data outboundkafka.CPTScheduleData
	if err := e.DataAs(&data); err != nil {
		t.Fatalf("DataAs CPTScheduleData: %v", err)
	}
	if data.SiteId != "sp1" || data.Timezone != "America/Sao_Paulo" {
		t.Fatalf("unexpected header: %+v", data)
	}
	if len(data.Cutoffs) != 1 {
		t.Fatalf("want 1 cutoff, got %d", len(data.Cutoffs))
	}
	got := data.Cutoffs[0]
	if got.CptId != "sp1-1500" || got.LocalTime != "15:00" || got.ShipMethod != "ground" {
		t.Fatalf("unexpected cutoff: %+v", got)
	}
	if len(got.EligiblePathIds) != 2 {
		t.Fatalf("want 2 eligiblePathIds, got %v", got.EligiblePathIds)
	}
}

func TestPublish_CPTScheduleChanged_GoesThroughOutboxSameAsProcessPathEvents(t *testing.T) {
	// Encode is the same function the outbox publisher calls -- proving
	// it accepts CPTScheduleChanged without error is sufficient evidence
	// that the outbox path (ADR 0003) handles the new event type
	// identically to the existing ones, since OutboxPublisher.Publish
	// simply calls each configured Encoder.Encode and inserts the
	// result -- no event-type-specific branching lives there.
	enc, err := outboundkafka.Encode(cptschedule.CPTScheduleChanged{
		SiteId:   "sp1",
		Timezone: "America/Sao_Paulo",
		Cutoffs: []cptschedule.CutoffSnapshot{
			{CptId: "sp1-1500", LocalTime: "15:00", DaysOfWeek: []cptschedule.Weekday{cptschedule.Monday}, ShipMethod: "ground", EligiblePathIds: []shared.PathId{"PICK"}},
		},
		At: time.Now(),
	}, "evt-10")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if enc.Topic != outboundkafka.Topic {
		t.Fatalf("want topic %s, got %s", outboundkafka.Topic, enc.Topic)
	}
	if enc.EventType != cloudevents.TypeCPTScheduleChanged {
		t.Fatalf("want event type %s, got %s", cloudevents.TypeCPTScheduleChanged, enc.EventType)
	}
}
