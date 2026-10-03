package kafka_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/claudioed/process-path-management/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/process-path-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

func TestAnalyticsEncoder_Encode_ProcessPathCreated(t *testing.T) {
	enc := outboundkafka.NewAnalyticsEncoder(func() string { return "evt-1" })
	now := time.Date(2026, 9, 11, 0, 0, 0, 0, time.UTC)

	got, err := enc.Encode(shared.ProcessPathCreated{
		PathId:               "PICK",
		MatchPrefix:          "pick",
		Direct:               true,
		RequiredCapabilities: []shared.Capability{"pick"},
		At:                   now,
	}, "evt-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Topic != outboundkafka.AnalyticsTopic {
		t.Fatalf("topic = %q, want %q", got.Topic, outboundkafka.AnalyticsTopic)
	}
	if got.Key != "PICK" {
		t.Fatalf("key = %q, want PICK", got.Key)
	}
	if got.EventType != cloudevents.TypeProcessPathCreated {
		t.Fatalf("event type = %q, want %q", got.EventType, cloudevents.TypeProcessPathCreated)
	}

	e, err := cloudevents.Decode(got.Value)
	if err != nil {
		t.Fatalf("decode cloudevent: %v", err)
	}
	if e.ID() != "evt-1" {
		t.Fatalf("id = %q, want evt-1", e.ID())
	}
	if e.Source() != cloudevents.Source {
		t.Fatalf("source = %q, want %q", e.Source(), cloudevents.Source)
	}
	if e.DataSchema() != "urn:warehouse:process-path-management:analytics:ProcessPathCreated:v1" {
		t.Fatalf("dataschema = %q", e.DataSchema())
	}
	if !e.Time().Equal(now) {
		t.Fatalf("time = %v, want %v", e.Time(), now)
	}

	var data map[string]json.RawMessage
	if err := e.DataAs(&data); err != nil {
		t.Fatalf("DataAs: %v", err)
	}
	if _, ok := data["match_prefix"]; !ok {
		t.Fatal("expected match_prefix in analytics data for ProcessPathCreated")
	}
}

func TestAnalyticsEncoder_Encode_ProcessPathDeactivated_OmitsDefinitionFields(t *testing.T) {
	enc := outboundkafka.NewAnalyticsEncoder(func() string { return "evt-2" })

	got, err := enc.Encode(shared.ProcessPathDeactivated{
		PathId: "PICK",
		At:     time.Now(),
	}, "evt-2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	e, err := cloudevents.Decode(got.Value)
	if err != nil {
		t.Fatalf("decode cloudevent: %v", err)
	}
	var data map[string]json.RawMessage
	if err := e.DataAs(&data); err != nil {
		t.Fatalf("DataAs: %v", err)
	}
	if _, present := data["match_prefix"]; present {
		t.Fatal("expected match_prefix to be omitted on a Deactivated analytics event")
	}
	if _, present := data["required_capabilities"]; present {
		t.Fatal("expected required_capabilities to be omitted on a Deactivated analytics event")
	}
}

func TestAnalyticsEncoder_Encode_UnknownEventType_ReturnsError(t *testing.T) {
	enc := outboundkafka.NewAnalyticsEncoder(func() string { return "evt-3" })

	_, err := enc.Encode(unmarshalableEvent{}, "evt-3")
	if err == nil {
		t.Fatal("expected an error for an unrecognized event type")
	}
}

func TestAnalyticsEncoder_SatisfiesEncoderInterface(t *testing.T) {
	var _ outboundkafka.Encoder = outboundkafka.NewAnalyticsEncoder(func() string { return "x" })
}
