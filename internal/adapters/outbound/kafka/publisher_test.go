package kafka_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/process-path-management/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/process-path-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

type fakeWriter struct {
	messages []kafkago.Message
}

func (w *fakeWriter) WriteMessages(_ context.Context, msgs ...kafkago.Message) error {
	w.messages = append(w.messages, msgs...)
	return nil
}

func TestPublish_ProcessPathCreated_WritesEnvelopeKeyedByPathId(t *testing.T) {
	w := &fakeWriter{}
	p := &outboundkafka.Publisher{Writer: w, NewId: func() string { return "evt-1" }}
	now := time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)

	err := p.Publish(context.Background(), shared.ProcessPathCreated{
		PathId:               "PICK",
		MatchPrefix:          "pick",
		Direct:               true,
		RequiredCapabilities: []shared.Capability{"pick"},
		At:                   now,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(w.messages) != 1 {
		t.Fatalf("want 1 message, got %d", len(w.messages))
	}
	if string(w.messages[0].Key) != "PICK" {
		t.Fatalf("want message key PICK, got %s", w.messages[0].Key)
	}

	e, err := cloudevents.Decode(w.messages[0].Value)
	if err != nil {
		t.Fatalf("decode cloudevent: %v", err)
	}
	if e.Type() != cloudevents.TypeProcessPathCreated {
		t.Fatalf("want type %s, got %s", cloudevents.TypeProcessPathCreated, e.Type())
	}
	if e.Source() != cloudevents.Source {
		t.Fatalf("want source %s, got %s", cloudevents.Source, e.Source())
	}
	if e.Subject() != "PICK" || e.ID() != "evt-1" || !e.Time().Equal(now) {
		t.Fatalf("subject/id/time = %q/%q/%v", e.Subject(), e.ID(), e.Time())
	}
	assertContentTypeHeader(t, w.messages[0])
}

// assertContentTypeHeader asserts msg carries the structured-mode
// CloudEvents content-type header (ADR 0016).
func assertContentTypeHeader(t *testing.T, msg kafkago.Message) {
	t.Helper()
	for _, h := range msg.Headers {
		if h.Key == "content-type" {
			if string(h.Value) != "application/cloudevents+json; charset=UTF-8" {
				t.Fatalf("content-type = %q", h.Value)
			}
			return
		}
	}
	t.Fatal("missing content-type header")
}

func TestPublish_ProcessPathDeactivated_OmitsDefinitionFields(t *testing.T) {
	w := &fakeWriter{}
	p := &outboundkafka.Publisher{Writer: w, NewId: func() string { return "evt-2" }}

	err := p.Publish(context.Background(), shared.ProcessPathDeactivated{
		PathId: "PICK",
		At:     time.Now(),
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
	if _, present := data["match_prefix"]; present {
		t.Fatal("expected match_prefix to be omitted on a Deactivated event")
	}
	if _, present := data["required_capabilities"]; present {
		t.Fatal("expected required_capabilities to be omitted on a Deactivated event")
	}
}

func TestPublish_UnknownEventType_ReturnsError(t *testing.T) {
	w := &fakeWriter{}
	p := &outboundkafka.Publisher{Writer: w, NewId: func() string { return "evt-3" }}

	type unknownEvent struct{}
	err := p.Publish(context.Background(), unmarshalableEvent{})
	_ = unknownEvent{}
	if err == nil {
		t.Fatal("expected an error for an unrecognized event type")
	}
}

// TestPublish_ProcessPathCreated_WithDestinationLocationRole_IsOnTheWire
// proves the optional field is included when the event carries a
// declared destination role (ADR 0006).
func TestPublish_ProcessPathCreated_WithDestinationLocationRole_IsOnTheWire(t *testing.T) {
	w := &fakeWriter{}
	p := &outboundkafka.Publisher{Writer: w, NewId: func() string { return "evt-4" }}

	err := p.Publish(context.Background(), shared.ProcessPathCreated{
		PathId:                  "PACK",
		MatchPrefix:             "pack",
		Direct:                  true,
		RequiredCapabilities:    []shared.Capability{"pack"},
		DestinationLocationRole: shared.DestinationLocationRoleDrop,
		At:                      time.Now(),
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
	var role string
	if err := json.Unmarshal(data["destination_location_role"], &role); err != nil {
		t.Fatalf("unmarshal destination_location_role: %v", err)
	}
	if role != "Drop" {
		t.Fatalf("want destination_location_role Drop, got %q", role)
	}
}

// TestPublish_ProcessPathCreated_WithoutDestinationLocationRole_OmitsField
// proves the field is omitted entirely (not empty-stringed) for a path
// that never declared one -- the default, most common case.
func TestPublish_ProcessPathCreated_WithoutDestinationLocationRole_OmitsField(t *testing.T) {
	w := &fakeWriter{}
	p := &outboundkafka.Publisher{Writer: w, NewId: func() string { return "evt-5" }}

	err := p.Publish(context.Background(), shared.ProcessPathCreated{
		PathId:               "PICK",
		MatchPrefix:          "pick",
		Direct:               true,
		RequiredCapabilities: []shared.Capability{"pick"},
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
	if _, present := data["destination_location_role"]; present {
		t.Fatal("expected destination_location_role to be omitted when unset")
	}
}

type unmarshalableEvent struct{}

func (unmarshalableEvent) EventName() string     { return "Unknown" }
func (unmarshalableEvent) OccurredAt() time.Time { return time.Now() }
