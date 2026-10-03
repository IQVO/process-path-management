package cloudevents_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/process-path-management/internal/adapters/kafka/cloudevents"
)

func TestTypeAndDataSchema(t *testing.T) {
	if got, want := cloudevents.Type("processpath", "ProcessPathCreated"), "com.warehouse.wes.process-path-management.processpath.ProcessPathCreated"; got != want {
		t.Fatalf("Type = %q, want %q", got, want)
	}
	if got, want := cloudevents.DataSchema(cloudevents.StreamAnalytics, "CPTScheduleChanged", 1), "urn:warehouse:process-path-management:analytics:CPTScheduleChanged:v1"; got != want {
		t.Fatalf("DataSchema = %q, want %q", got, want)
	}
	if cloudevents.Source != "/warehouse/process-path-management" {
		t.Fatalf("Source = %q", cloudevents.Source)
	}
}

func TestPublishedTypeCatalogue(t *testing.T) {
	cases := map[string]string{
		cloudevents.TypeProcessPathCreated:     cloudevents.Type(cloudevents.EntityProcessPath, "ProcessPathCreated"),
		cloudevents.TypeProcessPathUpdated:     cloudevents.Type(cloudevents.EntityProcessPath, "ProcessPathUpdated"),
		cloudevents.TypeProcessPathDeactivated: cloudevents.Type(cloudevents.EntityProcessPath, "ProcessPathDeactivated"),
		cloudevents.TypeCPTScheduleChanged:     cloudevents.Type(cloudevents.EntityCPTSchedule, "CPTScheduleChanged"),
	}
	for literal, built := range cases {
		if literal != built {
			t.Errorf("type constant %q != Type(...) %q", literal, built)
		}
	}
}

func TestNewThenDecodeRoundTrip(t *testing.T) {
	at := time.Date(2026, 9, 30, 12, 0, 0, 0, time.FixedZone("x", -3*3600))
	raw, err := cloudevents.New(cloudevents.Spec{
		ID: "11111111-1111-4111-8111-111111111111", Entity: "processpath", EventName: "ProcessPathCreated",
		Subject: "PICK", Time: at, Stream: cloudevents.StreamEvents, Data: map[string]string{"path_id": "PICK"},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"specversion":"1.0","id":"11111111-1111-4111-8111-111111111111","source":"/warehouse/process-path-management","type":"com.warehouse.wes.process-path-management.processpath.ProcessPathCreated","subject":"PICK","datacontenttype":"application/json","dataschema":"urn:warehouse:process-path-management:events:ProcessPathCreated:v1","time":"2026-09-30T15:00:00Z","data":{"path_id":"PICK"}}`
	if string(raw) != want {
		t.Fatalf("wire mismatch\n got: %s\nwant: %s", raw, want)
	}
	e, err := cloudevents.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	var data map[string]string
	if err := e.DataAs(&data); err != nil || data["path_id"] != "PICK" {
		t.Fatalf("DataAs = %v, %v", data, err)
	}
	if !e.Time().Equal(at) || e.Subject() != "PICK" {
		t.Fatalf("time/subject = %v/%q", e.Time(), e.Subject())
	}
}

func TestNewRejectsEmptySubjectAndId(t *testing.T) {
	if _, err := cloudevents.New(cloudevents.Spec{ID: "x", Entity: "processpath", EventName: "E", Time: time.Now(), Stream: "events"}); err == nil {
		t.Fatal("expected error for empty subject")
	}
	if _, err := cloudevents.New(cloudevents.Spec{Entity: "processpath", EventName: "E", Subject: "s", Time: time.Now(), Stream: "events"}); err == nil {
		t.Fatal("expected error for empty id")
	}
}

func TestDecodeRejectsLegacyFlatAndGarbage(t *testing.T) {
	flat, _ := json.Marshal(map[string]any{
		"event_id": "11111111-1111-4111-8111-111111111111", "event_type": "ProcessPathCreated",
		"occurred_at": "2026-09-06T00:00:00Z", "source": "process-path-management", "data": map[string]string{"path_id": "PICK"},
	})
	for name, raw := range map[string][]byte{"flat": flat, "garbage": []byte("not json"), "empty": []byte("{}")} {
		if _, err := cloudevents.Decode(raw); !errors.Is(err, cloudevents.ErrNotCloudEvent) {
			t.Fatalf("%s: err = %v, want ErrNotCloudEvent", name, err)
		}
	}
}

func TestContentTypeHeader(t *testing.T) {
	h := cloudevents.ContentTypeHeader()
	if h.Key != "content-type" || string(h.Value) != "application/cloudevents+json; charset=UTF-8" {
		t.Fatalf("header = %s: %s", h.Key, h.Value)
	}
}
