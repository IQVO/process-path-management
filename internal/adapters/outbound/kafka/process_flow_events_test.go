package kafka_test

import (
	"testing"

	outboundkafka "github.com/claudioed/process-path-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// The same flow event maps identically on both streams (only topic and
// dataschema differ), and keeps the stage ORDER, the version, and the full
// state on retirement.
func TestProcessFlowEvents_SameTypeSubjectAndDataOnBothStreams(t *testing.T) {
	for _, tc := range goldenFlowEvents() {
		t.Run(tc.name, func(t *testing.T) {
			integration, err := outboundkafka.Encode(tc.event, goldenId)
			if err != nil {
				t.Fatal(err)
			}
			analytics, err := outboundkafka.NewAnalyticsEncoder(nil).Encode(tc.event, goldenId)
			if err != nil {
				t.Fatal(err)
			}
			wantType := "com.warehouse.wes.process-path-management.processflow." + tc.name
			if integration.EventType != wantType || analytics.EventType != wantType {
				t.Fatalf("types %q / %q, want %q", integration.EventType, analytics.EventType, wantType)
			}
			if integration.Topic != outboundkafka.Topic || analytics.Topic != outboundkafka.AnalyticsTopic {
				t.Fatalf("topics %q / %q", integration.Topic, analytics.Topic)
			}
			if integration.Key != "single-unit" || analytics.Key != "single-unit" {
				t.Fatalf("keys %q / %q: the key is the flow_id", integration.Key, analytics.Key)
			}
		})
	}
}

func TestProcessFlowEvents_ADomainEventTypeNobodyMapsIsAnError(t *testing.T) {
	if _, err := outboundkafka.Encode(unmappedEvent{}, goldenId); err == nil {
		t.Fatal("an unmapped event type must be an error, never silently dropped")
	}
}

type unmappedEvent struct{ shared.DomainEvent }
