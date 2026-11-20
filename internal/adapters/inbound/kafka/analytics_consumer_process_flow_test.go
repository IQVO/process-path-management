package kafka_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	inboundkafka "github.com/claudioed/process-path-management/internal/adapters/inbound/kafka"
	"github.com/claudioed/process-path-management/internal/adapters/kafka/cloudevents"
)

// ADR 0042: the three ProcessFlow* types arrive on this context's own
// analytics topic. The projector ACKNOWLEDGES them (HandleMessage returns nil
// and, in Run, the offset is committed: no retry, no dead letter) and does not
// project them: the catalogue-growth report counts ProcessPath* events only,
// so a flow adds nothing to it. They are not marked processed either, exactly
// like CPTScheduleChanged.
func TestAnalyticsConsumer_AcknowledgesProcessFlowTypesWithoutProjectingThem(t *testing.T) {
	at := time.Date(2026, 11, 20, 8, 0, 0, 0, time.UTC)
	for _, name := range []string{"ProcessFlowDefined", "ProcessFlowRevised", "ProcessFlowRetired"} {
		t.Run(name, func(t *testing.T) {
			proj := &fakeProjection{}
			processed := newFakeProcessed()
			c := &inboundkafka.AnalyticsConsumer{Projection: proj, Processed: processed, Logger: slog.Default()}

			raw, err := cloudevents.New(cloudevents.Spec{
				ID: "flow-evt-" + name, Entity: cloudevents.EntityProcessFlow, EventName: name,
				Subject: "single-unit", Time: at, Stream: cloudevents.StreamAnalytics, Version: 1,
				Data: map[string]any{
					"flow_id": "single-unit", "name": "Single unit", "status": "ACTIVE", "version": 2,
					"stages": []map[string]string{{"path_id": "PICK", "process_type": "PICK"}},
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := c.HandleMessage(context.Background(), raw); err != nil {
				t.Fatalf("a ProcessFlow event must be acknowledged, got %v", err)
			}
			if len(proj.calls) != 0 {
				t.Fatalf("a ProcessFlow event projects nothing, got %d calls", len(proj.calls))
			}
			if len(processed.seen) != 0 {
				t.Fatalf("a non-projecting event is not marked processed: %v", processed.seen)
			}
		})
	}
}
