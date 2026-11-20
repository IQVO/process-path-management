package kafka

import (
	"fmt"

	"github.com/claudioed/process-path-management/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/process-path-management/internal/domain/processflow"
)

// ProcessFlowData is the payload shape for ALL THREE ProcessFlow* event types
// (ADR 0042): the FULL state of one flow at one version. Version is what a
// consumer compares: it applies a message only when its version is greater
// than the one it holds, so a republish (ProcessFlowRevised at the current
// version) and a redelivery are no-ops for a consumer that is caught up.
type ProcessFlowData struct {
	FlowId  string          `json:"flow_id"`
	Name    string          `json:"name"`
	Stages  []FlowStageData `json:"stages"`
	Status  string          `json:"status"`
	Version int             `json:"version"`
}

// FlowStageData is the wire shape of one stage; the array order IS the flow's
// order.
type FlowStageData struct {
	PathId      string `json:"path_id"`
	ProcessType string `json:"process_type"`
}

// mapProcessFlowEvent maps a ProcessFlow* domain event to its entity/name,
// aggregate key (the Kafka key AND the CloudEvents subject, so one flow's
// events stay in order on one partition) and payload.
func mapProcessFlowEvent(event any) (mapped, error) {
	var (
		name  string
		state processflow.State
	)
	switch e := event.(type) {
	case processflow.ProcessFlowDefined:
		name, state = e.EventName(), e.State
	case processflow.ProcessFlowRevised:
		name, state = e.EventName(), e.State
	case processflow.ProcessFlowRetired:
		name, state = e.EventName(), e.State
	default:
		return mapped{}, fmt.Errorf("kafka: unknown process flow event type %T", event)
	}
	key := string(state.FlowId)
	stages := make([]FlowStageData, len(state.Stages))
	for i, s := range state.Stages {
		stages[i] = FlowStageData{PathId: s.PathId, ProcessType: s.ProcessType}
	}
	return mapped{cloudevents.EntityProcessFlow, name, key, ProcessFlowData{
		FlowId:  key,
		Name:    state.Name,
		Stages:  stages,
		Status:  string(state.Status),
		Version: state.Version,
	}}, nil
}
