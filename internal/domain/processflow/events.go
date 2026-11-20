package processflow

import "time"

// StageSnapshot is the wire-shape-agnostic snapshot of one Stage carried on
// the flow events.
type StageSnapshot struct {
	PathId      string
	ProcessType string
}

// State is the FULL state of a flow at one version. Every flow event
// carries it (ADR 0042): a consumer needs no prior state to build its read
// model, and applies a message only when Version is greater than the one it
// holds.
type State struct {
	FlowId  FlowId
	Name    string
	Stages  []StageSnapshot
	Status  Status
	Version int
}

// ProcessFlowDefined is raised the first time a FlowId is defined.
type ProcessFlowDefined struct {
	State
	At time.Time
}

// ProcessFlowRevised is raised when an ACTIVE flow's name or stages change,
// and by the startup republish (ADR 0038) for every ACTIVE flow, with the
// current state and the current version.
type ProcessFlowRevised struct {
	State
	At time.Time
}

// ProcessFlowRetired is raised when a flow is retired: the full state with
// status RETIRED and the new version.
type ProcessFlowRetired struct {
	State
	At time.Time
}

func (e ProcessFlowDefined) EventName() string     { return "ProcessFlowDefined" }
func (e ProcessFlowDefined) OccurredAt() time.Time { return e.At }
func (e ProcessFlowRevised) EventName() string     { return "ProcessFlowRevised" }
func (e ProcessFlowRevised) OccurredAt() time.Time { return e.At }
func (e ProcessFlowRetired) EventName() string     { return "ProcessFlowRetired" }
func (e ProcessFlowRetired) OccurredAt() time.Time { return e.At }

// Snapshot folds the flow's current state into the event payload.
func (f *ProcessFlow) Snapshot() State {
	stages := make([]StageSnapshot, len(f.stages))
	for i, s := range f.stages {
		stages[i] = StageSnapshot{PathId: string(s.pathId), ProcessType: string(s.processType)}
	}
	return State{FlowId: f.id, Name: f.name, Stages: stages, Status: f.status, Version: f.version}
}

// DefinedEvent is the event for the flow's definition.
func (f *ProcessFlow) DefinedEvent(now time.Time) ProcessFlowDefined {
	return ProcessFlowDefined{State: f.Snapshot(), At: now}
}

// RevisedEvent is the event for a revision (or a republish) of the flow's
// current state.
func (f *ProcessFlow) RevisedEvent(now time.Time) ProcessFlowRevised {
	return ProcessFlowRevised{State: f.Snapshot(), At: now}
}

// RetiredEvent is the event for the flow's retirement.
func (f *ProcessFlow) RetiredEvent(now time.Time) ProcessFlowRetired {
	return ProcessFlowRetired{State: f.Snapshot(), At: now}
}
