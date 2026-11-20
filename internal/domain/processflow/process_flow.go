// Package processflow implements the ProcessFlow aggregate (ADR 0042): the
// ordered sequence of process-path stages a workload passes through, for
// example PICK, REBIN, PACK, SLAM. A ProcessPath is one stage of work; the
// flow is what says which stage follows which, so the floor's topology has
// one owner and a planning context reads it instead of declaring its own.
//
// A stage names an existing ACTIVE ProcessPath and a process type. That the
// path exists and is active needs the ProcessPathRepo, so the use case
// checks it (the same split ADR 0010 makes for a CPT schedule's
// eligiblePathIds); every other invariant lives here.
package processflow

import (
	"errors"
	"time"

	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// FlowId is the identity of a ProcessFlow. It follows the same id rules as
// a shared.PathId: any non-empty string, chosen by the caller.
type FlowId string

// ProcessType is the kind of work a stage performs. The set is closed: it
// is the task types fulfillment-execution publishes (ADR 0042).
type ProcessType string

const (
	ProcessTypePick      ProcessType = "PICK"
	ProcessTypeRebin     ProcessType = "REBIN"
	ProcessTypePack      ProcessType = "PACK"
	ProcessTypeSlam      ProcessType = "SLAM"
	ProcessTypeDispatch  ProcessType = "DISPATCH"
	ProcessTypeArrival   ProcessType = "ARRIVAL"
	ProcessTypeRelocate  ProcessType = "RELOCATE"
	ProcessTypeReplenish ProcessType = "REPLENISH"
)

// ProcessTypes lists the closed set in declaration order.
func ProcessTypes() []ProcessType {
	return []ProcessType{
		ProcessTypePick, ProcessTypeRebin, ProcessTypePack, ProcessTypeSlam,
		ProcessTypeDispatch, ProcessTypeArrival, ProcessTypeRelocate, ProcessTypeReplenish,
	}
}

// Valid reports whether t is one of the closed set.
func (t ProcessType) Valid() bool {
	for _, v := range ProcessTypes() {
		if t == v {
			return true
		}
	}
	return false
}

// Status is the lifecycle state of a flow.
type Status string

const (
	StatusActive  Status = "ACTIVE"
	StatusRetired Status = "RETIRED"
)

// Errors enforcing the aggregate's invariants (ADR 0042). A malformed flow
// is never constructible.
var (
	ErrEmptyFlowId        = errors.New("processflow: flowId must not be empty")
	ErrEmptyName          = errors.New("processflow: name must not be empty")
	ErrNoStages           = errors.New("processflow: at least one stage is required")
	ErrEmptyStagePathId   = errors.New("processflow: a stage's pathId must not be empty")
	ErrInvalidProcessType = errors.New("processflow: processType must be one of PICK, REBIN, PACK, SLAM, DISPATCH, ARRIVAL, RELOCATE, REPLENISH")
	ErrDuplicateStagePath = errors.New("processflow: a pathId may appear at most once in a flow")
	ErrFlowRetired        = errors.New("processflow: flow is retired and can no longer be revised")
)

// Stage is one step of a flow: the path that does the work and the type of
// work it does there.
type Stage struct {
	pathId      shared.PathId
	processType ProcessType
}

// NewStage validates and builds a stage.
func NewStage(pathId shared.PathId, processType ProcessType) (Stage, error) {
	if pathId == "" {
		return Stage{}, ErrEmptyStagePathId
	}
	if !processType.Valid() {
		return Stage{}, ErrInvalidProcessType
	}
	return Stage{pathId: pathId, processType: processType}, nil
}

func (s Stage) PathId() shared.PathId    { return s.pathId }
func (s Stage) ProcessType() ProcessType { return s.processType }

// ProcessFlow is the aggregate root.
//
// version starts at 1 and grows by one per ACCEPTED change (a revision that
// changes something, or the retirement). Unlike the version of the other
// aggregates, which is storage metadata, this one is part of the published
// event: consumers apply a message only when its version is greater than
// the one they hold (ADR 0042).
type ProcessFlow struct {
	id        FlowId
	name      string
	stages    []Stage
	status    Status
	createdAt time.Time
	updatedAt time.Time
	version   int
}

// Define builds a new ACTIVE flow at version 1.
func Define(id FlowId, name string, stages []Stage, now time.Time) (*ProcessFlow, error) {
	if id == "" {
		return nil, ErrEmptyFlowId
	}
	if err := validate(name, stages); err != nil {
		return nil, err
	}
	return &ProcessFlow{
		id:        id,
		name:      name,
		stages:    append([]Stage(nil), stages...),
		status:    StatusActive,
		createdAt: now,
		updatedAt: now,
		version:   1,
	}, nil
}

// Rehydrate rebuilds a flow from persisted state without re-validating.
func Rehydrate(id FlowId, name string, stages []Stage, status Status, createdAt, updatedAt time.Time, version int) *ProcessFlow {
	return &ProcessFlow{
		id: id, name: name, stages: stages, status: status,
		createdAt: createdAt, updatedAt: updatedAt, version: version,
	}
}

// ValidateDefinition checks the name and the stages of a would-be flow
// without building one, so a use case can reject a bad request before it
// takes any lock.
func ValidateDefinition(name string, stages []Stage) error {
	return validate(name, stages)
}

// Revise replaces the name and the stages wholesale. A retired flow cannot
// be revised. It returns changed=false, and leaves the version alone, when
// the new definition equals the current one.
func (f *ProcessFlow) Revise(name string, stages []Stage, now time.Time) (changed bool, err error) {
	if f.status == StatusRetired {
		return false, ErrFlowRetired
	}
	if err := validate(name, stages); err != nil {
		return false, err
	}
	if f.name == name && stagesEqual(f.stages, stages) {
		return false, nil
	}
	f.name = name
	f.stages = append([]Stage(nil), stages...)
	f.updatedAt = now
	f.version++
	return true, nil
}

// Retire ends the flow. It returns changed=false when the flow was already
// retired (the version does not move).
func (f *ProcessFlow) Retire(now time.Time) (changed bool) {
	if f.status == StatusRetired {
		return false
	}
	f.status = StatusRetired
	f.updatedAt = now
	f.version++
	return true
}

func (f *ProcessFlow) ID() FlowId           { return f.id }
func (f *ProcessFlow) Name() string         { return f.name }
func (f *ProcessFlow) Stages() []Stage      { return append([]Stage(nil), f.stages...) }
func (f *ProcessFlow) Status() Status       { return f.status }
func (f *ProcessFlow) IsActive() bool       { return f.status == StatusActive }
func (f *ProcessFlow) CreatedAt() time.Time { return f.createdAt }
func (f *ProcessFlow) UpdatedAt() time.Time { return f.updatedAt }
func (f *ProcessFlow) Version() int         { return f.version }
func (f *ProcessFlow) StagePathIds() []shared.PathId {
	out := make([]shared.PathId, len(f.stages))
	for i, s := range f.stages {
		out[i] = s.pathId
	}
	return out
}

func validate(name string, stages []Stage) error {
	if name == "" {
		return ErrEmptyName
	}
	if len(stages) == 0 {
		return ErrNoStages
	}
	seen := make(map[shared.PathId]bool, len(stages))
	for _, s := range stages {
		if s.pathId == "" {
			return ErrEmptyStagePathId
		}
		if !s.processType.Valid() {
			return ErrInvalidProcessType
		}
		if seen[s.pathId] {
			return ErrDuplicateStagePath
		}
		seen[s.pathId] = true
	}
	return nil
}

func stagesEqual(a, b []Stage) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
