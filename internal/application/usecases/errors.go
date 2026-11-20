package usecases

import "errors"

// ErrPathAlreadyExists is returned by DefinePath when the given PathId is
// already defined (active or deactivated) — a path's identity is
// permanent once created; a caller wanting to re-use an id after
// deactivation must be told explicitly, never silently overwritten.
var ErrPathAlreadyExists = errors.New("usecases: process path already exists")

// ErrPathNotFound is returned by any use case operating on a PathId that
// has never been defined.
var ErrPathNotFound = errors.New("usecases: process path not found")

// ErrCPTScheduleNotFound is returned by GetCPTSchedule when the given
// SiteId has no defined schedule.
var ErrCPTScheduleNotFound = errors.New("usecases: cpt schedule not found")

// ErrIneligiblePathId is returned by DefineCPTSchedule when a cutoff's
// eligiblePathIds references a PathId that is not an Active ProcessPath
// in this service's own store (ADR 0010's one cross-aggregate
// invariant, enforced here rather than by a foreign key).
var ErrIneligiblePathId = errors.New("usecases: eligiblePathIds must reference an Active process path")

// ErrPathReferencedByCPTSchedule is returned by DeactivatePath when at
// least one site's CPT schedule still lists the path in a cutoff's
// eligiblePathIds (ADR 0026). The wrapped message names the sites, so an
// operator knows which schedules to revise first.
var ErrPathReferencedByCPTSchedule = errors.New("usecases: process path is still referenced by a CPT schedule")

// ErrFlowNotFound is returned by GetProcessFlow and RetireProcessFlow when
// the given FlowId has never been defined.
var ErrFlowNotFound = errors.New("usecases: process flow not found")

// ErrIneligibleStagePath is returned by DefineProcessFlow when a stage names
// a path that is not an ACTIVE ProcessPath in this service's own catalogue
// (ADR 0042). The wrapped message names the offending path ids.
var ErrIneligibleStagePath = errors.New("usecases: a flow's stages must reference ACTIVE process paths")

// ErrPathInActiveFlow is returned by DeactivatePath when at least one ACTIVE
// ProcessFlow still has a stage on the path (ADR 0042, the ADR 0026
// precedent). The wrapped message names the flows, so an operator knows
// which ones to revise or retire first.
var ErrPathInActiveFlow = errors.New("usecases: process path is used by an active flow")
