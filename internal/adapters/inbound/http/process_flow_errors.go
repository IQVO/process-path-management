package http

import (
	"errors"
	"net/http"

	"github.com/claudioed/process-path-management/internal/application/usecases"
	"github.com/claudioed/process-path-management/internal/domain/processflow"
)

// flowProblems pins each ProcessFlow error (ADR 0042) to its HTTP status and
// RFC 7807 (type, title) pair, in the same spirit as problemCatalog. statusFor
// and problemFor consult it first, so the flow errors keep their own table
// instead of growing the generic switch.
var flowProblems = []struct {
	err    error
	status int
	info   problemInfo
}{
	{usecases.ErrFlowNotFound, http.StatusNotFound, problemInfo{"flow-not-found", "No process flow exists with this id"}},
	{usecases.ErrPathInActiveFlow, http.StatusConflict, problemInfo{"path-in-active-flow", "The process path is still used by an active process flow; revise or retire those flows before deactivating it"}},
	{usecases.ErrIneligibleStagePath, http.StatusUnprocessableEntity, problemInfo{"ineligible-stage-path", "every stage's pathId must reference an Active process path in this service's own catalogue"}},
	{processflow.ErrFlowRetired, http.StatusUnprocessableEntity, problemInfo{"flow-retired", "This process flow has been retired and can no longer be revised"}},
	{processflow.ErrEmptyFlowId, http.StatusUnprocessableEntity, problemInfo{"empty-flow-id", "flowId must not be empty"}},
	{processflow.ErrEmptyName, http.StatusUnprocessableEntity, problemInfo{"empty-flow-name", "name must not be empty"}},
	{processflow.ErrNoStages, http.StatusUnprocessableEntity, problemInfo{"no-stages", "at least one stage is required"}},
	{processflow.ErrEmptyStagePathId, http.StatusUnprocessableEntity, problemInfo{"empty-stage-path-id", "a stage's pathId must not be empty"}},
	{processflow.ErrInvalidProcessType, http.StatusUnprocessableEntity, problemInfo{"invalid-process-type", "processType must be one of PICK, REBIN, PACK, SLAM, DISPATCH, ARRIVAL, RELOCATE, REPLENISH"}},
	{processflow.ErrDuplicateStagePath, http.StatusUnprocessableEntity, problemInfo{"duplicate-stage-path", "a pathId may appear at most once in a flow"}},
}

// flowStatusFor reports the status of a ProcessFlow error.
func flowStatusFor(err error) (int, bool) {
	for _, p := range flowProblems {
		if errors.Is(err, p.err) {
			return p.status, true
		}
	}
	return 0, false
}

// flowProblemFor reports the problem type of a ProcessFlow error.
func flowProblemFor(err error) (problemInfo, bool) {
	for _, p := range flowProblems {
		if errors.Is(err, p.err) {
			return p.info, true
		}
	}
	return problemInfo{}, false
}
