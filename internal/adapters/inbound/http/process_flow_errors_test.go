package http

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/claudioed/process-path-management/internal/application/ports"
	"github.com/claudioed/process-path-management/internal/application/usecases"
	"github.com/claudioed/process-path-management/internal/domain/processflow"
)

// ADR 0042: each ProcessFlow error has its own status and RFC 7807 slug, and a
// wrapped one (the detail names the flows or paths) still maps to it.
func TestProcessFlowErrors_StatusAndSlug(t *testing.T) {
	tests := []struct {
		err    error
		status int
		slug   string
	}{
		{usecases.ErrFlowNotFound, http.StatusNotFound, "flow-not-found"},
		{fmt.Errorf("%w: path \"PICK\" is used by active flow(s) [a b]", usecases.ErrPathInActiveFlow), http.StatusConflict, "path-in-active-flow"},
		{fmt.Errorf("%w: [GHOST]", usecases.ErrIneligibleStagePath), http.StatusUnprocessableEntity, "ineligible-stage-path"},
		{processflow.ErrFlowRetired, http.StatusUnprocessableEntity, "flow-retired"},
		{processflow.ErrEmptyFlowId, http.StatusUnprocessableEntity, "empty-flow-id"},
		{processflow.ErrEmptyName, http.StatusUnprocessableEntity, "empty-flow-name"},
		{processflow.ErrNoStages, http.StatusUnprocessableEntity, "no-stages"},
		{processflow.ErrEmptyStagePathId, http.StatusUnprocessableEntity, "empty-stage-path-id"},
		{processflow.ErrInvalidProcessType, http.StatusUnprocessableEntity, "invalid-process-type"},
		{processflow.ErrDuplicateStagePath, http.StatusUnprocessableEntity, "duplicate-stage-path"},
		{ports.ErrConcurrentModification, http.StatusConflict, "concurrent-modification"},
	}
	for _, tc := range tests {
		if got := statusFor(tc.err); got != tc.status {
			t.Errorf("%v: status %d, want %d", tc.err, got, tc.status)
		}
		if got := problemFor(tc.err).slug; got != tc.slug {
			t.Errorf("%v: slug %q, want %q", tc.err, got, tc.slug)
		}
	}
}
