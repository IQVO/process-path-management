package mcp

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/process-path-management/internal/domain/processpath"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// ADR 0034: the read tools carry the path's intake, always as an array.

func TestGetProcessPathCarriesIntake(t *testing.T) {
	h := newHarness(t)
	in := shared.MustIntake(shared.DemandSourceSlotRelocation)
	p, err := processpath.Define("RELOCATE", "relocate", true, []shared.Capability{"relocate"}, shared.DestinationLocationRoleUnset, 2*time.Hour, shared.Eligibility{}, in, base)
	if err != nil {
		t.Fatalf("define: %v", err)
	}
	if err := h.repo.Save(context.Background(), p); err != nil {
		t.Fatalf("save: %v", err)
	}
	out, err := h.deps.getProcessPath(context.Background(), getProcessPathInput{PathId: "RELOCATE"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !slices.Equal(out.Intake, []string{"SLOT_RELOCATION"}) {
		t.Fatalf("intake = %v, want [SLOT_RELOCATION]", out.Intake)
	}
}

func TestListProcessPathsEncodesAnEmptyIntakeAsAnArray(t *testing.T) {
	h := newHarness(t)
	h.seedPath(t, "PACK", "pack", true, []string{"pack"}, true)
	out, err := h.deps.listProcessPaths(context.Background(), listProcessPathsInput{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	raw, err := json.Marshal(out)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(raw), `"intake":[]`) {
		t.Fatalf("want intake encoded as [], got %s", raw)
	}
}
