package mcp

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/process-path-management/internal/adapters/outbound/memory"
	"github.com/claudioed/process-path-management/internal/application/usecases"
	"github.com/claudioed/process-path-management/internal/domain/processpath"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

var base = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

// harness builds Deps over a real in-memory ProcessPathRepo, seeded
// directly with domain aggregates (bypassing the write use cases, which
// are not part of this adapter's read-only surface).
type harness struct {
	deps Deps
	repo *memory.ProcessPathRepo
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	repo := memory.NewProcessPathRepo()
	return &harness{
		deps: Deps{
			GetPath:   &usecases.GetPath{Repo: repo},
			ListPaths: &usecases.ListPaths{Repo: repo},
		},
		repo: repo,
	}
}

// seedPath defines and saves a ProcessPath directly into the repo.
func (h *harness) seedPath(t *testing.T, id, matchPrefix string, direct bool, caps []string, active bool) *processpath.ProcessPath {
	t.Helper()
	capabilities := make([]shared.Capability, 0, len(caps))
	for _, c := range caps {
		capabilities = append(capabilities, shared.Capability(c))
	}
	p, err := processpath.Define(shared.PathId(id), matchPrefix, direct, capabilities, shared.DestinationLocationRoleUnset, 2*time.Hour, shared.Eligibility{}, base)
	if err != nil {
		t.Fatalf("seedPath define: %v", err)
	}
	if !active {
		p.Deactivate(base.Add(time.Hour))
	}
	if err := h.repo.Save(context.Background(), p); err != nil {
		t.Fatalf("seedPath save: %v", err)
	}
	return p
}

func TestGetProcessPath(t *testing.T) {
	h := newHarness(t)
	h.seedPath(t, "PICK", "pick", true, []string{"pick"}, true)

	out, err := h.deps.getProcessPath(context.Background(), getProcessPathInput{PathId: "PICK"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.PathId != "PICK" || out.MatchPrefix != "pick" || !out.Direct || !out.Active {
		t.Fatalf("unexpected dto: %+v", out)
	}
	if len(out.RequiredCapabilities) != 1 || out.RequiredCapabilities[0] != "pick" {
		t.Fatalf("unexpected capabilities: %+v", out.RequiredCapabilities)
	}
	if out.Status != string(processpath.StatusActive) {
		t.Fatalf("status = %q, want ACTIVE", out.Status)
	}
	if out.CycleTimeP95 != (2 * time.Hour).String() {
		t.Fatalf("want cycleTimeP95 %s, got %q", (2 * time.Hour).String(), out.CycleTimeP95)
	}
	// destinationLocationRole is omitted (empty) when unset, mirroring
	// the REST DTO's omit-when-unknown discipline (ADR 0006/0009).
	if out.DestinationLocationRole != "" {
		t.Fatalf("destinationLocationRole = %q, want \"\" (unset) on the wire", out.DestinationLocationRole)
	}
}

// TestGetProcessPathCarriesDestinationLocationRole pins that the MCP DTO
// now carries the declared destination location role — the REST wire
// always did; an AI client should not need REST for that one field.
func TestGetProcessPathCarriesDestinationLocationRole(t *testing.T) {
	h := newHarness(t)
	p, err := processpath.Define(shared.PathId("PACK"), "pack", false, []shared.Capability{"pack"}, shared.DestinationLocationRoleWorkCenter, 2*time.Hour, shared.Eligibility{}, base)
	if err != nil {
		t.Fatalf("define: %v", err)
	}
	if err := h.repo.Save(context.Background(), p); err != nil {
		t.Fatalf("save: %v", err)
	}

	out, err := h.deps.getProcessPath(context.Background(), getProcessPathInput{PathId: "PACK"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.DestinationLocationRole != "WorkCenter" {
		t.Fatalf("destinationLocationRole = %q, want WorkCenter", out.DestinationLocationRole)
	}
}

func TestGetProcessPathNotFound(t *testing.T) {
	h := newHarness(t)
	if _, err := h.deps.getProcessPath(context.Background(), getProcessPathInput{PathId: "MISSING"}); err == nil {
		t.Fatal("expected error for unknown pathId")
	}
}

func TestGetProcessPathRejectsEmptyId(t *testing.T) {
	h := newHarness(t)
	if _, err := h.deps.getProcessPath(context.Background(), getProcessPathInput{PathId: ""}); err == nil {
		t.Fatal("expected error for empty pathId")
	}
}

func TestGetProcessPathDeactivated(t *testing.T) {
	h := newHarness(t)
	h.seedPath(t, "SLAM", "slam", false, []string{"slam"}, false)

	out, err := h.deps.getProcessPath(context.Background(), getProcessPathInput{PathId: "SLAM"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Active {
		t.Fatal("expected Active=false for a deactivated path")
	}
	if out.Status != string(processpath.StatusDeactivated) {
		t.Fatalf("status = %q, want DEACTIVATED", out.Status)
	}
}

func TestListProcessPathsDefaultsToActiveOnly(t *testing.T) {
	h := newHarness(t)
	h.seedPath(t, "PICK", "pick", true, []string{"pick"}, true)
	h.seedPath(t, "PACK", "pack", true, []string{"pack"}, true)
	h.seedPath(t, "REBIN", "rebin", false, []string{"rebin"}, false)

	out, err := h.deps.listProcessPaths(context.Background(), listProcessPathsInput{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out.Paths) != 2 {
		t.Fatalf("paths = %d, want 2 (active only, default)", len(out.Paths))
	}
	for _, p := range out.Paths {
		if !p.Active {
			t.Fatalf("expected only active paths, got %+v", p)
		}
	}
}

func TestListProcessPathsIncludesDeactivatedWhenRequested(t *testing.T) {
	h := newHarness(t)
	h.seedPath(t, "PICK", "pick", true, []string{"pick"}, true)
	h.seedPath(t, "REBIN", "rebin", false, []string{"rebin"}, false)

	inactive := false
	out, err := h.deps.listProcessPaths(context.Background(), listProcessPathsInput{ActiveOnly: &inactive})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out.Paths) != 2 {
		t.Fatalf("paths = %d, want 2 (activeOnly=false)", len(out.Paths))
	}
}

func TestListProcessPathsEmptyCatalogue(t *testing.T) {
	h := newHarness(t)
	out, err := h.deps.listProcessPaths(context.Background(), listProcessPathsInput{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out.Paths) != 0 {
		t.Fatalf("paths = %d, want 0", len(out.Paths))
	}
}
