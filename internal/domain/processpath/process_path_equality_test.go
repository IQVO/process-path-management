package processpath

import (
	"testing"
	"time"

	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// capabilitiesEqual backs Revise's no-op detection: it is the reason a
// revision with an unchanged capability list returns changed=false and
// raises no ProcessPathUpdated. These cases pin its slice semantics.
func TestCapabilitiesEqual(t *testing.T) {
	for _, tt := range []struct {
		name string
		a    []shared.Capability
		b    []shared.Capability
		want bool
	}{
		{"nil vs nil", nil, nil, true},
		{"nil vs empty", nil, []shared.Capability{}, true},
		{"empty vs nil", []shared.Capability{}, nil, true},
		{"identical", []shared.Capability{"pick", "hazmat"}, []shared.Capability{"pick", "hazmat"}, true},
		{"different lengths", []shared.Capability{"pick"}, []shared.Capability{"pick", "hazmat"}, false},
		{"same length, different capability", []shared.Capability{"pick", "hazmat"}, []shared.Capability{"pack", "hazmat"}, false},
		{"reordered capabilities are not equal", []shared.Capability{"pick", "hazmat"}, []shared.Capability{"hazmat", "pick"}, false},
	} {
		if got := capabilitiesEqual(tt.a, tt.b); got != tt.want {
			t.Fatalf("%s: capabilitiesEqual(%v, %v) = %v, want %v", tt.name, tt.a, tt.b, got, tt.want)
		}
	}
}

func TestRevise_IdenticalMultiCapability_ReturnsChangedFalse(t *testing.T) {
	p, err := Define("PICK", "pick", true, []shared.Capability{"pick", "hazmat"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, time.Now())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	changed, err := p.Revise("pick", []shared.Capability{"pick", "hazmat"}, testCycleTimeP95, shared.Eligibility{}, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if changed {
		t.Fatal("expected changed=false for an identical multi-element capability list")
	}
}

func TestRevise_SameCapabilityCount_DifferentCapability_ReturnsChangedTrue(t *testing.T) {
	p, err := Define("PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, time.Now())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	changed, err := p.Revise("pick", []shared.Capability{"pack"}, testCycleTimeP95, shared.Eligibility{}, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !changed {
		t.Fatal("expected changed=true when a same-length capability list swaps one element")
	}
	if got := p.RequiredCapabilities(); len(got) != 1 || got[0] != "pack" {
		t.Fatalf("want [pack], got %v", got)
	}
}

func TestRevise_ReorderedCapabilities_ReturnsChangedTrue(t *testing.T) {
	// requiredCapabilities is compared order-sensitively, so a
	// reordered-but-equal set is reported as a change (and republishes
	// ProcessPathUpdated).
	p, err := Define("PICK", "pick", true, []shared.Capability{"pick", "hazmat"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, time.Now())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	changed, err := p.Revise("pick", []shared.Capability{"hazmat", "pick"}, testCycleTimeP95, shared.Eligibility{}, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !changed {
		t.Fatal("expected changed=true for a reordered capability list (order is significant)")
	}
}
