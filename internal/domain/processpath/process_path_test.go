package processpath

import (
	"errors"
	"testing"
	"time"

	"github.com/claudioed/process-path-management/internal/domain/shared"
)

const testCycleTimeP95 = 2 * time.Hour

func TestDefine_RejectsEmptyPathId(t *testing.T) {
	_, err := Define("", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, time.Now())
	if !errors.Is(err, ErrEmptyPathId) {
		t.Fatalf("want ErrEmptyPathId, got %v", err)
	}
}

func TestDefine_RejectsEmptyMatchPrefix(t *testing.T) {
	_, err := Define("PICK", "", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, time.Now())
	if !errors.Is(err, ErrEmptyMatchPrefix) {
		t.Fatalf("want ErrEmptyMatchPrefix, got %v", err)
	}
}

func TestDefine_RejectsUppercaseMatchPrefix(t *testing.T) {
	_, err := Define("PICK", "Pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, time.Now())
	if !errors.Is(err, ErrMatchPrefixNotLowercase) {
		t.Fatalf("want ErrMatchPrefixNotLowercase, got %v", err)
	}
}

func TestDefine_RejectsEmptyRequiredCapabilities(t *testing.T) {
	_, err := Define("PICK", "pick", true, nil, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, time.Now())
	if !errors.Is(err, ErrNoRequiredCapabilities) {
		t.Fatalf("want ErrNoRequiredCapabilities, got %v", err)
	}
}

func TestDefine_RejectsInvalidDestinationLocationRole(t *testing.T) {
	_, err := Define("PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRole("Storage"), testCycleTimeP95, shared.Eligibility{}, time.Now())
	if !errors.Is(err, shared.ErrInvalidDestinationLocationRole) {
		t.Fatalf("want ErrInvalidDestinationLocationRole, got %v", err)
	}
}

func TestDefine_RejectsZeroCycleTimeP95(t *testing.T) {
	_, err := Define("PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, 0, shared.Eligibility{}, time.Now())
	if !errors.Is(err, ErrInvalidCycleTime) {
		t.Fatalf("want ErrInvalidCycleTime, got %v", err)
	}
}

func TestDefine_RejectsNegativeCycleTimeP95(t *testing.T) {
	_, err := Define("PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, -time.Hour, shared.Eligibility{}, time.Now())
	if !errors.Is(err, ErrInvalidCycleTime) {
		t.Fatalf("want ErrInvalidCycleTime, got %v", err)
	}
}

func TestDefine_ValidInput_IsActiveWithMatchingFields(t *testing.T) {
	now := time.Now()
	p, err := Define("PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, now)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.ID() != "PICK" || p.MatchPrefix() != "pick" || !p.Direct() {
		t.Fatalf("unexpected fields: %+v", p)
	}
	if !p.IsActive() {
		t.Fatal("expected newly defined path to be Active")
	}
	if p.CreatedAt() != now || p.UpdatedAt() != now {
		t.Fatal("expected createdAt/updatedAt to both equal the construction time")
	}
	if p.DestinationLocationRole() != shared.DestinationLocationRoleUnset {
		t.Fatalf("expected DestinationLocationRoleUnset by default, got %q", p.DestinationLocationRole())
	}
	if p.CycleTimeP95() != testCycleTimeP95 {
		t.Fatalf("want cycleTimeP95 %v, got %v", testCycleTimeP95, p.CycleTimeP95())
	}
	if !p.Eligibility().Equal(shared.Eligibility{}) {
		t.Fatalf("want permissive zero-value eligibility, got %+v", p.Eligibility())
	}
}

func TestDefine_WithDestinationLocationRole_IsPersistedOnTheAggregate(t *testing.T) {
	p, err := Define("PACK", "pack", true, []shared.Capability{"pack"}, shared.DestinationLocationRoleDrop, testCycleTimeP95, shared.Eligibility{}, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.DestinationLocationRole() != shared.DestinationLocationRoleDrop {
		t.Fatalf("want DestinationLocationRoleDrop, got %q", p.DestinationLocationRole())
	}
}

func TestDefine_WithEligibility_IsPersistedOnTheAggregate(t *testing.T) {
	maxUnits := 1
	eligibility := shared.NewEligibility(&maxUnits, []string{"giftWrap"}, []string{"hazmat"}, true)
	p, err := Define("SINGLES", "singles", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, eligibility, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !p.Eligibility().Equal(eligibility) {
		t.Fatalf("want eligibility %+v, got %+v", eligibility, p.Eligibility())
	}
}

func TestRevise_OnDeactivatedPath_ReturnsErrPathDeactivated(t *testing.T) {
	p, _ := Define("PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, time.Now())
	p.Deactivate(time.Now())
	_, err := p.Revise("pick-v2", []shared.Capability{"pick"}, testCycleTimeP95, shared.Eligibility{}, time.Now())
	if !errors.Is(err, ErrPathDeactivated) {
		t.Fatalf("want ErrPathDeactivated, got %v", err)
	}
}

func TestRevise_NoActualChange_ReturnsChangedFalse(t *testing.T) {
	p, _ := Define("PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, time.Now())
	changed, err := p.Revise("pick", []shared.Capability{"pick"}, testCycleTimeP95, shared.Eligibility{}, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if changed {
		t.Fatal("expected changed=false for an identical revision")
	}
}

func TestRevise_ActualChange_ReturnsChangedTrueAndUpdatesFields(t *testing.T) {
	p, _ := Define("PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, time.Now())
	later := time.Now().Add(time.Hour)
	changed, err := p.Revise("pick-zone-a", []shared.Capability{"pick", "hazmat"}, testCycleTimeP95, shared.Eligibility{}, later)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !changed {
		t.Fatal("expected changed=true")
	}
	if p.MatchPrefix() != "pick-zone-a" {
		t.Fatalf("want matchPrefix pick-zone-a, got %s", p.MatchPrefix())
	}
	if len(p.RequiredCapabilities()) != 2 {
		t.Fatalf("want 2 capabilities, got %d", len(p.RequiredCapabilities()))
	}
	if p.UpdatedAt() != later {
		t.Fatal("expected updatedAt to advance to the revision time")
	}
}

func TestRevise_InvalidInput_RejectedEvenOnActivePath(t *testing.T) {
	p, _ := Define("PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, time.Now())
	if _, err := p.Revise("", []shared.Capability{"pick"}, testCycleTimeP95, shared.Eligibility{}, time.Now()); !errors.Is(err, ErrEmptyMatchPrefix) {
		t.Fatalf("want ErrEmptyMatchPrefix, got %v", err)
	}
}

func TestRevise_RejectsZeroCycleTimeP95(t *testing.T) {
	p, _ := Define("PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, time.Now())
	if _, err := p.Revise("pick", []shared.Capability{"pick"}, 0, shared.Eligibility{}, time.Now()); !errors.Is(err, ErrInvalidCycleTime) {
		t.Fatalf("want ErrInvalidCycleTime, got %v", err)
	}
}

func TestRevise_DoesNotAlterDestinationLocationRole(t *testing.T) {
	p, _ := Define("PACK", "pack", true, []shared.Capability{"pack"}, shared.DestinationLocationRoleDrop, testCycleTimeP95, shared.Eligibility{}, time.Now())
	if _, err := p.Revise("pack-v2", []shared.Capability{"pack"}, testCycleTimeP95, shared.Eligibility{}, time.Now()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.DestinationLocationRole() != shared.DestinationLocationRoleDrop {
		t.Fatalf("expected DestinationLocationRole to remain Drop after Revise, got %q", p.DestinationLocationRole())
	}
}

func TestRevise_CycleTimeP95Change_ReturnsChangedTrue(t *testing.T) {
	p, _ := Define("PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, time.Now())
	changed, err := p.Revise("pick", []shared.Capability{"pick"}, 3*time.Hour, shared.Eligibility{}, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !changed {
		t.Fatal("expected changed=true when cycleTimeP95 differs, even if nothing else does")
	}
	if p.CycleTimeP95() != 3*time.Hour {
		t.Fatalf("want cycleTimeP95 3h, got %v", p.CycleTimeP95())
	}
}

func TestRevise_EligibilityChange_ReturnsChangedTrue(t *testing.T) {
	p, _ := Define("PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, time.Now())
	maxUnits := 1
	newEligibility := shared.NewEligibility(&maxUnits, nil, nil, false)
	changed, err := p.Revise("pick", []shared.Capability{"pick"}, testCycleTimeP95, newEligibility, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !changed {
		t.Fatal("expected changed=true when eligibility differs, even if nothing else does")
	}
	if !p.Eligibility().Equal(newEligibility) {
		t.Fatalf("want eligibility %+v, got %+v", newEligibility, p.Eligibility())
	}
}

func TestDeactivate_IsIdempotent(t *testing.T) {
	p, _ := Define("PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, time.Now())
	p.Deactivate(time.Now())
	if p.Status() != StatusDeactivated {
		t.Fatal("expected Deactivated after first call")
	}
	// second call must not panic or change status
	p.Deactivate(time.Now())
	if p.Status() != StatusDeactivated {
		t.Fatal("expected still Deactivated after redundant call")
	}
}

func TestRequiredCapabilities_ReturnsDefensiveCopy(t *testing.T) {
	p, _ := Define("PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, time.Now())
	caps := p.RequiredCapabilities()
	caps[0] = "mutated"
	if p.RequiredCapabilities()[0] != "pick" {
		t.Fatal("expected internal state to be unaffected by mutating the returned slice")
	}
}

func TestRehydrate_ReconstructsWithoutRevalidating(t *testing.T) {
	created := time.Now().Add(-time.Hour)
	updated := time.Now()
	// Rehydrate deliberately accepts state that Define would reject (e.g. an
	// uppercase matchPrefix), because it reconstructs already-persisted,
	// already-validated-at-write-time data -- repository adapters must
	// never re-run construction invariants on read.
	p := Rehydrate("PICK", "PICK-LEGACY", false, []shared.Capability{"pick"}, shared.DestinationLocationRoleWorkCenter, testCycleTimeP95, shared.Eligibility{}, StatusDeactivated, created, updated)
	if p.ID() != "PICK" {
		t.Fatalf("want id PICK, got %s", p.ID())
	}
	if p.MatchPrefix() != "PICK-LEGACY" {
		t.Fatalf("want matchPrefix PICK-LEGACY, got %s", p.MatchPrefix())
	}
	if p.Direct() {
		t.Fatal("want direct=false")
	}
	if p.Status() != StatusDeactivated || p.IsActive() {
		t.Fatal("want rehydrated status Deactivated")
	}
	if p.CreatedAt() != created || p.UpdatedAt() != updated {
		t.Fatal("want createdAt/updatedAt to match the rehydrated values exactly")
	}
	if p.DestinationLocationRole() != shared.DestinationLocationRoleWorkCenter {
		t.Fatalf("want rehydrated DestinationLocationRole WorkCenter, got %q", p.DestinationLocationRole())
	}
}

func TestRevise_DifferentCapabilityCount_ReturnsChangedTrue(t *testing.T) {
	p, _ := Define("PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, time.Now())
	changed, err := p.Revise("pick", []shared.Capability{"pick", "hazmat"}, testCycleTimeP95, shared.Eligibility{}, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !changed {
		t.Fatal("expected changed=true when the capability count differs, even if matchPrefix is unchanged")
	}
}
