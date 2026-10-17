package processpath

import (
	"testing"
	"time"

	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// ADR 0034: intake is routing policy carried by the aggregate, revisable
// like eligibility.

func TestDefine_WithIntake_IsPersistedOnTheAggregate(t *testing.T) {
	in := shared.MustIntake(shared.DemandSourceCustomerOrder)
	p, err := Define("PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, in, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !p.Intake().Equal(in) {
		t.Fatalf("Intake() = %v, want %v", p.Intake().Strings(), in.Strings())
	}
}

func TestDefine_WithEmptyIntake_IsValid(t *testing.T) {
	p, err := Define("PACK", "pack", true, []shared.Capability{"pack"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, shared.Intake{}, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !p.Intake().IsEmpty() {
		t.Fatalf("Intake() = %v, want the empty set", p.Intake().Strings())
	}
}

func TestRevise_IntakeChangeOnly_ReturnsChangedTrue(t *testing.T) {
	p, _ := Define("PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, shared.Intake{}, time.Now())
	in := shared.MustIntake(shared.DemandSourceCustomerOrder)
	later := time.Now().Add(time.Hour)
	changed, err := p.Revise("pick", []shared.Capability{"pick"}, testCycleTimeP95, shared.Eligibility{}, in, later)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !changed {
		t.Fatal("expected changed=true when only the intake differs")
	}
	if !p.Intake().Equal(in) {
		t.Fatalf("Intake() = %v, want %v", p.Intake().Strings(), in.Strings())
	}
	if p.UpdatedAt() != later {
		t.Fatal("expected updatedAt to advance to the revision time")
	}
}

func TestRevise_SameIntake_IsANoOp(t *testing.T) {
	in := shared.MustIntake(shared.DemandSourceSlotRelocation, shared.DemandSourceStockTransfer)
	p, _ := Define("RELOCATE", "relocate", true, []shared.Capability{"relocate"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, in, time.Now())
	same := shared.MustIntake(shared.DemandSourceStockTransfer, shared.DemandSourceSlotRelocation)
	changed, err := p.Revise("relocate", []shared.Capability{"relocate"}, testCycleTimeP95, shared.Eligibility{}, same, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if changed {
		t.Fatal("expected changed=false for the same intake set in another order")
	}
}

func TestRevise_ClearingIntake_ReturnsChangedTrue(t *testing.T) {
	p, _ := Define("PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, shared.MustIntake(shared.DemandSourceCustomerOrder), time.Now())
	changed, err := p.Revise("pick", []shared.Capability{"pick"}, testCycleTimeP95, shared.Eligibility{}, shared.Intake{}, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !changed || !p.Intake().IsEmpty() {
		t.Fatalf("changed=%v intake=%v, want true and the empty set", changed, p.Intake().Strings())
	}
}

func TestRevise_Replenishment_ChangesIntakeAndIsRevisable(t *testing.T) {
	in := shared.MustIntake(shared.DemandSourceReplenishment)
	p, err := Define("REPLENISH", "replenish", true, []shared.Capability{"replenish"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, in, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !p.Intake().Equal(in) {
		t.Fatalf("Intake() = %v, want [REPLENISHMENT]", p.Intake().Strings())
	}
	changed, err := p.Revise("replenish", []shared.Capability{"replenish"}, testCycleTimeP95, shared.Eligibility{}, in, time.Now())
	if err != nil || changed {
		t.Fatalf("Revise with the same intake = (changed %v, err %v), want (false, nil)", changed, err)
	}
	changed, err = p.Revise("replenish", []shared.Capability{"replenish"}, testCycleTimeP95, shared.Eligibility{}, shared.Intake{}, time.Now())
	if err != nil || !changed || !p.Intake().IsEmpty() {
		t.Fatalf("clearing REPLENISHMENT = (changed %v, err %v, intake %v), want (true, nil, empty)", changed, err, p.Intake().Strings())
	}
}

func TestRehydrate_CarriesIntake(t *testing.T) {
	in := shared.MustIntake(shared.DemandSourceInventoryControl)
	now := time.Now()
	p := Rehydrate("AMNESTY", "amnesty", true, []shared.Capability{"amnesty"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, in, StatusActive, now, now, 1)
	if !p.Intake().Equal(in) {
		t.Fatalf("Intake() = %v, want %v", p.Intake().Strings(), in.Strings())
	}
}
