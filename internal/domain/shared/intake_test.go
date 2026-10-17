package shared

import (
	"errors"
	"slices"
	"testing"
)

func TestParseIntake_EmptyAndNilAreTheValidEmptySet(t *testing.T) {
	for _, in := range [][]string{nil, {}} {
		got, err := ParseIntake(in)
		if err != nil {
			t.Fatalf("ParseIntake(%v) error = %v, want nil", in, err)
		}
		if !got.IsEmpty() || len(got.Sources()) != 0 {
			t.Fatalf("ParseIntake(%v) = %v, want the empty set", in, got.Sources())
		}
		if s := got.Strings(); s == nil || len(s) != 0 {
			t.Fatalf("Strings() of the empty set = %#v, want a non-nil empty slice", s)
		}
	}
}

func TestParseIntake_AcceptsEveryDemandSourceInCanonicalOrder(t *testing.T) {
	got, err := ParseIntake([]string{"STOCK_TRANSFER", "REPLENISHMENT", "INVENTORY_CONTROL", "SLOT_RELOCATION", "CUSTOMER_ORDER"})
	if err != nil {
		t.Fatalf("ParseIntake error = %v", err)
	}
	want := []string{"CUSTOMER_ORDER", "INVENTORY_CONTROL", "REPLENISHMENT", "SLOT_RELOCATION", "STOCK_TRANSFER"}
	if !slices.Equal(got.Strings(), want) {
		t.Fatalf("Strings() = %v, want %v", got.Strings(), want)
	}
	if got.IsEmpty() {
		t.Fatal("IsEmpty() = true for five sources")
	}
}

func TestParseIntake_ReplenishmentRoundTrips(t *testing.T) {
	got, err := ParseIntake([]string{"REPLENISHMENT"})
	if err != nil {
		t.Fatalf("ParseIntake([REPLENISHMENT]) error = %v", err)
	}
	if !slices.Equal(got.Strings(), []string{"REPLENISHMENT"}) || !got.Contains(DemandSourceReplenishment) {
		t.Fatalf("REPLENISHMENT did not round trip: %v", got.Strings())
	}
	if DemandSourceReplenishment != DemandSource("REPLENISHMENT") {
		t.Fatalf("DemandSourceReplenishment = %q, want REPLENISHMENT", DemandSourceReplenishment)
	}
	again, err := ParseIntake(got.Strings())
	if err != nil || !again.Equal(got) {
		t.Fatalf("re-parsing Strings() = (%v, %v), want an equal intake", again.Strings(), err)
	}
}

func TestParseIntake_ReplenishmentIsExactAndDistinct(t *testing.T) {
	for _, bad := range []string{"replenishment", "REPLENISH", "REPLENISHMENTS", " REPLENISHMENT"} {
		if _, err := ParseIntake([]string{bad}); !errors.Is(err, ErrInvalidIntake) {
			t.Fatalf("ParseIntake([%q]) error = %v, want ErrInvalidIntake", bad, err)
		}
	}
	if _, err := ParseIntake([]string{"REPLENISHMENT", "REPLENISHMENT"}); !errors.Is(err, ErrInvalidIntake) {
		t.Fatalf("duplicate REPLENISHMENT error = %v, want ErrInvalidIntake", err)
	}
}

func TestIntake_ReplenishmentContainsAndEquality(t *testing.T) {
	in := MustIntake(DemandSourceReplenishment)
	if !in.Contains(DemandSourceReplenishment) || in.Contains(DemandSourceSlotRelocation) {
		t.Fatalf("REPLENISHMENT-only intake Contains wrong: %v", in.Strings())
	}
	if (Intake{}).Contains(DemandSourceReplenishment) {
		t.Fatal("the empty intake contains REPLENISHMENT")
	}
	if in.Equal(MustIntake(DemandSourceSlotRelocation)) {
		t.Fatal("REPLENISHMENT equals SLOT_RELOCATION")
	}
	if !in.Equal(MustIntake(DemandSourceReplenishment)) {
		t.Fatal("two REPLENISHMENT intakes are not Equal")
	}
}

func TestParseIntake_RejectsUnknownBlankAndLowerCaseValues(t *testing.T) {
	for _, bad := range []string{"", "customer_order", "ORDER", " CUSTOMER_ORDER"} {
		if _, err := ParseIntake([]string{bad}); !errors.Is(err, ErrInvalidIntake) {
			t.Fatalf("ParseIntake([%q]) error = %v, want ErrInvalidIntake", bad, err)
		}
	}
}

func TestParseIntake_RejectsADuplicateSource(t *testing.T) {
	if _, err := ParseIntake([]string{"CUSTOMER_ORDER", "CUSTOMER_ORDER"}); !errors.Is(err, ErrInvalidIntake) {
		t.Fatalf("duplicate error = %v, want ErrInvalidIntake", err)
	}
}

func TestIntake_Contains(t *testing.T) {
	in := MustIntake(DemandSourceSlotRelocation)
	if !in.Contains(DemandSourceSlotRelocation) {
		t.Fatal("Contains(SLOT_RELOCATION) = false")
	}
	if in.Contains(DemandSourceCustomerOrder) {
		t.Fatal("Contains(CUSTOMER_ORDER) = true for a SLOT_RELOCATION-only intake")
	}
	if (Intake{}).Contains(DemandSourceCustomerOrder) {
		t.Fatal("the empty intake contains CUSTOMER_ORDER")
	}
}

func TestIntake_EqualIsSetEquality(t *testing.T) {
	a := MustIntake(DemandSourceCustomerOrder, DemandSourceStockTransfer)
	b := MustIntake(DemandSourceStockTransfer, DemandSourceCustomerOrder)
	if !a.Equal(b) {
		t.Fatal("same set in another order is not Equal")
	}
	if a.Equal(MustIntake(DemandSourceCustomerOrder)) {
		t.Fatal("a subset is Equal")
	}
	if a.Equal(MustIntake(DemandSourceCustomerOrder, DemandSourceSlotRelocation)) {
		t.Fatal("a different set of the same size is Equal")
	}
	if !(Intake{}).Equal(MustIntake()) {
		t.Fatal("two empty intakes are not Equal")
	}
}

func TestIntake_SourcesIsADefensiveCopy(t *testing.T) {
	in := MustIntake(DemandSourceCustomerOrder)
	s := in.Sources()
	s[0] = DemandSourceStockTransfer
	if !in.Contains(DemandSourceCustomerOrder) {
		t.Fatal("mutating Sources() changed the intake")
	}
}

func TestMustIntake_PanicsOnAnInvalidSource(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("MustIntake did not panic on an unknown source")
		}
	}()
	MustIntake(DemandSource("NOPE"))
}
