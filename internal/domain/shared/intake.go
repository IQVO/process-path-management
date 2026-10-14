package shared

import (
	"errors"
	"slices"
)

// DemandSource names one source of demand that may route work onto a
// process path (ADR 0034). The vocabulary is closed and owned by this
// service: it is routing policy the catalogue declares, not a value any
// consumer may extend.
//
//   - CUSTOMER_ORDER: order-management order lines (its PathSelectionPolicy).
//   - STOCK_TRANSFER: network-inventory-planning transfer legs, planned
//     through wes-work-planning.
//   - SLOT_RELOCATION: approved slot-plan moves from slotting-optimization,
//     planned through wes-work-planning.
//   - INVENTORY_CONTROL: internal inventory work (amnesty, damage,
//     problem solve).
type DemandSource string

const (
	DemandSourceCustomerOrder    DemandSource = "CUSTOMER_ORDER"
	DemandSourceStockTransfer    DemandSource = "STOCK_TRANSFER"
	DemandSourceSlotRelocation   DemandSource = "SLOT_RELOCATION"
	DemandSourceInventoryControl DemandSource = "INVENTORY_CONTROL"
)

// ErrInvalidIntake is returned when an intake entry is not one of the
// recognized demand sources, or when a source is listed twice (the intake
// is a set).
var ErrInvalidIntake = errors.New("shared: intake entries must be distinct values of CUSTOMER_ORDER, STOCK_TRANSFER, SLOT_RELOCATION, INVENTORY_CONTROL")

// Intake is the set of demand sources allowed to route work onto a
// process path (ADR 0034). The zero value is the empty set and is fully
// valid: such a path is reached only by an explicit path id from an
// upstream stage (PACK, REBIN and SLAM are fed by PICK's output), never
// chosen by a demand source's own path selection. An Intake can only be
// built through ParseIntake/MustIntake, so every value is a validated,
// duplicate-free set kept in canonical (sorted) order.
type Intake struct {
	sources []DemandSource
}

// ParseIntake validates values as a set of demand sources. nil and an
// empty slice are the empty set. An unknown, blank or repeated value is
// rejected with ErrInvalidIntake rather than silently dropped or merged,
// so what is persisted is exactly what was declared.
func ParseIntake(values []string) (Intake, error) {
	out := make([]DemandSource, 0, len(values))
	for _, v := range values {
		s := DemandSource(v)
		if !isDemandSource(s) {
			return Intake{}, ErrInvalidIntake
		}
		if slices.Contains(out, s) {
			return Intake{}, ErrInvalidIntake
		}
		out = append(out, s)
	}
	slices.Sort(out)
	return Intake{sources: out}, nil
}

// MustIntake builds an Intake from known-good sources; it panics on an
// invalid one. Meant for tests and compile-time declarations only.
func MustIntake(sources ...DemandSource) Intake {
	values := make([]string, len(sources))
	for i, s := range sources {
		values[i] = string(s)
	}
	in, err := ParseIntake(values)
	if err != nil {
		panic(err)
	}
	return in
}

func isDemandSource(s DemandSource) bool {
	return s == DemandSourceCustomerOrder || s == DemandSourceStockTransfer ||
		s == DemandSourceSlotRelocation || s == DemandSourceInventoryControl
}

// Sources returns a defensive copy of the set, in canonical order.
func (i Intake) Sources() []DemandSource { return append([]DemandSource(nil), i.sources...) }

// Strings returns the set as wire strings in canonical order: never nil,
// so the empty set encodes as [] rather than null.
func (i Intake) Strings() []string {
	out := make([]string, len(i.sources))
	for n, s := range i.sources {
		out[n] = string(s)
	}
	return out
}

// Contains reports whether s may route work onto the path.
func (i Intake) Contains(s DemandSource) bool { return slices.Contains(i.sources, s) }

// IsEmpty reports whether no demand source may route work onto the path.
func (i Intake) IsEmpty() bool { return len(i.sources) == 0 }

// Equal is set equality. Both sides are canonical (sorted, distinct), so
// an element-wise comparison is enough.
func (i Intake) Equal(other Intake) bool { return slices.Equal(i.sources, other.sources) }
