package processpath

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// The declared catalogue (catalogue/paths.json, ADR 0033) is the reviewed
// record of the paths every environment must define. Each entry is a
// POST /process-paths body that scripts/seed_catalogue.py applies through
// the public API. These tests hold every entry to the aggregate's own
// invariants, so a declaration the service would reject with a 4xx can
// never be merged.

const declaredCataloguePath = "../../../catalogue/paths.json"

func loadDeclaredCatalogue(t *testing.T) []map[string]any {
	t.Helper()
	raw, err := os.ReadFile(filepath.Clean(declaredCataloguePath))
	if err != nil {
		t.Fatalf("read %s: %v", declaredCataloguePath, err)
	}
	var doc map[string][]map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("decode %s: %v", declaredCataloguePath, err)
	}
	if len(doc["paths"]) == 0 {
		t.Fatalf("%s declares no paths", declaredCataloguePath)
	}
	return doc["paths"]
}

func declaredStrings(entry map[string]any, key string) []string {
	items, _ := entry[key].([]any)
	out := make([]string, 0, len(items))
	for _, it := range items {
		s, _ := it.(string)
		out = append(out, s)
	}
	return out
}

func declaredEligibility(entry map[string]any) shared.Eligibility {
	e, _ := entry["eligibility"].(map[string]any)
	var maxUnits *int
	if v, ok := e["maxUnitsPerLine"].(float64); ok {
		n := int(v)
		maxUnits = &n
	}
	nonSortable, _ := e["nonSortable"].(bool)
	return shared.NewEligibility(maxUnits, declaredStrings(e, "requiredProductAttributes"), declaredStrings(e, "excludedProductAttributes"), nonSortable)
}

// declaredIntake parses the entry's intake (ADR 0034). Every declared entry
// must carry the key, the empty array included: an omitted intake would
// silently mean "no demand source", which is a routing decision the
// catalogue has to make explicitly.
func declaredIntake(t *testing.T, entry map[string]any) shared.Intake {
	t.Helper()
	if _, ok := entry["intake"].([]any); !ok {
		t.Fatalf("declared entry %v has no intake array: every entry declares its demand sources (ADR 0034)", entry["pathId"])
	}
	in, err := shared.ParseIntake(declaredStrings(entry, "intake"))
	if err != nil {
		t.Fatalf("declared entry %v: intake %v: %v", entry["pathId"], entry["intake"], err)
	}
	return in
}

func defineDeclared(t *testing.T, entry map[string]any) (*ProcessPath, error) {
	t.Helper()
	id, _ := entry["pathId"].(string)
	prefix, _ := entry["matchPrefix"].(string)
	direct, _ := entry["direct"].(bool)
	role, _ := entry["destinationLocationRole"].(string)
	rawCycle, _ := entry["cycleTimeP95"].(string)
	cycle, err := time.ParseDuration(rawCycle)
	if err != nil {
		t.Fatalf("entry %q: cycleTimeP95 %q: %v", id, rawCycle, err)
	}
	caps := make([]shared.Capability, 0)
	for _, c := range declaredStrings(entry, "requiredCapabilities") {
		caps = append(caps, shared.Capability(c))
	}
	return Define(shared.PathId(id), prefix, direct, caps, shared.DestinationLocationRole(role), cycle, declaredEligibility(entry), declaredIntake(t, entry), time.Now())
}

func TestDeclaredCatalogue_EveryEntryIsAValidActivePath(t *testing.T) {
	seen := map[string]bool{}
	for _, entry := range loadDeclaredCatalogue(t) {
		p, err := defineDeclared(t, entry)
		if err != nil {
			t.Fatalf("declared entry %v is rejected by the aggregate: %v", entry["pathId"], err)
		}
		if !p.IsActive() {
			t.Fatalf("declared entry %s is not Active after Define", p.ID())
		}
		if seen[string(p.ID())] {
			t.Fatalf("declared entry %s appears twice", p.ID())
		}
		seen[string(p.ID())] = true
	}
}

func findDeclared(t *testing.T, id string) map[string]any {
	t.Helper()
	for _, entry := range loadDeclaredCatalogue(t) {
		if entry["pathId"] == id {
			return entry
		}
	}
	t.Fatalf("catalogue/paths.json does not declare %s", id)
	return nil
}

// TestDeclaredCatalogue_RelocatePath pins the RELOCATE entry ADR 0033
// decided: family prefix `relocate`, direct, one required capability
// `relocate`, no destination role, the permissive eligibility, and an
// intake of SLOT_RELOCATION only (ADR 0034), which is what keeps
// order-management's path selection from routing a customer order line
// onto it. No fake eligibility attribute does that job any more.
func TestDeclaredCatalogue_RelocatePath(t *testing.T) {
	p, err := defineDeclared(t, findDeclared(t, "RELOCATE"))
	if err != nil {
		t.Fatalf("Define(RELOCATE) = %v", err)
	}
	if p.MatchPrefix() != "relocate" || !p.Direct() {
		t.Fatalf("RELOCATE matchPrefix/direct = %q/%v, want relocate/true", p.MatchPrefix(), p.Direct())
	}
	if !slices.Equal(p.RequiredCapabilities(), []shared.Capability{"relocate"}) {
		t.Fatalf("RELOCATE requiredCapabilities = %v, want [relocate]", p.RequiredCapabilities())
	}
	if p.DestinationLocationRole() != shared.DestinationLocationRoleUnset {
		t.Fatalf("RELOCATE destinationLocationRole = %q, want unset", p.DestinationLocationRole())
	}
	if !p.Eligibility().Equal(shared.Eligibility{}) {
		t.Fatalf("RELOCATE eligibility = %+v, want the permissive zero value (intake keeps order lines off it)", p.Eligibility())
	}
	if !p.Intake().Equal(shared.MustIntake(shared.DemandSourceSlotRelocation)) {
		t.Fatalf("RELOCATE intake = %v, want [SLOT_RELOCATION]", p.Intake().Strings())
	}
}

// TestDeclaredCatalogue_DispatchPath pins the DISPATCH entry ADR 0035
// decided: family prefix `dispatch`, direct, one required capability
// `dispatch`, no destination role, the permissive eligibility, and an
// intake of STOCK_TRANSFER only (ADR 0034). The path id is the
// fulfillment-execution task type (consume_work_released.go builds
// task.Type(pathDef.Id)), so it is `DISPATCH`, never `TRANSFER_DISPATCH`;
// the prefix still matches the `dispatch-transfer` id network-inventory-
// planning releases the transfer dispatch leg under.
func TestDeclaredCatalogue_DispatchPath(t *testing.T) {
	p, err := defineDeclared(t, findDeclared(t, "DISPATCH"))
	if err != nil {
		t.Fatalf("Define(DISPATCH) = %v", err)
	}
	if p.MatchPrefix() != "dispatch" || !p.Direct() {
		t.Fatalf("DISPATCH matchPrefix/direct = %q/%v, want dispatch/true", p.MatchPrefix(), p.Direct())
	}
	if !slices.Equal(p.RequiredCapabilities(), []shared.Capability{"dispatch"}) {
		t.Fatalf("DISPATCH requiredCapabilities = %v, want [dispatch]", p.RequiredCapabilities())
	}
	if p.DestinationLocationRole() != shared.DestinationLocationRoleUnset {
		t.Fatalf("DISPATCH destinationLocationRole = %q, want unset", p.DestinationLocationRole())
	}
	if p.CycleTimeP95() != 2*time.Hour {
		t.Fatalf("DISPATCH cycleTimeP95 = %v, want 2h", p.CycleTimeP95())
	}
	if !p.Eligibility().Equal(shared.Eligibility{}) {
		t.Fatalf("DISPATCH eligibility = %+v, want the permissive zero value (intake keeps order lines off it)", p.Eligibility())
	}
	if !p.Intake().Equal(shared.MustIntake(shared.DemandSourceStockTransfer)) {
		t.Fatalf("DISPATCH intake = %v, want [STOCK_TRANSFER]", p.Intake().Strings())
	}
}

// TestDeclaredCatalogue_ReplenishPath pins the REPLENISH entry ADR 0036
// decided: family prefix `replenish`, direct, one required capability
// `replenish`, no destination role, the permissive eligibility, cycle time
// 2h copied from RELOCATE, and an intake of REPLENISHMENT only (ADR 0034),
// the only source allowed to route work onto it.
func TestDeclaredCatalogue_ReplenishPath(t *testing.T) {
	p, err := defineDeclared(t, findDeclared(t, "REPLENISH"))
	if err != nil {
		t.Fatalf("Define(REPLENISH) = %v", err)
	}
	if p.MatchPrefix() != "replenish" || !p.Direct() {
		t.Fatalf("REPLENISH matchPrefix/direct = %q/%v, want replenish/true", p.MatchPrefix(), p.Direct())
	}
	if !slices.Equal(p.RequiredCapabilities(), []shared.Capability{"replenish"}) {
		t.Fatalf("REPLENISH requiredCapabilities = %v, want [replenish]", p.RequiredCapabilities())
	}
	if p.DestinationLocationRole() != shared.DestinationLocationRoleUnset {
		t.Fatalf("REPLENISH destinationLocationRole = %q, want unset", p.DestinationLocationRole())
	}
	if p.CycleTimeP95() != 2*time.Hour {
		t.Fatalf("REPLENISH cycleTimeP95 = %v, want 2h", p.CycleTimeP95())
	}
	if !p.Eligibility().Equal(shared.Eligibility{}) {
		t.Fatalf("REPLENISH eligibility = %+v, want the permissive zero value (intake keeps order lines off it)", p.Eligibility())
	}
	if !p.Intake().Equal(shared.MustIntake(shared.DemandSourceReplenishment)) {
		t.Fatalf("REPLENISH intake = %v, want [REPLENISHMENT]", p.Intake().Strings())
	}
}

// TestDeclaredCatalogue_ReplenishmentRoutesOnlyToReplenish pins both
// halves of the REPLENISHMENT rule: REPLENISH carries no other source,
// and no other declared path accepts REPLENISHMENT.
func TestDeclaredCatalogue_ReplenishmentRoutesOnlyToReplenish(t *testing.T) {
	var carriers []string
	for _, entry := range loadDeclaredCatalogue(t) {
		if declaredIntake(t, entry).Contains(shared.DemandSourceReplenishment) {
			id, _ := entry["pathId"].(string)
			carriers = append(carriers, id)
		}
	}
	if !slices.Equal(carriers, []string{"REPLENISH"}) {
		t.Fatalf("REPLENISHMENT carriers = %v, want [REPLENISH]", carriers)
	}
	if got := declaredStrings(findDeclared(t, "REPLENISH"), "intake"); !slices.Equal(got, []string{"REPLENISHMENT"}) {
		t.Fatalf("REPLENISH intake = %v, want exactly [REPLENISHMENT]", got)
	}
}

// TestDeclaredCatalogue_ExistingEntriesAreUnchanged freezes every entry
// that predates REPLENISH, byte-for-field, so adding a path can never
// silently revise PICK, PACK, REBIN, SLAM, RELOCATE or DISPATCH.
func TestDeclaredCatalogue_ExistingEntriesAreUnchanged(t *testing.T) {
	want := map[string]string{
		"PICK":     `{"cycleTimeP95":"2h","direct":true,"intake":["CUSTOMER_ORDER"],"matchPrefix":"pick","pathId":"PICK","requiredCapabilities":["pick"]}`,
		"PACK":     `{"cycleTimeP95":"2h","direct":true,"intake":[],"matchPrefix":"pack","pathId":"PACK","requiredCapabilities":["pack"]}`,
		"REBIN":    `{"cycleTimeP95":"2h","direct":true,"intake":[],"matchPrefix":"rebin","pathId":"REBIN","requiredCapabilities":["rebin"]}`,
		"SLAM":     `{"cycleTimeP95":"2h","direct":true,"intake":[],"matchPrefix":"slam","pathId":"SLAM","requiredCapabilities":["slam"]}`,
		"RELOCATE": `{"cycleTimeP95":"2h","direct":true,"intake":["SLOT_RELOCATION"],"matchPrefix":"relocate","pathId":"RELOCATE","requiredCapabilities":["relocate"]}`,
		"DISPATCH": `{"cycleTimeP95":"2h","direct":true,"intake":["STOCK_TRANSFER"],"matchPrefix":"dispatch","pathId":"DISPATCH","requiredCapabilities":["dispatch"]}`,
	}
	for id, w := range want {
		got, err := json.Marshal(findDeclared(t, id))
		if err != nil {
			t.Fatalf("marshal %s: %v", id, err)
		}
		if string(got) != w {
			t.Fatalf("declared %s changed\n got: %s\nwant: %s", id, got, w)
		}
	}
}

// TestDeclaredCatalogue_IntakePerPath pins E5's routing table (ADR 0034):
// PICK is the only customer-order entry point; PACK, REBIN and SLAM are
// downstream stages reached only by an explicit path id; RELOCATE takes
// slot-relocation work only; DISPATCH takes stock-transfer work only
// (ADR 0035); REPLENISH takes replenishment work only (ADR 0036).
func TestDeclaredCatalogue_IntakePerPath(t *testing.T) {
	want := map[string][]string{
		"PICK":      {"CUSTOMER_ORDER"},
		"PACK":      {},
		"REBIN":     {},
		"SLAM":      {},
		"RELOCATE":  {"SLOT_RELOCATION"},
		"DISPATCH":  {"STOCK_TRANSFER"},
		"REPLENISH": {"REPLENISHMENT"},
	}
	for id, sources := range want {
		p, err := defineDeclared(t, findDeclared(t, id))
		if err != nil {
			t.Fatalf("Define(%s) = %v", id, err)
		}
		if !slices.Equal(p.Intake().Strings(), sources) {
			t.Fatalf("%s intake = %v, want %v", id, p.Intake().Strings(), sources)
		}
	}
}

// TestDeclaredCatalogue_ExactlyOneCustomerOrderEntryPoint guards the
// regression E5 found: with every path a candidate, a customer order line
// tied on cycle time and resolved to PACK. The catalogue must offer
// CUSTOMER_ORDER on PICK and nowhere else.
func TestDeclaredCatalogue_ExactlyOneCustomerOrderEntryPoint(t *testing.T) {
	var entryPoints []string
	for _, entry := range loadDeclaredCatalogue(t) {
		// ADR 0038: an entry point gated by a required product attribute
		// (FULL_CASE, CROSS_DOCK) cannot tie with or capture a line that
		// lacks the attribute, so only UNGATED entry points are counted;
		// TestDeclaredCatalogue_GatedEntryPoints pins the gated ones.
		if declaredIntake(t, entry).Contains(shared.DemandSourceCustomerOrder) &&
			len(declaredEligibility(entry).RequiredProductAttributes()) == 0 {
			id, _ := entry["pathId"].(string)
			entryPoints = append(entryPoints, id)
		}
	}
	if !slices.Equal(entryPoints, []string{"PICK"}) {
		t.Fatalf("ungated CUSTOMER_ORDER entry points = %v, want [PICK]", entryPoints)
	}
}

// TestDeclaredCatalogue_ReseedingAnIdenticalEntryChangesNothing is the
// domain half of the seed's "present and identical -> no write" rule: a
// Revise with the declared values on the defined path reports no change,
// so a re-run never publishes a spurious ProcessPathUpdated.
func TestDeclaredCatalogue_ReseedingAnIdenticalEntryChangesNothing(t *testing.T) {
	p, err := defineDeclared(t, findDeclared(t, "RELOCATE"))
	if err != nil {
		t.Fatalf("Define(RELOCATE) = %v", err)
	}
	changed, err := p.Revise(p.MatchPrefix(), p.RequiredCapabilities(), p.CycleTimeP95(), p.Eligibility(), p.Intake(), time.Now())
	if err != nil || changed {
		t.Fatalf("Revise with the declared values = (changed %v, err %v), want (false, nil)", changed, err)
	}
}
