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
	return Define(shared.PathId(id), prefix, direct, caps, shared.DestinationLocationRole(role), cycle, declaredEligibility(entry), shared.Intake{}, time.Now())
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
// `relocate`, no destination role, and an eligibility that only
// slot-relocation work satisfies (so order-management's path selection
// never routes a customer order line onto it).
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
	if !slices.Equal(p.Eligibility().RequiredProductAttributes(), []string{"slot-relocation"}) {
		t.Fatalf("RELOCATE requiredProductAttributes = %v, want [slot-relocation]", p.Eligibility().RequiredProductAttributes())
	}
	if p.Eligibility().MaxUnitsPerLine() != nil || len(p.Eligibility().ExcludedProductAttributes()) != 0 || p.Eligibility().NonSortable() {
		t.Fatalf("RELOCATE eligibility declares more than the slot-relocation rule: %+v", p.Eligibility())
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
	changed, err := p.Revise(p.MatchPrefix(), p.RequiredCapabilities(), p.CycleTimeP95(), p.Eligibility(), shared.Intake{}, time.Now())
	if err != nil || changed {
		t.Fatalf("Revise with the declared values = (changed %v, err %v), want (false, nil)", changed, err)
	}
}
