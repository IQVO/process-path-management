package processpath

import (
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// ADR 0038: the full-case flow (FULL_CASE, with BREAK_CASE for the loose
// remainder) and the cross-dock flow (CROSS_DOCK) are declared as catalogue
// data under the existing ProcessPath model. These tests pin each entry,
// the invariants that make the flows what they claim to be (no each
// handling on FULL_CASE, no put-away on CROSS_DOCK), and the routing the
// existing selection rules produce from the declared data.

const (
	attrFullCase  = "fullCase"
	attrCrossDock = "crossDock"
)

func definedDeclared(t *testing.T, id string) *ProcessPath {
	t.Helper()
	p, err := defineDeclared(t, findDeclared(t, id))
	if err != nil {
		t.Fatalf("Define(%s) = %v", id, err)
	}
	return p
}

func TestDeclaredCatalogue_FullCasePath(t *testing.T) {
	p := definedDeclared(t, "FULL_CASE")
	if p.MatchPrefix() != "full-case" || !p.Direct() {
		t.Fatalf("FULL_CASE matchPrefix/direct = %q/%v, want full-case/true", p.MatchPrefix(), p.Direct())
	}
	if !slices.Equal(p.RequiredCapabilities(), []shared.Capability{"pick"}) {
		t.Fatalf("FULL_CASE requiredCapabilities = %v, want [pick]", p.RequiredCapabilities())
	}
	if p.DestinationLocationRole() != shared.DestinationLocationRoleUnset {
		t.Fatalf("FULL_CASE destinationLocationRole = %q, want unset", p.DestinationLocationRole())
	}
	if p.CycleTimeP95() != 90*time.Minute {
		t.Fatalf("FULL_CASE cycleTimeP95 = %v, want 1h30m", p.CycleTimeP95())
	}
	if !p.Eligibility().Equal(shared.NewEligibility(nil, []string{attrFullCase}, nil, false)) {
		t.Fatalf("FULL_CASE eligibility = %+v, want only requiredProductAttributes [fullCase]", p.Eligibility())
	}
	if !p.Intake().Equal(shared.MustIntake(shared.DemandSourceCustomerOrder)) {
		t.Fatalf("FULL_CASE intake = %v, want [CUSTOMER_ORDER]", p.Intake().Strings())
	}
}

func TestDeclaredCatalogue_CrossDockPath(t *testing.T) {
	p := definedDeclared(t, "CROSS_DOCK")
	if p.MatchPrefix() != "cross-dock" || !p.Direct() {
		t.Fatalf("CROSS_DOCK matchPrefix/direct = %q/%v, want cross-dock/true", p.MatchPrefix(), p.Direct())
	}
	if !slices.Equal(p.RequiredCapabilities(), []shared.Capability{"dispatch"}) {
		t.Fatalf("CROSS_DOCK requiredCapabilities = %v, want [dispatch]", p.RequiredCapabilities())
	}
	if p.DestinationLocationRole() != shared.DestinationLocationRoleShipping {
		t.Fatalf("CROSS_DOCK destinationLocationRole = %q, want Shipping (received goods go straight to outbound)", p.DestinationLocationRole())
	}
	if p.CycleTimeP95() != time.Hour {
		t.Fatalf("CROSS_DOCK cycleTimeP95 = %v, want 1h", p.CycleTimeP95())
	}
	if !p.Eligibility().Equal(shared.NewEligibility(nil, []string{attrCrossDock}, nil, false)) {
		t.Fatalf("CROSS_DOCK eligibility = %+v, want only requiredProductAttributes [crossDock]", p.Eligibility())
	}
	if !p.Intake().Equal(shared.MustIntake(shared.DemandSourceCustomerOrder, shared.DemandSourceStockTransfer)) {
		t.Fatalf("CROSS_DOCK intake = %v, want [CUSTOMER_ORDER STOCK_TRANSFER]", p.Intake().Strings())
	}
}

func TestDeclaredCatalogue_BreakCasePath(t *testing.T) {
	p := definedDeclared(t, "BREAK_CASE")
	if p.MatchPrefix() != "break" || !p.Direct() {
		t.Fatalf("BREAK_CASE matchPrefix/direct = %q/%v, want break/true", p.MatchPrefix(), p.Direct())
	}
	if !slices.Equal(p.RequiredCapabilities(), []shared.Capability{"break"}) {
		t.Fatalf("BREAK_CASE requiredCapabilities = %v, want [break]", p.RequiredCapabilities())
	}
	if p.DestinationLocationRole() != shared.DestinationLocationRoleUnset {
		t.Fatalf("BREAK_CASE destinationLocationRole = %q, want unset", p.DestinationLocationRole())
	}
	if p.CycleTimeP95() != 2*time.Hour {
		t.Fatalf("BREAK_CASE cycleTimeP95 = %v, want 2h", p.CycleTimeP95())
	}
	if !p.Eligibility().Equal(shared.Eligibility{}) {
		t.Fatalf("BREAK_CASE eligibility = %+v, want the permissive zero value", p.Eligibility())
	}
	if !p.Intake().IsEmpty() {
		t.Fatalf("BREAK_CASE intake = %v, want [] (reached only by an explicit path id from an upstream stage)", p.Intake().Strings())
	}
}

func capabilityStrings(p *ProcessPath) []string {
	var out []string
	for _, c := range p.RequiredCapabilities() {
		out = append(out, string(c))
	}
	return out
}

// TestDeclaredCatalogue_FullCaseHandlesNoEach pins the flow's reason to
// exist: whole-case work needs neither the case-break capability nor any
// each-handling step, and a line that is not a whole number of cases cannot
// carry the attribute the path requires, so it keeps routing to PICK.
func TestDeclaredCatalogue_FullCaseHandlesNoEach(t *testing.T) {
	caps := capabilityStrings(definedDeclared(t, "FULL_CASE"))
	for _, forbidden := range []string{"break", "relocate", "replenish", "pack", "rebin", "slam"} {
		if slices.Contains(caps, forbidden) {
			t.Fatalf("FULL_CASE requires %q, a step whole-case work must not need: %v", forbidden, caps)
		}
	}
	if !slices.Contains(caps, "pick") {
		t.Fatalf("FULL_CASE requiredCapabilities = %v, want the existing pick capability reused", caps)
	}
	// break-case work is its own path, so its capability is never folded into the flow.
	if !slices.Equal(capabilityStrings(definedDeclared(t, "BREAK_CASE")), []string{"break"}) {
		t.Fatal("BREAK_CASE must stay the only path requiring the break capability")
	}
	if got := countPathsRequiring(t, "break"); got != 1 {
		t.Fatalf("%d declared paths require break, want exactly 1 (BREAK_CASE)", got)
	}
}

func countPathsRequiring(t *testing.T, capability string) int {
	t.Helper()
	n := 0
	for _, entry := range loadDeclaredCatalogue(t) {
		if slices.Contains(declaredStrings(entry, "requiredCapabilities"), capability) {
			n++
		}
	}
	return n
}

// TestDeclaredCatalogue_CrossDockHasNoPutAway pins the other flow: no
// put-away (relocate), no pick, no case break, and the completed work is
// destined for Shipping.
func TestDeclaredCatalogue_CrossDockHasNoPutAway(t *testing.T) {
	p := definedDeclared(t, "CROSS_DOCK")
	caps := capabilityStrings(p)
	for _, forbidden := range []string{"relocate", "replenish", "pick", "break", "stow", "putaway"} {
		if slices.Contains(caps, forbidden) {
			t.Fatalf("CROSS_DOCK requires %q, a storage step a cross-dock must skip: %v", forbidden, caps)
		}
	}
	if p.DestinationLocationRole() != shared.DestinationLocationRoleShipping {
		t.Fatalf("CROSS_DOCK destination = %q, want Shipping", p.DestinationLocationRole())
	}
	if got := countPathsRequiring(t, "relocate"); got != 1 {
		t.Fatalf("%d declared paths require relocate, want exactly 1 (RELOCATE)", got)
	}
}

// TestDeclaredCatalogue_NewMatchPrefixesNeverCollide applies the consumers'
// matching rule (id == prefix or id starts with prefix + "-") to every pair
// of declared prefixes plus ADR 0008's families, so no family can swallow
// another's ids. "break" must not match "break-case" siblings unexpectedly
// either: the rule is applied in both directions.
func TestDeclaredCatalogue_NewMatchPrefixesNeverCollide(t *testing.T) {
	prefixes := map[string]string{}
	for _, entry := range loadDeclaredCatalogue(t) {
		id, _ := entry["pathId"].(string)
		prefix, _ := entry["matchPrefix"].(string)
		prefixes[id] = prefix
	}
	for _, id := range []string{"PREP", "PROBLEM_SOLVE", "WATER_SPIDER", "AMNESTY", "RETURNS", "TRANSFER", "DAMAGE"} {
		prefixes[id] = strings.ToLower(strings.ReplaceAll(id, "_", "-"))
	}
	for a, pa := range prefixes {
		for b, pb := range prefixes {
			if a == b {
				continue
			}
			if pa == pb || strings.HasPrefix(pa, pb+"-") {
				t.Fatalf("matchPrefix %q (%s) collides with %q (%s)", pa, a, pb, b)
			}
		}
	}
}

// TestDeclaredCatalogue_GatedEntryPoints is the amended E5 guard (see
// TestDeclaredCatalogue_ExactlyOneCustomerOrderEntryPoint): PICK stays the
// only UNGATED customer-order entry point, and every other path that accepts
// CUSTOMER_ORDER must require a product attribute, so it can never tie with
// or capture a line that does not carry that attribute.
func TestDeclaredCatalogue_GatedEntryPoints(t *testing.T) {
	var gated []string
	for _, entry := range loadDeclaredCatalogue(t) {
		id, _ := entry["pathId"].(string)
		if !declaredIntake(t, entry).Contains(shared.DemandSourceCustomerOrder) || id == "PICK" {
			continue
		}
		if len(declaredEligibility(entry).RequiredProductAttributes()) == 0 {
			t.Fatalf("%s accepts CUSTOMER_ORDER without requiring a product attribute: it would compete with PICK for every line", id)
		}
		gated = append(gated, id)
	}
	sort.Strings(gated)
	if !slices.Equal(gated, []string{"CROSS_DOCK", "FULL_CASE"}) {
		t.Fatalf("gated CUSTOMER_ORDER entry points = %v, want [CROSS_DOCK FULL_CASE]", gated)
	}
	if got := declaredEligibility(findDeclared(t, "PICK")); !got.Equal(shared.Eligibility{}) {
		t.Fatalf("PICK eligibility = %+v, want the permissive zero value", got)
	}
}

func TestDeclaredCatalogue_StockTransferCarriers(t *testing.T) {
	var carriers []string
	for _, entry := range loadDeclaredCatalogue(t) {
		if declaredIntake(t, entry).Contains(shared.DemandSourceStockTransfer) {
			id, _ := entry["pathId"].(string)
			carriers = append(carriers, id)
		}
	}
	sort.Strings(carriers)
	if !slices.Equal(carriers, []string{"CROSS_DOCK", "DISPATCH"}) {
		t.Fatalf("STOCK_TRANSFER carriers = %v, want [CROSS_DOCK DISPATCH]", carriers)
	}
}

// TestDeclaredCatalogue_CycleTimesOrderTheFlows pins the strict order the
// selection relies on, so it never falls through to the PathId tie-break:
// cross-dock < full-case < pick.
func TestDeclaredCatalogue_CycleTimesOrderTheFlows(t *testing.T) {
	cross := definedDeclared(t, "CROSS_DOCK").CycleTimeP95()
	full := definedDeclared(t, "FULL_CASE").CycleTimeP95()
	pick := definedDeclared(t, "PICK").CycleTimeP95()
	if !(cross < full && full < pick) {
		t.Fatalf("cycle times cross-dock=%v full-case=%v pick=%v, want strictly increasing", cross, full, pick)
	}
}

// candidate mirrors the fields of order-management's ActivePathCandidate
// that its PathSelectionPolicy reads. The policy lives in that service; this
// is a faithful copy of its documented rule used only to prove what the
// declared data selects.
type candidate struct {
	id          string
	cycle       time.Duration
	eligibility shared.Eligibility
	intake      shared.Intake
}

func declaredCandidates(t *testing.T) []candidate {
	t.Helper()
	var out []candidate
	for _, entry := range loadDeclaredCatalogue(t) {
		p, err := defineDeclared(t, entry)
		if err != nil {
			t.Fatalf("define %v: %v", entry["pathId"], err)
		}
		out = append(out, candidate{string(p.ID()), p.CycleTimeP95(), p.Eligibility(), p.Intake()})
	}
	return out
}

func selectPath(cands []candidate, quantity int, attrs []string) (string, bool) {
	var eligible []candidate
	for _, c := range cands {
		if !c.intake.Contains(shared.DemandSourceCustomerOrder) {
			continue
		}
		if max := c.eligibility.MaxUnitsPerLine(); max != nil && quantity > *max {
			continue
		}
		ok := true
		for _, req := range c.eligibility.RequiredProductAttributes() {
			ok = ok && slices.Contains(attrs, req)
		}
		for _, ex := range c.eligibility.ExcludedProductAttributes() {
			ok = ok && !slices.Contains(attrs, ex)
		}
		if ok {
			eligible = append(eligible, c)
		}
	}
	if len(eligible) == 0 {
		return "", false
	}
	sort.Slice(eligible, func(i, j int) bool {
		if eligible[i].cycle != eligible[j].cycle {
			return eligible[i].cycle < eligible[j].cycle
		}
		return eligible[i].id < eligible[j].id
	})
	return eligible[0].id, true
}

// TestDeclaredCatalogue_ExistingSelectionRulesRouteTheFlows runs the
// existing rule over the declared data. A line with no pack-derived
// attribute (everything that exists today) still resolves to PICK.
func TestDeclaredCatalogue_ExistingSelectionRulesRouteTheFlows(t *testing.T) {
	cands := declaredCandidates(t)
	cases := []struct {
		name  string
		qty   int
		attrs []string
		want  string
	}{
		{"no attributes, as every line today", 3, nil, "PICK"},
		{"hazmat only", 1, []string{"hazmat"}, "PICK"},
		{"whole cases", 24, []string{attrFullCase}, "FULL_CASE"},
		{"cross-dock stock", 5, []string{attrCrossDock}, "CROSS_DOCK"},
		{"both attributes: the faster cross-dock wins", 24, []string{attrFullCase, attrCrossDock}, "CROSS_DOCK"},
		{"a near-miss attribute spelling is not the attribute", 24, []string{"fullcase"}, "PICK"},
		{"another near miss", 24, []string{"cross-dock"}, "PICK"},
		{"single unit", 1, []string{attrFullCase}, "FULL_CASE"},
	}
	for _, tc := range cases {
		got, ok := selectPath(cands, tc.qty, tc.attrs)
		if !ok || got != tc.want {
			t.Errorf("%s: selected %q (ok %v), want %q", tc.name, got, ok, tc.want)
		}
	}
}

// TestDeclaredCatalogue_AdditiveForEveryExistingLine proves the additive
// claim: with the three new entries removed, the selection for any line
// without the new attributes is identical to the selection with them.
func TestDeclaredCatalogue_AdditiveForEveryExistingLine(t *testing.T) {
	all := declaredCandidates(t)
	var legacy []candidate
	for _, c := range all {
		if !slices.Contains([]string{"FULL_CASE", "CROSS_DOCK", "BREAK_CASE"}, c.id) {
			legacy = append(legacy, c)
		}
	}
	if len(legacy) != 7 {
		t.Fatalf("catalogue has %d pre-existing entries, want 7", len(legacy))
	}
	lines := [][]string{nil, {}, {"hazmat"}, {"fragile"}, {"giftWrap"}, {"hazmat", "fragile", "giftWrap"}}
	for _, attrs := range lines {
		for _, qty := range []int{1, 2, 100} {
			before, okBefore := selectPath(legacy, qty, attrs)
			after, okAfter := selectPath(all, qty, attrs)
			if before != after || okBefore != okAfter {
				t.Fatalf("qty %d attrs %v: selection changed %q/%v -> %q/%v", qty, attrs, before, okBefore, after, okAfter)
			}
		}
	}
}

// TestDeclaredCatalogue_NewEntriesReseedAsNoOps is the domain half of the
// seed's idempotence for each new entry.
func TestDeclaredCatalogue_NewEntriesReseedAsNoOps(t *testing.T) {
	for _, id := range []string{"FULL_CASE", "CROSS_DOCK", "BREAK_CASE"} {
		p := definedDeclared(t, id)
		changed, err := p.Revise(p.MatchPrefix(), p.RequiredCapabilities(), p.CycleTimeP95(), p.Eligibility(), p.Intake(), time.Now())
		if err != nil || changed {
			t.Fatalf("%s: Revise with the declared values = (changed %v, err %v), want (false, nil)", id, changed, err)
		}
		changed, err = p.Revise(p.MatchPrefix(), p.RequiredCapabilities(), p.CycleTimeP95()+time.Nanosecond, p.Eligibility(), p.Intake(), time.Now())
		if err != nil || !changed {
			t.Fatalf("%s: a 1ns cycle-time drift = (changed %v, err %v), want (true, nil)", id, changed, err)
		}
	}
}

// TestDeclaredCatalogue_NewEntriesSitOnTheValidityBoundary re-defines each
// new entry one step either side of every invariant it depends on.
func TestDeclaredCatalogue_NewEntriesSitOnTheValidityBoundary(t *testing.T) {
	now := time.Now()
	for _, id := range []string{"FULL_CASE", "CROSS_DOCK", "BREAK_CASE"} {
		p := definedDeclared(t, id)
		def := func(prefix string, caps []shared.Capability, cycle time.Duration) error {
			_, err := Define(p.ID(), prefix, p.Direct(), caps, p.DestinationLocationRole(), cycle, p.Eligibility(), p.Intake(), now)
			return err
		}
		caps := p.RequiredCapabilities()
		if err := def(p.MatchPrefix(), caps, time.Nanosecond); err != nil {
			t.Errorf("%s: cycle time 1ns = %v, want accepted", id, err)
		}
		if err := def(p.MatchPrefix(), caps, 0); err != ErrInvalidCycleTime {
			t.Errorf("%s: cycle time 0 = %v, want ErrInvalidCycleTime", id, err)
		}
		if err := def(p.MatchPrefix(), nil, p.CycleTimeP95()); err != ErrNoRequiredCapabilities {
			t.Errorf("%s: no capabilities = %v, want ErrNoRequiredCapabilities", id, err)
		}
		if err := def(strings.ToUpper(p.MatchPrefix()), caps, p.CycleTimeP95()); err != ErrMatchPrefixNotLowercase {
			t.Errorf("%s: upper-case prefix = %v, want ErrMatchPrefixNotLowercase", id, err)
		}
		if err := def("", caps, p.CycleTimeP95()); err != ErrEmptyMatchPrefix {
			t.Errorf("%s: empty prefix = %v, want ErrEmptyMatchPrefix", id, err)
		}
	}
}
