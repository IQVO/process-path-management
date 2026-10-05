package cptschedule

import (
	"errors"
	"testing"
	"time"

	"github.com/claudioed/process-path-management/internal/domain/shared"
)

func validCutoff(t *testing.T) Cutoff {
	t.Helper()
	c, err := NewCutoff("sp1-1500", "15:00", []Weekday{Monday, Tuesday, Wednesday, Thursday, Friday}, "ground", []shared.PathId{"PICK"})
	if err != nil {
		t.Fatalf("unexpected error building a valid cutoff: %v", err)
	}
	return c
}

// --- Cutoff invariants -----------------------------------------------------

func TestNewCutoff_RejectsEmptyCptId(t *testing.T) {
	_, err := NewCutoff("", "15:00", []Weekday{Monday}, "ground", []shared.PathId{"PICK"})
	if !errors.Is(err, ErrEmptyCptId) {
		t.Fatalf("want ErrEmptyCptId, got %v", err)
	}
}

func TestNewCutoff_RejectsEmptyLocalTime(t *testing.T) {
	_, err := NewCutoff("sp1-1500", "", []Weekday{Monday}, "ground", []shared.PathId{"PICK"})
	if !errors.Is(err, ErrEmptyLocalTime) {
		t.Fatalf("want ErrEmptyLocalTime, got %v", err)
	}
}

func TestNewCutoff_RejectsMalformedLocalTime(t *testing.T) {
	for _, bad := range []string{"3pm", "15:00:00", "25:00", "15:60", "1500", "15-00"} {
		_, err := NewCutoff("sp1-1500", bad, []Weekday{Monday}, "ground", []shared.PathId{"PICK"})
		if !errors.Is(err, ErrInvalidLocalTime) {
			t.Fatalf("localTime=%q: want ErrInvalidLocalTime, got %v", bad, err)
		}
	}
}

func TestNewCutoff_AcceptsValidLocalTimeBoundaries(t *testing.T) {
	for _, good := range []string{"00:00", "23:59", "09:05", "15:00"} {
		if _, err := NewCutoff("sp1-1500", good, []Weekday{Monday}, "ground", []shared.PathId{"PICK"}); err != nil {
			t.Fatalf("localTime=%q: unexpected error: %v", good, err)
		}
	}
}

func TestNewCutoff_RejectsEmptyDaysOfWeek(t *testing.T) {
	_, err := NewCutoff("sp1-1500", "15:00", nil, "ground", []shared.PathId{"PICK"})
	if !errors.Is(err, ErrNoDaysOfWeek) {
		t.Fatalf("want ErrNoDaysOfWeek, got %v", err)
	}
}

func TestNewCutoff_RejectsInvalidDayOfWeek(t *testing.T) {
	_, err := NewCutoff("sp1-1500", "15:00", []Weekday{"Funday"}, "ground", []shared.PathId{"PICK"})
	if !errors.Is(err, ErrInvalidDayOfWeek) {
		t.Fatalf("want ErrInvalidDayOfWeek, got %v", err)
	}
}

func TestNewCutoff_RejectsEmptyShipMethod(t *testing.T) {
	_, err := NewCutoff("sp1-1500", "15:00", []Weekday{Monday}, "", []shared.PathId{"PICK"})
	if !errors.Is(err, ErrEmptyShipMethod) {
		t.Fatalf("want ErrEmptyShipMethod, got %v", err)
	}
}

func TestNewCutoff_RejectsEmptyEligiblePathIds(t *testing.T) {
	_, err := NewCutoff("sp1-1500", "15:00", []Weekday{Monday}, "ground", nil)
	if !errors.Is(err, ErrNoEligiblePathIds) {
		t.Fatalf("want ErrNoEligiblePathIds, got %v", err)
	}
}

func TestCutoff_Accessors_ReturnDefensiveCopies(t *testing.T) {
	c := validCutoff(t)
	days := c.DaysOfWeek()
	days[0] = "Funday"
	if c.DaysOfWeek()[0] != Monday {
		t.Fatal("expected internal daysOfWeek to be unaffected by mutating the returned slice")
	}
	ids := c.EligiblePathIds()
	ids[0] = "MUTATED"
	if c.EligiblePathIds()[0] != "PICK" {
		t.Fatal("expected internal eligiblePathIds to be unaffected by mutating the returned slice")
	}
}

// --- CPTSchedule.Define invariants ------------------------------------------

func TestDefine_RejectsEmptyTimezone(t *testing.T) {
	_, err := Define("sp1", "", []Cutoff{validCutoff(t)}, time.Now())
	if !errors.Is(err, ErrEmptyTimezone) {
		t.Fatalf("want ErrEmptyTimezone, got %v", err)
	}
}

func TestDefine_RejectsUnrecognizedTimezone(t *testing.T) {
	_, err := Define("sp1", "Not/AZone", []Cutoff{validCutoff(t)}, time.Now())
	if !errors.Is(err, ErrInvalidTimezone) {
		t.Fatalf("want ErrInvalidTimezone, got %v", err)
	}
}

func TestDefine_RejectsNoCutoffs(t *testing.T) {
	_, err := Define("sp1", "America/Sao_Paulo", nil, time.Now())
	if !errors.Is(err, ErrNoCutoffs) {
		t.Fatalf("want ErrNoCutoffs, got %v", err)
	}
}

func TestDefine_RejectsDuplicateCptId(t *testing.T) {
	c1 := validCutoff(t)
	c2, err := NewCutoff("sp1-1500", "18:00", []Weekday{Monday}, "same-day", []shared.PathId{"PACK"})
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	_, err = Define("sp1", "America/Sao_Paulo", []Cutoff{c1, c2}, time.Now())
	if !errors.Is(err, ErrDuplicateCptId) {
		t.Fatalf("want ErrDuplicateCptId, got %v", err)
	}
}

func TestDefine_ValidInput_ConstructsSchedule(t *testing.T) {
	now := time.Now()
	c := validCutoff(t)
	s, err := Define("sp1", "America/Sao_Paulo", []Cutoff{c}, now)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.SiteId() != "sp1" || s.Timezone() != "America/Sao_Paulo" {
		t.Fatalf("unexpected fields: %+v", s)
	}
	if len(s.Cutoffs()) != 1 {
		t.Fatalf("want 1 cutoff, got %d", len(s.Cutoffs()))
	}
	if s.CreatedAt() != now || s.UpdatedAt() != now {
		t.Fatal("expected createdAt/updatedAt to both equal the construction time")
	}
}

func TestDefine_DoesNotEnforceEligiblePathIdsAgainstAnyStore(t *testing.T) {
	// The domain package itself accepts any non-empty eligiblePathIds --
	// the cross-aggregate "must be Active in this service's own store"
	// check is a use-case-level concern (ADR 0010), not a domain
	// invariant, since it needs the ProcessPathRepo.
	c, err := NewCutoff("sp1-1500", "15:00", []Weekday{Monday}, "ground", []shared.PathId{"DOES-NOT-EXIST"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := Define("sp1", "America/Sao_Paulo", []Cutoff{c}, time.Now()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// --- CPTSchedule.Revise -----------------------------------------------------

func TestRevise_NoActualChange_ReturnsChangedFalse(t *testing.T) {
	c := validCutoff(t)
	s, _ := Define("sp1", "America/Sao_Paulo", []Cutoff{c}, time.Now())
	changed, err := s.Revise("America/Sao_Paulo", []Cutoff{c}, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if changed {
		t.Fatal("expected changed=false for an identical revision")
	}
}

func TestRevise_TimezoneChange_ReturnsChangedTrue(t *testing.T) {
	c := validCutoff(t)
	s, _ := Define("sp1", "America/Sao_Paulo", []Cutoff{c}, time.Now())
	later := time.Now().Add(time.Hour)
	changed, err := s.Revise("America/New_York", []Cutoff{c}, later)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !changed {
		t.Fatal("expected changed=true when timezone differs")
	}
	if s.Timezone() != "America/New_York" {
		t.Fatalf("want America/New_York, got %s", s.Timezone())
	}
	if s.UpdatedAt() != later {
		t.Fatal("expected updatedAt to advance to the revision time")
	}
}

func TestRevise_CutoffsChange_ReturnsChangedTrue(t *testing.T) {
	c1 := validCutoff(t)
	s, _ := Define("sp1", "America/Sao_Paulo", []Cutoff{c1}, time.Now())
	c2, err := NewCutoff("sp1-1800", "18:00", []Weekday{Tuesday}, "same-day", []shared.PathId{"PACK"})
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	changed, err := s.Revise("America/Sao_Paulo", []Cutoff{c1, c2}, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !changed {
		t.Fatal("expected changed=true when the cutoff set differs")
	}
	if len(s.Cutoffs()) != 2 {
		t.Fatalf("want 2 cutoffs, got %d", len(s.Cutoffs()))
	}
}

func TestRevise_InvalidInput_RejectedWithoutMutatingState(t *testing.T) {
	c := validCutoff(t)
	s, _ := Define("sp1", "America/Sao_Paulo", []Cutoff{c}, time.Now())
	if _, err := s.Revise("", []Cutoff{c}, time.Now()); !errors.Is(err, ErrEmptyTimezone) {
		t.Fatalf("want ErrEmptyTimezone, got %v", err)
	}
	if s.Timezone() != "America/Sao_Paulo" {
		t.Fatal("expected the schedule to be unchanged after a rejected revision")
	}
}

func TestRevise_DuplicateCptId_Rejected(t *testing.T) {
	c1 := validCutoff(t)
	s, _ := Define("sp1", "America/Sao_Paulo", []Cutoff{c1}, time.Now())
	c2, err := NewCutoff(c1.CptId(), "18:00", []Weekday{Tuesday}, "same-day", []shared.PathId{"PACK"})
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	if _, err := s.Revise("America/Sao_Paulo", []Cutoff{c1, c2}, time.Now()); !errors.Is(err, ErrDuplicateCptId) {
		t.Fatalf("want ErrDuplicateCptId, got %v", err)
	}
}

// --- Rehydrate ---------------------------------------------------------------

func TestRehydrate_ReconstructsWithoutRevalidating(t *testing.T) {
	created := time.Now().Add(-time.Hour)
	updated := time.Now()
	// Rehydrate deliberately accepts state Define would reject (e.g. an
	// empty timezone), mirroring processpath.Rehydrate's own posture:
	// repository adapters never re-run construction invariants on read.
	s := Rehydrate("sp1", "", nil, created, updated, 5)
	if s.SiteId() != "sp1" {
		t.Fatalf("want siteId sp1, got %s", s.SiteId())
	}
	if s.CreatedAt() != created || s.UpdatedAt() != updated {
		t.Fatal("want createdAt/updatedAt to match the rehydrated values exactly")
	}
	// ADR 0017: Rehydrate preserves the row's optimistic-concurrency
	// version exactly; it is metadata the domain never rewrites.
	if s.Version() != 5 {
		t.Fatalf("want rehydrated version 5, got %d", s.Version())
	}
}

// --- no-op revision detection: localTime parsing ------------------------------

func TestIsValidLocalTime(t *testing.T) {
	for _, tt := range []struct {
		in   string
		want bool
	}{
		{"00:00", true},
		{"23:59", true},
		{"09:05", true},
		{"15:00", true},
		{"3pm", false},
		{"15:00:00", false},
		{"1500", false},
		{"15-00", false},
		{"1a:00", false},
		{"a0:30", false},
		{"15:0a", false},
		{"15:a0", false},
		{"24:00", false},
		{"25:00", false},
		{"15:60", false},
		{"-1:30", false},
		{"", false},
	} {
		if got := isValidLocalTime(tt.in); got != tt.want {
			t.Fatalf("isValidLocalTime(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

func TestParseDigits(t *testing.T) {
	for _, tt := range []struct {
		in      string
		want    int
		wantErr bool
	}{
		{"00", 0, false},
		{"07", 7, false},
		{"09", 9, false},
		{"23", 23, false},
		{"59", 59, false},
		{"99", 99, false},
		{"1a", 0, true},
		{"a1", 0, true},
		{"-1", 0, true},
		{" 1", 0, true},
		{":0", 0, true},
		{"", 0, false},
	} {
		got, err := parseDigits(tt.in)
		if tt.wantErr {
			if err == nil {
				t.Fatalf("parseDigits(%q): want error, got %d", tt.in, got)
			}
			continue
		}
		if err != nil {
			t.Fatalf("parseDigits(%q): unexpected error %v", tt.in, err)
		}
		if got != tt.want {
			t.Fatalf("parseDigits(%q) = %d, want %d", tt.in, got, tt.want)
		}
	}
}

// --- no-op revision detection: equality helpers --------------------------------

func TestWeekdaysEqual(t *testing.T) {
	for _, tt := range []struct {
		name string
		a    []Weekday
		b    []Weekday
		want bool
	}{
		{"nil vs nil", nil, nil, true},
		{"nil vs empty", nil, []Weekday{}, true},
		{"empty vs nil", []Weekday{}, nil, true},
		{"identical", []Weekday{Monday, Tuesday}, []Weekday{Monday, Tuesday}, true},
		{"different lengths", []Weekday{Monday}, []Weekday{Monday, Tuesday}, false},
		{"same length, different day", []Weekday{Monday, Tuesday}, []Weekday{Monday, Wednesday}, false},
		{"reordered days are not equal", []Weekday{Monday, Tuesday}, []Weekday{Tuesday, Monday}, false},
	} {
		if got := weekdaysEqual(tt.a, tt.b); got != tt.want {
			t.Fatalf("%s: weekdaysEqual(%v, %v) = %v, want %v", tt.name, tt.a, tt.b, got, tt.want)
		}
	}
}

func TestPathIdsEqual(t *testing.T) {
	for _, tt := range []struct {
		name string
		a    []shared.PathId
		b    []shared.PathId
		want bool
	}{
		{"nil vs nil", nil, nil, true},
		{"nil vs empty", nil, []shared.PathId{}, true},
		{"empty vs nil", []shared.PathId{}, nil, true},
		{"identical", []shared.PathId{"PICK", "PACK"}, []shared.PathId{"PICK", "PACK"}, true},
		{"different lengths", []shared.PathId{"PICK"}, []shared.PathId{"PICK", "PACK"}, false},
		{"same length, different id", []shared.PathId{"PICK", "PACK"}, []shared.PathId{"PICK", "SLAM"}, false},
		{"reordered ids are not equal", []shared.PathId{"PICK", "PACK"}, []shared.PathId{"PACK", "PICK"}, false},
	} {
		if got := pathIdsEqual(tt.a, tt.b); got != tt.want {
			t.Fatalf("%s: pathIdsEqual(%v, %v) = %v, want %v", tt.name, tt.a, tt.b, got, tt.want)
		}
	}
}

func TestCutoffsEqual(t *testing.T) {
	base := func() Cutoff {
		return Cutoff{
			cptId:           "sp1-1500",
			localTime:       "15:00",
			daysOfWeek:      []Weekday{Monday, Tuesday},
			shipMethod:      "ground",
			eligiblePathIds: []shared.PathId{"PICK", "PACK"},
		}
	}
	for _, tt := range []struct {
		name   string
		mutate func(*Cutoff)
		want   bool
	}{
		{"identical cutoffs", nil, true},
		{"different cptId", func(c *Cutoff) { c.cptId = "sp1-1800" }, false},
		{"different localTime", func(c *Cutoff) { c.localTime = "18:00" }, false},
		{"different shipMethod", func(c *Cutoff) { c.shipMethod = "same-day" }, false},
		{"different daysOfWeek", func(c *Cutoff) { c.daysOfWeek = []Weekday{Monday, Wednesday} }, false},
		{"different eligiblePathIds", func(c *Cutoff) { c.eligiblePathIds = []shared.PathId{"PICK", "SLAM"} }, false},
	} {
		a := base()
		b := base()
		if tt.mutate != nil {
			tt.mutate(&b)
		}
		if got := cutoffsEqual([]Cutoff{a}, []Cutoff{b}); got != tt.want {
			t.Fatalf("%s: cutoffsEqual = %v, want %v", tt.name, got, tt.want)
		}
	}
	if cutoffsEqual([]Cutoff{base()}, []Cutoff{base(), base()}) {
		t.Fatal("expected cutoffsEqual=false for differing cutoff counts")
	}
	if !cutoffsEqual(nil, []Cutoff{}) {
		t.Fatal("expected cutoffsEqual(nil, empty)=true")
	}
}

// --- Revise changed-detection per mutated dimension -----------------------------

func mustCutoff(t *testing.T, cptId, localTime string, days []Weekday, shipMethod string, ids []shared.PathId) Cutoff {
	t.Helper()
	c, err := NewCutoff(cptId, localTime, days, shipMethod, ids)
	if err != nil {
		t.Fatalf("unexpected error building cutoff: %v", err)
	}
	return c
}

func TestRevise_MutatedCutoffDimension_ReturnsChangedTrue(t *testing.T) {
	weekdays := []Weekday{Monday, Tuesday, Wednesday, Thursday, Friday}
	for _, tt := range []struct {
		name     string
		revision Cutoff
	}{
		{"different cptId", mustCutoff(t, "sp1-1800", "15:00", weekdays, "ground", []shared.PathId{"PICK"})},
		{"different localTime", mustCutoff(t, "sp1-1500", "18:00", weekdays, "ground", []shared.PathId{"PICK"})},
		{"different shipMethod", mustCutoff(t, "sp1-1500", "15:00", weekdays, "same-day", []shared.PathId{"PICK"})},
		{"different daysOfWeek, same count", mustCutoff(t, "sp1-1500", "15:00", []Weekday{Monday, Tuesday, Wednesday, Thursday, Saturday}, "ground", []shared.PathId{"PICK"})},
		{"different eligiblePathIds, same count", mustCutoff(t, "sp1-1500", "15:00", weekdays, "ground", []shared.PathId{"PACK"})},
	} {
		s, err := Define("sp1", "America/Sao_Paulo", []Cutoff{validCutoff(t)}, time.Now())
		if err != nil {
			t.Fatalf("%s: setup: %v", tt.name, err)
		}
		changed, err := s.Revise("America/Sao_Paulo", []Cutoff{tt.revision}, time.Now())
		if err != nil {
			t.Fatalf("%s: unexpected error: %v", tt.name, err)
		}
		if !changed {
			t.Fatalf("%s: expected changed=true when only that dimension differs", tt.name)
		}
	}
}

func TestRevise_ReorderedDaysOfWeek_ReturnsChangedTrue(t *testing.T) {
	// daysOfWeek is compared order-sensitively, so a reordered-but-equal
	// set is reported as a change (and republishes CPTScheduleChanged).
	s, err := Define("sp1", "America/Sao_Paulo", []Cutoff{mustCutoff(t, "sp1-1500", "15:00", []Weekday{Monday, Tuesday}, "ground", []shared.PathId{"PICK"})}, time.Now())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	changed, err := s.Revise("America/Sao_Paulo", []Cutoff{mustCutoff(t, "sp1-1500", "15:00", []Weekday{Tuesday, Monday}, "ground", []shared.PathId{"PICK"})}, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !changed {
		t.Fatal("expected changed=true for a reordered daysOfWeek list (order is significant)")
	}
}

func TestRevise_ReorderedCutoffs_ReturnsChangedTrue(t *testing.T) {
	// cutoffs are compared positionally, so the same cutoffs in a
	// different order count as a change.
	c1 := mustCutoff(t, "sp1-1500", "15:00", []Weekday{Monday}, "ground", []shared.PathId{"PICK"})
	c2 := mustCutoff(t, "sp1-1800", "18:00", []Weekday{Tuesday}, "same-day", []shared.PathId{"PACK"})
	s, err := Define("sp1", "America/Sao_Paulo", []Cutoff{c1, c2}, time.Now())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	changed, err := s.Revise("America/Sao_Paulo", []Cutoff{c2, c1}, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !changed {
		t.Fatal("expected changed=true for reordered cutoffs (comparison is positional)")
	}
}

// --- AllEligiblePathIds ------------------------------------------------------

func TestAllEligiblePathIds_DedupesAndSortsAcrossCutoffs(t *testing.T) {
	c1, err := NewCutoff("sp1-1500", "15:00", []Weekday{Monday}, "ground", []shared.PathId{"PACK", "PICK"})
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	c2, err := NewCutoff("sp1-1800", "18:00", []Weekday{Tuesday}, "same-day", []shared.PathId{"PICK", "SLAM"})
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	s, err := Define("sp1", "America/Sao_Paulo", []Cutoff{c1, c2}, time.Now())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	got := s.AllEligiblePathIds()
	want := []shared.PathId{"PACK", "PICK", "SLAM"}
	if len(got) != len(want) {
		t.Fatalf("want %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("want %v, got %v", want, got)
		}
	}
}
