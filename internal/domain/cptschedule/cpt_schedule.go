// Package cptschedule implements the CPTSchedule aggregate: a
// site-scoped, recurring Critical Pull Time (CPT) schedule (ADR 0010,
// the fulfillment capability contract). A CPT is a property of a
// departure, not of a path — several paths feed the same truck, and a
// slow path simply cannot make the later ones — so the schedule is
// modelled once per site rather than duplicated on every path.
package cptschedule

import (
	"errors"
	"slices"
	"time"

	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// Weekday is one of the seven days of the week, expressed independently
// of time.Weekday so this domain package never depends on a particular
// wire encoding — see the constants below.
type Weekday string

const (
	Monday    Weekday = "Mon"
	Tuesday   Weekday = "Tue"
	Wednesday Weekday = "Wed"
	Thursday  Weekday = "Thu"
	Friday    Weekday = "Fri"
	Saturday  Weekday = "Sat"
	Sunday    Weekday = "Sun"
)

// validWeekdays is the closed set every Weekday must belong to.
var validWeekdays = map[Weekday]bool{
	Monday: true, Tuesday: true, Wednesday: true, Thursday: true,
	Friday: true, Saturday: true, Sunday: true,
}

// Errors enforcing this aggregate's invariants (ADR 0010). A malformed
// schedule must never be constructible — the same "no fix it later
// state" posture the ProcessPath aggregate already establishes.
var (
	ErrEmptyTimezone     = errors.New("cptschedule: timezone must not be empty")
	ErrInvalidTimezone   = errors.New("cptschedule: timezone is not a recognized IANA zone")
	ErrNoCutoffs         = errors.New("cptschedule: at least one cutoff is required")
	ErrEmptyCptId        = errors.New("cptschedule: cptId must not be empty")
	ErrDuplicateCptId    = errors.New("cptschedule: cptId must be unique within a site")
	ErrEmptyLocalTime    = errors.New("cptschedule: localTime must not be empty")
	ErrInvalidLocalTime  = errors.New("cptschedule: localTime must be in HH:MM 24-hour form")
	ErrNoDaysOfWeek      = errors.New("cptschedule: daysOfWeek must be non-empty")
	ErrInvalidDayOfWeek  = errors.New("cptschedule: daysOfWeek entries must be one of Mon..Sun")
	ErrEmptyShipMethod   = errors.New("cptschedule: shipMethod must not be empty")
	ErrNoEligiblePathIds = errors.New("cptschedule: eligiblePathIds must be non-empty")
)

// Cutoff is one recurring daily cutoff entity within a CPTSchedule.
type Cutoff struct {
	cptId           string
	localTime       string
	daysOfWeek      []Weekday
	shipMethod      string
	eligiblePathIds []shared.PathId
}

// NewCutoff constructs a Cutoff, validating its own invariants. Callers
// build a []Cutoff and pass it to Define/Revise, which additionally
// enforces the cross-cutoff invariant (unique cptId within the schedule).
func NewCutoff(cptId, localTime string, daysOfWeek []Weekday, shipMethod string, eligiblePathIds []shared.PathId) (Cutoff, error) {
	if cptId == "" {
		return Cutoff{}, ErrEmptyCptId
	}
	if localTime == "" {
		return Cutoff{}, ErrEmptyLocalTime
	}
	if !isValidLocalTime(localTime) {
		return Cutoff{}, ErrInvalidLocalTime
	}
	if len(daysOfWeek) == 0 {
		return Cutoff{}, ErrNoDaysOfWeek
	}
	for _, d := range daysOfWeek {
		if !validWeekdays[d] {
			return Cutoff{}, ErrInvalidDayOfWeek
		}
	}
	if shipMethod == "" {
		return Cutoff{}, ErrEmptyShipMethod
	}
	if len(eligiblePathIds) == 0 {
		return Cutoff{}, ErrNoEligiblePathIds
	}
	return Cutoff{
		cptId:           cptId,
		localTime:       localTime,
		daysOfWeek:      append([]Weekday(nil), daysOfWeek...),
		shipMethod:      shipMethod,
		eligiblePathIds: append([]shared.PathId(nil), eligiblePathIds...),
	}, nil
}

func (c Cutoff) CptId() string      { return c.cptId }
func (c Cutoff) LocalTime() string  { return c.localTime }
func (c Cutoff) ShipMethod() string { return c.shipMethod }
func (c Cutoff) DaysOfWeek() []Weekday {
	return append([]Weekday(nil), c.daysOfWeek...)
}
func (c Cutoff) EligiblePathIds() []shared.PathId {
	return append([]shared.PathId(nil), c.eligiblePathIds...)
}

// isValidLocalTime validates the strict "HH:MM" 24-hour form (e.g.
// "15:00"), deliberately not using time.Parse's more permissive layouts
// so a malformed value like "3pm" or "15:00:00" is rejected rather than
// silently accepted or truncated.
func isValidLocalTime(s string) bool {
	if len(s) != 5 || s[2] != ':' {
		return false
	}
	h, herr := parseDigits(s[0:2])
	m, merr := parseDigits(s[3:5])
	if herr != nil || merr != nil {
		return false
	}
	return h >= 0 && h <= 23 && m >= 0 && m <= 59
}

func parseDigits(s string) (int, error) {
	n := 0
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0, errors.New("not a digit")
		}
		n = n*10 + int(r-'0')
	}
	return n, nil
}

// CPTSchedule is the aggregate root: one recurring cutoff schedule per
// site (id = SiteId).
//
// version is the optimistic-concurrency token (ADR 0017), same posture
// as ProcessPath's own: inert infrastructure metadata the domain never
// reads. The schedule is mutable via PUT /sites/{siteId}/cpt-schedule,
// so its upsert is version-guarded the same way.
type CPTSchedule struct {
	siteId    shared.SiteId
	timezone  string
	cutoffs   []Cutoff
	createdAt time.Time
	updatedAt time.Time
	version   int
}

// Define constructs a brand-new CPTSchedule. Invariants: timezone must be
// non-empty and a recognized IANA zone; at least one cutoff; cptIds
// unique within the schedule. The eligiblePathIds cross-aggregate check
// (every id must reference an Active ProcessPath in this service's own
// store) is NOT enforced here — it requires the ProcessPathRepo, so it
// lives in the DefineCPTSchedule use case, not the domain package (ADR
// 0010: "checked at write time... enforced in the use case, not by a
// foreign key").
func Define(siteId shared.SiteId, timezone string, cutoffs []Cutoff, now time.Time) (*CPTSchedule, error) {
	if err := validate(timezone, cutoffs); err != nil {
		return nil, err
	}
	return &CPTSchedule{
		siteId:    siteId,
		timezone:  timezone,
		cutoffs:   append([]Cutoff(nil), cutoffs...),
		createdAt: now,
		updatedAt: now,
		version:   1,
	}, nil
}

// Rehydrate reconstructs a CPTSchedule from persisted state without
// re-validating construction invariants — same pattern as
// processpath.Rehydrate. version is the row's optimistic-concurrency
// version (ADR 0017).
func Rehydrate(siteId shared.SiteId, timezone string, cutoffs []Cutoff, createdAt, updatedAt time.Time, version int) *CPTSchedule {
	return &CPTSchedule{
		siteId:    siteId,
		timezone:  timezone,
		cutoffs:   cutoffs,
		createdAt: createdAt,
		updatedAt: updatedAt,
		version:   version,
	}
}

// Revise replaces the schedule's timezone/cutoffs wholesale — a
// CPTSchedule has no partial-update semantics, matching ADR 0010's
// "revising a schedule raises a new event carrying the full schedule."
// Returns changed=false (and raises nothing) if the new definition is
// identical to the current one, same "changed bool" convention as
// ProcessPath.Revise.
func (s *CPTSchedule) Revise(timezone string, cutoffs []Cutoff, now time.Time) (changed bool, err error) {
	if err := validate(timezone, cutoffs); err != nil {
		return false, err
	}
	if s.timezone == timezone && cutoffsEqual(s.cutoffs, cutoffs) {
		return false, nil
	}
	s.timezone = timezone
	s.cutoffs = append([]Cutoff(nil), cutoffs...)
	s.updatedAt = now
	return true, nil
}

func (s *CPTSchedule) SiteId() shared.SiteId { return s.siteId }
func (s *CPTSchedule) Timezone() string      { return s.timezone }
func (s *CPTSchedule) Cutoffs() []Cutoff     { return append([]Cutoff(nil), s.cutoffs...) }
func (s *CPTSchedule) CreatedAt() time.Time  { return s.createdAt }
func (s *CPTSchedule) UpdatedAt() time.Time  { return s.updatedAt }

// Version is this aggregate's optimistic-concurrency version (ADR 0017);
// see ProcessPath.Version for the posture.
func (s *CPTSchedule) Version() int { return s.version }

// AllEligiblePathIds returns the de-duplicated union of every cutoff's
// eligiblePathIds — the set the cross-aggregate use-case check validates
// against the ProcessPathRepo.
func (s *CPTSchedule) AllEligiblePathIds() []shared.PathId {
	seen := make(map[shared.PathId]bool)
	var out []shared.PathId
	for _, c := range s.cutoffs {
		for _, id := range c.eligiblePathIds {
			if !seen[id] {
				seen[id] = true
				out = append(out, id)
			}
		}
	}
	slices.Sort(out)
	return out
}

func validate(timezone string, cutoffs []Cutoff) error {
	if timezone == "" {
		return ErrEmptyTimezone
	}
	if _, err := time.LoadLocation(timezone); err != nil {
		return ErrInvalidTimezone
	}
	if len(cutoffs) == 0 {
		return ErrNoCutoffs
	}
	seen := make(map[string]bool, len(cutoffs))
	for _, c := range cutoffs {
		if seen[c.cptId] {
			return ErrDuplicateCptId
		}
		seen[c.cptId] = true
	}
	return nil
}

func cutoffsEqual(a, b []Cutoff) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].cptId != b[i].cptId ||
			a[i].localTime != b[i].localTime ||
			a[i].shipMethod != b[i].shipMethod ||
			!weekdaysEqual(a[i].daysOfWeek, b[i].daysOfWeek) ||
			!pathIdsEqual(a[i].eligiblePathIds, b[i].eligiblePathIds) {
			return false
		}
	}
	return true
}

func weekdaysEqual(a, b []Weekday) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func pathIdsEqual(a, b []shared.PathId) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
