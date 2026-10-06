package http

import (
	"errors"
	"net/http"

	"github.com/claudioed/process-path-management/internal/application/ports"
	"github.com/claudioed/process-path-management/internal/application/usecases"
	"github.com/claudioed/process-path-management/internal/domain/cptschedule"
	"github.com/claudioed/process-path-management/internal/domain/processpath"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// statusFor maps a typed domain/application error to an HTTP status code.
func statusFor(err error) int {
	switch {
	case errors.Is(err, usecases.ErrPathNotFound),
		errors.Is(err, usecases.ErrCPTScheduleNotFound):
		return http.StatusNotFound

	case errors.Is(err, usecases.ErrPathAlreadyExists),
		errors.Is(err, usecases.ErrPathReferencedByCPTSchedule),
		errors.Is(err, ports.ErrConcurrentModification):
		// ErrConcurrentModification is a different 409 than the
		// natural-key conflict: a concurrent writer committed a version
		// this caller never saw between its load and its Save (ADR
		// 0017, optimistic concurrency) — same status family, its own
		// distinct category so a caller can tell "re-fetch and retry"
		// apart from a domain-rule rejection.
		return http.StatusConflict

	case errors.Is(err, processpath.ErrPathDeactivated),
		errors.Is(err, processpath.ErrEmptyPathId),
		errors.Is(err, processpath.ErrEmptyMatchPrefix),
		errors.Is(err, processpath.ErrMatchPrefixNotLowercase),
		errors.Is(err, processpath.ErrNoRequiredCapabilities),
		errors.Is(err, processpath.ErrInvalidCycleTime),
		errors.Is(err, shared.ErrInvalidDestinationLocationRole),
		errors.Is(err, usecases.ErrIneligiblePathId),
		errors.Is(err, cptschedule.ErrEmptyTimezone),
		errors.Is(err, cptschedule.ErrInvalidTimezone),
		errors.Is(err, cptschedule.ErrNoCutoffs),
		errors.Is(err, cptschedule.ErrEmptyCptId),
		errors.Is(err, cptschedule.ErrDuplicateCptId),
		errors.Is(err, cptschedule.ErrEmptyLocalTime),
		errors.Is(err, cptschedule.ErrInvalidLocalTime),
		errors.Is(err, cptschedule.ErrNoDaysOfWeek),
		errors.Is(err, cptschedule.ErrInvalidDayOfWeek),
		errors.Is(err, cptschedule.ErrEmptyShipMethod),
		errors.Is(err, cptschedule.ErrNoEligiblePathIds):
		return http.StatusUnprocessableEntity

	default:
		return http.StatusInternalServerError
	}
}

// problemBaseURI is the namespace for this service's RFC 7807 "type" URIs.
// It does not need to resolve to a real page — it's an identifier, unique
// per distinct error category in this service.
const problemBaseURI = "https://errors.process-path-management.warehouse-systems.dev/"

// problemInfo is the fixed, category-level (type, title) pair for an RFC
// 7807 problem response. slug becomes the last path segment of "type";
// title is a fixed human string for the category (the dynamic detail
// comes from err.Error() at write time, not from this table).
type problemInfo struct {
	slug  string
	title string
}

// problemCatalog pins each typed domain/application error to its RFC 7807
// (type, title) pair. It is a slice, not a map, because order matters:
// problemFor matches the FIRST entry whose target equals (or wraps) the
// error, exactly as the switch it replaced evaluated errors.Is top-to-bottom.
func problemCatalog() []struct {
	err  error
	info problemInfo
} {
	return []struct {
		err  error
		info problemInfo
	}{
		{usecases.ErrPathNotFound, problemInfo{"path-not-found", "No process path exists with this id"}},
		{usecases.ErrPathAlreadyExists, problemInfo{"path-already-exists", "A process path with this id already exists (active or deactivated)"}},
		{usecases.ErrPathReferencedByCPTSchedule, problemInfo{"path-referenced-by-cpt-schedule", "The process path is still listed by a CPT schedule; revise those schedules before deactivating it"}},
		{ports.ErrConcurrentModification, problemInfo{"concurrent-modification", "The resource was modified by another request; reload and retry"}},
		{processpath.ErrPathDeactivated, problemInfo{"path-deactivated", "This process path has been deactivated and can no longer be revised"}},
		{processpath.ErrEmptyPathId, problemInfo{"empty-path-id", "pathId must not be empty"}},
		{processpath.ErrEmptyMatchPrefix, problemInfo{"empty-match-prefix", "matchPrefix must not be empty"}},
		{processpath.ErrMatchPrefixNotLowercase, problemInfo{"match-prefix-not-lowercase", "matchPrefix must be lower-case"}},
		{processpath.ErrNoRequiredCapabilities, problemInfo{"no-required-capabilities", "requiredCapabilities must be non-empty"}},
		{shared.ErrInvalidDestinationLocationRole, problemInfo{"invalid-destination-location-role", "destinationLocationRole must be one of Drop, WorkCenter, Shipping, or omitted"}},
		{processpath.ErrInvalidCycleTime, problemInfo{"invalid-cycle-time-p95", "cycleTimeP95 must be a positive duration"}},
		{usecases.ErrCPTScheduleNotFound, problemInfo{"cpt-schedule-not-found", "No CPT schedule exists for this site"}},
		{usecases.ErrIneligiblePathId, problemInfo{"ineligible-path-id", "eligiblePathIds must reference an Active process path in this service's own store"}},
		{cptschedule.ErrEmptyTimezone, problemInfo{"empty-timezone", "timezone must not be empty"}},
		{cptschedule.ErrInvalidTimezone, problemInfo{"invalid-timezone", "timezone is not a recognized IANA zone"}},
		{cptschedule.ErrNoCutoffs, problemInfo{"no-cutoffs", "at least one cutoff is required"}},
		{cptschedule.ErrEmptyCptId, problemInfo{"empty-cpt-id", "cptId must not be empty"}},
		{cptschedule.ErrDuplicateCptId, problemInfo{"duplicate-cpt-id", "cptId must be unique within a site"}},
		{cptschedule.ErrEmptyLocalTime, problemInfo{"empty-local-time", "localTime must not be empty"}},
		{cptschedule.ErrInvalidLocalTime, problemInfo{"invalid-local-time", "localTime must be in HH:MM 24-hour form"}},
		{cptschedule.ErrNoDaysOfWeek, problemInfo{"no-days-of-week", "daysOfWeek must be non-empty"}},
		{cptschedule.ErrInvalidDayOfWeek, problemInfo{"invalid-day-of-week", "daysOfWeek entries must be one of Mon..Sun"}},
		{cptschedule.ErrEmptyShipMethod, problemInfo{"empty-ship-method", "shipMethod must not be empty"}},
		{cptschedule.ErrNoEligiblePathIds, problemInfo{"no-eligible-path-ids", "eligiblePathIds must be non-empty"}},
	}
}

// problemFor maps a typed domain/application error to its RFC 7807
// (type, title) pair by walking problemCatalog in order. Mirrors statusFor's
// error groupings one-for-one.
func problemFor(err error) problemInfo {
	for _, entry := range problemCatalog() {
		if errors.Is(err, entry.err) {
			return entry.info
		}
	}
	return problemInfo{"internal-error", "An unexpected internal error occurred"}
}
