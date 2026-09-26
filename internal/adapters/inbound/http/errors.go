package http

import (
	"errors"
	"net/http"

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

	case errors.Is(err, usecases.ErrPathAlreadyExists):
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

// problemFor maps a typed domain/application error to its RFC 7807
// (type, title) pair. Mirrors statusFor's error groupings one-for-one.
func problemFor(err error) problemInfo {
	switch {
	case errors.Is(err, usecases.ErrPathNotFound):
		return problemInfo{"path-not-found", "No process path exists with this id"}
	case errors.Is(err, usecases.ErrPathAlreadyExists):
		return problemInfo{"path-already-exists", "A process path with this id already exists (active or deactivated)"}
	case errors.Is(err, processpath.ErrPathDeactivated):
		return problemInfo{"path-deactivated", "This process path has been deactivated and can no longer be revised"}
	case errors.Is(err, processpath.ErrEmptyPathId):
		return problemInfo{"empty-path-id", "pathId must not be empty"}
	case errors.Is(err, processpath.ErrEmptyMatchPrefix):
		return problemInfo{"empty-match-prefix", "matchPrefix must not be empty"}
	case errors.Is(err, processpath.ErrMatchPrefixNotLowercase):
		return problemInfo{"match-prefix-not-lowercase", "matchPrefix must be lower-case"}
	case errors.Is(err, processpath.ErrNoRequiredCapabilities):
		return problemInfo{"no-required-capabilities", "requiredCapabilities must be non-empty"}
	case errors.Is(err, shared.ErrInvalidDestinationLocationRole):
		return problemInfo{"invalid-destination-location-role", "destinationLocationRole must be one of Drop, WorkCenter, Shipping, or omitted"}
	case errors.Is(err, processpath.ErrInvalidCycleTime):
		return problemInfo{"invalid-cycle-time-p95", "cycleTimeP95 must be a positive duration"}
	case errors.Is(err, usecases.ErrCPTScheduleNotFound):
		return problemInfo{"cpt-schedule-not-found", "No CPT schedule exists for this site"}
	case errors.Is(err, usecases.ErrIneligiblePathId):
		return problemInfo{"ineligible-path-id", "eligiblePathIds must reference an Active process path in this service's own store"}
	case errors.Is(err, cptschedule.ErrEmptyTimezone):
		return problemInfo{"empty-timezone", "timezone must not be empty"}
	case errors.Is(err, cptschedule.ErrInvalidTimezone):
		return problemInfo{"invalid-timezone", "timezone is not a recognized IANA zone"}
	case errors.Is(err, cptschedule.ErrNoCutoffs):
		return problemInfo{"no-cutoffs", "at least one cutoff is required"}
	case errors.Is(err, cptschedule.ErrEmptyCptId):
		return problemInfo{"empty-cpt-id", "cptId must not be empty"}
	case errors.Is(err, cptschedule.ErrDuplicateCptId):
		return problemInfo{"duplicate-cpt-id", "cptId must be unique within a site"}
	case errors.Is(err, cptschedule.ErrEmptyLocalTime):
		return problemInfo{"empty-local-time", "localTime must not be empty"}
	case errors.Is(err, cptschedule.ErrInvalidLocalTime):
		return problemInfo{"invalid-local-time", "localTime must be in HH:MM 24-hour form"}
	case errors.Is(err, cptschedule.ErrNoDaysOfWeek):
		return problemInfo{"no-days-of-week", "daysOfWeek must be non-empty"}
	case errors.Is(err, cptschedule.ErrInvalidDayOfWeek):
		return problemInfo{"invalid-day-of-week", "daysOfWeek entries must be one of Mon..Sun"}
	case errors.Is(err, cptschedule.ErrEmptyShipMethod):
		return problemInfo{"empty-ship-method", "shipMethod must not be empty"}
	case errors.Is(err, cptschedule.ErrNoEligiblePathIds):
		return problemInfo{"no-eligible-path-ids", "eligiblePathIds must be non-empty"}
	default:
		return problemInfo{"internal-error", "An unexpected internal error occurred"}
	}
}
