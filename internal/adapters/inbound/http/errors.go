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
	if status, ok := flowStatusFor(err); ok {
		return status
	}
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
		errors.Is(err, shared.ErrInvalidIntake),
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

// statusKey is the JSON field name of the tiny {"status": ...} payloads the
// health and reports endpoints serve.
const statusKey = "status"

// problemInternalError is the 5xx fallback of problemFor and of the
// idempotency middleware itself: a server error is never stored, so its
// class is RETRYABLE.
var problemInternalError = problemInfo{"internal-error", "An unexpected internal error occurred", classRetryable}

// problemMalformedRequestBody is the RFC 7807 slug shared by every
// request-body decode failure (unreadable body, invalid JSON, NUL byte).
const problemMalformedRequestBody = "malformed-request-body"

// problemInfo is the fixed, category-level (type, title) pair for an RFC
// 7807 problem response. slug becomes the last path segment of "type";
// title is a fixed human string for the category (the dynamic detail
// comes from err.Error() at write time, not from this table). class is
// the replay class of the problem type (replay_class.go): what the
// Idempotency-Key middleware does with this answer when the same request
// is retried under the same key. An entry with no class fails
// TestEveryProblemTypeDeclaresAReplayClass; there is no runtime default.
type problemInfo struct {
	slug  string
	title string
	class replayClass
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
		{usecases.ErrPathNotFound, problemInfo{"path-not-found", "No process path exists with this id", classRetryable}},
		{usecases.ErrPathAlreadyExists, problemInfo{"path-already-exists", "A process path with this id already exists (active or deactivated)", classFinal}},
		{usecases.ErrPathReferencedByCPTSchedule, problemInfo{"path-referenced-by-cpt-schedule", "The process path is still listed by a CPT schedule; revise those schedules before deactivating it", classRetryable}},
		{ports.ErrConcurrentModification, problemInfo{"concurrent-modification", "The resource was modified by another request; reload and retry", classRetryable}},
		{processpath.ErrPathDeactivated, problemInfo{"path-deactivated", "This process path has been deactivated and can no longer be revised", classFinal}},
		{processpath.ErrEmptyPathId, problemInfo{"empty-path-id", "pathId must not be empty", classFinal}},
		{processpath.ErrEmptyMatchPrefix, problemInfo{"empty-match-prefix", "matchPrefix must not be empty", classFinal}},
		{processpath.ErrMatchPrefixNotLowercase, problemInfo{"match-prefix-not-lowercase", "matchPrefix must be lower-case", classFinal}},
		{processpath.ErrNoRequiredCapabilities, problemInfo{"no-required-capabilities", "requiredCapabilities must be non-empty", classFinal}},
		{shared.ErrInvalidDestinationLocationRole, problemInfo{"invalid-destination-location-role", "destinationLocationRole must be one of Drop, WorkCenter, Shipping, or omitted", classFinal}},
		{processpath.ErrInvalidCycleTime, problemInfo{"invalid-cycle-time-p95", "cycleTimeP95 must be a positive duration", classFinal}},
		{shared.ErrInvalidIntake, problemInfo{"invalid-intake", "intake must be a set of distinct demand sources: CUSTOMER_ORDER, STOCK_TRANSFER, SLOT_RELOCATION, INVENTORY_CONTROL, REPLENISHMENT", classFinal}},
		{usecases.ErrCPTScheduleNotFound, problemInfo{"cpt-schedule-not-found", "No CPT schedule exists for this site", classRetryable}},
		{usecases.ErrIneligiblePathId, problemInfo{"ineligible-path-id", "eligiblePathIds must reference an Active process path in this service's own store", classRetryable}},
		{cptschedule.ErrEmptyTimezone, problemInfo{"empty-timezone", "timezone must not be empty", classFinal}},
		{cptschedule.ErrInvalidTimezone, problemInfo{"invalid-timezone", "timezone is not a recognized IANA zone", classFinal}},
		{cptschedule.ErrNoCutoffs, problemInfo{"no-cutoffs", "at least one cutoff is required", classFinal}},
		{cptschedule.ErrEmptyCptId, problemInfo{"empty-cpt-id", "cptId must not be empty", classFinal}},
		{cptschedule.ErrDuplicateCptId, problemInfo{"duplicate-cpt-id", "cptId must be unique within a site", classFinal}},
		{cptschedule.ErrEmptyLocalTime, problemInfo{"empty-local-time", "localTime must not be empty", classFinal}},
		{cptschedule.ErrInvalidLocalTime, problemInfo{"invalid-local-time", "localTime must be in HH:MM 24-hour form", classFinal}},
		{cptschedule.ErrNoDaysOfWeek, problemInfo{"no-days-of-week", "daysOfWeek must be non-empty", classFinal}},
		{cptschedule.ErrInvalidDayOfWeek, problemInfo{"invalid-day-of-week", "daysOfWeek entries must be one of Mon..Sun", classFinal}},
		{cptschedule.ErrEmptyShipMethod, problemInfo{"empty-ship-method", "shipMethod must not be empty", classFinal}},
		{cptschedule.ErrNoEligiblePathIds, problemInfo{"no-eligible-path-ids", "eligiblePathIds must be non-empty", classFinal}},
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
	if info, ok := flowProblemFor(err); ok {
		return info
	}
	return problemInternalError
}

// standaloneProblems are the problem types written outside the two typed
// catalogues above: the Idempotency-Key middleware's own answers, the shared
// malformed-request-body family, the NUL-path 404, the pagination and reports
// 400s of the GET routes, the readiness 503 and the 5xx fallback. They are
// declared HERE, with their replay class, so every problem type of the
// package is classified (replay_class.go's allProblemTypes lists them).
var (
	// problemIdempotencyKeyRequired/Invalid: answered before the store is
	// touched; a function of the request alone.
	problemIdempotencyKeyRequired = problemInfo{"idempotency-key-required", "Idempotency-Key header is required", classFinal}
	problemIdempotencyKeyInvalid  = problemInfo{"idempotency-key-invalid", "Idempotency-Key header is invalid", classFinal}
	// problemIdempotencyKeyReused: the store's own answer on the replay
	// path; nothing was written for this request.
	problemIdempotencyKeyReused = problemInfo{"idempotency-key-reused", "Idempotency-Key was already used with a different request", classFinal}
	// The malformed-request-body family (one slug, three titles): the
	// request's own bytes alone decide, so the same key replays it.
	problemBodyUnreadable  = problemInfo{problemMalformedRequestBody, "The request body could not be read", classFinal}
	problemBodyContainsNUL = problemInfo{problemMalformedRequestBody, "The request body contains a NUL character", classFinal}
	problemBodyInvalidJSON = problemInfo{problemMalformedRequestBody, "The request body is not valid JSON", classFinal}
	// problemRouteNotFound: no operation matches the request; the request
	// alone decides (answered before the idempotency middleware).
	problemRouteNotFound = problemInfo{"route-not-found", "No route matches this request", classFinal}
	// The 400s of the paginated collection GETs: the query alone decides.
	problemInvalidLimit      = problemInfo{"invalid-limit", "limit must be an integer between 1 and 500", classFinal}
	problemInvalidCursor     = problemInfo{"invalid-cursor", "cursor is not a valid page token", classFinal}
	problemUnknownParam      = problemInfo{"unknown-query-parameter", "the collection does not declare this query parameter", classFinal}
	problemInvalidQueryParam = problemInfo{"invalid-query-parameter", "The 'all' query parameter must be a boolean (true or false)", classFinal}
	// problemDependencyUnavailable: the /readyz 503; a 5xx is never stored.
	problemDependencyUnavailable = problemInfo{"dependency-unavailable", "A runtime dependency is unavailable", classRetryable}
	// The reports reader's own answers (a read-only binary, never behind the
	// idempotency middleware): the query alone / a 5xx.
	problemInvalidReportQuery = problemInfo{"invalid-report-query", "The report query is malformed or missing a required parameter", classFinal}
	problemReportStoreError   = problemInfo{"report-store-error", "The report could not be served", classRetryable}
)

// standaloneProblems are the problem types written outside the two typed
// catalogues above: the Idempotency-Key middleware's own answers, the shared
// malformed-request-body family, the NUL-path 404, the pagination and reports
// 400s of the GET routes, the readiness 503 and the 5xx fallback. They are
// declared HERE, with their replay class, so every problem type of the
// package is classified (replay_class.go's allProblemTypes lists them).
var standaloneProblems = []struct {
	status int
	info   problemInfo
}{
	// problemIdempotencyKeyRequired/Invalid: answered before the store is
	// touched; a function of the request alone.
	{http.StatusBadRequest, problemIdempotencyKeyRequired},
	{http.StatusBadRequest, problemIdempotencyKeyInvalid},
	// problemIdempotencyKeyReused: the store's own answer on the replay
	// path; nothing was written for this request.
	{http.StatusConflict, problemIdempotencyKeyReused},
	// The malformed-request-body family (one slug, three titles): the
	// request's own bytes alone decide, so the same key replays it.
	{http.StatusBadRequest, problemBodyUnreadable},
	// problemRouteNotFound: no operation matches the request; the request
	// alone decides (answered before the idempotency middleware).
	{http.StatusNotFound, problemRouteNotFound},
	// The 400s of the paginated collection GETs: the query alone decides.
	{http.StatusBadRequest, problemInvalidLimit},
	{http.StatusBadRequest, problemInvalidCursor},
	{http.StatusBadRequest, problemUnknownParam},
	{http.StatusBadRequest, problemInvalidQueryParam},
	// problemDependencyUnavailable: the /readyz 503; a 5xx is never stored.
	{http.StatusServiceUnavailable, problemDependencyUnavailable},
	// The reports reader's own answers (a read-only binary, never behind the
	// idempotency middleware): the query alone / a 5xx.
	{http.StatusBadRequest, problemInvalidReportQuery},
	{http.StatusInternalServerError, problemReportStoreError},
	// The 5xx fallback itself: never stored.
	{http.StatusInternalServerError, problemInternalError},
}
