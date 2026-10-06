// Package http is the inbound chi adapter: DTOs, handlers, routing, and
// domain-error-to-HTTP-status mapping. Domain structs never cross this
// boundary — every response below is a DTO owned by this package.
package http

// eligibilityRequest is the wire shape of shared.Eligibility on the
// define/revise request bodies. Every field is optional; an absent
// eligibility object is the fully permissive zero value, matching ADR
// 0010's "a permissive eligibility ({}) is valid" requirement.
type eligibilityRequest struct {
	MaxUnitsPerLine           *int     `json:"maxUnitsPerLine,omitempty"`
	RequiredProductAttributes []string `json:"requiredProductAttributes,omitempty"`
	ExcludedProductAttributes []string `json:"excludedProductAttributes,omitempty"`
	NonSortable               bool     `json:"nonSortable,omitempty"`
}

// eligibilityResponse mirrors eligibilityRequest for the response side —
// kept as its own type (not reused) for the same boundary-DTO discipline
// the rest of this package already follows.
type eligibilityResponse struct {
	MaxUnitsPerLine           *int     `json:"maxUnitsPerLine,omitempty"`
	RequiredProductAttributes []string `json:"requiredProductAttributes,omitempty"`
	ExcludedProductAttributes []string `json:"excludedProductAttributes,omitempty"`
	NonSortable               bool     `json:"nonSortable,omitempty"`
}

// defineProcessPathRequest is the POST /process-paths request body.
type defineProcessPathRequest struct {
	PathId               string   `json:"pathId"`
	MatchPrefix          string   `json:"matchPrefix"`
	Direct               bool     `json:"direct"`
	RequiredCapabilities []string `json:"requiredCapabilities"`
	// DestinationLocationRole is an OPTIONAL declaration of what kind of
	// facility-layout LocationRole this path's completed work is
	// destined for (Drop | WorkCenter | Shipping) — see ADR 0009. Omitted
	// entirely means "no destination role declared", the default and
	// most common case.
	DestinationLocationRole string `json:"destinationLocationRole,omitempty"`
	// CycleTimeP95 is the operator-declared p95 end-to-end cycle time
	// (ADR 0010), required, encoded as a Go duration string (e.g.
	// "2h", "90m").
	CycleTimeP95 string `json:"cycleTimeP95"`
	// Eligibility is optional; an absent object is the fully permissive
	// zero value (ADR 0010).
	Eligibility *eligibilityRequest `json:"eligibility,omitempty"`
}

// reviseProcessPathRequest is the PUT /process-paths/{pathId} request
// body. PathId, Direct, and destinationLocationRole are not revisable
// (see the aggregate's own doc comment on why they are immutable), so
// this DTO deliberately does not carry them.
type reviseProcessPathRequest struct {
	MatchPrefix          string   `json:"matchPrefix"`
	RequiredCapabilities []string `json:"requiredCapabilities"`
	// CycleTimeP95 and Eligibility are revisable (ADR 0010).
	CycleTimeP95 string              `json:"cycleTimeP95"`
	Eligibility  *eligibilityRequest `json:"eligibility,omitempty"`
}

// processPathResponse is the response body for every endpoint that
// returns one ProcessPath.
type processPathResponse struct {
	PathId               string   `json:"pathId"`
	MatchPrefix          string   `json:"matchPrefix"`
	Direct               bool     `json:"direct"`
	RequiredCapabilities []string `json:"requiredCapabilities"`
	// DestinationLocationRole is omitted entirely (not defaulted to "")
	// when no destination role was declared for this path — mirrors this
	// fleet's omit-when-unknown discipline (e.g. wes-work-planning's
	// travelDistanceM, ADR-0017 there).
	DestinationLocationRole string `json:"destinationLocationRole,omitempty"`
	// CycleTimeP95 and Eligibility are the fulfillment capability
	// contract (ADR 0010).
	CycleTimeP95 string              `json:"cycleTimeP95"`
	Eligibility  eligibilityResponse `json:"eligibility"`
	Status       string              `json:"status"`
	CreatedAt    string              `json:"createdAt"`
	UpdatedAt    string              `json:"updatedAt"`
}

// --- CPT schedule (ADR 0010) -----------------------------------------------

// cutoffRequest is the wire shape of one cptschedule.Cutoff on the
// PUT /sites/{siteId}/cpt-schedule request body.
type cutoffRequest struct {
	CptId           string   `json:"cptId"`
	LocalTime       string   `json:"localTime"`
	DaysOfWeek      []string `json:"daysOfWeek"`
	ShipMethod      string   `json:"shipMethod"`
	EligiblePathIds []string `json:"eligiblePathIds"`
}

// defineCPTScheduleRequest is the PUT /sites/{siteId}/cpt-schedule
// request body.
type defineCPTScheduleRequest struct {
	Timezone string          `json:"timezone"`
	Cutoffs  []cutoffRequest `json:"cutoffs"`
}

// cutoffResponse mirrors cutoffRequest for the response side.
type cutoffResponse struct {
	CptId           string   `json:"cptId"`
	LocalTime       string   `json:"localTime"`
	DaysOfWeek      []string `json:"daysOfWeek"`
	ShipMethod      string   `json:"shipMethod"`
	EligiblePathIds []string `json:"eligiblePathIds"`
}

// cptScheduleResponse is the response body for GET/PUT
// /sites/{siteId}/cpt-schedule.
type cptScheduleResponse struct {
	SiteId    string           `json:"siteId"`
	Timezone  string           `json:"timezone"`
	Cutoffs   []cutoffResponse `json:"cutoffs"`
	CreatedAt string           `json:"createdAt"`
	UpdatedAt string           `json:"updatedAt"`
}

// problemDetails is the RFC 7807 (Problem Details for HTTP APIs) response
// body for every error this API returns.
type problemDetails struct {
	Type     string `json:"type"`
	Title    string `json:"title"`
	Status   int    `json:"status"`
	Detail   string `json:"detail"`
	Instance string `json:"instance"`
}
