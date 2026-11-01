package http

import (
	"time"

	"github.com/claudioed/process-path-management/internal/domain/cptschedule"
	"github.com/claudioed/process-path-management/internal/domain/processpath"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

const timeFormat = time.RFC3339

func toProcessPathResponse(p *processpath.ProcessPath) processPathResponse {
	caps := p.RequiredCapabilities()
	strCaps := make([]string, len(caps))
	for i, c := range caps {
		strCaps[i] = string(c)
	}
	return processPathResponse{
		PathId:                  string(p.ID()),
		MatchPrefix:             p.MatchPrefix(),
		Direct:                  p.Direct(),
		RequiredCapabilities:    strCaps,
		DestinationLocationRole: string(p.DestinationLocationRole()),
		CycleTimeP95:            p.CycleTimeP95().String(),
		Eligibility:             toEligibilityResponse(p.Eligibility()),
		Intake:                  p.Intake().Strings(),
		Status:                  string(p.Status()),
		CreatedAt:               p.CreatedAt().UTC().Format(timeFormat),
		UpdatedAt:               p.UpdatedAt().UTC().Format(timeFormat),
	}
}

func toEligibilityResponse(e shared.Eligibility) eligibilityResponse {
	return eligibilityResponse{
		MaxUnitsPerLine:           e.MaxUnitsPerLine(),
		RequiredProductAttributes: e.RequiredProductAttributes(),
		ExcludedProductAttributes: e.ExcludedProductAttributes(),
		NonSortable:               e.NonSortable(),
	}
}

// toEligibility maps the request DTO onto shared.Eligibility. A nil req
// (no "eligibility" key on the request body) maps to the fully
// permissive zero value (ADR 0010).
func toEligibility(req *eligibilityRequest) shared.Eligibility {
	if req == nil {
		return shared.Eligibility{}
	}
	return shared.NewEligibility(req.MaxUnitsPerLine, req.RequiredProductAttributes, req.ExcludedProductAttributes, req.NonSortable)
}

// toOptionalIntake maps the revise body's optional intake (ADR 0034): nil
// (absent or null on the wire) keeps the path's current intake, anything
// else is validated as the replacement set ([] included).
func toOptionalIntake(req *nonNullStrings) (*shared.Intake, error) {
	if req == nil {
		return nil, nil
	}
	in, err := shared.ParseIntake(*req)
	if err != nil {
		return nil, err
	}
	return &in, nil
}

// parseCycleTimeP95 parses a Go duration string (e.g. "2h", "90m"). A
// malformed value is reported the same way the domain's own
// ErrInvalidCycleTime is (422) via statusFor/problemFor's default
// mapping, so callers see a consistent RFC 7807 shape either way.
func parseCycleTimeP95(s string) (time.Duration, error) {
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, processpath.ErrInvalidCycleTime
	}
	return d, nil
}

func toCutoffs(reqs []cutoffRequest) ([]cptschedule.Cutoff, error) {
	out := make([]cptschedule.Cutoff, 0, len(reqs))
	for _, r := range reqs {
		c, err := cptschedule.NewCutoff(r.CptId, r.LocalTime, toWeekdays(r.DaysOfWeek), r.ShipMethod, toPathIds(r.EligiblePathIds))
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, nil
}

func toWeekdays(ss []string) []cptschedule.Weekday {
	out := make([]cptschedule.Weekday, len(ss))
	for i, s := range ss {
		out[i] = cptschedule.Weekday(s)
	}
	return out
}

func toPathIds(ss []string) []shared.PathId {
	out := make([]shared.PathId, len(ss))
	for i, s := range ss {
		out[i] = shared.PathId(s)
	}
	return out
}

func toCPTScheduleResponse(s *cptschedule.CPTSchedule) cptScheduleResponse {
	cutoffs := s.Cutoffs()
	out := make([]cutoffResponse, 0, len(cutoffs))
	for _, c := range cutoffs {
		out = append(out, cutoffResponse{
			CptId:           c.CptId(),
			LocalTime:       c.LocalTime(),
			DaysOfWeek:      weekdaysToStrings(c.DaysOfWeek()),
			ShipMethod:      c.ShipMethod(),
			EligiblePathIds: pathIdsToStrings(c.EligiblePathIds()),
		})
	}
	return cptScheduleResponse{
		SiteId:    string(s.SiteId()),
		Timezone:  s.Timezone(),
		Cutoffs:   out,
		CreatedAt: s.CreatedAt().UTC().Format(timeFormat),
		UpdatedAt: s.UpdatedAt().UTC().Format(timeFormat),
	}
}

func weekdaysToStrings(ds []cptschedule.Weekday) []string {
	out := make([]string, len(ds))
	for i, d := range ds {
		out[i] = string(d)
	}
	return out
}

func pathIdsToStrings(ids []shared.PathId) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = string(id)
	}
	return out
}

func toCapabilities(ss []string) []shared.Capability {
	out := make([]shared.Capability, len(ss))
	for i, s := range ss {
		out[i] = shared.Capability(s)
	}
	return out
}
