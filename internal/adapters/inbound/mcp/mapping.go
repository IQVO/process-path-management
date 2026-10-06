// Package mcp is the inbound Model Context Protocol adapter: it exposes
// this bounded context to the AI ecosystem as a second driving adapter
// over the same application-layer use cases the HTTP adapter uses. It is
// built on the official MCP Go SDK and served over Streamable HTTP.
//
// Per the fleet's now-established MCP pattern (see e.g.
// inventory-storage's ADR-0008), this package depends inward on the
// application layer (use cases) and the domain only — never on an
// outbound adapter. The composition root (cmd/mcp) wires the concrete
// ProcessPathRepo into the same GetPath/ListPaths use case structs
// cmd/pathmgmt already uses. Tool handlers call those use cases; domain
// structs never leak across the tool boundary.
package mcp

import (
	"context"

	"github.com/claudioed/process-path-management/internal/domain/cptschedule"
	"github.com/claudioed/process-path-management/internal/domain/processpath"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// GetPathQuery is the narrow read port the get_process_path tool needs.
// It is satisfied directly by *usecases.GetPath — no new outbound port is
// required, the use case's own Execute signature already fits.
type GetPathQuery interface {
	Execute(ctx context.Context, id shared.PathId) (*processpath.ProcessPath, error)
}

// ListPathsQuery is the narrow read port the list_process_paths tool
// needs. Satisfied directly by *usecases.ListPaths.
type ListPathsQuery interface {
	Execute(ctx context.Context, activeOnly bool) ([]*processpath.ProcessPath, error)
}

// GetCPTScheduleQuery is the narrow read port the get_cpt_schedule tool
// needs (ADR 0010). Satisfied directly by *usecases.GetCPTSchedule.
type GetCPTScheduleQuery interface {
	Execute(ctx context.Context, siteId shared.SiteId) (*cptschedule.CPTSchedule, error)
}

// processPathDTO is the tool-boundary representation of a ProcessPath
// aggregate: exactly the fields exposed by its own accessors (ID,
// MatchPrefix, Direct, RequiredCapabilities, Status, IsActive, CreatedAt,
// UpdatedAt), nothing more. Domain types never cross the tool boundary.
//
// CycleTimeP95 and Eligibility are the fulfillment capability contract
// (ADR 0010), added here since they are cheap to include (the read use
// case already loads the full aggregate) and the tool's whole purpose is
// giving an AI client the full definition of a path.
// DestinationLocationRole (ADR 0006/0009) is included for the same
// reason: the REST wire already carries it, and an AI client reading a
// path's routing intent should not have to fall back to REST for one
// field. It is omitted (omitempty) when unset, mirroring the REST DTO's
// own omit-when-unknown discipline.
type processPathDTO struct {
	PathId                  string         `json:"pathId"`
	MatchPrefix             string         `json:"matchPrefix"`
	Direct                  bool           `json:"direct"`
	RequiredCapabilities    []string       `json:"requiredCapabilities"`
	DestinationLocationRole string         `json:"destinationLocationRole,omitempty"`
	Status                  string         `json:"status"`
	Active                  bool           `json:"active"`
	CreatedAt               string         `json:"createdAt"`
	UpdatedAt               string         `json:"updatedAt"`
	CycleTimeP95            string         `json:"cycleTimeP95"`
	Eligibility             eligibilityDTO `json:"eligibility"`
}

// eligibilityDTO is the tool-boundary representation of
// shared.Eligibility.
type eligibilityDTO struct {
	MaxUnitsPerLine           *int     `json:"maxUnitsPerLine,omitempty"`
	RequiredProductAttributes []string `json:"requiredProductAttributes,omitempty"`
	ExcludedProductAttributes []string `json:"excludedProductAttributes,omitempty"`
	NonSortable               bool     `json:"nonSortable,omitempty"`
}

// toProcessPathDTO folds a ProcessPath aggregate into the tool-boundary
// DTO via its own read accessors only.
func toProcessPathDTO(p *processpath.ProcessPath) processPathDTO {
	caps := p.RequiredCapabilities()
	capStrs := make([]string, 0, len(caps))
	for _, c := range caps {
		capStrs = append(capStrs, string(c))
	}
	e := p.Eligibility()
	return processPathDTO{
		PathId:                  string(p.ID()),
		MatchPrefix:             p.MatchPrefix(),
		Direct:                  p.Direct(),
		RequiredCapabilities:    capStrs,
		DestinationLocationRole: string(p.DestinationLocationRole()),
		Status:                  string(p.Status()),
		Active:                  p.IsActive(),
		CreatedAt:               p.CreatedAt().Format(timeLayout),
		UpdatedAt:               p.UpdatedAt().Format(timeLayout),
		CycleTimeP95:            p.CycleTimeP95().String(),
		Eligibility: eligibilityDTO{
			MaxUnitsPerLine:           e.MaxUnitsPerLine(),
			RequiredProductAttributes: e.RequiredProductAttributes(),
			ExcludedProductAttributes: e.ExcludedProductAttributes(),
			NonSortable:               e.NonSortable(),
		},
	}
}

// cutoffDTO is the tool-boundary representation of one cptschedule.Cutoff.
type cutoffDTO struct {
	CptId           string   `json:"cptId"`
	LocalTime       string   `json:"localTime"`
	DaysOfWeek      []string `json:"daysOfWeek"`
	ShipMethod      string   `json:"shipMethod"`
	EligiblePathIds []string `json:"eligiblePathIds"`
}

// cptScheduleDTO is the tool-boundary representation of a CPTSchedule
// aggregate (ADR 0010).
type cptScheduleDTO struct {
	SiteId    string      `json:"siteId"`
	Timezone  string      `json:"timezone"`
	Cutoffs   []cutoffDTO `json:"cutoffs"`
	CreatedAt string      `json:"createdAt"`
	UpdatedAt string      `json:"updatedAt"`
}

// toCPTScheduleDTO folds a CPTSchedule aggregate into the tool-boundary
// DTO via its own read accessors only.
func toCPTScheduleDTO(s *cptschedule.CPTSchedule) cptScheduleDTO {
	cutoffs := s.Cutoffs()
	out := make([]cutoffDTO, 0, len(cutoffs))
	for _, c := range cutoffs {
		days := make([]string, 0, len(c.DaysOfWeek()))
		for _, d := range c.DaysOfWeek() {
			days = append(days, string(d))
		}
		ids := make([]string, 0, len(c.EligiblePathIds()))
		for _, id := range c.EligiblePathIds() {
			ids = append(ids, string(id))
		}
		out = append(out, cutoffDTO{
			CptId:           c.CptId(),
			LocalTime:       c.LocalTime(),
			DaysOfWeek:      days,
			ShipMethod:      c.ShipMethod(),
			EligiblePathIds: ids,
		})
	}
	return cptScheduleDTO{
		SiteId:    string(s.SiteId()),
		Timezone:  s.Timezone(),
		Cutoffs:   out,
		CreatedAt: s.CreatedAt().Format(timeLayout),
		UpdatedAt: s.UpdatedAt().Format(timeLayout),
	}
}

// timeLayout is RFC 3339, matching what every other JSON boundary in this
// fleet uses for timestamps.
const timeLayout = "2006-01-02T15:04:05Z07:00"
