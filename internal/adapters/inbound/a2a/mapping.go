package a2a

import (
	"fmt"
	"strings"

	"github.com/claudioed/process-path-management/internal/domain/processpath"
)

// problemTypeBase namespaces this context's problem types. It is the SAME
// namespace as the REST adapter's RFC 7807 types (ADR 0020), so a delegating
// agent sees the identifiers a REST caller would for the domain outcomes both
// surfaces share.
const problemTypeBase = "https://errors.process-path-management.warehouse-systems.dev"

// problem is the RFC 7807-shaped rejection an agent client receives instead of
// a result: domain rejections and request-shape failures are business
// answers, not transport errors, so they travel in the message envelope.
type problem struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Detail string `json:"detail"`
	Status int    `json:"status"`
}

// summary renders the one-line form used for the reply's text part.
func (p *problem) summary() string {
	return fmt.Sprintf("%s (%d): %s", p.Title, p.Status, p.Detail)
}

func problemMalformedRequest(detail string) *problem {
	return &problem{Type: problemTypeBase + "/malformed-skill-request", Title: "Malformed skill request", Detail: detail, Status: 400}
}

func problemUnknownSkill(skill string) *problem {
	return &problem{Type: problemTypeBase + "/unknown-skill", Title: "Unknown skill", Detail: fmt.Sprintf("skill %q is not offered by this agent; the Agent Card lists the supported skills", skill), Status: 400}
}

func problemInvalidArgument(err error) *problem {
	return &problem{Type: problemTypeBase + "/invalid-argument", Title: "Invalid skill argument", Detail: err.Error(), Status: 400}
}

// envelope is the structured payload of every skill answer: exactly one of
// Data (a skill DTO) or Problem (why there is no result), plus the skill id
// the answer belongs to.
type envelope struct {
	Skill   string   `json:"skill"`
	Data    skillDTO `json:"data,omitempty"`
	Problem *problem `json:"problem,omitempty"`
}

// skillDTO is the union of the skill result DTOs. A2A parts are
// machine-readable JSON; each skill fills its own field, keeping the envelope
// flat and versioned as one contract.
type skillDTO struct {
	ProcessPath  *processPathDTO  `json:"processPath,omitempty"`
	ProcessPaths *processPathsDTO `json:"processPaths,omitempty"`
	summary      string
}

// Summary is the human-readable one-liner for the reply's text part.
func (s skillDTO) Summary() string { return s.summary }

// processPathDTO is the message-boundary representation of a ProcessPath
// aggregate: exactly the fields its own accessors expose, the same set the
// MCP tool and the REST wire carry. Domain types never cross the boundary.
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
	// Intake is the set of demand sources allowed to route work onto the
	// path; always present, [] when only an explicit path id reaches it.
	Intake []string `json:"intake"`
}

// eligibilityDTO is the message-boundary representation of shared.Eligibility.
type eligibilityDTO struct {
	MaxUnitsPerLine           *int     `json:"maxUnitsPerLine,omitempty"`
	RequiredProductAttributes []string `json:"requiredProductAttributes,omitempty"`
	ExcludedProductAttributes []string `json:"excludedProductAttributes,omitempty"`
	NonSortable               bool     `json:"nonSortable,omitempty"`
}

// processPathsDTO is the list_process_paths result: the catalogue view that
// was asked for, and the paths in it.
type processPathsDTO struct {
	ActiveOnly bool             `json:"activeOnly"`
	Paths      []processPathDTO `json:"paths"`
}

// timeLayout is RFC 3339, matching every other JSON boundary in this fleet.
const timeLayout = "2006-01-02T15:04:05Z07:00"

// toProcessPathDTO folds a ProcessPath aggregate into the boundary DTO via
// its own read accessors only.
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
		Intake: p.Intake().Strings(),
	}
}

// summarizePath is the one-line text form of one path.
func summarizePath(p processPathDTO) string {
	directness := "not direct"
	if p.Direct {
		directness = "direct"
	}
	return fmt.Sprintf("%s: %s, %s, matchPrefix %s, requires %s",
		p.PathId, strings.ToLower(p.Status), directness, p.MatchPrefix, strings.Join(p.RequiredCapabilities, ","))
}

// summarizePaths is the one-line text form of the catalogue view.
func summarizePaths(list processPathsDTO) string {
	ids := make([]string, 0, len(list.Paths))
	for _, p := range list.Paths {
		ids = append(ids, p.PathId)
	}
	scope := "including deactivated"
	if list.ActiveOnly {
		scope = "active only"
	}
	return fmt.Sprintf("%d process paths (%s): %s", len(list.Paths), scope, strings.Join(ids, ", "))
}
