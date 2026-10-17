package mcp

import (
	"context"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// tracerName is the OTel instrumentation scope for MCP tool spans.
const tracerName = "github.com/claudioed/process-path-management/internal/adapters/inbound/mcp"

// Deps is everything the MCP tools need, injected by the composition
// root. It carries the SAME read use cases the HTTP adapter uses
// (GetPath, ListPaths) — this adapter never constructs an outbound
// adapter itself, and never writes.
type Deps struct {
	// GetPath is the existing read use case, reused unchanged. It answers
	// "what is the full definition of this process path".
	GetPath GetPathQuery
	// ListPaths is the existing read use case, reused unchanged. It
	// answers "what process paths currently exist" (active-only by
	// default, or every path including deactivated ones).
	ListPaths ListPathsQuery
	// GetCPTSchedule is the read use case for a site's CPT schedule (ADR
	// 0010). Nil disables the get_cpt_schedule tool without disabling
	// the others — same optional-tool convention Reports already uses.
	GetCPTSchedule GetCPTScheduleQuery
	// Reports is the pathmgmt-reports REST client the catalogue-growth
	// report tool calls. Nil disables that tool (e.g. a deployment with
	// analytics not yet enabled) without disabling the other two tools.
	Reports ReportsClient
}

// --- get_process_path -----------------------------------------------------

type getProcessPathInput struct {
	PathId string `json:"pathId" jsonschema:"the canonical process path id to look up, e.g. PICK, PACK, REBIN, SLAM"`
}

func (d Deps) getProcessPath(ctx context.Context, in getProcessPathInput) (processPathDTO, error) {
	if in.PathId == "" {
		return processPathDTO{}, fmt.Errorf("pathId is required")
	}
	p, err := d.GetPath.Execute(ctx, shared.PathId(in.PathId))
	if err != nil {
		// usecases.ErrPathNotFound (and any other use case error) surfaces
		// unchanged as the tool error, matching the fleet's convention
		// that a not-found result is an error result, not a zero-value
		// success.
		return processPathDTO{}, err
	}
	return toProcessPathDTO(p), nil
}

// --- list_process_paths ----------------------------------------------------

type listProcessPathsInput struct {
	// ActiveOnly defaults to true when omitted (see registration below),
	// matching the HTTP adapter's own default view.
	ActiveOnly *bool `json:"activeOnly,omitempty" jsonschema:"when true (the default), only ACTIVE paths are returned; when false, deactivated paths are included too"`
}

type listProcessPathsOutput struct {
	Paths []processPathDTO `json:"paths"`
}

func (d Deps) listProcessPaths(ctx context.Context, in listProcessPathsInput) (listProcessPathsOutput, error) {
	activeOnly := true
	if in.ActiveOnly != nil {
		activeOnly = *in.ActiveOnly
	}
	paths, err := d.ListPaths.Execute(ctx, activeOnly)
	if err != nil {
		return listProcessPathsOutput{}, err
	}
	dtos := make([]processPathDTO, 0, len(paths))
	for _, p := range paths {
		dtos = append(dtos, toProcessPathDTO(p))
	}
	return listProcessPathsOutput{Paths: dtos}, nil
}

// --- get_cpt_schedule (ADR 0010) --------------------------------------------

type getCPTScheduleInput struct {
	SiteId string `json:"siteId" jsonschema:"the site id to look up the CPT schedule for, e.g. sp1"`
}

func (d Deps) getCPTSchedule(ctx context.Context, in getCPTScheduleInput) (cptScheduleDTO, error) {
	if in.SiteId == "" {
		return cptScheduleDTO{}, fmt.Errorf("siteId is required")
	}
	s, err := d.GetCPTSchedule.Execute(ctx, shared.SiteId(in.SiteId))
	if err != nil {
		// usecases.ErrCPTScheduleNotFound (and any other use case error)
		// surfaces unchanged as the tool error, matching
		// get_process_path's own convention.
		return cptScheduleDTO{}, err
	}
	return toCPTScheduleDTO(s), nil
}

// --- registration -----------------------------------------------------------

// registerTools adds every tool to the server, each wrapped so its
// handler runs inside an OTel span named "mcp.tool <name>", mirroring the
// pattern used across the fleet's other cmd/mcp adapters (e.g.
// inventory-storage's tools.go).
func (d Deps) registerTools(server *mcp.Server) {
	readOnly := true

	addTool(server, &mcp.Tool{
		Name:        "get_process_path",
		Description: "Return the full definition of one process path by its canonical id: matchPrefix, direct, requiredCapabilities, cycleTimeP95, eligibility, intake (the demand sources allowed to route work onto it: CUSTOMER_ORDER, STOCK_TRANSFER, SLOT_RELOCATION, INVENTORY_CONTROL, REPLENISHMENT; [] means only an explicit path id reaches it), status, and timestamps. Not found is returned as a tool-level error.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: readOnly},
	}, d.getProcessPath)

	addTool(server, &mcp.Tool{
		Name:        "list_process_paths",
		Description: "List process paths in the catalogue. activeOnly (default true) restricts the result to ACTIVE paths; set it false to include deactivated ones too.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: readOnly},
	}, d.listProcessPaths)

	if d.GetCPTSchedule != nil {
		addTool(server, &mcp.Tool{
			Name:        "get_cpt_schedule",
			Description: "Return one site's Critical Pull Time (CPT) schedule: timezone and the recurring daily cutoffs (cptId, localTime, daysOfWeek, shipMethod, eligiblePathIds). Not found is returned as a tool-level error. Read-only (ADR 0010) — there is no write tool for the schedule.",
			Annotations: &mcp.ToolAnnotations{ReadOnlyHint: readOnly},
		}, d.getCPTSchedule)
	}

	d.registerReportTool(server)
}

// addTool registers one tool. It centralises the cross-cutting concerns
// every tool shares: a span per call and mapping a handler error onto the
// span before returning it.
func addTool[In, Out any](
	server *mcp.Server,
	tool *mcp.Tool,
	handle func(context.Context, In) (Out, error),
) {
	mcp.AddTool(server, tool, func(ctx context.Context, _ *mcp.CallToolRequest, in In) (*mcp.CallToolResult, Out, error) {
		var zero Out
		ctx, span := otel.Tracer(tracerName).Start(ctx, "mcp.tool "+tool.Name,
			trace.WithAttributes(
				attribute.String("mcp.tool.name", tool.Name),
			),
		)
		defer span.End()

		out, err := handle(ctx, in)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return nil, zero, err
		}
		return nil, out, nil
	})
}
