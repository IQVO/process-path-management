package mcp

import (
	"context"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/claudioed/process-path-management/internal/domain/processflow"
)

// GetProcessFlowQuery is the narrow read port the get_process_flow tool needs
// (ADR 0042). Satisfied directly by *usecases.GetProcessFlow.
type GetProcessFlowQuery interface {
	Execute(ctx context.Context, id processflow.FlowId) (*processflow.ProcessFlow, error)
}

// ListProcessFlowsQuery is the narrow read port the list_process_flows tool
// needs. Satisfied directly by *usecases.ListProcessFlows.
type ListProcessFlowsQuery interface {
	Execute(ctx context.Context, activeOnly bool) ([]*processflow.ProcessFlow, error)
}

// stageDTO is the tool-boundary representation of one processflow.Stage.
type stageDTO struct {
	PathId      string `json:"pathId"`
	ProcessType string `json:"processType"`
}

// processFlowDTO is the tool-boundary representation of a ProcessFlow
// aggregate: the same fields as the REST response, the stages in order.
type processFlowDTO struct {
	FlowId    string     `json:"flowId"`
	Name      string     `json:"name"`
	Stages    []stageDTO `json:"stages"`
	Status    string     `json:"status"`
	Version   int        `json:"version"`
	CreatedAt string     `json:"createdAt"`
	UpdatedAt string     `json:"updatedAt"`
}

func toProcessFlowDTO(f *processflow.ProcessFlow) processFlowDTO {
	stages := f.Stages()
	out := make([]stageDTO, 0, len(stages))
	for _, s := range stages {
		out = append(out, stageDTO{PathId: string(s.PathId()), ProcessType: string(s.ProcessType())})
	}
	return processFlowDTO{
		FlowId:    string(f.ID()),
		Name:      f.Name(),
		Stages:    out,
		Status:    string(f.Status()),
		Version:   f.Version(),
		CreatedAt: f.CreatedAt().Format(timeLayout),
		UpdatedAt: f.UpdatedAt().Format(timeLayout),
	}
}

// --- get_process_flow (ADR 0042) ----------------------------------------------

type getProcessFlowInput struct {
	FlowId string `json:"flowId" jsonschema:"the process flow id to look up, e.g. single-unit-outbound"`
}

func (d Deps) getProcessFlow(ctx context.Context, in getProcessFlowInput) (processFlowDTO, error) {
	if in.FlowId == "" {
		return processFlowDTO{}, fmt.Errorf("flowId is required")
	}
	f, err := d.GetProcessFlow.Execute(ctx, processflow.FlowId(in.FlowId))
	if err != nil {
		// usecases.ErrFlowNotFound surfaces unchanged as the tool error,
		// like get_process_path's own convention.
		return processFlowDTO{}, err
	}
	return toProcessFlowDTO(f), nil
}

// --- list_process_flows (ADR 0042) --------------------------------------------

type listProcessFlowsInput struct {
	ActiveOnly *bool `json:"activeOnly,omitempty" jsonschema:"when true (the default), only ACTIVE flows are returned; when false, retired flows are included too"`
}

type listProcessFlowsOutput struct {
	Flows []processFlowDTO `json:"flows"`
}

func (d Deps) listProcessFlows(ctx context.Context, in listProcessFlowsInput) (listProcessFlowsOutput, error) {
	activeOnly := true
	if in.ActiveOnly != nil {
		activeOnly = *in.ActiveOnly
	}
	flows, err := d.ListProcessFlows.Execute(ctx, activeOnly)
	if err != nil {
		return listProcessFlowsOutput{}, err
	}
	dtos := make([]processFlowDTO, 0, len(flows))
	for _, f := range flows {
		dtos = append(dtos, toProcessFlowDTO(f))
	}
	return listProcessFlowsOutput{Flows: dtos}, nil
}

// registerProcessFlowTools adds the two read-only flow tools, each only when
// its use case is wired (the same optional-tool convention get_cpt_schedule
// uses). Both are L1: they read facts and change nothing (ADR 0041).
func (d Deps) registerProcessFlowTools(server *mcp.Server) {
	readOnly := true
	if d.GetProcessFlow != nil {
		addTool(server, &mcp.Tool{
			Name:        "get_process_flow",
			Description: "Return one process flow by id (ADR 0042): its name, the ordered stages (each a pathId naming an existing process path and the processType of the work done there: PICK, REBIN, PACK, SLAM, DISPATCH, ARRIVAL, RELOCATE or REPLENISH), status (ACTIVE or RETIRED), version and timestamps. Works for retired flows too. Not found is returned as a tool-level error. Read-only: flows are changed through REST only.",
			Annotations: &mcp.ToolAnnotations{ReadOnlyHint: readOnly},
			Meta:        authorityMeta(authorityL1),
		}, d.getProcessFlow)
	}
	if d.ListProcessFlows != nil {
		addTool(server, &mcp.Tool{
			Name:        "list_process_flows",
			Description: "List process flows in id order. activeOnly (default true) restricts the result to ACTIVE flows; set it false to include retired ones too. Each entry carries the same fields as get_process_flow.",
			Annotations: &mcp.ToolAnnotations{ReadOnlyHint: readOnly},
			Meta:        authorityMeta(authorityL1),
		}, d.listProcessFlows)
	}
}
