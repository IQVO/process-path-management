package a2a

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/claudioed/process-path-management/internal/application/usecases"
	"github.com/claudioed/process-path-management/internal/domain/processflow"
)

// Skill ids of the ProcessFlow reads (ADR 0042): the MCP tool names of the same
// reads, as for the path skills.
const (
	skillGetProcessFlow   = "get_process_flow"
	skillListProcessFlows = "list_process_flows"
)

type getProcessFlowArgs struct {
	FlowId string `json:"flowId"`
}

type listProcessFlowsArgs struct {
	// ActiveOnly defaults to true when omitted, like the REST and MCP views.
	ActiveOnly *bool `json:"activeOnly"`
}

// stageDTO is the message-boundary representation of one processflow.Stage.
type stageDTO struct {
	PathId      string `json:"pathId"`
	ProcessType string `json:"processType"`
}

// processFlowDTO is the message-boundary representation of a ProcessFlow
// aggregate: the same fields the MCP tool and the REST wire carry, the stages
// in order.
type processFlowDTO struct {
	FlowId    string     `json:"flowId"`
	Name      string     `json:"name"`
	Stages    []stageDTO `json:"stages"`
	Status    string     `json:"status"`
	Version   int        `json:"version"`
	CreatedAt string     `json:"createdAt"`
	UpdatedAt string     `json:"updatedAt"`
}

// processFlowsDTO is the list_process_flows result.
type processFlowsDTO struct {
	ActiveOnly bool             `json:"activeOnly"`
	Flows      []processFlowDTO `json:"flows"`
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

// summarizeFlow is the one-line text form of one flow: its stages in order.
func summarizeFlow(f processFlowDTO) string {
	stages := make([]string, 0, len(f.Stages))
	for _, s := range f.Stages {
		stages = append(stages, s.PathId+"("+s.ProcessType+")")
	}
	return fmt.Sprintf("%s: %s, version %d, stages %s", f.FlowId, strings.ToLower(f.Status), f.Version, strings.Join(stages, " > "))
}

// summarizeFlows is the one-line text form of the flow list.
func summarizeFlows(list processFlowsDTO) string {
	ids := make([]string, 0, len(list.Flows))
	for _, f := range list.Flows {
		ids = append(ids, f.FlowId)
	}
	scope := "including retired"
	if list.ActiveOnly {
		scope = "active only"
	}
	return fmt.Sprintf("%d process flows (%s): %s", len(list.Flows), scope, strings.Join(ids, ", "))
}

// runFlowSkill dispatches the two flow skills; handled is false for any other
// skill id, or when the flow use case it needs is not wired.
func runFlowSkill(ctx context.Context, deps Deps, req skillRequest) (dto skillDTO, prob *problem, handled bool) {
	switch {
	case req.Skill == skillGetProcessFlow && deps.GetProcessFlow != nil:
		data, prob := deps.getProcessFlow(ctx, req.Arguments)
		if prob != nil {
			return skillDTO{}, prob, true
		}
		return skillDTO{ProcessFlow: &data, summary: summarizeFlow(data)}, nil, true
	case req.Skill == skillListProcessFlows && deps.ListProcessFlows != nil:
		data, prob := deps.listProcessFlows(ctx, req.Arguments)
		if prob != nil {
			return skillDTO{}, prob, true
		}
		return skillDTO{ProcessFlows: &data, summary: summarizeFlows(data)}, nil, true
	}
	return skillDTO{}, nil, false
}

// getProcessFlow is the single-flow read skill, through the same GetProcessFlow
// use case the REST and MCP adapters use.
func (d Deps) getProcessFlow(ctx context.Context, rawArgs json.RawMessage) (processFlowDTO, *problem) {
	var args getProcessFlowArgs
	if err := decodeStrict(rawArgs, &args); err != nil {
		return processFlowDTO{}, problemInvalidArgument(fmt.Errorf("arguments: %w", err))
	}
	if strings.TrimSpace(args.FlowId) == "" {
		return processFlowDTO{}, problemInvalidArgument(errors.New("flowId is required"))
	}
	f, err := d.GetProcessFlow.Execute(ctx, processflow.FlowId(args.FlowId))
	if err != nil {
		return processFlowDTO{}, problemFromFlowUseCase(err, args.FlowId)
	}
	return toProcessFlowDTO(f), nil
}

// listProcessFlows is the flow list read skill, through the same
// ListProcessFlows use case. activeOnly defaults to true.
func (d Deps) listProcessFlows(ctx context.Context, rawArgs json.RawMessage) (processFlowsDTO, *problem) {
	var args listProcessFlowsArgs
	if err := decodeStrict(rawArgs, &args); err != nil {
		return processFlowsDTO{}, problemInvalidArgument(fmt.Errorf("arguments: %w", err))
	}
	activeOnly := true
	if args.ActiveOnly != nil {
		activeOnly = *args.ActiveOnly
	}
	flows, err := d.ListProcessFlows.Execute(ctx, activeOnly)
	if err != nil {
		return processFlowsDTO{}, problemFromFlowUseCase(err, "")
	}
	dtos := make([]processFlowDTO, 0, len(flows))
	for _, f := range flows {
		dtos = append(dtos, toProcessFlowDTO(f))
	}
	return processFlowsDTO{ActiveOnly: activeOnly, Flows: dtos}, nil
}

// problemFromFlowUseCase maps a flow read error the way the REST adapter does:
// a missing flow is 404 flow-not-found, anything else 500 (via the shared
// problemFromUseCase).
func problemFromFlowUseCase(err error, flowId string) *problem {
	if errors.Is(err, usecases.ErrFlowNotFound) {
		return &problem{Type: problemTypeBase + "/flow-not-found", Title: "No process flow exists with this id", Detail: fmt.Sprintf("no process flow exists with flowId %q", flowId), Status: 404}
	}
	return problemFromUseCase(err, "")
}
