package http

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/claudioed/process-path-management/internal/adapters/inbound/http/page"
	"github.com/claudioed/process-path-management/internal/domain/processflow"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// stageRequest is one stage on the PUT /process-flows/{flowId} body.
type stageRequest struct {
	PathId      string `json:"pathId"`
	ProcessType string `json:"processType"`
}

// putProcessFlowRequest is the PUT /process-flows/{flowId} request body: the
// flow's FULL definition (ADR 0042). The flow id is the path parameter.
type putProcessFlowRequest struct {
	Name   string         `json:"name"`
	Stages []stageRequest `json:"stages"`
}

// stageResponse mirrors stageRequest for the response side.
type stageResponse struct {
	PathId      string `json:"pathId"`
	ProcessType string `json:"processType"`
}

// processFlowResponse is the response body for every endpoint that returns
// one ProcessFlow. Version is the same number the flow's events carry.
type processFlowResponse struct {
	FlowId    string          `json:"flowId"`
	Name      string          `json:"name"`
	Stages    []stageResponse `json:"stages"`
	Status    string          `json:"status"`
	Version   int             `json:"version"`
	CreatedAt string          `json:"createdAt"`
	UpdatedAt string          `json:"updatedAt"`
}

func toProcessFlowResponse(f *processflow.ProcessFlow) processFlowResponse {
	stages := f.Stages()
	out := make([]stageResponse, len(stages))
	for i, s := range stages {
		out[i] = stageResponse{PathId: string(s.PathId()), ProcessType: string(s.ProcessType())}
	}
	return processFlowResponse{
		FlowId:    string(f.ID()),
		Name:      f.Name(),
		Stages:    out,
		Status:    string(f.Status()),
		Version:   f.Version(),
		CreatedAt: f.CreatedAt().UTC().Format(timeFormat),
		UpdatedAt: f.UpdatedAt().UTC().Format(timeFormat),
	}
}

func toStages(reqs []stageRequest) ([]processflow.Stage, error) {
	out := make([]processflow.Stage, 0, len(reqs))
	for _, r := range reqs {
		s, err := processflow.NewStage(shared.PathId(r.PathId), processflow.ProcessType(r.ProcessType))
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

// handlePutProcessFlow defines or revises a flow from its full state. It is
// idempotent by nature, so it takes no Idempotency-Key: 201 when the flow is
// new, 200 for every later PUT, including one that changes nothing (which
// raises no event and keeps the version).
func (s *Server) handlePutProcessFlow(w http.ResponseWriter, r *http.Request) {
	flowId := processflow.FlowId(chi.URLParam(r, "flowId"))

	var req putProcessFlowRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	stages, err := toStages(req.Stages)
	if err != nil {
		writeError(w, r, err)
		return
	}
	f, created, err := s.DefineProcessFlow.Execute(r.Context(), flowId, req.Name, stages)
	if err != nil {
		writeError(w, r, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, toProcessFlowResponse(f))
}

func (s *Server) handleGetProcessFlow(w http.ResponseWriter, r *http.Request) {
	f, err := s.GetProcessFlow.Execute(r.Context(), processflow.FlowId(chi.URLParam(r, "flowId")))
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toProcessFlowResponse(f))
}

// handleRetireProcessFlow retires a flow. Retiring an already retired flow
// answers 204 again (the same idempotent shape as DELETE /process-paths); an
// unknown flow is a 404.
func (s *Server) handleRetireProcessFlow(w http.ResponseWriter, r *http.Request) {
	if err := s.RetireProcessFlow.Execute(r.Context(), processflow.FlowId(chi.URLParam(r, "flowId"))); err != nil {
		writeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleListProcessFlows backs GET /process-flows: one keyset page, {items,
// nextCursor}, in flowId order (REST API guideline section 3). ACTIVE flows
// only by default; ?all=true adds the retired ones.
func (s *Server) handleListProcessFlows(w http.ResponseWriter, r *http.Request) {
	limit, activeOnly, after, ok := listQuery(w, r)
	if !ok {
		return
	}
	flows, err := s.ListProcessFlows.ExecutePage(r.Context(), activeOnly, processflow.FlowId(after), page.FetchSize(limit))
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, pageOf(flows, limit, processFlowPosition, toProcessFlowResponse))
}

// processFlowPosition is the keyset position of a flow in GET
// /process-flows: its id, which is unique.
func processFlowPosition(f *processflow.ProcessFlow) page.Position {
	return page.Position{Sort: string(f.ID()), ID: string(f.ID())}
}
