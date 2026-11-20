package memory

import (
	"context"
	"sort"
	"sync"

	"github.com/claudioed/process-path-management/internal/application/ports"
	"github.com/claudioed/process-path-management/internal/domain/processflow"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// ProcessFlowRepo is an in-memory implementation of ports.ProcessFlowRepo.
// It stores and returns COPIES of the aggregates, so a use case that loads a
// flow and revises it only changes the stored one through Save, where the
// version guard (ADR 0017) is applied exactly as the Postgres adapter does.
type ProcessFlowRepo struct {
	mu    sync.RWMutex
	flows map[processflow.FlowId]*processflow.ProcessFlow
}

// NewProcessFlowRepo constructs an empty ProcessFlowRepo.
func NewProcessFlowRepo() *ProcessFlowRepo {
	return &ProcessFlowRepo{flows: make(map[processflow.FlowId]*processflow.ProcessFlow)}
}

func cloneFlow(f *processflow.ProcessFlow) *processflow.ProcessFlow {
	if f == nil {
		return nil
	}
	return processflow.Rehydrate(f.ID(), f.Name(), f.Stages(), f.Status(), f.CreatedAt(), f.UpdatedAt(), f.Version())
}

func (r *ProcessFlowRepo) Create(_ context.Context, f *processflow.ProcessFlow) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.flows[f.ID()]; exists {
		return ports.ErrAlreadyExists
	}
	r.flows[f.ID()] = cloneFlow(f)
	return nil
}

// Save applies the version guard: f carries the new version and the stored
// one must be exactly one lower.
func (r *ProcessFlowRepo) Save(_ context.Context, f *processflow.ProcessFlow) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	stored, ok := r.flows[f.ID()]
	if !ok || stored.Version() != f.Version()-1 {
		return ports.ErrConcurrentModification
	}
	r.flows[f.ID()] = cloneFlow(f)
	return nil
}

func (r *ProcessFlowRepo) FindByID(_ context.Context, id processflow.FlowId) (*processflow.ProcessFlow, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return cloneFlow(r.flows[id]), nil
}

// ListPage: keyset semantics identical to the Postgres adapter (WHERE
// flow_id > $after ORDER BY flow_id LIMIT $size).
func (r *ProcessFlowRepo) ListPage(_ context.Context, activeOnly bool, after processflow.FlowId, size int) ([]*processflow.ProcessFlow, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]processflow.FlowId, 0, len(r.flows))
	for id, f := range r.flows {
		if id > after && (!activeOnly || f.IsActive()) {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	if len(ids) > size {
		ids = ids[:size]
	}
	out := make([]*processflow.ProcessFlow, 0, len(ids))
	for _, id := range ids {
		out = append(out, cloneFlow(r.flows[id]))
	}
	return out, nil
}

// ListActiveFlowIDsReferencingPath returns, ascending, every ACTIVE flow with
// a stage on id.
func (r *ProcessFlowRepo) ListActiveFlowIDsReferencingPath(_ context.Context, id shared.PathId) ([]processflow.FlowId, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []processflow.FlowId
	for flowId, f := range r.flows {
		if !f.IsActive() {
			continue
		}
		for _, p := range f.StagePathIds() {
			if p == id {
				out = append(out, flowId)
				break
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

// LockActiveForShare returns every ACTIVE flow id, ascending. The in-memory
// repo has no row locks; this is the test-double equivalent.
func (r *ProcessFlowRepo) LockActiveForShare(_ context.Context) ([]processflow.FlowId, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]processflow.FlowId, 0, len(r.flows))
	for id, f := range r.flows {
		if f.IsActive() {
			out = append(out, id)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

var _ ports.ProcessFlowRepo = (*ProcessFlowRepo)(nil)
