package memory

import (
	"context"
	"sort"
	"sync"

	"github.com/claudioed/process-path-management/internal/application/ports"
	"github.com/claudioed/process-path-management/internal/domain/processpath"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// ProcessPathRepo is an in-memory implementation of
// ports.ProcessPathRepo.
type ProcessPathRepo struct {
	mu    sync.RWMutex
	paths map[shared.PathId]*processpath.ProcessPath
}

// NewProcessPathRepo constructs an empty ProcessPathRepo.
func NewProcessPathRepo() *ProcessPathRepo {
	return &ProcessPathRepo{paths: make(map[shared.PathId]*processpath.ProcessPath)}
}

func (r *ProcessPathRepo) Create(_ context.Context, p *processpath.ProcessPath) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.paths[p.ID()]; exists {
		return ports.ErrAlreadyExists
	}
	r.paths[p.ID()] = p
	return nil
}

func (r *ProcessPathRepo) Save(_ context.Context, p *processpath.ProcessPath) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.paths[p.ID()] = p
	return nil
}

func (r *ProcessPathRepo) FindByID(_ context.Context, id shared.PathId) (*processpath.ProcessPath, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.paths[id], nil
}

func (r *ProcessPathRepo) ListActive(_ context.Context) ([]*processpath.ProcessPath, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []*processpath.ProcessPath
	for _, p := range r.paths {
		if p.IsActive() {
			out = append(out, p)
		}
	}
	return out, nil
}

func (r *ProcessPathRepo) ListAll(_ context.Context) ([]*processpath.ProcessPath, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*processpath.ProcessPath, 0, len(r.paths))
	for _, p := range r.paths {
		out = append(out, p)
	}
	return out, nil
}

// FindByIDForUpdate implements ports.ProcessPathRepo. The in-memory adapter
// has no transactions to hold a row lock in (its UnitOfWork is a
// pass-through), so this is FindByID; the Postgres adapter is the one that
// enforces ADR 0028.
func (r *ProcessPathRepo) FindByIDForUpdate(ctx context.Context, id shared.PathId) (*processpath.ProcessPath, error) {
	return r.FindByID(ctx, id)
}

// LockByIDsForShare implements ports.ProcessPathRepo: the known paths among
// ids in ascending id order, with no real lock (see FindByIDForUpdate).
func (r *ProcessPathRepo) LockByIDsForShare(_ context.Context, ids []shared.PathId) ([]*processpath.ProcessPath, error) {
	sorted := append([]shared.PathId(nil), ids...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []*processpath.ProcessPath
	for i, id := range sorted {
		if i > 0 && sorted[i-1] == id {
			continue
		}
		if p, ok := r.paths[id]; ok {
			out = append(out, p)
		}
	}
	return out, nil
}
