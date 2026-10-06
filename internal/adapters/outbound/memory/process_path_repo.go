package memory

import (
	"context"
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
