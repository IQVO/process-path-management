package usecases_test

import (
	"context"
	"sync"
	"time"

	"github.com/claudioed/process-path-management/internal/application/ports"
	"github.com/claudioed/process-path-management/internal/domain/processpath"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// fakeRepo is a minimal in-memory ports.ProcessPathRepo for use-case
// tests -- kept in the usecases_test package rather than
// adapters/outbound/memory so use-case tests never depend on the real
// adapter package (matches the fleet's own convention of use cases being
// tested against small local fakes, adapters tested separately).
type fakeRepo struct {
	mu    sync.Mutex
	paths map[shared.PathId]*processpath.ProcessPath
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{paths: make(map[shared.PathId]*processpath.ProcessPath)}
}

func (r *fakeRepo) Create(_ context.Context, p *processpath.ProcessPath) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.paths[p.ID()]; exists {
		return ports.ErrAlreadyExists
	}
	r.paths[p.ID()] = p
	return nil
}

func (r *fakeRepo) Save(_ context.Context, p *processpath.ProcessPath) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.paths[p.ID()] = p
	return nil
}

func (r *fakeRepo) FindByID(_ context.Context, id shared.PathId) (*processpath.ProcessPath, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.paths[id], nil
}

func (r *fakeRepo) ListActive(_ context.Context) ([]*processpath.ProcessPath, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*processpath.ProcessPath
	for _, p := range r.paths {
		if p.IsActive() {
			out = append(out, p)
		}
	}
	return out, nil
}

func (r *fakeRepo) ListAll(_ context.Context) ([]*processpath.ProcessPath, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*processpath.ProcessPath
	for _, p := range r.paths {
		out = append(out, p)
	}
	return out, nil
}

// fakePublisher records every event published, for assertions.
type fakePublisher struct {
	mu     sync.Mutex
	events []shared.DomainEvent
}

func (p *fakePublisher) Publish(_ context.Context, event shared.DomainEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, event)
	return nil
}

func (p *fakePublisher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.events)
}

func (p *fakePublisher) last() shared.DomainEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.events) == 0 {
		return nil
	}
	return p.events[len(p.events)-1]
}

// fixedClock returns a constant time, so tests assert exact timestamps
// rather than tolerating a time.Now() race.
type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time { return c.t }

// erroringRepo wraps a fakeRepo and forces every FindByID/Save/List call to
// fail, so use-case error-propagation branches (repository failures) are
// exercised without a real database.
type erroringRepo struct {
	*fakeRepo
	findErr error
	saveErr error
	listErr error
}

func (r *erroringRepo) FindByID(ctx context.Context, id shared.PathId) (*processpath.ProcessPath, error) {
	if r.findErr != nil {
		return nil, r.findErr
	}
	return r.fakeRepo.FindByID(ctx, id)
}

func (r *erroringRepo) Create(ctx context.Context, p *processpath.ProcessPath) error {
	if r.saveErr != nil {
		return r.saveErr
	}
	return r.fakeRepo.Create(ctx, p)
}

func (r *erroringRepo) Save(ctx context.Context, p *processpath.ProcessPath) error {
	if r.saveErr != nil {
		return r.saveErr
	}
	return r.fakeRepo.Save(ctx, p)
}

func (r *erroringRepo) ListActive(ctx context.Context) ([]*processpath.ProcessPath, error) {
	if r.listErr != nil {
		return nil, r.listErr
	}
	return r.fakeRepo.ListActive(ctx)
}

func (r *erroringRepo) ListAll(ctx context.Context) ([]*processpath.ProcessPath, error) {
	if r.listErr != nil {
		return nil, r.listErr
	}
	return r.fakeRepo.ListAll(ctx)
}

// erroringPublisher always fails Publish, so use-case error-propagation
// branches (publish failures) are exercised.
type erroringPublisher struct{ err error }

func (p *erroringPublisher) Publish(context.Context, shared.DomainEvent) error { return p.err }

// recordingMetrics records PathDefinitionAccepted/Rejected calls, so
// DefinePath's optional-metrics branches are exercised (a nil Metrics is
// already covered by every other test in this file).
type recordingMetrics struct {
	accepted int
	rejected int
}

func (m *recordingMetrics) PathDefinitionAccepted(context.Context) { m.accepted++ }
func (m *recordingMetrics) PathDefinitionRejected(context.Context) { m.rejected++ }
