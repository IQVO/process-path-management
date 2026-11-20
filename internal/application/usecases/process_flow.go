package usecases

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/claudioed/process-path-management/internal/application/ports"
	"github.com/claudioed/process-path-management/internal/domain/processflow"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// DefineProcessFlow defines a flow, or wholesale-revises an existing one,
// and publishes ProcessFlowDefined or ProcessFlowRevised (ADR 0042). It is
// the PUT use case, so it is idempotent by nature: the request is the full
// state, and a request that changes nothing raises no event and keeps the
// version.
//
// The one cross-aggregate rule, that every stage names an ACTIVE
// ProcessPath of this service's own catalogue, is enforced here, exactly as
// DefineCPTSchedule does it (ADR 0010, ADR 0028): the whole use case runs in
// one unit of work and the named paths are locked FOR SHARE (sorted) until
// commit, which serialises it against DeactivatePath.
type DefineProcessFlow struct {
	Repo            ports.ProcessFlowRepo
	ProcessPathRepo ports.ProcessPathRepo
	Publisher       ports.EventPublisher
	Clock           ports.Clock
	// UnitOfWork brackets the writes and the event atomically (ADR 0003);
	// nil means no transactional backing (see DefinePath).
	UnitOfWork ports.UnitOfWork
}

// Execute defines or revises flow id. created is true when the flow did not
// exist before this call.
func (uc *DefineProcessFlow) Execute(ctx context.Context, id processflow.FlowId, name string, stages []processflow.Stage) (flow *processflow.ProcessFlow, created bool, err error) {
	if id == "" {
		return nil, false, processflow.ErrEmptyFlowId
	}
	// Reject a malformed request before taking any lock.
	if err := processflow.ValidateDefinition(name, stages); err != nil {
		return nil, false, err
	}
	now := uc.Clock.Now()

	err = atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		created = false
		existing, err := uc.Repo.FindByID(ctx, id)
		if err != nil {
			return err
		}
		if existing != nil && !existing.IsActive() {
			return processflow.ErrFlowRetired
		}
		if err := uc.lockAndValidateStagePaths(ctx, stages); err != nil {
			return err
		}
		if existing == nil {
			flow, err = uc.create(ctx, id, name, stages, now)
			created = err == nil
			return err
		}
		flow = existing
		changed, err := flow.Revise(name, stages, now)
		if err != nil {
			return err
		}
		if !changed {
			return nil
		}
		if err := uc.Repo.Save(ctx, flow); err != nil {
			return err
		}
		return uc.Publisher.Publish(ctx, flow.RevisedEvent(now))
	})
	if err != nil {
		return nil, false, err
	}
	return flow, created, nil
}

func (uc *DefineProcessFlow) create(ctx context.Context, id processflow.FlowId, name string, stages []processflow.Stage, now time.Time) (*processflow.ProcessFlow, error) {
	f, err := processflow.Define(id, name, stages, now)
	if err != nil {
		return nil, err
	}
	// Create is insert-only: a concurrent first define that won the race
	// makes it report ErrAlreadyExists, which the caller sees as a
	// concurrent modification (reload and retry) instead of a second
	// ProcessFlowDefined.
	if err := uc.Repo.Create(ctx, f); err != nil {
		if errors.Is(err, ports.ErrAlreadyExists) {
			return nil, ports.ErrConcurrentModification
		}
		return nil, err
	}
	return f, uc.Publisher.Publish(ctx, f.DefinedEvent(now))
}

// lockAndValidateStagePaths share-locks the distinct paths the stages name
// in ascending id order (the stable lock order that keeps concurrent
// definers from deadlocking) and requires each to be an ACTIVE path.
func (uc *DefineProcessFlow) lockAndValidateStagePaths(ctx context.Context, stages []processflow.Stage) error {
	ids := make([]shared.PathId, 0, len(stages))
	for _, s := range stages {
		ids = append(ids, s.PathId())
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	locked, err := uc.ProcessPathRepo.LockByIDsForShare(ctx, ids)
	if err != nil {
		return err
	}
	active := make(map[shared.PathId]bool, len(locked))
	for _, p := range locked {
		if p != nil && p.IsActive() {
			active[p.ID()] = true
		}
	}
	var missing []shared.PathId
	for _, id := range ids {
		if !active[id] {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("%w: %v", ErrIneligibleStagePath, missing)
	}
	return nil
}

// RetireProcessFlow retires a flow and publishes ProcessFlowRetired, the
// full state with status RETIRED and the next version. Retiring an already
// retired flow succeeds without an event (the same idempotent end-to-end
// shape as DeactivatePath).
type RetireProcessFlow struct {
	Repo       ports.ProcessFlowRepo
	Publisher  ports.EventPublisher
	Clock      ports.Clock
	UnitOfWork ports.UnitOfWork
}

func (uc *RetireProcessFlow) Execute(ctx context.Context, id processflow.FlowId) error {
	now := uc.Clock.Now()
	return atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		f, err := uc.Repo.FindByID(ctx, id)
		if err != nil {
			return err
		}
		if f == nil {
			return ErrFlowNotFound
		}
		if !f.Retire(now) {
			return nil
		}
		if err := uc.Repo.Save(ctx, f); err != nil {
			return err
		}
		return uc.Publisher.Publish(ctx, f.RetiredEvent(now))
	})
}

// GetProcessFlow returns one flow by id, active or retired.
type GetProcessFlow struct {
	Repo ports.ProcessFlowRepo
}

func (uc *GetProcessFlow) Execute(ctx context.Context, id processflow.FlowId) (*processflow.ProcessFlow, error) {
	f, err := uc.Repo.FindByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if f == nil {
		return nil, ErrFlowNotFound
	}
	return f, nil
}

// ListProcessFlows lists flows. activeOnly=true is the default view.
type ListProcessFlows struct {
	Repo ports.ProcessFlowRepo
}

// ExecutePage is one keyset page of the list behind GET /process-flows: at
// most size flows ordered by id, strictly after `after` ("" is the first
// page). The REST adapter asks for limit+1 rows to learn whether a next page
// exists.
func (uc *ListProcessFlows) ExecutePage(ctx context.Context, activeOnly bool, after processflow.FlowId, size int) ([]*processflow.ProcessFlow, error) {
	return uc.Repo.ListPage(ctx, activeOnly, after, size)
}

// Execute returns the COMPLETE list, in id order, for the MCP and A2A
// adapters, whose tool schemas have no cursor: it walks the keyset pages
// until the last one.
func (uc *ListProcessFlows) Execute(ctx context.Context, activeOnly bool) ([]*processflow.ProcessFlow, error) {
	var all []*processflow.ProcessFlow
	var after processflow.FlowId
	for {
		rows, err := uc.ExecutePage(ctx, activeOnly, after, listAllPageSize+1)
		if err != nil {
			return nil, err
		}
		if len(rows) <= listAllPageSize {
			return append(all, rows...), nil
		}
		all = append(all, rows[:listAllPageSize]...)
		after = rows[listAllPageSize-1].ID()
	}
}
