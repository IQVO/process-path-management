// Package usecases implements the application layer: one struct per use
// case, orchestrating the processpath domain package against the ports
// interfaces. No adapter/framework type ever appears here.
package usecases

import (
	"context"
	"errors"
	"time"

	"github.com/claudioed/process-path-management/internal/application/ports"
	"github.com/claudioed/process-path-management/internal/domain/processpath"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// DefinePath defines a brand-new process path and publishes
// ProcessPathCreated. Rejects with ErrPathAlreadyExists if the PathId is
// already taken (active or deactivated) — see that sentinel's doc comment
// for why this is never a silent overwrite.
type DefinePath struct {
	Repo      ports.ProcessPathRepo
	Publisher ports.EventPublisher
	Clock     ports.Clock
	// UnitOfWork brackets Save + Publish atomically (ADR 0003). Optional:
	// a nil value means "no transactional backing" and the two calls run
	// back to back, which is exactly the in-memory / log-publisher dev
	// configuration.
	UnitOfWork ports.UnitOfWork
	// Metrics is optional (fleet convention: a nil value means "not
	// instrumented", see ports.PathMetrics's doc comment).
	Metrics ports.PathMetrics
}

func (uc *DefinePath) Execute(ctx context.Context, id shared.PathId, matchPrefix string, direct bool, requiredCapabilities []shared.Capability, destinationLocationRole shared.DestinationLocationRole, cycleTimeP95 time.Duration, eligibility shared.Eligibility) (*processpath.ProcessPath, error) {
	existing, err := uc.Repo.FindByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		uc.recordRejected(ctx)
		return nil, ErrPathAlreadyExists
	}

	now := uc.Clock.Now()
	p, err := processpath.Define(id, matchPrefix, direct, requiredCapabilities, destinationLocationRole, cycleTimeP95, eligibility, now)
	if err != nil {
		uc.recordRejected(ctx)
		return nil, err
	}
	err = atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		// Create is insert-only: a concurrent define that won the race
		// between the FindByID above and this insert makes Create report
		// ErrAlreadyExists instead of upserting over the winner's row and
		// publishing a second ProcessPathCreated.
		if err := uc.Repo.Create(ctx, p); err != nil {
			return err
		}
		return uc.Publisher.Publish(ctx, shared.ProcessPathCreated{
			PathId:                  p.ID(),
			MatchPrefix:             p.MatchPrefix(),
			Direct:                  p.Direct(),
			RequiredCapabilities:    p.RequiredCapabilities(),
			DestinationLocationRole: p.DestinationLocationRole(),
			CycleTimeP95:            p.CycleTimeP95(),
			Eligibility:             p.Eligibility(),
			At:                      now,
		})
	})
	if errors.Is(err, ports.ErrAlreadyExists) {
		uc.recordRejected(ctx)
		return nil, ErrPathAlreadyExists
	}
	if err != nil {
		return nil, err
	}
	if uc.Metrics != nil {
		uc.Metrics.PathDefinitionAccepted(ctx)
	}
	return p, nil
}

func (uc *DefinePath) recordRejected(ctx context.Context) {
	if uc.Metrics != nil {
		uc.Metrics.PathDefinitionRejected(ctx)
	}
}

// atomically runs fn inside uow when one is wired, or directly otherwise.
// Keeping this in one place means every use case treats a nil UnitOfWork
// identically instead of each re-deciding the fallback.
func atomically(ctx context.Context, uow ports.UnitOfWork, fn func(ctx context.Context) error) error {
	if uow == nil {
		return fn(ctx)
	}
	return uow.Execute(ctx, fn)
}
