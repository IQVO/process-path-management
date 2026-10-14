package usecases

import (
	"context"
	"time"

	"github.com/claudioed/process-path-management/internal/application/ports"
	"github.com/claudioed/process-path-management/internal/domain/processpath"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// RevisePath updates an existing Active path's matchPrefix/
// requiredCapabilities/cycleTimeP95/eligibility/intake and publishes
// ProcessPathUpdated -- but ONLY if the revision actually changes
// something (see ProcessPath.Revise's changed return value). A no-op
// revision request is a successful no-op, not an error and not a
// spuriously republished event.
type RevisePath struct {
	Repo      ports.ProcessPathRepo
	Publisher ports.EventPublisher
	Clock     ports.Clock
	// UnitOfWork brackets Save + Publish atomically (ADR 0003); nil means
	// no transactional backing (see DefinePath).
	UnitOfWork ports.UnitOfWork
}

// Execute revises the path. intake is optional (ADR 0034): nil keeps the path's current intake, so a
// client that predates the field (the console's revise form, an older
// seed) never silently clears it; a non-nil value, the empty set
// included, replaces it.
func (uc *RevisePath) Execute(ctx context.Context, id shared.PathId, matchPrefix string, requiredCapabilities []shared.Capability, cycleTimeP95 time.Duration, eligibility shared.Eligibility, intake *shared.Intake) (*processpath.ProcessPath, error) {
	p, err := uc.Repo.FindByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if p == nil {
		return nil, ErrPathNotFound
	}

	revisedIntake := p.Intake()
	if intake != nil {
		revisedIntake = *intake
	}
	now := uc.Clock.Now()
	changed, err := p.Revise(matchPrefix, requiredCapabilities, cycleTimeP95, eligibility, revisedIntake, now)
	if err != nil {
		return nil, err
	}
	if !changed {
		return p, nil
	}
	err = atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		if err := uc.Repo.Save(ctx, p); err != nil {
			return err
		}
		return uc.Publisher.Publish(ctx, shared.ProcessPathUpdated{
			PathId:                  p.ID(),
			MatchPrefix:             p.MatchPrefix(),
			Direct:                  p.Direct(),
			RequiredCapabilities:    p.RequiredCapabilities(),
			DestinationLocationRole: p.DestinationLocationRole(),
			CycleTimeP95:            p.CycleTimeP95(),
			Eligibility:             p.Eligibility(),
			Intake:                  p.Intake(),
			At:                      now,
		})
	})
	if err != nil {
		return nil, err
	}
	return p, nil
}
