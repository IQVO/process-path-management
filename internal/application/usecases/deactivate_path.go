package usecases

import (
	"context"
	"fmt"

	"github.com/claudioed/process-path-management/internal/application/ports"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// DeactivatePath retires a path and publishes ProcessPathDeactivated.
// Idempotent end-to-end: deactivating an already-deactivated path
// succeeds without republishing the event (mirrors
// ProcessPath.Deactivate's own idempotency, extended here so a retried
// HTTP call or redelivered command never double-publishes).
//
// A path that any site's CPT schedule still lists in a cutoff's
// eligiblePathIds cannot be deactivated: ADR 0010 requires every
// eligiblePathIds entry to name an Active path, and ADR 0026 keeps that
// true after the write by refusing the deactivation with
// ErrPathReferencedByCPTSchedule (HTTP 409) until the operator revises
// the schedule.
type DeactivatePath struct {
	Repo      ports.ProcessPathRepo
	Publisher ports.EventPublisher
	Clock     ports.Clock
	// UnitOfWork brackets Save + Publish atomically (ADR 0003); nil means
	// no transactional backing (see DefinePath).
	UnitOfWork ports.UnitOfWork
	// CPTSchedules is consulted for schedules that still name the path
	// (ADR 0026). Always wired by the composition root; nil disables the
	// check and exists only for narrow use-case tests that do not model
	// schedules.
	CPTSchedules ports.CPTScheduleRepo
}

func (uc *DeactivatePath) Execute(ctx context.Context, id shared.PathId) error {
	p, err := uc.Repo.FindByID(ctx, id)
	if err != nil {
		return err
	}
	if p == nil {
		return ErrPathNotFound
	}
	if !p.IsActive() {
		return nil
	}

	now := uc.Clock.Now()
	return atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		if err := uc.rejectIfReferenced(ctx, id); err != nil {
			return err
		}
		p.Deactivate(now)
		if err := uc.Repo.Save(ctx, p); err != nil {
			return err
		}
		return uc.Publisher.Publish(ctx, shared.ProcessPathDeactivated{
			PathId: p.ID(),
			At:     now,
		})
	})
}

// rejectIfReferenced fails with ErrPathReferencedByCPTSchedule, naming the
// sites, when any CPT schedule still lists id.
func (uc *DeactivatePath) rejectIfReferenced(ctx context.Context, id shared.PathId) error {
	if uc.CPTSchedules == nil {
		return nil
	}
	sites, err := uc.CPTSchedules.ListSiteIDsReferencingPath(ctx, id)
	if err != nil {
		return err
	}
	if len(sites) > 0 {
		return fmt.Errorf("%w: path %q is listed by the schedule of site(s) %v; revise those schedules first", ErrPathReferencedByCPTSchedule, id, sites)
	}
	return nil
}
