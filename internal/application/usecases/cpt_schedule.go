package usecases

import (
	"context"
	"sort"

	"github.com/claudioed/process-path-management/internal/application/ports"
	"github.com/claudioed/process-path-management/internal/domain/cptschedule"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// DefineCPTSchedule defines or wholesale-revises a site's CPTSchedule and
// publishes CPTScheduleChanged (ADR 0010). There is no separate
// "RevisePath"-style use case: a CPTSchedule has no partial-update
// semantics — every PUT replaces the schedule's timezone/cutoffs in full,
// matching the aggregate's own Define/Revise (Revise mirrors Define's
// signature exactly).
//
// The one cross-aggregate invariant ADR 0010 calls out — every cutoff's
// eligiblePathIds must reference an Active ProcessPath in this service's
// own store — is enforced HERE, via the injected ProcessPathRepo, not in
// the cptschedule domain package: it is a use-case-level check against a
// sibling aggregate's repo, never a foreign key.
type DefineCPTSchedule struct {
	Repo            ports.CPTScheduleRepo
	ProcessPathRepo ports.ProcessPathRepo
	Publisher       ports.EventPublisher
	Clock           ports.Clock
	// UnitOfWork brackets Save + Publish atomically (ADR 0003); nil means
	// no transactional backing (see DefinePath).
	UnitOfWork ports.UnitOfWork
}

func (uc *DefineCPTSchedule) Execute(ctx context.Context, siteId shared.SiteId, timezone string, cutoffs []cptschedule.Cutoff) (*cptschedule.CPTSchedule, error) {
	now := uc.Clock.Now()

	var schedule *cptschedule.CPTSchedule
	err := atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		// The cross-aggregate eligiblePathIds check: every referenced
		// PathId must resolve to an Active ProcessPath in this service's
		// own store. It runs INSIDE the unit of work and takes the paths'
		// share locks, which are held until commit (ADR 0028): a
		// DeactivatePath of any of them blocks until this schedule is
		// committed, and then refuses with 409. Checked against the union
		// across all cutoffs before any domain construction, so a caller
		// gets one clear error rather than a partial write.
		if err := uc.lockAndValidateEligiblePathIds(ctx, cutoffs); err != nil {
			return err
		}

		existing, err := uc.Repo.FindBySiteID(ctx, siteId)
		if err != nil {
			return err
		}
		if existing == nil {
			schedule, err = cptschedule.Define(siteId, timezone, cutoffs, now)
			if err != nil {
				return err
			}
		} else {
			schedule = existing
			changed, err := schedule.Revise(timezone, cutoffs, now)
			if err != nil {
				return err
			}
			if !changed {
				return nil
			}
		}

		if err := uc.Repo.Save(ctx, schedule); err != nil {
			return err
		}
		return uc.Publisher.Publish(ctx, cptschedule.ToSnapshot(schedule, now))
	})
	if err != nil {
		return nil, err
	}
	return schedule, nil
}

// lockAndValidateEligiblePathIds enforces ADR 0010's one cross-aggregate
// invariant: every eligiblePathIds entry across every cutoff must
// reference an Active ProcessPath. The distinct ids are sorted and locked
// FOR SHARE in one repository call (ADR 0028) — the sort is the stable lock
// order that keeps concurrent definers from deadlocking — and each locked
// row is judged on its committed state at lock time, so a deactivation that
// committed first is seen here.
func (uc *DefineCPTSchedule) lockAndValidateEligiblePathIds(ctx context.Context, cutoffs []cptschedule.Cutoff) error {
	seen := make(map[shared.PathId]bool)
	var ids []shared.PathId
	for _, c := range cutoffs {
		for _, id := range c.EligiblePathIds() {
			if !seen[id] {
				seen[id] = true
				ids = append(ids, id)
			}
		}
	}
	if len(ids) == 0 {
		return nil
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
	for _, id := range ids {
		if !active[id] {
			return ErrIneligiblePathId
		}
	}
	return nil
}

// GetCPTSchedule returns one site's schedule.
type GetCPTSchedule struct {
	Repo ports.CPTScheduleRepo
}

func (uc *GetCPTSchedule) Execute(ctx context.Context, siteId shared.SiteId) (*cptschedule.CPTSchedule, error) {
	s, err := uc.Repo.FindBySiteID(ctx, siteId)
	if err != nil {
		return nil, err
	}
	if s == nil {
		return nil, ErrCPTScheduleNotFound
	}
	return s, nil
}
