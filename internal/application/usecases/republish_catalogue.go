package usecases

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/claudioed/process-path-management/internal/application/ports"
	"github.com/claudioed/process-path-management/internal/domain/cptschedule"
	"github.com/claudioed/process-path-management/internal/domain/processpath"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// MaxRepublishedPerKind bounds how many ACTIVE paths and how many CPT
// schedules one RepublishCatalogue run enqueues (ADR 0038). The catalogue is
// operator-defined and tens of entries large; the cap only guards the
// startup transaction against a runaway table. Anything beyond it is
// counted in RepublishResult.Omitted, never dropped silently.
const MaxRepublishedPerKind = 1000

// Metric kinds reported by RepublishCatalogue.
const (
	republishKindPath     = "path"
	republishKindSchedule = "schedule"
	republishKindFlow     = "flow"
)

// RepublishResult reports what one RepublishCatalogue run enqueued.
type RepublishResult struct {
	// Paths is the number of ProcessPathUpdated events enqueued.
	Paths int
	// Schedules is the number of CPTScheduleChanged events enqueued.
	Schedules int
	// Flows is the number of ProcessFlowRevised events enqueued, one per
	// ACTIVE flow (ADR 0042).
	Flows int
	// Omitted is the number of paths, schedules and flows NOT enqueued because a
	// kind exceeded MaxRepublishedPerKind. Non-zero means the topic is
	// still incomplete and the caller must report it loudly.
	Omitted int
}

// RepublishCatalogue enqueues the whole CURRENT catalogue: one
// ProcessPathUpdated (the full state, the exact payload RevisePath builds)
// per ACTIVE path and one CPTScheduleChanged (the full cutoff snapshot) per
// site, in ascending id order, so the integration topic always carries the
// current state of every live path and schedule regardless of the broker's
// retention (ADR 0038). Consumers rebuild their in-memory catalogue by
// replaying the topic from the earliest offset; a path whose Created event
// expired would otherwise be missing from every freshly started consumer.
//
// Deactivated paths are deliberately NOT republished: a cache that never
// saw them cannot hold them, and one that did already processed the
// ProcessPathDeactivated.
//
// The whole run is ONE unit of work and every path and schedule it
// republishes is locked FOR SHARE (ascending, ADR 0028) until commit. A
// concurrent RevisePath / DefineCPTSchedule / DeactivatePath therefore either
// committed first (its fresh state is what is republished) or waits and
// enqueues its own event AFTER the republished one; a stale snapshot can
// never overtake a newer event in the outbox. Publisher is the same
// transactional outbox the other use cases publish through.
type RepublishCatalogue struct {
	Repo         ports.ProcessPathRepo
	CPTSchedules ports.CPTScheduleRepo
	// Flows holds the ProcessFlow aggregates (ADR 0042). Optional: nil
	// republishes no flows, as in a use-case test that models none.
	Flows     ports.ProcessFlowRepo
	Publisher ports.EventPublisher
	Clock     ports.Clock
	// UnitOfWork brackets the whole republish atomically (ADR 0003); nil
	// means no transactional backing (see DefinePath).
	UnitOfWork ports.UnitOfWork
	// Metrics is optional: nil means "not instrumented".
	Metrics ports.CatalogueMetrics
}

// Execute enqueues the catalogue. It is all-or-nothing: on error nothing is
// enqueued, the error is returned (and counted) and the caller decides what
// a failure means for the process.
func (uc *RepublishCatalogue) Execute(ctx context.Context) (RepublishResult, error) {
	var res RepublishResult
	err := atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		res = RepublishResult{}
		now := uc.Clock.Now()
		if err := uc.republishPaths(ctx, now, &res); err != nil {
			return err
		}
		if err := uc.republishSchedules(ctx, now, &res); err != nil {
			return err
		}
		return uc.republishFlows(ctx, now, &res)
	})
	if err != nil {
		if uc.Metrics != nil {
			uc.Metrics.CatalogueRepublishFailed(ctx)
		}
		return RepublishResult{}, fmt.Errorf("republish catalogue: %w", err)
	}
	if uc.Metrics != nil {
		uc.Metrics.CatalogueRepublished(ctx, republishKindPath, res.Paths)
		uc.Metrics.CatalogueRepublished(ctx, republishKindSchedule, res.Schedules)
		if uc.Flows != nil {
			uc.Metrics.CatalogueRepublished(ctx, republishKindFlow, res.Flows)
		}
	}
	return res, nil
}

// republishPaths lists the ACTIVE paths, takes their share locks, and
// publishes the state read UNDER the lock (a path deactivated or revised
// since the unlocked listing is judged on its committed state).
func (uc *RepublishCatalogue) republishPaths(ctx context.Context, now time.Time, res *RepublishResult) error {
	listed, err := uc.Repo.ListActive(ctx)
	if err != nil {
		return err
	}
	ids := make([]shared.PathId, 0, len(listed))
	for _, p := range listed {
		ids = append(ids, p.ID())
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	ids, res.Omitted = capIDs(ids, res.Omitted)
	if len(ids) == 0 {
		return nil
	}
	locked, err := uc.Repo.LockByIDsForShare(ctx, ids)
	if err != nil {
		return err
	}
	active := make([]*processpath.ProcessPath, 0, len(locked))
	for _, p := range locked {
		if p != nil && p.IsActive() {
			active = append(active, p)
		}
	}
	sort.Slice(active, func(i, j int) bool { return active[i].ID() < active[j].ID() })
	for _, p := range active {
		if err := uc.Publisher.Publish(ctx, processPathUpdated(p, now)); err != nil {
			return err
		}
		res.Paths++
	}
	return nil
}

// republishSchedules share-locks every schedule and publishes each one's
// current snapshot, ascending by site.
func (uc *RepublishCatalogue) republishSchedules(ctx context.Context, now time.Time, res *RepublishResult) error {
	sites, err := uc.CPTSchedules.LockAllForShare(ctx)
	if err != nil {
		return err
	}
	sort.Slice(sites, func(i, j int) bool { return sites[i] < sites[j] })
	sites, res.Omitted = capIDs(sites, res.Omitted)
	for _, site := range sites {
		s, err := uc.CPTSchedules.FindBySiteID(ctx, site)
		if err != nil {
			return err
		}
		if s == nil {
			continue
		}
		if err := uc.Publisher.Publish(ctx, cptschedule.ToSnapshot(s, now)); err != nil {
			return err
		}
		res.Schedules++
	}
	return nil
}

// republishFlows share-locks every ACTIVE flow and publishes each one's
// current state as ProcessFlowRevised (ADR 0042), ascending by id. The
// version is the flow's current one, so a consumer that is caught up (it
// applies a message only when the version is greater) ignores it and a fresh
// one learns the flow. RETIRED flows are not republished, like deactivated
// paths.
func (uc *RepublishCatalogue) republishFlows(ctx context.Context, now time.Time, res *RepublishResult) error {
	if uc.Flows == nil {
		return nil
	}
	ids, err := uc.Flows.LockActiveForShare(ctx)
	if err != nil {
		return err
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	ids, res.Omitted = capIDs(ids, res.Omitted)
	for _, id := range ids {
		f, err := uc.Flows.FindByID(ctx, id)
		if err != nil {
			return err
		}
		if f == nil || !f.IsActive() {
			continue
		}
		if err := uc.Publisher.Publish(ctx, f.RevisedEvent(now)); err != nil {
			return err
		}
		res.Flows++
	}
	return nil
}

// capIDs truncates ids (already ascending) to MaxRepublishedPerKind and adds
// the number cut off to omitted.
func capIDs[T any](ids []T, omitted int) ([]T, int) {
	if len(ids) <= MaxRepublishedPerKind {
		return ids, omitted
	}
	return ids[:MaxRepublishedPerKind], omitted + len(ids) - MaxRepublishedPerKind
}
