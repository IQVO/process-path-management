//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/claudioed/process-path-management/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/process-path-management/internal/application/usecases"
	"github.com/claudioed/process-path-management/internal/domain/cptschedule"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

func scheduleFor(t *testing.T, siteId shared.SiteId, cutoffs ...cptschedule.Cutoff) *cptschedule.CPTSchedule {
	t.Helper()
	s, err := cptschedule.Define(siteId, "America/Sao_Paulo", cutoffs, time.Now().UTC().Truncate(time.Microsecond))
	if err != nil {
		t.Fatalf("define schedule %s: %v", siteId, err)
	}
	return s
}

func cutoffNaming(t *testing.T, cptId string, pathIds ...shared.PathId) cptschedule.Cutoff {
	t.Helper()
	c, err := cptschedule.NewCutoff(cptId, "15:00", []cptschedule.Weekday{cptschedule.Monday}, "ground", pathIds)
	if err != nil {
		t.Fatalf("new cutoff %s: %v", cptId, err)
	}
	return c
}

// TestCPTScheduleRepo_ListSiteIDsReferencingPath proves the query behind
// ADR 0026 against real Postgres: it matches a path in ANY cutoff's
// eligible_path_ids, returns each site once (even when several cutoffs
// name the path) in ascending order, and returns nothing for a path no
// schedule names.
func TestCPTScheduleRepo_ListSiteIDsReferencingPath(t *testing.T) {
	pool := outboxDB(t)
	repo := postgres.NewCPTScheduleRepo(pool)
	ctx := context.Background()

	for _, s := range []*cptschedule.CPTSchedule{
		scheduleFor(t, "sp1", cutoffNaming(t, "sp1-1500", "PICK", "PACK"), cutoffNaming(t, "sp1-1800", "PICK")),
		scheduleFor(t, "rj1", cutoffNaming(t, "rj1-1500", "PACK")),
		scheduleFor(t, "ba1", cutoffNaming(t, "ba1-1500", "PICK")),
	} {
		if err := repo.Save(ctx, s); err != nil {
			t.Fatalf("save %s: %v", s.SiteId(), err)
		}
	}

	got, err := repo.ListSiteIDsReferencingPath(ctx, "PICK")
	if err != nil {
		t.Fatalf("list PICK: %v", err)
	}
	if len(got) != 2 || got[0] != "ba1" || got[1] != "sp1" {
		t.Fatalf("want [ba1 sp1] (each site once, ascending), got %v", got)
	}
	got, err = repo.ListSiteIDsReferencingPath(ctx, "REBIN")
	if err != nil || len(got) != 0 {
		t.Fatalf("an unreferenced path must list no sites, got %v err=%v", got, err)
	}
}

// TestDeactivatePath_ReferencedByCPTSchedule_IsRefusedWithNoOutboxRow
// runs the real use case over the real UnitOfWork + outbox: a referenced
// path is refused (ADR 0026) with no ProcessPathDeactivated outbox row
// and the row still ACTIVE; after the schedule drops the path the same
// call succeeds.
func TestDeactivatePath_ReferencedByCPTSchedule_IsRefusedWithNoOutboxRow(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	paths := postgres.NewProcessPathRepo(pool)
	schedules := postgres.NewCPTScheduleRepo(pool)
	pub := postgres.NewOutboxPublisher(pool, uuid.NewString)
	uow := postgres.NewUnitOfWork(pool)

	define := &usecases.DefinePath{Repo: paths, Publisher: pub, Clock: fixedClock{t: now}, UnitOfWork: uow}
	for _, id := range []shared.PathId{"PICK", "PACK"} {
		if _, err := define.Execute(ctx, id, "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, 2*time.Hour, shared.Eligibility{}); err != nil {
			t.Fatalf("define %s: %v", id, err)
		}
	}
	setSchedule := &usecases.DefineCPTSchedule{Repo: schedules, ProcessPathRepo: paths, Publisher: pub, Clock: fixedClock{t: now}, UnitOfWork: uow}
	if _, err := setSchedule.Execute(ctx, "sp1", "America/Sao_Paulo", []cptschedule.Cutoff{cutoffNaming(t, "sp1-1500", "PICK")}); err != nil {
		t.Fatalf("define schedule: %v", err)
	}

	deactivate := &usecases.DeactivatePath{Repo: paths, Publisher: pub, Clock: fixedClock{t: now.Add(time.Minute)}, UnitOfWork: uow, CPTSchedules: schedules}
	err := deactivate.Execute(ctx, "PICK")
	if !errors.Is(err, usecases.ErrPathReferencedByCPTSchedule) {
		t.Fatalf("want ErrPathReferencedByCPTSchedule, got %v", err)
	}
	if got := countOutbox(t, pool, "event_type = '"+cloudevents.TypeProcessPathDeactivated+"' AND aggregate_id = 'PICK'"); got != 0 {
		t.Fatalf("a refused deactivation must not enqueue ProcessPathDeactivated, got %d rows", got)
	}
	if p, err := paths.FindByID(ctx, "PICK"); err != nil || p == nil || !p.IsActive() {
		t.Fatalf("PICK must stay ACTIVE, got %v err=%v", p, err)
	}

	// The schedule moves to PACK only; now PICK can be retired.
	if _, err := setSchedule.Execute(ctx, "sp1", "America/Sao_Paulo", []cptschedule.Cutoff{cutoffNaming(t, "sp1-1500", "PACK")}); err != nil {
		t.Fatalf("revise schedule: %v", err)
	}
	if err := deactivate.Execute(ctx, "PICK"); err != nil {
		t.Fatalf("deactivate after the schedule dropped PICK: %v", err)
	}
	if got := countOutbox(t, pool, "event_type = '"+cloudevents.TypeProcessPathDeactivated+"' AND aggregate_id = 'PICK'"); got != 1 {
		t.Fatalf("want exactly 1 ProcessPathDeactivated for PICK, got %d", got)
	}
}
