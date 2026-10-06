package usecases_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/process-path-management/internal/application/usecases"
	"github.com/claudioed/process-path-management/internal/domain/cptschedule"
	"github.com/claudioed/process-path-management/internal/domain/processpath"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// definedSchedule defines sp's schedule naming pathIds through the real
// DefineCPTSchedule use case, so the fixtures obey the Active-path rule.
func definedSchedule(t *testing.T, pathRepo *fakeRepo, scheduleRepo *fakeCPTScheduleRepo, siteId shared.SiteId, pathIds ...shared.PathId) {
	t.Helper()
	uc := &usecases.DefineCPTSchedule{Repo: scheduleRepo, ProcessPathRepo: pathRepo, Publisher: &fakePublisher{}, Clock: fixedClock{time.Now()}}
	if _, err := uc.Execute(context.Background(), siteId, "America/Sao_Paulo", []cptschedule.Cutoff{validCutoffFor(t, pathIds...)}); err != nil {
		t.Fatalf("setup schedule %s: %v", siteId, err)
	}
}

// ADR 0026: a path any CPT schedule still lists cannot be deactivated;
// the aggregate stays Active and no ProcessPathDeactivated is published.
func TestDeactivatePath_ReferencedByCPTSchedule_IsRejectedAndChangesNothing(t *testing.T) {
	pathRepo := newFakeRepo()
	seedActivePath(t, pathRepo, "PICK")
	scheduleRepo := newFakeCPTScheduleRepo()
	definedSchedule(t, pathRepo, scheduleRepo, "sp1", "PICK")
	definedSchedule(t, pathRepo, scheduleRepo, "rj1", "PICK")
	pub := &fakePublisher{}
	uc := &usecases.DeactivatePath{Repo: pathRepo, Publisher: pub, Clock: fixedClock{time.Now()}, CPTSchedules: scheduleRepo}

	err := uc.Execute(context.Background(), "PICK")
	if !errors.Is(err, usecases.ErrPathReferencedByCPTSchedule) {
		t.Fatalf("want ErrPathReferencedByCPTSchedule, got %v", err)
	}
	if !strings.Contains(err.Error(), "[rj1 sp1]") {
		t.Fatalf("the error must name the referencing sites in order, got %q", err.Error())
	}
	if pub.count() != 0 {
		t.Fatalf("a rejected deactivation must not publish, got %d events", pub.count())
	}
	p, _ := pathRepo.FindByID(context.Background(), "PICK")
	if p == nil || p.Status() != processpath.StatusActive {
		t.Fatalf("the path must stay Active after a rejected deactivation, got %+v", p)
	}
}

// Once the schedule no longer lists the path (a PUT replaces cutoffs
// wholesale), the deactivation goes through and publishes.
func TestDeactivatePath_AfterScheduleDropsIt_Succeeds(t *testing.T) {
	pathRepo := newFakeRepo()
	seedActivePath(t, pathRepo, "PICK")
	seedActivePath(t, pathRepo, "PACK")
	scheduleRepo := newFakeCPTScheduleRepo()
	definedSchedule(t, pathRepo, scheduleRepo, "sp1", "PICK")
	pub := &fakePublisher{}
	uc := &usecases.DeactivatePath{Repo: pathRepo, Publisher: pub, Clock: fixedClock{time.Now()}, CPTSchedules: scheduleRepo}
	if err := uc.Execute(context.Background(), "PICK"); !errors.Is(err, usecases.ErrPathReferencedByCPTSchedule) {
		t.Fatalf("setup: want rejection while referenced, got %v", err)
	}

	// Revise sp1's schedule to name PACK only.
	revise := &usecases.DefineCPTSchedule{Repo: scheduleRepo, ProcessPathRepo: pathRepo, Publisher: &fakePublisher{}, Clock: fixedClock{time.Now().Add(time.Hour)}}
	if _, err := revise.Execute(context.Background(), "sp1", "America/Sao_Paulo", []cptschedule.Cutoff{validCutoffFor(t, "PACK")}); err != nil {
		t.Fatalf("revise schedule: %v", err)
	}

	if err := uc.Execute(context.Background(), "PICK"); err != nil {
		t.Fatalf("deactivation after the schedule dropped the path: %v", err)
	}
	if pub.count() != 1 {
		t.Fatalf("want 1 ProcessPathDeactivated, got %d", pub.count())
	}
}

// A path no schedule names is deactivated exactly as before, and a second
// deactivation of it stays an idempotent no-op.
func TestDeactivatePath_UnreferencedAndAlreadyDeactivated_Unchanged(t *testing.T) {
	pathRepo := newFakeRepo()
	seedActivePath(t, pathRepo, "PICK")
	seedActivePath(t, pathRepo, "PACK")
	scheduleRepo := newFakeCPTScheduleRepo()
	definedSchedule(t, pathRepo, scheduleRepo, "sp1", "PICK")
	pub := &fakePublisher{}
	uc := &usecases.DeactivatePath{Repo: pathRepo, Publisher: pub, Clock: fixedClock{time.Now()}, CPTSchedules: scheduleRepo}

	if err := uc.Execute(context.Background(), "PACK"); err != nil {
		t.Fatalf("deactivate unreferenced PACK: %v", err)
	}
	if err := uc.Execute(context.Background(), "PACK"); err != nil {
		t.Fatalf("second deactivate of PACK must stay idempotent: %v", err)
	}
	if pub.count() != 1 {
		t.Fatalf("want exactly 1 ProcessPathDeactivated, got %d", pub.count())
	}
}

func TestDeactivatePath_ScheduleLookupError_Propagates(t *testing.T) {
	pathRepo := newFakeRepo()
	seedActivePath(t, pathRepo, "PICK")
	wantErr := errors.New("db down")
	schedules := &erroringCPTScheduleRepo{fakeCPTScheduleRepo: newFakeCPTScheduleRepo(), listErr: wantErr}
	pub := &fakePublisher{}
	uc := &usecases.DeactivatePath{Repo: pathRepo, Publisher: pub, Clock: fixedClock{time.Now()}, CPTSchedules: schedules}

	if err := uc.Execute(context.Background(), "PICK"); !errors.Is(err, wantErr) {
		t.Fatalf("want the lookup error propagated, got %v", err)
	}
	if pub.count() != 0 {
		t.Fatal("nothing may be published when the schedule lookup fails")
	}
	if p, _ := pathRepo.FindByID(context.Background(), "PICK"); p == nil || !p.IsActive() {
		t.Fatal("the path must stay Active when the schedule lookup fails")
	}
}
