package usecases_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/claudioed/process-path-management/internal/application/usecases"
	"github.com/claudioed/process-path-management/internal/domain/cptschedule"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// ADR 0028: DefineCPTSchedule locks every referenced path FOR SHARE, inside
// its unit of work, once each, BEFORE it writes the schedule.
func TestDefineCPTSchedule_LocksReferencedPathsForShareInsideUnitOfWork(t *testing.T) {
	pathRepo := newFakeRepo()
	for _, id := range []shared.PathId{"PACK", "PICK", "REBIN"} {
		seedActivePath(t, pathRepo, id)
	}
	scheduleRepo := newFakeCPTScheduleRepo()
	uc := &usecases.DefineCPTSchedule{Repo: scheduleRepo, ProcessPathRepo: pathRepo, Publisher: &fakePublisher{}, Clock: fixedClock{time.Now()}, UnitOfWork: recordingUoW{repo: pathRepo}}

	c1 := validCutoffFor(t, "REBIN", "PICK")
	c2, err := cptschedule.NewCutoff("sp1-1800", "18:00", []cptschedule.Weekday{cptschedule.Tuesday}, "same-day", []shared.PathId{"PICK", "PACK"})
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	if _, err := uc.Execute(context.Background(), "sp1", "America/Sao_Paulo", []cptschedule.Cutoff{c1, c2}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// One lock call, inside the unit of work, with each id exactly once in
	// ascending order (the stable lock order that rules out deadlocks).
	want := []string{"uow-begin", "for-share:PACK,PICK,REBIN", "uow-end"}
	if got := pathRepo.callLog(); !reflect.DeepEqual(got, want) {
		t.Fatalf("want lock calls %v, got %v", want, got)
	}
}

// The lock request must be sorted by the use case itself, not rely on the
// adapter: an unsorted caller-order request is the deadlock recipe.
func TestDefineCPTSchedule_LockRequestIsSortedAndDeduped(t *testing.T) {
	pathRepo := newFakeRepo()
	for _, id := range []shared.PathId{"PACK", "PICK"} {
		seedActivePath(t, pathRepo, id)
	}
	uc := &usecases.DefineCPTSchedule{Repo: newFakeCPTScheduleRepo(), ProcessPathRepo: pathRepo, Publisher: &fakePublisher{}, Clock: fixedClock{time.Now()}}

	if _, err := uc.Execute(context.Background(), "sp1", "America/Sao_Paulo", []cptschedule.Cutoff{validCutoffFor(t, "PICK", "PACK", "PICK")}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got, want := pathRepo.callLog(), []string{"for-share:PACK,PICK"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("want %v, got %v", want, got)
	}
}

// A deactivated path is found by the lock but refused: nothing written or
// published (the schedule never reaches Save).
func TestDefineCPTSchedule_LockedPathInactive_RejectsBeforeWriting(t *testing.T) {
	pathRepo := newFakeRepo()
	seedActivePath(t, pathRepo, "PICK")
	seedActivePath(t, pathRepo, "PACK")
	p, _ := pathRepo.FindByID(context.Background(), "PACK")
	p.Deactivate(time.Now())
	scheduleRepo := newFakeCPTScheduleRepo()
	pub := &fakePublisher{}
	uc := &usecases.DefineCPTSchedule{Repo: scheduleRepo, ProcessPathRepo: pathRepo, Publisher: pub, Clock: fixedClock{time.Now()}}

	_, err := uc.Execute(context.Background(), "sp1", "America/Sao_Paulo", []cptschedule.Cutoff{validCutoffFor(t, "PICK", "PACK")})
	if !errors.Is(err, usecases.ErrIneligiblePathId) {
		t.Fatalf("want ErrIneligiblePathId, got %v", err)
	}
	if s, _ := scheduleRepo.FindBySiteID(context.Background(), "sp1"); s != nil {
		t.Fatal("a rejected define must not persist the schedule")
	}
	if pub.count() != 0 {
		t.Fatalf("a rejected define must not publish, got %d events", pub.count())
	}
}

// An id that no row answers to (the lock returns fewer paths than asked)
// is ineligible, same as before.
func TestDefineCPTSchedule_LockedPathMissing_Rejects(t *testing.T) {
	pathRepo := newFakeRepo()
	seedActivePath(t, pathRepo, "PICK")
	uc := &usecases.DefineCPTSchedule{Repo: newFakeCPTScheduleRepo(), ProcessPathRepo: pathRepo, Publisher: &fakePublisher{}, Clock: fixedClock{time.Now()}}

	_, err := uc.Execute(context.Background(), "sp1", "America/Sao_Paulo", []cptschedule.Cutoff{validCutoffFor(t, "PICK", "GHOST")})
	if !errors.Is(err, usecases.ErrIneligiblePathId) {
		t.Fatalf("want ErrIneligiblePathId, got %v", err)
	}
}

// ADR 0028: DeactivatePath takes the path row FOR UPDATE inside the unit of
// work, and only then looks for referencing schedules.
func TestDeactivatePath_LocksPathForUpdateInsideUnitOfWork(t *testing.T) {
	pathRepo := newFakeRepo()
	seedActivePath(t, pathRepo, "PICK")
	pub := &fakePublisher{}
	uc := &usecases.DeactivatePath{Repo: pathRepo, Publisher: pub, Clock: fixedClock{time.Now()}, UnitOfWork: recordingUoW{repo: pathRepo}, CPTSchedules: newFakeCPTScheduleRepo()}

	if err := uc.Execute(context.Background(), "PICK"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"uow-begin", "for-update:PICK", "uow-end"}
	if got := pathRepo.callLog(); !reflect.DeepEqual(got, want) {
		t.Fatalf("want lock calls %v, got %v", want, got)
	}
	if pub.count() != 1 {
		t.Fatalf("want 1 ProcessPathDeactivated, got %d", pub.count())
	}
}

// Unknown path stays a 404 and an already-deactivated one an idempotent
// no-op, both decided on the locked row.
func TestDeactivatePath_UnknownAndAlreadyInactive_OnLockedRow(t *testing.T) {
	pathRepo := newFakeRepo()
	seedActivePath(t, pathRepo, "PICK")
	pub := &fakePublisher{}
	uc := &usecases.DeactivatePath{Repo: pathRepo, Publisher: pub, Clock: fixedClock{time.Now()}, CPTSchedules: newFakeCPTScheduleRepo()}

	if err := uc.Execute(context.Background(), "GHOST"); !errors.Is(err, usecases.ErrPathNotFound) {
		t.Fatalf("want ErrPathNotFound, got %v", err)
	}
	if err := uc.Execute(context.Background(), "PICK"); err != nil {
		t.Fatalf("first deactivate: %v", err)
	}
	if err := uc.Execute(context.Background(), "PICK"); err != nil {
		t.Fatalf("second deactivate must be an idempotent no-op: %v", err)
	}
	if pub.count() != 1 {
		t.Fatalf("want exactly 1 ProcessPathDeactivated, got %d", pub.count())
	}
}

func TestDeactivatePath_LockError_Propagates(t *testing.T) {
	wantErr := errors.New("boom: lock failed")
	pathRepo := &erroringRepo{fakeRepo: newFakeRepo(), findErr: wantErr}
	pub := &fakePublisher{}
	uc := &usecases.DeactivatePath{Repo: pathRepo, Publisher: pub, Clock: fixedClock{time.Now()}, CPTSchedules: newFakeCPTScheduleRepo()}

	if err := uc.Execute(context.Background(), "PICK"); !errors.Is(err, wantErr) {
		t.Fatalf("want the lock error propagated, got %v", err)
	}
	if pub.count() != 0 {
		t.Fatal("nothing may be published when the lock fails")
	}
}
