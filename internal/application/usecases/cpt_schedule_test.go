package usecases_test

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/claudioed/process-path-management/internal/application/usecases"
	"github.com/claudioed/process-path-management/internal/domain/cptschedule"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// fakeCPTScheduleRepo is a minimal in-memory ports.CPTScheduleRepo for
// use-case tests, mirroring fakeRepo's own convention.
type fakeCPTScheduleRepo struct {
	mu        sync.Mutex
	schedules map[shared.SiteId]*cptschedule.CPTSchedule
}

func newFakeCPTScheduleRepo() *fakeCPTScheduleRepo {
	return &fakeCPTScheduleRepo{schedules: make(map[shared.SiteId]*cptschedule.CPTSchedule)}
}

func (r *fakeCPTScheduleRepo) Save(_ context.Context, s *cptschedule.CPTSchedule) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.schedules[s.SiteId()] = s
	return nil
}

func (r *fakeCPTScheduleRepo) FindBySiteID(_ context.Context, siteId shared.SiteId) (*cptschedule.CPTSchedule, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.schedules[siteId], nil
}

func (r *fakeCPTScheduleRepo) ListSiteIDsReferencingPath(_ context.Context, id shared.PathId) ([]shared.SiteId, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var sites []shared.SiteId
	for siteId, s := range r.schedules {
		for _, c := range s.Cutoffs() {
			for _, p := range c.EligiblePathIds() {
				if p == id {
					sites = append(sites, siteId)
				}
			}
		}
	}
	sort.Slice(sites, func(i, j int) bool { return sites[i] < sites[j] })
	return sites, nil
}

// erroringCPTScheduleRepo forces every call to fail, so error-propagation
// branches are exercised.
type erroringCPTScheduleRepo struct {
	*fakeCPTScheduleRepo
	findErr error
	saveErr error
	listErr error
}

func (r *erroringCPTScheduleRepo) ListSiteIDsReferencingPath(ctx context.Context, id shared.PathId) ([]shared.SiteId, error) {
	if r.listErr != nil {
		return nil, r.listErr
	}
	return r.fakeCPTScheduleRepo.ListSiteIDsReferencingPath(ctx, id)
}

func (r *erroringCPTScheduleRepo) FindBySiteID(ctx context.Context, siteId shared.SiteId) (*cptschedule.CPTSchedule, error) {
	if r.findErr != nil {
		return nil, r.findErr
	}
	return r.fakeCPTScheduleRepo.FindBySiteID(ctx, siteId)
}

func (r *erroringCPTScheduleRepo) Save(ctx context.Context, s *cptschedule.CPTSchedule) error {
	if r.saveErr != nil {
		return r.saveErr
	}
	return r.fakeCPTScheduleRepo.Save(ctx, s)
}

func validCutoffFor(t *testing.T, pathIds ...shared.PathId) cptschedule.Cutoff {
	t.Helper()
	c, err := cptschedule.NewCutoff("sp1-1500", "15:00", []cptschedule.Weekday{cptschedule.Monday}, "ground", pathIds)
	if err != nil {
		t.Fatalf("unexpected error building a valid cutoff: %v", err)
	}
	return c
}

// seedActivePath defines an Active ProcessPath directly in repo, for
// tests that need one to exist before DefineCPTSchedule's cross-aggregate
// check will accept it.
func seedActivePath(t *testing.T, repo *fakeRepo, id shared.PathId) {
	t.Helper()
	define := &usecases.DefinePath{Repo: repo, Publisher: &fakePublisher{}, Clock: fixedClock{time.Now()}}
	if _, err := define.Execute(context.Background(), id, "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, 2*time.Hour, shared.Eligibility{}); err != nil {
		t.Fatalf("seedActivePath: %v", err)
	}
}

func TestDefineCPTSchedule_NewSite_PublishesChangedAndPersists(t *testing.T) {
	pathRepo := newFakeRepo()
	seedActivePath(t, pathRepo, "PICK")
	scheduleRepo := newFakeCPTScheduleRepo()
	pub := &fakePublisher{}
	now := time.Date(2026, 9, 13, 0, 0, 0, 0, time.UTC)
	uc := &usecases.DefineCPTSchedule{Repo: scheduleRepo, ProcessPathRepo: pathRepo, Publisher: pub, Clock: fixedClock{now}}

	c := validCutoffFor(t, "PICK")
	s, err := uc.Execute(context.Background(), "sp1", "America/Sao_Paulo", []cptschedule.Cutoff{c})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.SiteId() != "sp1" {
		t.Fatalf("unexpected siteId: %v", s.SiteId())
	}
	if pub.count() != 1 {
		t.Fatalf("want exactly 1 event published, got %d", pub.count())
	}
	changed, ok := pub.last().(cptschedule.CPTScheduleChanged)
	if !ok {
		t.Fatalf("want CPTScheduleChanged, got %T", pub.last())
	}
	if changed.SiteId != "sp1" || changed.At != now {
		t.Fatalf("unexpected event payload: %+v", changed)
	}

	persisted, _ := scheduleRepo.FindBySiteID(context.Background(), "sp1")
	if persisted == nil {
		t.Fatal("expected the schedule to be persisted")
	}
}

func TestDefineCPTSchedule_IneligiblePathId_RejectsWithoutPublishing(t *testing.T) {
	pathRepo := newFakeRepo()
	// PICK is never defined at all.
	scheduleRepo := newFakeCPTScheduleRepo()
	pub := &fakePublisher{}
	uc := &usecases.DefineCPTSchedule{Repo: scheduleRepo, ProcessPathRepo: pathRepo, Publisher: pub, Clock: fixedClock{time.Now()}}

	c := validCutoffFor(t, "PICK")
	_, err := uc.Execute(context.Background(), "sp1", "America/Sao_Paulo", []cptschedule.Cutoff{c})
	if !errors.Is(err, usecases.ErrIneligiblePathId) {
		t.Fatalf("want ErrIneligiblePathId, got %v", err)
	}
	if pub.count() != 0 {
		t.Fatal("expected no event published on a rejected definition")
	}
}

func TestDefineCPTSchedule_DeactivatedPathId_Rejects(t *testing.T) {
	pathRepo := newFakeRepo()
	seedActivePath(t, pathRepo, "PICK")
	deactivate := &usecases.DeactivatePath{Repo: pathRepo, Publisher: &fakePublisher{}, Clock: fixedClock{time.Now()}}
	if err := deactivate.Execute(context.Background(), "PICK"); err != nil {
		t.Fatalf("setup: %v", err)
	}

	scheduleRepo := newFakeCPTScheduleRepo()
	uc := &usecases.DefineCPTSchedule{Repo: scheduleRepo, ProcessPathRepo: pathRepo, Publisher: &fakePublisher{}, Clock: fixedClock{time.Now()}}

	c := validCutoffFor(t, "PICK")
	_, err := uc.Execute(context.Background(), "sp1", "America/Sao_Paulo", []cptschedule.Cutoff{c})
	if !errors.Is(err, usecases.ErrIneligiblePathId) {
		t.Fatalf("want ErrIneligiblePathId for a deactivated path, got %v", err)
	}
}

func TestDefineCPTSchedule_ExistingSite_Revises(t *testing.T) {
	pathRepo := newFakeRepo()
	seedActivePath(t, pathRepo, "PICK")
	seedActivePath(t, pathRepo, "PACK")
	scheduleRepo := newFakeCPTScheduleRepo()
	pub := &fakePublisher{}
	now := time.Now()
	uc := &usecases.DefineCPTSchedule{Repo: scheduleRepo, ProcessPathRepo: pathRepo, Publisher: pub, Clock: fixedClock{now}}

	c1 := validCutoffFor(t, "PICK")
	if _, err := uc.Execute(context.Background(), "sp1", "America/Sao_Paulo", []cptschedule.Cutoff{c1}); err != nil {
		t.Fatalf("setup: %v", err)
	}

	uc.Clock = fixedClock{now.Add(time.Hour)}
	c2, err := cptschedule.NewCutoff("sp1-1800", "18:00", []cptschedule.Weekday{cptschedule.Tuesday}, "same-day", []shared.PathId{"PACK"})
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	s, err := uc.Execute(context.Background(), "sp1", "America/New_York", []cptschedule.Cutoff{c1, c2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.Timezone() != "America/New_York" || len(s.Cutoffs()) != 2 {
		t.Fatalf("unexpected revised schedule: %+v", s)
	}
	if pub.count() != 2 {
		t.Fatalf("want 2 events total (define + revise), got %d", pub.count())
	}
}

func TestDefineCPTSchedule_ExistingSite_NoOpRevision_DoesNotPublish(t *testing.T) {
	pathRepo := newFakeRepo()
	seedActivePath(t, pathRepo, "PICK")
	scheduleRepo := newFakeCPTScheduleRepo()
	pub := &fakePublisher{}
	now := time.Now()
	uc := &usecases.DefineCPTSchedule{Repo: scheduleRepo, ProcessPathRepo: pathRepo, Publisher: pub, Clock: fixedClock{now}}

	c := validCutoffFor(t, "PICK")
	if _, err := uc.Execute(context.Background(), "sp1", "America/Sao_Paulo", []cptschedule.Cutoff{c}); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if _, err := uc.Execute(context.Background(), "sp1", "America/Sao_Paulo", []cptschedule.Cutoff{c}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pub.count() != 1 {
		t.Fatalf("want no second event published for a no-op revision, got %d total", pub.count())
	}
}

func TestDefineCPTSchedule_InvalidDomainInput_Rejected(t *testing.T) {
	pathRepo := newFakeRepo()
	seedActivePath(t, pathRepo, "PICK")
	scheduleRepo := newFakeCPTScheduleRepo()
	uc := &usecases.DefineCPTSchedule{Repo: scheduleRepo, ProcessPathRepo: pathRepo, Publisher: &fakePublisher{}, Clock: fixedClock{time.Now()}}

	c := validCutoffFor(t, "PICK")
	if _, err := uc.Execute(context.Background(), "sp1", "", []cptschedule.Cutoff{c}); !errors.Is(err, cptschedule.ErrEmptyTimezone) {
		t.Fatalf("want ErrEmptyTimezone, got %v", err)
	}
}

func TestDefineCPTSchedule_ScheduleRepoFindError_Propagates(t *testing.T) {
	pathRepo := newFakeRepo()
	seedActivePath(t, pathRepo, "PICK")
	wantErr := errors.New("boom: repo unreachable")
	scheduleRepo := &erroringCPTScheduleRepo{fakeCPTScheduleRepo: newFakeCPTScheduleRepo(), findErr: wantErr}
	uc := &usecases.DefineCPTSchedule{Repo: scheduleRepo, ProcessPathRepo: pathRepo, Publisher: &fakePublisher{}, Clock: fixedClock{time.Now()}}

	c := validCutoffFor(t, "PICK")
	_, err := uc.Execute(context.Background(), "sp1", "America/Sao_Paulo", []cptschedule.Cutoff{c})
	if !errors.Is(err, wantErr) {
		t.Fatalf("want the repo error propagated, got %v", err)
	}
}

func TestDefineCPTSchedule_ScheduleRepoSaveError_Propagates(t *testing.T) {
	pathRepo := newFakeRepo()
	seedActivePath(t, pathRepo, "PICK")
	wantErr := errors.New("boom: save failed")
	scheduleRepo := &erroringCPTScheduleRepo{fakeCPTScheduleRepo: newFakeCPTScheduleRepo(), saveErr: wantErr}
	uc := &usecases.DefineCPTSchedule{Repo: scheduleRepo, ProcessPathRepo: pathRepo, Publisher: &fakePublisher{}, Clock: fixedClock{time.Now()}}

	c := validCutoffFor(t, "PICK")
	_, err := uc.Execute(context.Background(), "sp1", "America/Sao_Paulo", []cptschedule.Cutoff{c})
	if !errors.Is(err, wantErr) {
		t.Fatalf("want the save error propagated, got %v", err)
	}
}

func TestDefineCPTSchedule_PublishError_Propagates(t *testing.T) {
	pathRepo := newFakeRepo()
	seedActivePath(t, pathRepo, "PICK")
	scheduleRepo := newFakeCPTScheduleRepo()
	wantErr := errors.New("boom: publish failed")
	uc := &usecases.DefineCPTSchedule{Repo: scheduleRepo, ProcessPathRepo: pathRepo, Publisher: &erroringPublisher{err: wantErr}, Clock: fixedClock{time.Now()}}

	c := validCutoffFor(t, "PICK")
	_, err := uc.Execute(context.Background(), "sp1", "America/Sao_Paulo", []cptschedule.Cutoff{c})
	if !errors.Is(err, wantErr) {
		t.Fatalf("want the publish error propagated, got %v", err)
	}
}

func TestDefineCPTSchedule_ProcessPathRepoError_Propagates(t *testing.T) {
	wantErr := errors.New("boom: process path repo unreachable")
	pathRepo := &erroringRepo{fakeRepo: newFakeRepo(), findErr: wantErr}
	scheduleRepo := newFakeCPTScheduleRepo()
	uc := &usecases.DefineCPTSchedule{Repo: scheduleRepo, ProcessPathRepo: pathRepo, Publisher: &fakePublisher{}, Clock: fixedClock{time.Now()}}

	c := validCutoffFor(t, "PICK")
	_, err := uc.Execute(context.Background(), "sp1", "America/Sao_Paulo", []cptschedule.Cutoff{c})
	if !errors.Is(err, wantErr) {
		t.Fatalf("want the process path repo error propagated, got %v", err)
	}
}

func TestDefineCPTSchedule_DedupesEligiblePathIdLookups(t *testing.T) {
	// A PathId referenced by TWO cutoffs must only be looked up once --
	// exercised indirectly here via a schedule with two cutoffs sharing
	// PICK, proving no duplicate-lookup panics/errors occur and the
	// schedule is accepted.
	pathRepo := newFakeRepo()
	seedActivePath(t, pathRepo, "PICK")
	scheduleRepo := newFakeCPTScheduleRepo()
	uc := &usecases.DefineCPTSchedule{Repo: scheduleRepo, ProcessPathRepo: pathRepo, Publisher: &fakePublisher{}, Clock: fixedClock{time.Now()}}

	c1 := validCutoffFor(t, "PICK")
	c2, err := cptschedule.NewCutoff("sp1-1800", "18:00", []cptschedule.Weekday{cptschedule.Tuesday}, "same-day", []shared.PathId{"PICK"})
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	if _, err := uc.Execute(context.Background(), "sp1", "America/Sao_Paulo", []cptschedule.Cutoff{c1, c2}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGetCPTSchedule_NotFound_ReturnsErrCPTScheduleNotFound(t *testing.T) {
	scheduleRepo := newFakeCPTScheduleRepo()
	uc := &usecases.GetCPTSchedule{Repo: scheduleRepo}
	_, err := uc.Execute(context.Background(), "sp1")
	if !errors.Is(err, usecases.ErrCPTScheduleNotFound) {
		t.Fatalf("want ErrCPTScheduleNotFound, got %v", err)
	}
}

func TestGetCPTSchedule_Found_ReturnsIt(t *testing.T) {
	pathRepo := newFakeRepo()
	seedActivePath(t, pathRepo, "PICK")
	scheduleRepo := newFakeCPTScheduleRepo()
	define := &usecases.DefineCPTSchedule{Repo: scheduleRepo, ProcessPathRepo: pathRepo, Publisher: &fakePublisher{}, Clock: fixedClock{time.Now()}}
	c := validCutoffFor(t, "PICK")
	if _, err := define.Execute(context.Background(), "sp1", "America/Sao_Paulo", []cptschedule.Cutoff{c}); err != nil {
		t.Fatalf("setup: %v", err)
	}

	get := &usecases.GetCPTSchedule{Repo: scheduleRepo}
	s, err := get.Execute(context.Background(), "sp1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.SiteId() != "sp1" {
		t.Fatalf("unexpected siteId: %v", s.SiteId())
	}
}

func TestGetCPTSchedule_RepoError_Propagates(t *testing.T) {
	wantErr := errors.New("boom: repo unreachable")
	scheduleRepo := &erroringCPTScheduleRepo{fakeCPTScheduleRepo: newFakeCPTScheduleRepo(), findErr: wantErr}
	uc := &usecases.GetCPTSchedule{Repo: scheduleRepo}
	_, err := uc.Execute(context.Background(), "sp1")
	if !errors.Is(err, wantErr) {
		t.Fatalf("want the repo error propagated, got %v", err)
	}
}
