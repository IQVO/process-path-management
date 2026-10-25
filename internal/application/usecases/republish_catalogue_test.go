package usecases_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/process-path-management/internal/application/usecases"
	"github.com/claudioed/process-path-management/internal/domain/cptschedule"
	"github.com/claudioed/process-path-management/internal/domain/processpath"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

var republishNow = time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)

// republishMetrics records the CatalogueMetrics calls.
type republishMetrics struct {
	published map[string]int
	failed    int
}

func (m *republishMetrics) CatalogueRepublished(_ context.Context, kind string, n int) {
	if m.published == nil {
		m.published = map[string]int{}
	}
	m.published[kind] += n
}
func (m *republishMetrics) CatalogueRepublishFailed(context.Context) { m.failed++ }

// seedPathWith defines an Active path with a distinctive state, so a test can
// tell the republished payload carries the FULL current state.
func seedPathWith(t *testing.T, repo *fakeRepo, id shared.PathId, cycle time.Duration) {
	t.Helper()
	define := &usecases.DefinePath{Repo: repo, Publisher: &fakePublisher{}, Clock: fixedClock{republishNow.Add(-72 * time.Hour)}}
	_, err := define.Execute(context.Background(), id, strings.ToLower(string(id))+"-prefix", true, []shared.Capability{"pick", "pack"}, shared.DestinationLocationRoleUnset, cycle, shared.Eligibility{}, shared.Intake{})
	if err != nil {
		t.Fatalf("seed %s: %v", id, err)
	}
}

func seedSchedule(t *testing.T, repo *fakeCPTScheduleRepo, site shared.SiteId, paths ...shared.PathId) {
	t.Helper()
	c, err := cptschedule.NewCutoff(string(site)+"-1500", "15:00", []cptschedule.Weekday{cptschedule.Monday}, "ground", paths)
	if err != nil {
		t.Fatalf("cutoff: %v", err)
	}
	s, err := cptschedule.Define(site, "America/Sao_Paulo", []cptschedule.Cutoff{c}, republishNow.Add(-72*time.Hour))
	if err != nil {
		t.Fatalf("schedule: %v", err)
	}
	if err := repo.Save(context.Background(), s); err != nil {
		t.Fatalf("save schedule: %v", err)
	}
}

func newRepublish(paths *fakeRepo, schedules *fakeCPTScheduleRepo, pub *fakePublisher) *usecases.RepublishCatalogue {
	return &usecases.RepublishCatalogue{Repo: paths, CPTSchedules: schedules, Publisher: pub, Clock: fixedClock{republishNow}}
}

func TestRepublishCatalogue_PublishesEveryActivePathThenEverySchedule_Sorted(t *testing.T) {
	paths, schedules, pub := newFakeRepo(), newFakeCPTScheduleRepo(), &fakePublisher{}
	for _, id := range []shared.PathId{"REBIN", "PICK", "PACK"} {
		seedPathWith(t, paths, id, 90*time.Minute)
	}
	seedSchedule(t, schedules, "sp1", "PICK")
	seedSchedule(t, schedules, "ba1", "PACK")

	res, err := newRepublish(paths, schedules, pub).Execute(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Paths != 3 || res.Schedules != 2 || res.Omitted != 0 {
		t.Fatalf("result = %+v, want 3 paths, 2 schedules, 0 omitted", res)
	}

	var got []string
	for _, e := range pub.events {
		switch ev := e.(type) {
		case shared.ProcessPathUpdated:
			got = append(got, "path:"+string(ev.PathId))
		case cptschedule.CPTScheduleChanged:
			got = append(got, "schedule:"+string(ev.SiteId))
		default:
			t.Fatalf("unexpected event %T", e)
		}
	}
	want := []string{"path:PACK", "path:PICK", "path:REBIN", "schedule:ba1", "schedule:sp1"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("publish order = %v, want %v", got, want)
	}
}

// The republished event is the exact payload RevisePath builds: the path's
// FULL current state, stamped with the republish time.
func TestRepublishCatalogue_PathEventCarriesFullCurrentState(t *testing.T) {
	paths, schedules, pub := newFakeRepo(), newFakeCPTScheduleRepo(), &fakePublisher{}
	seedPathWith(t, paths, "PICK", 90*time.Minute)
	p, _ := paths.FindByID(context.Background(), "PICK")

	if _, err := newRepublish(paths, schedules, pub).Execute(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := shared.ProcessPathUpdated{
		PathId: "PICK", MatchPrefix: "pick-prefix", Direct: true,
		RequiredCapabilities: p.RequiredCapabilities(), DestinationLocationRole: p.DestinationLocationRole(),
		CycleTimeP95: 90 * time.Minute, Eligibility: p.Eligibility(), Intake: p.Intake(), At: republishNow,
	}
	if got, ok := pub.last().(shared.ProcessPathUpdated); !ok || !reflect.DeepEqual(got, want) {
		t.Fatalf("event = %#v, want %#v", pub.last(), want)
	}
}

func TestRepublishCatalogue_ScheduleEventCarriesCurrentCutoffs(t *testing.T) {
	paths, schedules, pub := newFakeRepo(), newFakeCPTScheduleRepo(), &fakePublisher{}
	seedSchedule(t, schedules, "sp1", "PICK")

	if _, err := newRepublish(paths, schedules, pub).Execute(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ev, ok := pub.last().(cptschedule.CPTScheduleChanged)
	if !ok {
		t.Fatalf("last event = %T, want CPTScheduleChanged", pub.last())
	}
	if ev.SiteId != "sp1" || ev.Timezone != "America/Sao_Paulo" || !ev.At.Equal(republishNow) {
		t.Fatalf("event header = %+v", ev)
	}
	if len(ev.Cutoffs) != 1 || ev.Cutoffs[0].CptId != "sp1-1500" || ev.Cutoffs[0].LocalTime != "15:00" ||
		ev.Cutoffs[0].ShipMethod != "ground" || !reflect.DeepEqual(ev.Cutoffs[0].EligiblePathIds, []shared.PathId{"PICK"}) {
		t.Fatalf("cutoffs = %+v", ev.Cutoffs)
	}
}

func TestRepublishCatalogue_DeactivatedPathsAreNotRepublished(t *testing.T) {
	paths, schedules, pub := newFakeRepo(), newFakeCPTScheduleRepo(), &fakePublisher{}
	seedPathWith(t, paths, "PICK", time.Hour)
	seedPathWith(t, paths, "SLAM", time.Hour)
	dead, _ := paths.FindByID(context.Background(), "SLAM")
	dead.Deactivate(republishNow)

	res, err := newRepublish(paths, schedules, pub).Execute(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Paths != 1 || pub.count() != 1 || pub.last().(shared.ProcessPathUpdated).PathId != "PICK" {
		t.Fatalf("result %+v events %v, want only PICK", res, pub.events)
	}
}

// deactivatingRepo deactivates victim just before the share lock is taken,
// i.e. after the unlocked listing: the path must be judged on its committed
// state at lock time and skipped.
type deactivatingRepo struct {
	*fakeRepo
	victim shared.PathId
}

func (r *deactivatingRepo) LockByIDsForShare(ctx context.Context, ids []shared.PathId) ([]*processpath.ProcessPath, error) {
	if p, _ := r.fakeRepo.FindByID(ctx, r.victim); p != nil {
		p.Deactivate(republishNow)
	}
	return r.fakeRepo.LockByIDsForShare(ctx, ids)
}

func TestRepublishCatalogue_PathDeactivatedBeforeLockIsSkipped(t *testing.T) {
	base, schedules, pub := newFakeRepo(), newFakeCPTScheduleRepo(), &fakePublisher{}
	seedPathWith(t, base, "PICK", time.Hour)
	seedPathWith(t, base, "PACK", time.Hour)
	uc := newRepublish(base, schedules, pub)
	uc.Repo = &deactivatingRepo{fakeRepo: base, victim: "PACK"}

	res, err := uc.Execute(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Paths != 1 || pub.last().(shared.ProcessPathUpdated).PathId != "PICK" {
		t.Fatalf("result %+v, want only PICK republished", res)
	}
}

func TestRepublishCatalogue_EmptyCatalogueIsANoOp(t *testing.T) {
	paths, schedules, pub := newFakeRepo(), newFakeCPTScheduleRepo(), &fakePublisher{}
	res, err := newRepublish(paths, schedules, pub).Execute(context.Background())
	if err != nil || res != (usecases.RepublishResult{}) || pub.count() != 0 {
		t.Fatalf("res %+v err %v events %d, want nothing", res, err, pub.count())
	}
	if len(paths.callLog()) != 0 {
		t.Fatalf("no ids to lock, yet repo was asked to lock: %v", paths.callLog())
	}
}

// Every path is share-locked in ONE request, ascending, inside the unit of
// work, so a concurrent revise cannot be overtaken by the republished state.
func TestRepublishCatalogue_LocksPathsForShareInsideUnitOfWork(t *testing.T) {
	paths, schedules, pub := newFakeRepo(), newFakeCPTScheduleRepo(), &fakePublisher{}
	for _, id := range []shared.PathId{"REBIN", "PICK", "PACK"} {
		seedPathWith(t, paths, id, time.Hour)
	}
	uc := newRepublish(paths, schedules, pub)
	uc.UnitOfWork = recordingUoW{repo: paths}

	if _, err := uc.Execute(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"uow-begin", "for-share:PACK,PICK,REBIN", "uow-end"}
	if got := paths.callLog(); !reflect.DeepEqual(got, want) {
		t.Fatalf("calls = %v, want %v", got, want)
	}
}

func TestRepublishCatalogue_ErrorsAreSurfacedAndCounted(t *testing.T) {
	boom := errors.New("boom")
	tests := []struct {
		name  string
		build func(t *testing.T) *usecases.RepublishCatalogue
	}{
		{"list active paths", func(t *testing.T) *usecases.RepublishCatalogue {
			uc := newRepublish(newFakeRepo(), newFakeCPTScheduleRepo(), &fakePublisher{})
			uc.Repo = &erroringRepo{fakeRepo: newFakeRepo(), listErr: boom}
			return uc
		}},
		{"lock paths", func(t *testing.T) *usecases.RepublishCatalogue {
			base := newFakeRepo()
			seedPathWith(t, base, "PICK", time.Hour)
			uc := newRepublish(base, newFakeCPTScheduleRepo(), &fakePublisher{})
			uc.Repo = &erroringRepo{fakeRepo: base, findErr: boom}
			return uc
		}},
		{"lock schedules", func(t *testing.T) *usecases.RepublishCatalogue {
			uc := newRepublish(newFakeRepo(), newFakeCPTScheduleRepo(), &fakePublisher{})
			uc.CPTSchedules = &erroringCPTScheduleRepo{fakeCPTScheduleRepo: newFakeCPTScheduleRepo(), listErr: boom}
			return uc
		}},
		{"load schedule", func(t *testing.T) *usecases.RepublishCatalogue {
			sched := newFakeCPTScheduleRepo()
			seedSchedule(t, sched, "sp1", "PICK")
			uc := newRepublish(newFakeRepo(), sched, &fakePublisher{})
			uc.CPTSchedules = &erroringCPTScheduleRepo{fakeCPTScheduleRepo: sched, findErr: boom}
			return uc
		}},
		{"publish a path", func(t *testing.T) *usecases.RepublishCatalogue {
			base := newFakeRepo()
			seedPathWith(t, base, "PICK", time.Hour)
			uc := newRepublish(base, newFakeCPTScheduleRepo(), &fakePublisher{})
			uc.Publisher = &erroringPublisher{err: boom}
			return uc
		}},
		{"publish a schedule", func(t *testing.T) *usecases.RepublishCatalogue {
			sched := newFakeCPTScheduleRepo()
			seedSchedule(t, sched, "sp1", "PICK")
			uc := newRepublish(newFakeRepo(), sched, &fakePublisher{})
			uc.Publisher = &erroringPublisher{err: boom}
			return uc
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := &republishMetrics{}
			uc := tc.build(t)
			uc.Metrics = m
			res, err := uc.Execute(context.Background())
			if !errors.Is(err, boom) {
				t.Fatalf("err = %v, want it to wrap %v", err, boom)
			}
			if res != (usecases.RepublishResult{}) {
				t.Fatalf("result on failure = %+v, want zero", res)
			}
			if m.failed != 1 || len(m.published) != 0 {
				t.Fatalf("metrics failed=%d published=%v, want one failure and no published", m.failed, m.published)
			}
		})
	}
}

func TestRepublishCatalogue_RecordsPublishedCountsPerKind(t *testing.T) {
	paths, schedules, pub := newFakeRepo(), newFakeCPTScheduleRepo(), &fakePublisher{}
	seedPathWith(t, paths, "PICK", time.Hour)
	seedPathWith(t, paths, "PACK", time.Hour)
	seedSchedule(t, schedules, "sp1", "PICK")
	m := &republishMetrics{}
	uc := newRepublish(paths, schedules, pub)
	uc.Metrics = m

	if _, err := uc.Execute(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m.published["path"] != 2 || m.published["schedule"] != 1 || m.failed != 0 {
		t.Fatalf("metrics = %+v", m)
	}
}

// Bounded size: exactly MaxRepublishedPerKind is fully republished; one more
// is cut off and reported in Omitted (never dropped silently).
func TestRepublishCatalogue_BoundIsExactAndOverflowIsReported(t *testing.T) {
	for _, tc := range []struct {
		extra       int
		wantOmitted int
	}{
		{0, 0},
		{1, 1},
	} {
		t.Run(fmt.Sprintf("max+%d", tc.extra), func(t *testing.T) {
			paths, schedules, pub := newFakeRepo(), newFakeCPTScheduleRepo(), &fakePublisher{}
			for i := 0; i < usecases.MaxRepublishedPerKind+tc.extra; i++ {
				seedPathWith(t, paths, shared.PathId(fmt.Sprintf("P%05d", i)), time.Hour)
			}
			res, err := newRepublish(paths, schedules, pub).Execute(context.Background())
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if res.Paths != usecases.MaxRepublishedPerKind || res.Omitted != tc.wantOmitted || pub.count() != usecases.MaxRepublishedPerKind {
				t.Fatalf("result %+v events %d, want %d paths and %d omitted", res, pub.count(), usecases.MaxRepublishedPerKind, tc.wantOmitted)
			}
			// The cut keeps the lowest ids (deterministic), dropping the tail.
			last := pub.last().(shared.ProcessPathUpdated).PathId
			if want := shared.PathId(fmt.Sprintf("P%05d", usecases.MaxRepublishedPerKind-1)); last != want {
				t.Fatalf("last republished = %s, want %s", last, want)
			}
		})
	}
}

func TestRepublishCatalogue_ScheduleOverflowIsReported(t *testing.T) {
	paths, schedules, pub := newFakeRepo(), newFakeCPTScheduleRepo(), &fakePublisher{}
	for i := 0; i < usecases.MaxRepublishedPerKind+1; i++ {
		seedSchedule(t, schedules, shared.SiteId(fmt.Sprintf("s%05d", i)), "PICK")
	}
	res, err := newRepublish(paths, schedules, pub).Execute(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Schedules != usecases.MaxRepublishedPerKind || res.Omitted != 1 {
		t.Fatalf("result %+v, want %d schedules and 1 omitted", res, usecases.MaxRepublishedPerKind)
	}
}
