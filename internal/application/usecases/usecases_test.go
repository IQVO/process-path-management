package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/process-path-management/internal/application/usecases"
	"github.com/claudioed/process-path-management/internal/domain/processpath"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

const testCycleTimeP95 = 2 * time.Hour

func TestDefinePath_NewId_PublishesCreatedAndPersists(t *testing.T) {
	repo := newFakeRepo()
	pub := &fakePublisher{}
	now := time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)
	uc := &usecases.DefinePath{Repo: repo, Publisher: pub, Clock: fixedClock{now}}

	p, err := uc.Execute(context.Background(), "PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, shared.Intake{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.ID() != "PICK" {
		t.Fatalf("unexpected id: %v", p.ID())
	}
	if pub.count() != 1 {
		t.Fatalf("want exactly 1 event published, got %d", pub.count())
	}
	created, ok := pub.last().(shared.ProcessPathCreated)
	if !ok {
		t.Fatalf("want ProcessPathCreated, got %T", pub.last())
	}
	if created.PathId != "PICK" || created.At != now {
		t.Fatalf("unexpected event payload: %+v", created)
	}

	persisted, _ := repo.FindByID(context.Background(), "PICK")
	if persisted == nil {
		t.Fatal("expected the path to be persisted")
	}
}

func TestDefinePath_DuplicateId_RejectsWithoutPublishing(t *testing.T) {
	repo := newFakeRepo()
	pub := &fakePublisher{}
	uc := &usecases.DefinePath{Repo: repo, Publisher: pub, Clock: fixedClock{time.Now()}}
	ctx := context.Background()

	if _, err := uc.Execute(ctx, "PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, shared.Intake{}); err != nil {
		t.Fatalf("unexpected error on first define: %v", err)
	}
	_, err := uc.Execute(ctx, "PICK", "pick-v2", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, shared.Intake{})
	if !errors.Is(err, usecases.ErrPathAlreadyExists) {
		t.Fatalf("want ErrPathAlreadyExists, got %v", err)
	}
	if pub.count() != 1 {
		t.Fatalf("want no second event published, got %d total", pub.count())
	}
}

func TestDefinePath_DeactivatedIdIsStillTaken(t *testing.T) {
	repo := newFakeRepo()
	pub := &fakePublisher{}
	now := time.Now()
	uc := &usecases.DefinePath{Repo: repo, Publisher: pub, Clock: fixedClock{now}}
	deactivate := &usecases.DeactivatePath{Repo: repo, Publisher: pub, Clock: fixedClock{now}}
	ctx := context.Background()

	if _, err := uc.Execute(ctx, "PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, shared.Intake{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := deactivate.Execute(ctx, "PICK"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_, err := uc.Execute(ctx, "PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, shared.Intake{})
	if !errors.Is(err, usecases.ErrPathAlreadyExists) {
		t.Fatalf("want ErrPathAlreadyExists even for a deactivated id, got %v", err)
	}
}

func TestRevisePath_NotFound_ReturnsErrPathNotFound(t *testing.T) {
	repo := newFakeRepo()
	pub := &fakePublisher{}
	uc := &usecases.RevisePath{Repo: repo, Publisher: pub, Clock: fixedClock{time.Now()}}
	_, err := uc.Execute(context.Background(), "MISSING", "x", []shared.Capability{"x"}, testCycleTimeP95, shared.Eligibility{}, nil)
	if !errors.Is(err, usecases.ErrPathNotFound) {
		t.Fatalf("want ErrPathNotFound, got %v", err)
	}
}

func TestRevisePath_ActualChange_PublishesUpdated(t *testing.T) {
	repo := newFakeRepo()
	pub := &fakePublisher{}
	now := time.Now()
	define := &usecases.DefinePath{Repo: repo, Publisher: pub, Clock: fixedClock{now}}
	revise := &usecases.RevisePath{Repo: repo, Publisher: pub, Clock: fixedClock{now.Add(time.Hour)}}
	ctx := context.Background()

	if _, err := define.Execute(ctx, "PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, shared.Intake{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	p, err := revise.Execute(ctx, "PICK", "pick-zone-a", []shared.Capability{"pick", "hazmat"}, testCycleTimeP95, shared.Eligibility{}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.MatchPrefix() != "pick-zone-a" {
		t.Fatalf("unexpected matchPrefix: %s", p.MatchPrefix())
	}
	if pub.count() != 2 {
		t.Fatalf("want 2 events total (Created + Updated), got %d", pub.count())
	}
	if _, ok := pub.last().(shared.ProcessPathUpdated); !ok {
		t.Fatalf("want last event to be ProcessPathUpdated, got %T", pub.last())
	}
}

func TestRevisePath_NoOpRevision_DoesNotPublish(t *testing.T) {
	repo := newFakeRepo()
	pub := &fakePublisher{}
	now := time.Now()
	define := &usecases.DefinePath{Repo: repo, Publisher: pub, Clock: fixedClock{now}}
	revise := &usecases.RevisePath{Repo: repo, Publisher: pub, Clock: fixedClock{now.Add(time.Hour)}}
	ctx := context.Background()

	if _, err := define.Execute(ctx, "PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, shared.Intake{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := revise.Execute(ctx, "PICK", "pick", []shared.Capability{"pick"}, testCycleTimeP95, shared.Eligibility{}, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pub.count() != 1 {
		t.Fatalf("want no second event published for a no-op revision, got %d total", pub.count())
	}
}

func TestRevisePath_OnDeactivatedPath_Rejects(t *testing.T) {
	repo := newFakeRepo()
	pub := &fakePublisher{}
	now := time.Now()
	define := &usecases.DefinePath{Repo: repo, Publisher: pub, Clock: fixedClock{now}}
	deactivate := &usecases.DeactivatePath{Repo: repo, Publisher: pub, Clock: fixedClock{now}}
	revise := &usecases.RevisePath{Repo: repo, Publisher: pub, Clock: fixedClock{now}}
	ctx := context.Background()

	if _, err := define.Execute(ctx, "PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, shared.Intake{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := deactivate.Execute(ctx, "PICK"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := revise.Execute(ctx, "PICK", "pick-v2", []shared.Capability{"pick"}, testCycleTimeP95, shared.Eligibility{}, nil); !errors.Is(err, processpath.ErrPathDeactivated) {
		t.Fatalf("want ErrPathDeactivated, got %v", err)
	}
}

func TestDeactivatePath_NotFound_ReturnsErrPathNotFound(t *testing.T) {
	repo := newFakeRepo()
	pub := &fakePublisher{}
	uc := &usecases.DeactivatePath{Repo: repo, Publisher: pub, Clock: fixedClock{time.Now()}}
	err := uc.Execute(context.Background(), "MISSING")
	if !errors.Is(err, usecases.ErrPathNotFound) {
		t.Fatalf("want ErrPathNotFound, got %v", err)
	}
}

func TestDeactivatePath_Idempotent_DoesNotDoublePublish(t *testing.T) {
	repo := newFakeRepo()
	pub := &fakePublisher{}
	now := time.Now()
	define := &usecases.DefinePath{Repo: repo, Publisher: pub, Clock: fixedClock{now}}
	deactivate := &usecases.DeactivatePath{Repo: repo, Publisher: pub, Clock: fixedClock{now}}
	ctx := context.Background()

	if _, err := define.Execute(ctx, "PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, shared.Intake{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := deactivate.Execute(ctx, "PICK"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := deactivate.Execute(ctx, "PICK"); err != nil {
		t.Fatalf("unexpected error on redundant deactivate: %v", err)
	}
	if pub.count() != 2 {
		t.Fatalf("want exactly 2 events (Created + Deactivated, no duplicate), got %d", pub.count())
	}
}

func TestListPaths_ActiveOnlyExcludesDeactivated(t *testing.T) {
	repo := newFakeRepo()
	pub := &fakePublisher{}
	now := time.Now()
	define := &usecases.DefinePath{Repo: repo, Publisher: pub, Clock: fixedClock{now}}
	deactivate := &usecases.DeactivatePath{Repo: repo, Publisher: pub, Clock: fixedClock{now}}
	list := &usecases.ListPaths{Repo: repo}
	ctx := context.Background()

	if _, err := define.Execute(ctx, "PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, shared.Intake{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := define.Execute(ctx, "PACK", "pack", true, []shared.Capability{"pack"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, shared.Intake{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := deactivate.Execute(ctx, "PACK"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	active, err := list.Execute(ctx, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(active) != 1 || active[0].ID() != "PICK" {
		t.Fatalf("want exactly [PICK] active, got %+v", active)
	}

	all, err := list.Execute(ctx, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("want 2 total paths, got %d", len(all))
	}
}

func TestGetPath_NotFound_ReturnsErrPathNotFound(t *testing.T) {
	repo := newFakeRepo()
	uc := &usecases.GetPath{Repo: repo}
	_, err := uc.Execute(context.Background(), "MISSING")
	if !errors.Is(err, usecases.ErrPathNotFound) {
		t.Fatalf("want ErrPathNotFound, got %v", err)
	}
}

func TestDefinePath_RepoFindError_PropagatesAndSkipsMetrics(t *testing.T) {
	wantErr := errors.New("boom: repo unreachable")
	repo := &erroringRepo{fakeRepo: newFakeRepo(), findErr: wantErr}
	pub := &fakePublisher{}
	metrics := &recordingMetrics{}
	uc := &usecases.DefinePath{Repo: repo, Publisher: pub, Clock: fixedClock{time.Now()}, Metrics: metrics}

	_, err := uc.Execute(context.Background(), "PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, shared.Intake{})
	if !errors.Is(err, wantErr) {
		t.Fatalf("want the repo error propagated, got %v", err)
	}
	if metrics.accepted != 0 || metrics.rejected != 0 {
		t.Fatalf("want no metrics recorded on a repo lookup failure, got %+v", metrics)
	}
}

func TestDefinePath_RepoSaveError_Propagates(t *testing.T) {
	wantErr := errors.New("boom: save failed")
	repo := &erroringRepo{fakeRepo: newFakeRepo(), saveErr: wantErr}
	pub := &fakePublisher{}
	uc := &usecases.DefinePath{Repo: repo, Publisher: pub, Clock: fixedClock{time.Now()}}

	_, err := uc.Execute(context.Background(), "PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, shared.Intake{})
	if !errors.Is(err, wantErr) {
		t.Fatalf("want the save error propagated, got %v", err)
	}
}

func TestDefinePath_PublishError_Propagates(t *testing.T) {
	wantErr := errors.New("boom: publish failed")
	repo := newFakeRepo()
	pub := &erroringPublisher{err: wantErr}
	uc := &usecases.DefinePath{Repo: repo, Publisher: pub, Clock: fixedClock{time.Now()}}

	_, err := uc.Execute(context.Background(), "PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, shared.Intake{})
	if !errors.Is(err, wantErr) {
		t.Fatalf("want the publish error propagated, got %v", err)
	}
}

func TestDefinePath_InvalidInput_RecordsRejectedMetric(t *testing.T) {
	repo := newFakeRepo()
	pub := &fakePublisher{}
	metrics := &recordingMetrics{}
	uc := &usecases.DefinePath{Repo: repo, Publisher: pub, Clock: fixedClock{time.Now()}, Metrics: metrics}

	_, err := uc.Execute(context.Background(), "PICK", "", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, shared.Intake{})
	if err == nil {
		t.Fatal("expected an error for an empty matchPrefix")
	}
	if metrics.rejected != 1 || metrics.accepted != 0 {
		t.Fatalf("want 1 rejected metric recorded, got %+v", metrics)
	}
}

func TestDefinePath_ValidInput_RecordsAcceptedMetric(t *testing.T) {
	repo := newFakeRepo()
	pub := &fakePublisher{}
	metrics := &recordingMetrics{}
	uc := &usecases.DefinePath{Repo: repo, Publisher: pub, Clock: fixedClock{time.Now()}, Metrics: metrics}

	if _, err := uc.Execute(context.Background(), "PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, shared.Intake{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if metrics.accepted != 1 || metrics.rejected != 0 {
		t.Fatalf("want 1 accepted metric recorded, got %+v", metrics)
	}
}

func TestDefinePath_DuplicateId_RecordsRejectedMetric(t *testing.T) {
	repo := newFakeRepo()
	pub := &fakePublisher{}
	metrics := &recordingMetrics{}
	uc := &usecases.DefinePath{Repo: repo, Publisher: pub, Clock: fixedClock{time.Now()}, Metrics: metrics}
	ctx := context.Background()

	if _, err := uc.Execute(ctx, "PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, shared.Intake{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := uc.Execute(ctx, "PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, shared.Intake{}); !errors.Is(err, usecases.ErrPathAlreadyExists) {
		t.Fatalf("want ErrPathAlreadyExists, got %v", err)
	}
	if metrics.rejected != 1 || metrics.accepted != 1 {
		t.Fatalf("want 1 accepted + 1 rejected metric, got %+v", metrics)
	}
}

func TestRevisePath_RepoFindError_Propagates(t *testing.T) {
	wantErr := errors.New("boom: repo unreachable")
	repo := &erroringRepo{fakeRepo: newFakeRepo(), findErr: wantErr}
	pub := &fakePublisher{}
	uc := &usecases.RevisePath{Repo: repo, Publisher: pub, Clock: fixedClock{time.Now()}}

	_, err := uc.Execute(context.Background(), "PICK", "pick", []shared.Capability{"pick"}, testCycleTimeP95, shared.Eligibility{}, nil)
	if !errors.Is(err, wantErr) {
		t.Fatalf("want the repo error propagated, got %v", err)
	}
}

func TestRevisePath_RepoSaveError_Propagates(t *testing.T) {
	inner := newFakeRepo()
	pub := &fakePublisher{}
	define := &usecases.DefinePath{Repo: inner, Publisher: pub, Clock: fixedClock{time.Now()}}
	if _, err := define.Execute(context.Background(), "PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, shared.Intake{}); err != nil {
		t.Fatalf("setup: unexpected error: %v", err)
	}

	wantErr := errors.New("boom: save failed")
	repo := &erroringRepo{fakeRepo: inner, saveErr: wantErr}
	revise := &usecases.RevisePath{Repo: repo, Publisher: pub, Clock: fixedClock{time.Now()}}

	_, err := revise.Execute(context.Background(), "PICK", "pick-v2", []shared.Capability{"pick"}, testCycleTimeP95, shared.Eligibility{}, nil)
	if !errors.Is(err, wantErr) {
		t.Fatalf("want the save error propagated, got %v", err)
	}
}

func TestRevisePath_PublishError_Propagates(t *testing.T) {
	repo := newFakeRepo()
	pub := &fakePublisher{}
	define := &usecases.DefinePath{Repo: repo, Publisher: pub, Clock: fixedClock{time.Now()}}
	if _, err := define.Execute(context.Background(), "PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, shared.Intake{}); err != nil {
		t.Fatalf("setup: unexpected error: %v", err)
	}

	wantErr := errors.New("boom: publish failed")
	revise := &usecases.RevisePath{Repo: repo, Publisher: &erroringPublisher{err: wantErr}, Clock: fixedClock{time.Now()}}
	_, err := revise.Execute(context.Background(), "PICK", "pick-v2", []shared.Capability{"pick"}, testCycleTimeP95, shared.Eligibility{}, nil)
	if !errors.Is(err, wantErr) {
		t.Fatalf("want the publish error propagated, got %v", err)
	}
}

func TestDeactivatePath_RepoFindError_Propagates(t *testing.T) {
	wantErr := errors.New("boom: repo unreachable")
	repo := &erroringRepo{fakeRepo: newFakeRepo(), findErr: wantErr}
	pub := &fakePublisher{}
	uc := &usecases.DeactivatePath{Repo: repo, Publisher: pub, Clock: fixedClock{time.Now()}}

	if err := uc.Execute(context.Background(), "PICK"); !errors.Is(err, wantErr) {
		t.Fatalf("want the repo error propagated, got %v", err)
	}
}

func TestDeactivatePath_RepoSaveError_Propagates(t *testing.T) {
	inner := newFakeRepo()
	pub := &fakePublisher{}
	define := &usecases.DefinePath{Repo: inner, Publisher: pub, Clock: fixedClock{time.Now()}}
	if _, err := define.Execute(context.Background(), "PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, shared.Intake{}); err != nil {
		t.Fatalf("setup: unexpected error: %v", err)
	}

	wantErr := errors.New("boom: save failed")
	repo := &erroringRepo{fakeRepo: inner, saveErr: wantErr}
	uc := &usecases.DeactivatePath{Repo: repo, Publisher: pub, Clock: fixedClock{time.Now()}}

	if err := uc.Execute(context.Background(), "PICK"); !errors.Is(err, wantErr) {
		t.Fatalf("want the save error propagated, got %v", err)
	}
}

func TestDeactivatePath_PublishError_Propagates(t *testing.T) {
	repo := newFakeRepo()
	pub := &fakePublisher{}
	define := &usecases.DefinePath{Repo: repo, Publisher: pub, Clock: fixedClock{time.Now()}}
	if _, err := define.Execute(context.Background(), "PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, shared.Intake{}); err != nil {
		t.Fatalf("setup: unexpected error: %v", err)
	}

	wantErr := errors.New("boom: publish failed")
	uc := &usecases.DeactivatePath{Repo: repo, Publisher: &erroringPublisher{err: wantErr}, Clock: fixedClock{time.Now()}}
	if err := uc.Execute(context.Background(), "PICK"); !errors.Is(err, wantErr) {
		t.Fatalf("want the publish error propagated, got %v", err)
	}
}

func TestListPaths_RepoError_Propagates(t *testing.T) {
	wantErr := errors.New("boom: repo unreachable")
	repo := &erroringRepo{fakeRepo: newFakeRepo(), listErr: wantErr}
	list := &usecases.ListPaths{Repo: repo}

	if _, err := list.Execute(context.Background(), true); !errors.Is(err, wantErr) {
		t.Fatalf("want the repo error propagated for activeOnly=true, got %v", err)
	}
	if _, err := list.Execute(context.Background(), false); !errors.Is(err, wantErr) {
		t.Fatalf("want the repo error propagated for activeOnly=false, got %v", err)
	}
}

func TestGetPath_RepoError_Propagates(t *testing.T) {
	wantErr := errors.New("boom: repo unreachable")
	repo := &erroringRepo{fakeRepo: newFakeRepo(), findErr: wantErr}
	uc := &usecases.GetPath{Repo: repo}

	if _, err := uc.Execute(context.Background(), "PICK"); !errors.Is(err, wantErr) {
		t.Fatalf("want the repo error propagated, got %v", err)
	}
}
