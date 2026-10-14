package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/process-path-management/internal/application/usecases"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// scopeKey marks a context as "inside the unit of work" so the fakes can
// assert every Save/Publish happened within the scope, never outside it.
type scopeKey struct{}

// recordingUnitOfWork is a ports.UnitOfWork fake that (a) tags the ctx it
// hands to fn, (b) counts how many scopes were opened, and (c) reports
// whether the last scope committed (fn returned nil) or rolled back.
type recordingUnitOfWork struct {
	opened     int
	committed  int
	rolledBack int
	beginErr   error
}

func (u *recordingUnitOfWork) Execute(ctx context.Context, fn func(ctx context.Context) error) error {
	if u.beginErr != nil {
		return u.beginErr
	}
	u.opened++
	err := fn(context.WithValue(ctx, scopeKey{}, true))
	if err != nil {
		u.rolledBack++
		return err
	}
	u.committed++
	return nil
}

func inScope(ctx context.Context) bool {
	v, _ := ctx.Value(scopeKey{}).(bool)
	return v
}

// scopedPublisher records whether each Publish happened inside a scope.
type scopedPublisher struct {
	fakePublisher
	inScope []bool
	err     error
}

func (p *scopedPublisher) Publish(ctx context.Context, event shared.DomainEvent) error {
	p.inScope = append(p.inScope, inScope(ctx))
	if p.err != nil {
		return p.err
	}
	return p.fakePublisher.Publish(ctx, event)
}

func TestDefinePath_SaveAndPublishRunInsideOneUnitOfWork(t *testing.T) {
	repo := newFakeRepo()
	pub := &scopedPublisher{}
	uow := &recordingUnitOfWork{}
	uc := &usecases.DefinePath{Repo: repo, Publisher: pub, Clock: fixedClock{t: time.Unix(1700000000, 0).UTC()}, UnitOfWork: uow}

	if _, err := uc.Execute(context.Background(), "PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, shared.Intake{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if uow.opened != 1 || uow.committed != 1 || uow.rolledBack != 0 {
		t.Fatalf("expected exactly one committed scope, got opened=%d committed=%d rolledBack=%d", uow.opened, uow.committed, uow.rolledBack)
	}
	if len(pub.inScope) != 1 || !pub.inScope[0] {
		t.Fatalf("expected Publish to run inside the unit of work, got %v", pub.inScope)
	}
}

func TestDefinePath_PublishFailure_RollsBackTheUnitOfWork(t *testing.T) {
	repo := newFakeRepo()
	pub := &scopedPublisher{err: errors.New("outbox insert failed")}
	uow := &recordingUnitOfWork{}
	uc := &usecases.DefinePath{Repo: repo, Publisher: pub, Clock: fixedClock{t: time.Unix(1700000000, 0).UTC()}, UnitOfWork: uow}

	_, err := uc.Execute(context.Background(), "PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, shared.Intake{})
	if err == nil || err.Error() != "outbox insert failed" {
		t.Fatalf("expected the publish error to propagate, got %v", err)
	}
	if uow.rolledBack != 1 || uow.committed != 0 {
		t.Fatalf("expected the scope to roll back, got committed=%d rolledBack=%d", uow.committed, uow.rolledBack)
	}
}

func TestDefinePath_UnitOfWorkBeginFailure_Propagates(t *testing.T) {
	repo := newFakeRepo()
	pub := &scopedPublisher{}
	uow := &recordingUnitOfWork{beginErr: errors.New("begin failed")}
	uc := &usecases.DefinePath{Repo: repo, Publisher: pub, Clock: fixedClock{t: time.Unix(1700000000, 0).UTC()}, UnitOfWork: uow}

	if _, err := uc.Execute(context.Background(), "PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, shared.Intake{}); err == nil || err.Error() != "begin failed" {
		t.Fatalf("expected begin error, got %v", err)
	}
	if pub.count() != 0 {
		t.Fatal("expected nothing published when the unit of work cannot begin")
	}
}

func TestDefinePath_NilUnitOfWork_StillSavesAndPublishes(t *testing.T) {
	repo := newFakeRepo()
	pub := &scopedPublisher{}
	uc := &usecases.DefinePath{Repo: repo, Publisher: pub, Clock: fixedClock{t: time.Unix(1700000000, 0).UTC()}}

	if _, err := uc.Execute(context.Background(), "PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, shared.Intake{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pub.count() != 1 || pub.inScope[0] {
		t.Fatalf("expected one publish outside any scope, got count=%d inScope=%v", pub.count(), pub.inScope)
	}
}

func TestRevisePath_SaveAndPublishRunInsideOneUnitOfWork(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	repo := newFakeRepo()
	define := &usecases.DefinePath{Repo: repo, Publisher: &fakePublisher{}, Clock: fixedClock{t: now}}
	if _, err := define.Execute(context.Background(), "PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, shared.Intake{}); err != nil {
		t.Fatalf("setup: %v", err)
	}

	pub := &scopedPublisher{}
	uow := &recordingUnitOfWork{}
	uc := &usecases.RevisePath{Repo: repo, Publisher: pub, Clock: fixedClock{t: now.Add(time.Minute)}, UnitOfWork: uow}
	if _, err := uc.Execute(context.Background(), "PICK", "pick-zone", []shared.Capability{"pick"}, testCycleTimeP95, shared.Eligibility{}, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if uow.committed != 1 || len(pub.inScope) != 1 || !pub.inScope[0] {
		t.Fatalf("expected one committed scope with Publish inside it, got committed=%d inScope=%v", uow.committed, pub.inScope)
	}
}

func TestRevisePath_NoOp_OpensNoUnitOfWork(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	repo := newFakeRepo()
	define := &usecases.DefinePath{Repo: repo, Publisher: &fakePublisher{}, Clock: fixedClock{t: now}}
	if _, err := define.Execute(context.Background(), "PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, shared.Intake{}); err != nil {
		t.Fatalf("setup: %v", err)
	}

	uow := &recordingUnitOfWork{}
	uc := &usecases.RevisePath{Repo: repo, Publisher: &scopedPublisher{}, Clock: fixedClock{t: now.Add(time.Minute)}, UnitOfWork: uow}
	if _, err := uc.Execute(context.Background(), "PICK", "pick", []shared.Capability{"pick"}, testCycleTimeP95, shared.Eligibility{}, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if uow.opened != 0 {
		t.Fatalf("a no-op revision must not open a unit of work, got opened=%d", uow.opened)
	}
}

func TestDeactivatePath_SaveAndPublishRunInsideOneUnitOfWork(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	repo := newFakeRepo()
	define := &usecases.DefinePath{Repo: repo, Publisher: &fakePublisher{}, Clock: fixedClock{t: now}}
	if _, err := define.Execute(context.Background(), "PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, shared.Intake{}); err != nil {
		t.Fatalf("setup: %v", err)
	}

	pub := &scopedPublisher{}
	uow := &recordingUnitOfWork{}
	uc := &usecases.DeactivatePath{Repo: repo, Publisher: pub, Clock: fixedClock{t: now.Add(time.Minute)}, UnitOfWork: uow}
	if err := uc.Execute(context.Background(), "PICK"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if uow.committed != 1 || len(pub.inScope) != 1 || !pub.inScope[0] {
		t.Fatalf("expected one committed scope with Publish inside it, got committed=%d inScope=%v", uow.committed, pub.inScope)
	}
	// Second call is idempotent and must not open a scope at all.
	if err := uc.Execute(context.Background(), "PICK"); err != nil {
		t.Fatalf("unexpected error on idempotent repeat: %v", err)
	}
	if uow.opened != 1 {
		t.Fatalf("idempotent deactivate must not open a second unit of work, got opened=%d", uow.opened)
	}
}

func TestDeactivatePath_PublishFailure_RollsBack(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	repo := newFakeRepo()
	define := &usecases.DefinePath{Repo: repo, Publisher: &fakePublisher{}, Clock: fixedClock{t: now}}
	if _, err := define.Execute(context.Background(), "PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, shared.Intake{}); err != nil {
		t.Fatalf("setup: %v", err)
	}

	uow := &recordingUnitOfWork{}
	uc := &usecases.DeactivatePath{Repo: repo, Publisher: &scopedPublisher{err: errors.New("boom")}, Clock: fixedClock{t: now.Add(time.Minute)}, UnitOfWork: uow}
	if err := uc.Execute(context.Background(), "PICK"); err == nil {
		t.Fatal("expected the publish error to propagate")
	}
	if uow.rolledBack != 1 {
		t.Fatalf("expected a rollback, got rolledBack=%d", uow.rolledBack)
	}
}
