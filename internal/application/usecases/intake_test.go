package usecases_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/process-path-management/internal/application/usecases"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// ADR 0034: DefinePath carries intake onto ProcessPathCreated; RevisePath
// treats a nil intake as "keep the current one" and a non-nil one (the
// empty set included) as the replacement, and publishes it on
// ProcessPathUpdated.

func defineWithIntake(t *testing.T, repo *fakeRepo, pub *fakePublisher, now time.Time, in shared.Intake) {
	t.Helper()
	define := &usecases.DefinePath{Repo: repo, Publisher: pub, Clock: fixedClock{now}}
	if _, err := define.Execute(context.Background(), "PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, in); err != nil {
		t.Fatalf("define: %v", err)
	}
}

func TestDefinePath_PublishesIntakeOnCreated(t *testing.T) {
	repo, pub := newFakeRepo(), &fakePublisher{}
	in := shared.MustIntake(shared.DemandSourceCustomerOrder)
	defineWithIntake(t, repo, pub, time.Now(), in)
	created, ok := pub.last().(shared.ProcessPathCreated)
	if !ok {
		t.Fatalf("want ProcessPathCreated, got %T", pub.last())
	}
	if !created.Intake.Equal(in) {
		t.Fatalf("created.Intake = %v, want %v", created.Intake.Strings(), in.Strings())
	}
}

func TestRevisePath_NilIntake_KeepsTheCurrentIntake(t *testing.T) {
	repo, pub := newFakeRepo(), &fakePublisher{}
	now := time.Now()
	in := shared.MustIntake(shared.DemandSourceCustomerOrder)
	defineWithIntake(t, repo, pub, now, in)
	revise := &usecases.RevisePath{Repo: repo, Publisher: pub, Clock: fixedClock{now.Add(time.Hour)}}

	p, err := revise.Execute(context.Background(), "PICK", "pick", []shared.Capability{"pick"}, testCycleTimeP95, shared.Eligibility{}, nil)
	if err != nil {
		t.Fatalf("revise: %v", err)
	}
	if !p.Intake().Equal(in) {
		t.Fatalf("Intake() = %v, want it kept as %v", p.Intake().Strings(), in.Strings())
	}
	if pub.count() != 1 {
		t.Fatalf("want no ProcessPathUpdated for a revision that keeps everything, got %d events", pub.count())
	}
}

func TestRevisePath_IntakeChange_PublishesUpdatedWithIntake(t *testing.T) {
	repo, pub := newFakeRepo(), &fakePublisher{}
	now := time.Now()
	defineWithIntake(t, repo, pub, now, shared.Intake{})
	revise := &usecases.RevisePath{Repo: repo, Publisher: pub, Clock: fixedClock{now.Add(time.Hour)}}
	in := shared.MustIntake(shared.DemandSourceCustomerOrder)

	if _, err := revise.Execute(context.Background(), "PICK", "pick", []shared.Capability{"pick"}, testCycleTimeP95, shared.Eligibility{}, &in); err != nil {
		t.Fatalf("revise: %v", err)
	}
	updated, ok := pub.last().(shared.ProcessPathUpdated)
	if !ok {
		t.Fatalf("want ProcessPathUpdated, got %T", pub.last())
	}
	if !updated.Intake.Equal(in) {
		t.Fatalf("updated.Intake = %v, want %v", updated.Intake.Strings(), in.Strings())
	}
}

func TestRevisePath_EmptyIntake_ClearsIt(t *testing.T) {
	repo, pub := newFakeRepo(), &fakePublisher{}
	now := time.Now()
	defineWithIntake(t, repo, pub, now, shared.MustIntake(shared.DemandSourceCustomerOrder))
	revise := &usecases.RevisePath{Repo: repo, Publisher: pub, Clock: fixedClock{now.Add(time.Hour)}}
	empty := shared.Intake{}

	p, err := revise.Execute(context.Background(), "PICK", "pick", []shared.Capability{"pick"}, testCycleTimeP95, shared.Eligibility{}, &empty)
	if err != nil {
		t.Fatalf("revise: %v", err)
	}
	if !p.Intake().IsEmpty() {
		t.Fatalf("Intake() = %v, want the empty set", p.Intake().Strings())
	}
	if _, ok := pub.last().(shared.ProcessPathUpdated); !ok {
		t.Fatalf("want ProcessPathUpdated, got %T", pub.last())
	}
}
