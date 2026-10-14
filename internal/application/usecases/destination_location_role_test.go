package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/process-path-management/internal/application/usecases"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// TestDefinePath_WithDestinationLocationRole_IsPersistedAndPublished
// proves the optional destination role flows all the way through the use
// case: onto the persisted aggregate AND onto the published
// ProcessPathCreated event (ADR 0006).
func TestDefinePath_WithDestinationLocationRole_IsPersistedAndPublished(t *testing.T) {
	repo := newFakeRepo()
	pub := &fakePublisher{}
	now := time.Date(2026, 9, 13, 0, 0, 0, 0, time.UTC)
	uc := &usecases.DefinePath{Repo: repo, Publisher: pub, Clock: fixedClock{now}}

	p, err := uc.Execute(context.Background(), "PACK", "pack", true, []shared.Capability{"pack"}, shared.DestinationLocationRoleDrop, testCycleTimeP95, shared.Eligibility{}, shared.Intake{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.DestinationLocationRole() != shared.DestinationLocationRoleDrop {
		t.Fatalf("want DestinationLocationRoleDrop on the returned aggregate, got %q", p.DestinationLocationRole())
	}

	persisted, err := repo.FindByID(context.Background(), "PACK")
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	if persisted.DestinationLocationRole() != shared.DestinationLocationRoleDrop {
		t.Fatalf("want persisted DestinationLocationRoleDrop, got %q", persisted.DestinationLocationRole())
	}

	created, ok := pub.last().(shared.ProcessPathCreated)
	if !ok {
		t.Fatalf("want a ProcessPathCreated event, got %T", pub.last())
	}
	if created.DestinationLocationRole != shared.DestinationLocationRoleDrop {
		t.Fatalf("want published event's DestinationLocationRoleDrop, got %q", created.DestinationLocationRole)
	}
}

// TestDefinePath_NoDestinationLocationRole_DefaultsToUnset proves the
// zero-value default (most paths never declare a destination role at
// all) still works exactly as it did before this feature existed.
func TestDefinePath_NoDestinationLocationRole_DefaultsToUnset(t *testing.T) {
	repo := newFakeRepo()
	pub := &fakePublisher{}
	uc := &usecases.DefinePath{Repo: repo, Publisher: pub, Clock: fixedClock{time.Now()}}

	p, err := uc.Execute(context.Background(), "PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, testCycleTimeP95, shared.Eligibility{}, shared.Intake{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.DestinationLocationRole() != shared.DestinationLocationRoleUnset {
		t.Fatalf("want DestinationLocationRoleUnset, got %q", p.DestinationLocationRole())
	}

	created := pub.last().(shared.ProcessPathCreated)
	if created.DestinationLocationRole != shared.DestinationLocationRoleUnset {
		t.Fatalf("want published event's DestinationLocationRoleUnset, got %q", created.DestinationLocationRole)
	}
}

// TestDefinePath_RejectsInvalidDestinationLocationRole proves the use
// case surfaces the domain's own validation error rather than silently
// accepting an unrecognized value or a real facility-layout role this
// service does not treat as a valid destination (e.g. Storage).
func TestDefinePath_RejectsInvalidDestinationLocationRole(t *testing.T) {
	repo := newFakeRepo()
	pub := &fakePublisher{}
	uc := &usecases.DefinePath{Repo: repo, Publisher: pub, Clock: fixedClock{time.Now()}}

	_, err := uc.Execute(context.Background(), "PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRole("Storage"), testCycleTimeP95, shared.Eligibility{}, shared.Intake{})
	if !errors.Is(err, shared.ErrInvalidDestinationLocationRole) {
		t.Fatalf("want ErrInvalidDestinationLocationRole, got %v", err)
	}
	if pub.count() != 0 {
		t.Fatal("expected no event published on a rejected definition")
	}
}

// TestRevisePath_DoesNotAlterDestinationLocationRole proves Revise never
// touches the Define-time destination role, matching the aggregate's own
// immutability guarantee (mirrors Direct's own established posture).
func TestRevisePath_DoesNotAlterDestinationLocationRole(t *testing.T) {
	repo := newFakeRepo()
	pub := &fakePublisher{}
	now := time.Now()
	define := &usecases.DefinePath{Repo: repo, Publisher: pub, Clock: fixedClock{now}}
	if _, err := define.Execute(context.Background(), "PACK", "pack", true, []shared.Capability{"pack"}, shared.DestinationLocationRoleWorkCenter, testCycleTimeP95, shared.Eligibility{}, shared.Intake{}); err != nil {
		t.Fatalf("setup: %v", err)
	}

	revise := &usecases.RevisePath{Repo: repo, Publisher: pub, Clock: fixedClock{now.Add(time.Hour)}}
	p, err := revise.Execute(context.Background(), "PACK", "pack-v2", []shared.Capability{"pack"}, testCycleTimeP95, shared.Eligibility{}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.DestinationLocationRole() != shared.DestinationLocationRoleWorkCenter {
		t.Fatalf("expected DestinationLocationRole to remain WorkCenter after Revise, got %q", p.DestinationLocationRole())
	}

	updated, ok := pub.last().(shared.ProcessPathUpdated)
	if !ok {
		t.Fatalf("want a ProcessPathUpdated event, got %T", pub.last())
	}
	if updated.DestinationLocationRole != shared.DestinationLocationRoleWorkCenter {
		t.Fatalf("want published event's DestinationLocationRoleWorkCenter, got %q", updated.DestinationLocationRole)
	}
}
