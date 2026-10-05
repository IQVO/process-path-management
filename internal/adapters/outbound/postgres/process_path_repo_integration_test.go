//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/process-path-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/process-path-management/internal/domain/processpath"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

func TestPostgres_ProcessPathRoundTrip(t *testing.T) {
	// testcontainers Postgres (outboxDB boots and migrates it): the test
	// owns its own database, never an external DATABASE_URL, never t.Skip.
	pool := outboxDB(t)
	ctx := context.Background()

	repo := postgres.NewProcessPathRepo(pool)

	id := shared.PathId("PICK-IT-" + time.Now().UTC().Format("150405.000000"))
	now := time.Now().UTC().Truncate(time.Microsecond)

	p, err := processpath.Define(id, "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, 2*time.Hour, shared.Eligibility{}, now)
	if err != nil {
		t.Fatalf("unexpected error defining path: %v", err)
	}
	if err := repo.Save(ctx, p); err != nil {
		t.Fatalf("unexpected error saving path: %v", err)
	}

	found, err := repo.FindByID(ctx, id)
	if err != nil {
		t.Fatalf("unexpected error finding path: %v", err)
	}
	if found == nil {
		t.Fatal("expected to find the saved path")
	}
	if found.MatchPrefix() != "pick" {
		t.Fatalf("expected matchPrefix=pick, got %q", found.MatchPrefix())
	}
	if !found.IsActive() {
		t.Fatal("expected the freshly-saved path to be Active")
	}
	if len(found.RequiredCapabilities()) != 1 || found.RequiredCapabilities()[0] != "pick" {
		t.Fatalf("expected requiredCapabilities=[pick], got %v", found.RequiredCapabilities())
	}

	// Revise, save, re-read: verify the update path (ON CONFLICT DO UPDATE)
	// round-trips correctly, not just the initial insert.
	changed, err := found.Revise("pick-zone-a", []shared.Capability{"pick", "hazmat"}, 3*time.Hour, shared.Eligibility{}, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("unexpected error revising path: %v", err)
	}
	if !changed {
		t.Fatal("expected the revision to report changed=true")
	}
	if err := repo.Save(ctx, found); err != nil {
		t.Fatalf("unexpected error saving revised path: %v", err)
	}

	revised, err := repo.FindByID(ctx, id)
	if err != nil {
		t.Fatalf("unexpected error re-finding path: %v", err)
	}
	if revised.MatchPrefix() != "pick-zone-a" {
		t.Fatalf("expected revised matchPrefix=pick-zone-a, got %q", revised.MatchPrefix())
	}
	if len(revised.RequiredCapabilities()) != 2 {
		t.Fatalf("expected 2 required capabilities after revision, got %d", len(revised.RequiredCapabilities()))
	}

	// Deactivate, save, verify ListActive/ListAll both agree.
	revised.Deactivate(now.Add(2 * time.Minute))
	if err := repo.Save(ctx, revised); err != nil {
		t.Fatalf("unexpected error saving deactivated path: %v", err)
	}

	active, err := repo.ListActive(ctx)
	if err != nil {
		t.Fatalf("unexpected error listing active paths: %v", err)
	}
	for _, a := range active {
		if a.ID() == id {
			t.Fatalf("expected %s to be excluded from ListActive after deactivation", id)
		}
	}

	all, err := repo.ListAll(ctx)
	if err != nil {
		t.Fatalf("unexpected error listing all paths: %v", err)
	}
	var foundInAll bool
	for _, a := range all {
		if a.ID() == id {
			foundInAll = true
			if a.IsActive() {
				t.Fatalf("expected %s to be Deactivated in ListAll, got Active", id)
			}
		}
	}
	if !foundInAll {
		t.Fatalf("expected %s to still appear in ListAll after deactivation", id)
	}
}

func TestPostgres_FindByID_MissingReturnsNilNotError(t *testing.T) {
	// testcontainers Postgres (outboxDB boots and migrates it): the test
	// owns its own database, never an external DATABASE_URL, never t.Skip.
	pool := outboxDB(t)
	ctx := context.Background()

	repo := postgres.NewProcessPathRepo(pool)

	found, err := repo.FindByID(ctx, shared.PathId("DOES-NOT-EXIST-INTEGRATION"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found != nil {
		t.Fatalf("expected nil for a missing path, got %+v", found)
	}
}
