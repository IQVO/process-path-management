//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/claudioed/process-path-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/process-path-management/internal/application/ports"
	"github.com/claudioed/process-path-management/internal/domain/cptschedule"
	"github.com/claudioed/process-path-management/internal/domain/processpath"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// Optimistic-concurrency (ADR 0017) repo-level tests: a stale-version Save
// fails with ports.ErrConcurrentModification, a current-version Save
// succeeds and advances the row's version by exactly one, and a real
// two-goroutine race on the SAME loaded aggregate resolves to exactly one
// winner — for BOTH mutable aggregates (ProcessPath and CPTSchedule).
// All run against a real testcontainers Postgres (outboxDB), never an
// external DATABASE_URL, never t.Skip.

func definePath(t *testing.T, repo *postgres.ProcessPathRepo, ctx context.Context, id shared.PathId, matchPrefix string) *processpath.ProcessPath {
	t.Helper()
	p, err := processpath.Define(id, matchPrefix, true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, 2*time.Hour, shared.Eligibility{}, time.Now())
	if err != nil {
		t.Fatalf("define %s: %v", id, err)
	}
	if err := repo.Save(ctx, p); err != nil {
		t.Fatalf("initial save %s: %v", id, err)
	}
	return p
}

func TestProcessPathRepo_Save_StaleVersionFails(t *testing.T) {
	pool := outboxDB(t)
	repo := postgres.NewProcessPathRepo(pool)
	ctx := context.Background()
	definePath(t, repo, ctx, "PICK", "pick")

	// Two independent loads of the same row — simulating two requests
	// that each read-modify-write without knowing about the other.
	first, err := repo.FindByID(ctx, "PICK")
	if err != nil {
		t.Fatalf("load first: %v", err)
	}
	second, err := repo.FindByID(ctx, "PICK")
	if err != nil {
		t.Fatalf("load second: %v", err)
	}
	if first.Version() != 1 || second.Version() != 1 {
		t.Fatalf("expected both loads at version 1, got %d/%d", first.Version(), second.Version())
	}

	if _, err := first.Revise("pick-a", []shared.Capability{"pick"}, 2*time.Hour, shared.Eligibility{}, time.Now()); err != nil {
		t.Fatalf("revise first: %v", err)
	}
	if err := repo.Save(ctx, first); err != nil {
		t.Fatalf("save first: %v", err)
	}

	// second still holds version 1; the row is now at version 2.
	if _, err := second.Revise("pick-b", []shared.Capability{"pick"}, 2*time.Hour, shared.Eligibility{}, time.Now()); err != nil {
		t.Fatalf("revise second: %v", err)
	}
	err = repo.Save(ctx, second)
	if !errors.Is(err, ports.ErrConcurrentModification) {
		t.Fatalf("expected ErrConcurrentModification on stale-version save, got %v", err)
	}

	// The first writer's change survived; the second's was rejected, not
	// silently clobbered.
	loaded, err := repo.FindByID(ctx, "PICK")
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if loaded.MatchPrefix() != "pick-a" {
		t.Fatalf("expected the first writer's matchPrefix (pick-a) to persist, got %q", loaded.MatchPrefix())
	}
	if loaded.Version() != 2 {
		t.Fatalf("expected version 2 after one successful save, got %d", loaded.Version())
	}
}

func TestProcessPathRepo_Save_CurrentVersionSucceedsAndIncrements(t *testing.T) {
	pool := outboxDB(t)
	repo := postgres.NewProcessPathRepo(pool)
	ctx := context.Background()
	definePath(t, repo, ctx, "PACK", "pack")

	loaded, err := repo.FindByID(ctx, "PACK")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if loaded.Version() != 1 {
		t.Fatalf("expected version 1 after first save, got %d", loaded.Version())
	}
	if _, err := loaded.Revise("pack-zone", []shared.Capability{"pack"}, 3*time.Hour, shared.Eligibility{}, time.Now()); err != nil {
		t.Fatalf("revise: %v", err)
	}
	if err := repo.Save(ctx, loaded); err != nil {
		t.Fatalf("save at current version: %v", err)
	}
	// Save must not mutate the caller's in-memory version.
	if loaded.Version() != 1 {
		t.Fatalf("Save must not mutate the caller's in-memory version; got %d", loaded.Version())
	}

	reloaded, err := repo.FindByID(ctx, "PACK")
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if reloaded.Version() != 2 {
		t.Fatalf("expected version to advance to 2, got %d", reloaded.Version())
	}
	if reloaded.MatchPrefix() != "pack-zone" {
		t.Fatalf("expected revised matchPrefix to persist, got %q", reloaded.MatchPrefix())
	}
}

// TestProcessPathRepo_ConcurrentSaves_ExactlyOneWinner is the real proof
// the lost-update race is closed: two goroutines each load the SAME row,
// mutate their own in-memory copy, and Save concurrently (synchronized
// start so both genuinely race). Exactly one must succeed; the other must
// observe ports.ErrConcurrentModification; the row's version must advance
// by exactly one; exactly one of the two mutations may be visible on
// reload — never both (a lost update slipped through), never neither.
func TestProcessPathRepo_ConcurrentSaves_ExactlyOneWinner(t *testing.T) {
	pool := outboxDB(t)
	repo := postgres.NewProcessPathRepo(pool)
	ctx := context.Background()
	definePath(t, repo, ctx, "SLAM", "slam")

	loadedA, err := repo.FindByID(ctx, "SLAM")
	if err != nil {
		t.Fatalf("load A: %v", err)
	}
	loadedB, err := repo.FindByID(ctx, "SLAM")
	if err != nil {
		t.Fatalf("load B: %v", err)
	}
	if _, err := loadedA.Revise("slam-a", []shared.Capability{"slam"}, 2*time.Hour, shared.Eligibility{}, time.Now()); err != nil {
		t.Fatalf("mutate A: %v", err)
	}
	if _, err := loadedB.Revise("slam-b", []shared.Capability{"slam"}, 2*time.Hour, shared.Eligibility{}, time.Now()); err != nil {
		t.Fatalf("mutate B: %v", err)
	}

	var wg sync.WaitGroup
	errs := make([]error, 2)
	start := make(chan struct{})
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		errs[0] = repo.Save(ctx, loadedA)
	}()
	go func() {
		defer wg.Done()
		<-start
		errs[1] = repo.Save(ctx, loadedB)
	}()
	close(start)
	wg.Wait()

	successes, conflicts := 0, 0
	for _, err := range errs {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ports.ErrConcurrentModification):
			conflicts++
		default:
			t.Fatalf("unexpected error from concurrent Save: %v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("expected exactly one success and one conflict, got successes=%d conflicts=%d (errs=%v)", successes, conflicts, errs)
	}

	final, err := repo.FindByID(ctx, "SLAM")
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if final.Version() != 2 {
		t.Fatalf("expected exactly one version increment despite two concurrent attempts, got %d", final.Version())
	}
	if final.MatchPrefix() != "slam-a" && final.MatchPrefix() != "slam-b" {
		t.Fatalf("expected one of the two mutations to land, got %q", final.MatchPrefix())
	}
}

// --- CPTSchedule (the other mutable aggregate: PUT /sites/{siteId}/cpt-schedule) ---

func cutoff(t *testing.T, cptId, localTime string) cptschedule.Cutoff {
	t.Helper()
	c, err := cptschedule.NewCutoff(cptId, localTime, []cptschedule.Weekday{cptschedule.Monday}, "ground", []shared.PathId{"PICK"})
	if err != nil {
		t.Fatalf("new cutoff %s: %v", cptId, err)
	}
	return c
}

func TestCPTScheduleRepo_Save_StaleVersionFails(t *testing.T) {
	pool := outboxDB(t)
	repo := postgres.NewCPTScheduleRepo(pool)
	ctx := context.Background()

	s, err := cptschedule.Define("sp-stale", "America/Sao_Paulo", []cptschedule.Cutoff{cutoff(t, "sp-stale-1500", "15:00")}, time.Now())
	if err != nil {
		t.Fatalf("define: %v", err)
	}
	if err := repo.Save(ctx, s); err != nil {
		t.Fatalf("initial save: %v", err)
	}

	first, err := repo.FindBySiteID(ctx, "sp-stale")
	if err != nil {
		t.Fatalf("load first: %v", err)
	}
	second, err := repo.FindBySiteID(ctx, "sp-stale")
	if err != nil {
		t.Fatalf("load second: %v", err)
	}

	if _, err := first.Revise("America/New_York", []cptschedule.Cutoff{cutoff(t, "sp-stale-1600", "16:00")}, time.Now()); err != nil {
		t.Fatalf("revise first: %v", err)
	}
	if err := repo.Save(ctx, first); err != nil {
		t.Fatalf("save first: %v", err)
	}

	if _, err := second.Revise("Europe/Berlin", []cptschedule.Cutoff{cutoff(t, "sp-stale-1700", "17:00")}, time.Now()); err != nil {
		t.Fatalf("revise second: %v", err)
	}
	err = repo.Save(ctx, second)
	if !errors.Is(err, ports.ErrConcurrentModification) {
		t.Fatalf("expected ErrConcurrentModification on stale-version save, got %v", err)
	}

	loaded, err := repo.FindBySiteID(ctx, "sp-stale")
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if loaded.Timezone() != "America/New_York" {
		t.Fatalf("expected the first writer's timezone to persist, got %q", loaded.Timezone())
	}
	if loaded.Version() != 2 {
		t.Fatalf("expected version 2 after one successful save, got %d", loaded.Version())
	}
}

func TestCPTScheduleRepo_ConcurrentSaves_ExactlyOneWinner(t *testing.T) {
	pool := outboxDB(t)
	repo := postgres.NewCPTScheduleRepo(pool)
	ctx := context.Background()

	s, err := cptschedule.Define("sp-race", "America/Sao_Paulo", []cptschedule.Cutoff{cutoff(t, "sp-race-1500", "15:00")}, time.Now())
	if err != nil {
		t.Fatalf("define: %v", err)
	}
	if err := repo.Save(ctx, s); err != nil {
		t.Fatalf("initial save: %v", err)
	}

	loadedA, err := repo.FindBySiteID(ctx, "sp-race")
	if err != nil {
		t.Fatalf("load A: %v", err)
	}
	loadedB, err := repo.FindBySiteID(ctx, "sp-race")
	if err != nil {
		t.Fatalf("load B: %v", err)
	}
	if _, err := loadedA.Revise("America/New_York", []cptschedule.Cutoff{cutoff(t, "sp-race-1600", "16:00")}, time.Now()); err != nil {
		t.Fatalf("mutate A: %v", err)
	}
	if _, err := loadedB.Revise("Europe/Berlin", []cptschedule.Cutoff{cutoff(t, "sp-race-1700", "17:00")}, time.Now()); err != nil {
		t.Fatalf("mutate B: %v", err)
	}

	var wg sync.WaitGroup
	errs := make([]error, 2)
	start := make(chan struct{})
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		errs[0] = repo.Save(ctx, loadedA)
	}()
	go func() {
		defer wg.Done()
		<-start
		errs[1] = repo.Save(ctx, loadedB)
	}()
	close(start)
	wg.Wait()

	successes, conflicts := 0, 0
	for _, err := range errs {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ports.ErrConcurrentModification):
			conflicts++
		default:
			t.Fatalf("unexpected error from concurrent Save: %v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("expected exactly one success and one conflict, got successes=%d conflicts=%d (errs=%v)", successes, conflicts, errs)
	}

	final, err := repo.FindBySiteID(ctx, "sp-race")
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if final.Version() != 2 {
		t.Fatalf("expected exactly one version increment despite two concurrent attempts, got %d", final.Version())
	}
	if final.Timezone() != "America/New_York" && final.Timezone() != "Europe/Berlin" {
		t.Fatalf("expected one of the two timezones to land, got %q", final.Timezone())
	}
}
