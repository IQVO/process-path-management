//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/process-path-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/process-path-management/internal/domain/cptschedule"
	"github.com/claudioed/process-path-management/internal/domain/processpath"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

func TestPostgres_CPTScheduleRoundTrip(t *testing.T) {
	// testcontainers Postgres (outboxDB boots and migrates it): the test
	// owns its own database, never an external DATABASE_URL, never t.Skip.
	pool := outboxDB(t)
	ctx := context.Background()

	repo := postgres.NewCPTScheduleRepo(pool)
	siteId := shared.SiteId("SP-IT-" + time.Now().UTC().Format("150405.000000"))
	now := time.Now().UTC().Truncate(time.Microsecond)

	c1, err := cptschedule.NewCutoff("cpt-1500", "15:00", []cptschedule.Weekday{cptschedule.Monday, cptschedule.Tuesday}, "ground", []shared.PathId{"PICK"})
	if err != nil {
		t.Fatalf("unexpected error building cutoff: %v", err)
	}
	s, err := cptschedule.Define(siteId, "America/Sao_Paulo", []cptschedule.Cutoff{c1}, now)
	if err != nil {
		t.Fatalf("unexpected error defining schedule: %v", err)
	}
	if err := repo.Save(ctx, s); err != nil {
		t.Fatalf("unexpected error saving schedule: %v", err)
	}

	found, err := repo.FindBySiteID(ctx, siteId)
	if err != nil {
		t.Fatalf("unexpected error finding schedule: %v", err)
	}
	if found == nil {
		t.Fatal("expected to find the saved schedule")
	}
	if found.Timezone() != "America/Sao_Paulo" {
		t.Fatalf("expected timezone=America/Sao_Paulo, got %q", found.Timezone())
	}
	if len(found.Cutoffs()) != 1 {
		t.Fatalf("expected 1 cutoff, got %d", len(found.Cutoffs()))
	}
	gotCutoff := found.Cutoffs()[0]
	if gotCutoff.CptId() != "cpt-1500" || gotCutoff.LocalTime() != "15:00" || gotCutoff.ShipMethod() != "ground" {
		t.Fatalf("unexpected cutoff round-trip: %+v", gotCutoff)
	}
	if len(gotCutoff.DaysOfWeek()) != 2 {
		t.Fatalf("expected 2 daysOfWeek, got %v", gotCutoff.DaysOfWeek())
	}
	if len(gotCutoff.EligiblePathIds()) != 1 || gotCutoff.EligiblePathIds()[0] != "PICK" {
		t.Fatalf("unexpected eligiblePathIds: %v", gotCutoff.EligiblePathIds())
	}

	// Revise, save, re-read: verify the cutoff set is replaced wholesale
	// (not merged) -- a second cutoff added and the first one removed
	// must leave exactly the new set behind.
	c2, err := cptschedule.NewCutoff("cpt-1800", "18:00", []cptschedule.Weekday{cptschedule.Wednesday}, "same-day", []shared.PathId{"PACK"})
	if err != nil {
		t.Fatalf("unexpected error building second cutoff: %v", err)
	}
	changed, err := found.Revise("America/New_York", []cptschedule.Cutoff{c2}, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("unexpected error revising schedule: %v", err)
	}
	if !changed {
		t.Fatal("expected the revision to report changed=true")
	}
	if err := repo.Save(ctx, found); err != nil {
		t.Fatalf("unexpected error saving revised schedule: %v", err)
	}

	revised, err := repo.FindBySiteID(ctx, siteId)
	if err != nil {
		t.Fatalf("unexpected error re-finding schedule: %v", err)
	}
	if revised.Timezone() != "America/New_York" {
		t.Fatalf("expected revised timezone=America/New_York, got %q", revised.Timezone())
	}
	if len(revised.Cutoffs()) != 1 || revised.Cutoffs()[0].CptId() != "cpt-1800" {
		t.Fatalf("expected exactly the new cutoff set (wholesale replace), got %+v", revised.Cutoffs())
	}
}

func TestPostgres_CPTScheduleFindBySiteID_MissingReturnsNilNotError(t *testing.T) {
	// testcontainers Postgres (outboxDB boots and migrates it): the test
	// owns its own database, never an external DATABASE_URL, never t.Skip.
	pool := outboxDB(t)
	ctx := context.Background()

	repo := postgres.NewCPTScheduleRepo(pool)
	found, err := repo.FindBySiteID(ctx, shared.SiteId("DOES-NOT-EXIST-INTEGRATION"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found != nil {
		t.Fatalf("expected nil for a missing schedule, got %+v", found)
	}
}

// TestPostgres_ProcessPathRoundTrip_CarriesCycleTimeP95AndEligibility
// proves the new ADR 0010 columns actually round-trip through Postgres,
// not just the in-memory adapter.
func TestPostgres_ProcessPathRoundTrip_CarriesCycleTimeP95AndEligibility(t *testing.T) {
	// testcontainers Postgres (outboxDB boots and migrates it): the test
	// owns its own database, never an external DATABASE_URL, never t.Skip.
	pool := outboxDB(t)
	ctx := context.Background()

	repo := postgres.NewProcessPathRepo(pool)
	id := shared.PathId("SINGLES-IT-" + time.Now().UTC().Format("150405.000000"))
	now := time.Now().UTC().Truncate(time.Microsecond)

	maxUnits := 1
	eligibility := shared.NewEligibility(&maxUnits, []string{"giftWrap"}, []string{"hazmat"}, true)

	p, err := processpath.Define(id, "singles", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, 90*time.Minute, eligibility, now)
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
	if found.CycleTimeP95() != 90*time.Minute {
		t.Fatalf("expected cycleTimeP95=90m, got %v", found.CycleTimeP95())
	}
	if !found.Eligibility().Equal(eligibility) {
		t.Fatalf("expected eligibility %+v, got %+v", eligibility, found.Eligibility())
	}
}
