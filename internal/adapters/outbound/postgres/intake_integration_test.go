//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/process-path-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/process-path-management/internal/domain/processpath"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// ADR 0034: the intake column (migration 0009) round-trips through Save,
// FindByID, ListActive and a revision; a row written before the column
// existed reads back as the empty set; a stored value outside the
// vocabulary fails the read loudly instead of routing on a guess.
func TestPostgres_IntakeRoundTrip(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	repo := postgres.NewProcessPathRepo(pool)

	id := shared.PathId("INTAKE-IT-" + time.Now().UTC().Format("150405.000000"))
	now := time.Now().UTC().Truncate(time.Microsecond)
	in := shared.MustIntake(shared.DemandSourceSlotRelocation, shared.DemandSourceCustomerOrder)

	p, err := processpath.Define(id, "intake-it", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, 2*time.Hour, shared.Eligibility{}, in, now)
	if err != nil {
		t.Fatalf("define: %v", err)
	}
	if err := repo.Create(ctx, p); err != nil {
		t.Fatalf("create: %v", err)
	}
	found, err := repo.FindByID(ctx, id)
	if err != nil || found == nil {
		t.Fatalf("find: %v %v", found, err)
	}
	if !found.Intake().Equal(in) {
		t.Fatalf("FindByID intake = %v, want %v", found.Intake().Strings(), in.Strings())
	}
	assertListedWithIntake(t, repo, id, in)

	changed, err := found.Revise("intake-it", []shared.Capability{"pick"}, 2*time.Hour, shared.Eligibility{}, shared.Intake{}, now.Add(time.Minute))
	if err != nil || !changed {
		t.Fatalf("revise: changed=%v err=%v", changed, err)
	}
	if err := repo.Save(ctx, found); err != nil {
		t.Fatalf("save: %v", err)
	}
	revised, err := repo.FindByID(ctx, id)
	if err != nil {
		t.Fatalf("re-find: %v", err)
	}
	if !revised.Intake().IsEmpty() {
		t.Fatalf("revised intake = %v, want the empty set", revised.Intake().Strings())
	}
}

func assertListedWithIntake(t *testing.T, repo *postgres.ProcessPathRepo, id shared.PathId, in shared.Intake) {
	t.Helper()
	active, err := repo.ListActive(context.Background())
	if err != nil {
		t.Fatalf("list active: %v", err)
	}
	for _, a := range active {
		if a.ID() == id {
			if !a.Intake().Equal(in) {
				t.Fatalf("ListActive intake = %v, want %v", a.Intake().Strings(), in.Strings())
			}
			return
		}
	}
	t.Fatalf("%s missing from ListActive", id)
}

func TestPostgres_RowWithoutIntake_ReadsAsTheEmptySet(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	id := "LEGACY-IT-" + time.Now().UTC().Format("150405.000000")
	// A row as a pre-0009 binary writes it: the intake column is not named.
	_, err := pool.Exec(ctx, `
		INSERT INTO process_paths (id, match_prefix, direct, required_capabilities, cycle_time_p95, eligibility, status, created_at, updated_at)
		VALUES ($1, 'legacy-it', true, ARRAY['pick'], '2 hours', '{}', 'ACTIVE', now(), now())`, id)
	if err != nil {
		t.Fatalf("legacy insert: %v", err)
	}
	found, err := postgres.NewProcessPathRepo(pool).FindByID(ctx, shared.PathId(id))
	if err != nil || found == nil {
		t.Fatalf("find: %v %v", found, err)
	}
	if !found.Intake().IsEmpty() {
		t.Fatalf("intake = %v, want the empty set", found.Intake().Strings())
	}
}

func TestPostgres_UnknownStoredIntake_FailsTheRead(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	id := "BADINTAKE-IT-" + time.Now().UTC().Format("150405.000000")
	_, err := pool.Exec(ctx, `
		INSERT INTO process_paths (id, match_prefix, direct, required_capabilities, cycle_time_p95, eligibility, intake, status, created_at, updated_at)
		VALUES ($1, 'bad-it', true, ARRAY['pick'], '2 hours', '{}', ARRAY['ORDERS'], 'ACTIVE', now(), now())`, id)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = postgres.NewProcessPathRepo(pool).FindByID(ctx, shared.PathId(id))
	if !errors.Is(err, shared.ErrInvalidIntake) {
		t.Fatalf("FindByID error = %v, want ErrInvalidIntake", err)
	}
	// Clean up so ListActive-based tests sharing this database stay valid.
	if _, err := pool.Exec(ctx, `DELETE FROM process_paths WHERE id = $1`, id); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
}
