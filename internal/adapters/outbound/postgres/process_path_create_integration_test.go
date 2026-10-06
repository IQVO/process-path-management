//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/process-path-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/process-path-management/internal/application/ports"
	"github.com/claudioed/process-path-management/internal/domain/processpath"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// TestProcessPathRepo_Create_IsInsertOnly pins the repo-level contract the
// DefinePath use case relies on: Create inserts a new row and refuses
// (with ports.ErrAlreadyExists, leaving the stored row untouched) when the
// id is taken, whether the existing row is Active or Deactivated.
func TestProcessPathRepo_Create_IsInsertOnly(t *testing.T) {
	pool := outboxDB(t)
	repo := postgres.NewProcessPathRepo(pool)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)

	first, err := processpath.Define("DUP", "dup", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, 2*time.Hour, shared.Eligibility{}, now)
	if err != nil {
		t.Fatalf("define first: %v", err)
	}
	if err := repo.Create(ctx, first); err != nil {
		t.Fatalf("create first: %v", err)
	}

	second, err := processpath.Define("DUP", "other", true, []shared.Capability{"pack"}, shared.DestinationLocationRoleUnset, 3*time.Hour, shared.Eligibility{}, now)
	if err != nil {
		t.Fatalf("define second: %v", err)
	}
	if err := repo.Create(ctx, second); !errors.Is(err, ports.ErrAlreadyExists) {
		t.Fatalf("second create of the same id: want ports.ErrAlreadyExists, got %v", err)
	}
	loaded, err := repo.FindByID(ctx, "DUP")
	if err != nil || loaded == nil {
		t.Fatalf("reload: %v %v", loaded, err)
	}
	if loaded.MatchPrefix() != "dup" || loaded.Version() != 1 {
		t.Fatalf("the losing create must not touch the stored row, got matchPrefix=%q version=%d", loaded.MatchPrefix(), loaded.Version())
	}

	// A DEACTIVATED row still owns the id (ADR 0001's permanent identity).
	loaded.Deactivate(now.Add(time.Minute))
	if err := repo.Save(ctx, loaded); err != nil {
		t.Fatalf("deactivate save: %v", err)
	}
	third, err := processpath.Define("DUP", "dup", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, 2*time.Hour, shared.Eligibility{}, now)
	if err != nil {
		t.Fatalf("define third: %v", err)
	}
	if err := repo.Create(ctx, third); !errors.Is(err, ports.ErrAlreadyExists) {
		t.Fatalf("create over a deactivated id: want ports.ErrAlreadyExists, got %v", err)
	}
}
