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

// racedRepo simulates the losing side of a concurrent define: FindByID
// sees no row (the winner has not committed yet), but by the time Create
// runs the winner's row exists, so the insert reports ErrAlreadyExists.
type racedRepo struct {
	*fakeRepo
}

func (r *racedRepo) FindByID(context.Context, shared.PathId) (*processpath.ProcessPath, error) {
	return nil, nil
}

// TestDefinePath_LosingARace_ReturnsPathAlreadyExistsAndPublishesNothing
// proves a define that loses the FindByID-then-insert race surfaces as
// ErrPathAlreadyExists (HTTP 409 path-already-exists), is counted as a
// rejection, and publishes no ProcessPathCreated.
func TestDefinePath_LosingARace_ReturnsPathAlreadyExistsAndPublishesNothing(t *testing.T) {
	now := time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)
	winner, err := processpath.Define("PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, 2*time.Hour, shared.Eligibility{}, now)
	if err != nil {
		t.Fatalf("define winner: %v", err)
	}
	repo := &racedRepo{fakeRepo: newFakeRepo()}
	if err := repo.Create(context.Background(), winner); err != nil {
		t.Fatalf("seed winner: %v", err)
	}
	pub := &fakePublisher{}
	metrics := &recordingMetrics{}
	uc := &usecases.DefinePath{Repo: repo, Publisher: pub, Clock: fixedClock{t: now}, Metrics: metrics}

	_, err = uc.Execute(context.Background(), "PICK", "pick-b", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, 3*time.Hour, shared.Eligibility{})
	if !errors.Is(err, usecases.ErrPathAlreadyExists) {
		t.Fatalf("want ErrPathAlreadyExists, got %v", err)
	}
	if pub.count() != 0 {
		t.Fatalf("a lost race must not publish ProcessPathCreated, got %d events", pub.count())
	}
	if metrics.rejected != 1 || metrics.accepted != 0 {
		t.Fatalf("want 1 rejected / 0 accepted, got rejected=%d accepted=%d", metrics.rejected, metrics.accepted)
	}
	stored, _ := repo.fakeRepo.FindByID(context.Background(), "PICK")
	if stored == nil || stored.MatchPrefix() != "pick" {
		t.Fatalf("the winner's row must be untouched, got %+v", stored)
	}
}
