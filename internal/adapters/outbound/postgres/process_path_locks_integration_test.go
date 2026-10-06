//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/process-path-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/process-path-management/internal/application/usecases"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

func seedPaths(t *testing.T, paths *postgres.ProcessPathRepo, uow *postgres.UnitOfWork, ids ...shared.PathId) {
	t.Helper()
	define := &usecases.DefinePath{Repo: paths, Publisher: noopPublisher{}, Clock: fixedClock{t: time.Now().UTC().Truncate(time.Microsecond)}, UnitOfWork: uow}
	for _, id := range ids {
		if _, err := define.Execute(context.Background(), id, "p", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, time.Hour, shared.Eligibility{}); err != nil {
			t.Fatalf("define %s: %v", id, err)
		}
	}
}

type noopPublisher struct{}

func (noopPublisher) Publish(context.Context, shared.DomainEvent) error { return nil }

// TestProcessPathRepo_LockByIDsForShare_ReturnsSortedAndSkipsUnknown covers
// the adapter contract the use case relies on (ADR 0028).
func TestProcessPathRepo_LockByIDsForShare_ReturnsSortedAndSkipsUnknown(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	paths := postgres.NewProcessPathRepo(pool)
	uow := postgres.NewUnitOfWork(pool)
	seedPaths(t, paths, uow, "C", "A", "B")

	var got []shared.PathId
	err := uow.Execute(ctx, func(ctx context.Context) error {
		locked, err := paths.LockByIDsForShare(ctx, []shared.PathId{"C", "GHOST", "A"})
		for _, p := range locked {
			got = append(got, p.ID())
		}
		return err
	})
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	if len(got) != 2 || got[0] != "A" || got[1] != "C" {
		t.Fatalf("want [A C] (sorted, unknown skipped), got %v", got)
	}
}

// TestProcessPathRepo_ShareLockBlocksDeactivationLock proves the row lock is
// really held to the end of the unit of work: while a define holds FOR SHARE
// on PICK, a deactivation's FOR UPDATE on PICK waits for it.
func TestProcessPathRepo_ShareLockBlocksDeactivationLock(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	paths := postgres.NewProcessPathRepo(pool)
	uow := postgres.NewUnitOfWork(pool)
	seedPaths(t, paths, uow, "PICK")

	holding := make(chan struct{})
	release := make(chan struct{})
	holderDone := make(chan error, 1)
	go func() {
		holderDone <- uow.Execute(ctx, func(ctx context.Context) error {
			if _, err := paths.LockByIDsForShare(ctx, []shared.PathId{"PICK"}); err != nil {
				return err
			}
			close(holding)
			<-release
			return nil
		})
	}()
	<-holding

	acquired := make(chan struct{})
	waiterDone := make(chan error, 1)
	go func() {
		waiterDone <- uow.Execute(ctx, func(ctx context.Context) error {
			p, err := paths.FindByIDForUpdate(ctx, "PICK")
			if err == nil && p != nil {
				close(acquired)
			}
			return err
		})
	}()

	select {
	case <-acquired:
		t.Fatal("FOR UPDATE was granted while another transaction held FOR SHARE on the row")
	case <-time.After(500 * time.Millisecond):
	}
	close(release)
	if err := <-holderDone; err != nil {
		t.Fatalf("holder: %v", err)
	}
	select {
	case <-acquired:
	case <-time.After(10 * time.Second):
		t.Fatal("FOR UPDATE was never granted after the share lock was released")
	}
	if err := <-waiterDone; err != nil {
		t.Fatalf("waiter: %v", err)
	}
}

// TestProcessPathRepo_UpdateLockBlocksShareLock_AndShareSeesDeactivation is
// the other half: a define arriving while a deactivation holds FOR UPDATE
// waits, and once the deactivation commits it reads the path as inactive
// (READ COMMITTED re-reads the locked row), which the use case refuses.
func TestProcessPathRepo_UpdateLockBlocksShareLock_AndShareSeesDeactivation(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	paths := postgres.NewProcessPathRepo(pool)
	uow := postgres.NewUnitOfWork(pool)
	seedPaths(t, paths, uow, "PICK")

	holding := make(chan struct{})
	release := make(chan struct{})
	holderDone := make(chan error, 1)
	go func() {
		holderDone <- uow.Execute(ctx, func(ctx context.Context) error {
			p, err := paths.FindByIDForUpdate(ctx, "PICK")
			if err != nil {
				return err
			}
			p.Deactivate(time.Now().UTC().Truncate(time.Microsecond))
			if err := paths.Save(ctx, p); err != nil {
				return err
			}
			close(holding)
			<-release
			return nil
		})
	}()
	<-holding

	type result struct {
		active bool
		found  int
		err    error
	}
	got := make(chan result, 1)
	go func() {
		var r result
		r.err = uow.Execute(ctx, func(ctx context.Context) error {
			locked, err := paths.LockByIDsForShare(ctx, []shared.PathId{"PICK"})
			r.found = len(locked)
			if len(locked) == 1 {
				r.active = locked[0].IsActive()
			}
			return err
		})
		got <- r
	}()

	select {
	case r := <-got:
		t.Fatalf("FOR SHARE was granted while a deactivation held FOR UPDATE: %+v", r)
	case <-time.After(500 * time.Millisecond):
	}
	close(release)
	if err := <-holderDone; err != nil {
		t.Fatalf("holder: %v", err)
	}
	select {
	case r := <-got:
		if r.err != nil || r.found != 1 || r.active {
			t.Fatalf("want the committed (inactive) path, got %+v", r)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("FOR SHARE was never granted after the deactivation committed")
	}
}
