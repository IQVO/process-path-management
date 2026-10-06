//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/claudioed/process-path-management/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/process-path-management/internal/application/usecases"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// TestDefinePath_ConcurrentSameId_ExactlyOneCreated is the proof that
// path creation is insert-only. DefinePath does FindByID (nil for every
// racer) and then saves a FRESH aggregate at version 1; with the old
// "INSERT ... ON CONFLICT (id) DO UPDATE ... WHERE version = 1" upsert the
// losers' version guard matched the winner's row (also version 1), so a
// second concurrent define silently updated the first row and BOTH
// published ProcessPathCreated. Exactly one racer may win; every other
// one must see ErrPathAlreadyExists (HTTP 409 path-already-exists), and
// exactly one ProcessPathCreated may reach the outbox.
func TestDefinePath_ConcurrentSameId_ExactlyOneCreated(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	uc := &usecases.DefinePath{
		Repo:       postgres.NewProcessPathRepo(pool),
		Publisher:  postgres.NewOutboxPublisher(pool, uuid.NewString),
		Clock:      fixedClock{t: time.Now().UTC().Truncate(time.Microsecond)},
		UnitOfWork: postgres.NewUnitOfWork(pool),
	}

	const racers = 8
	var wg sync.WaitGroup
	errs := make([]error, racers)
	start := make(chan struct{})
	wg.Add(racers)
	for i := 0; i < racers; i++ {
		go func() {
			defer wg.Done()
			<-start
			// Every racer proposes a DIFFERENT matchPrefix, so a silent
			// overwrite is also visible in the surviving row.
			_, errs[i] = uc.Execute(ctx, "RACE", "race-"+string(rune('a'+i)), true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, 2*time.Hour, shared.Eligibility{})
		}()
	}
	close(start)
	wg.Wait()

	successes, conflicts := 0, 0
	for _, err := range errs {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, usecases.ErrPathAlreadyExists):
			conflicts++
		default:
			t.Fatalf("unexpected error from concurrent define: %v", err)
		}
	}
	if successes != 1 || conflicts != racers-1 {
		t.Fatalf("want exactly 1 success and %d path-already-exists, got successes=%d conflicts=%d (errs=%v)", racers-1, successes, conflicts, errs)
	}
	if got := countOutbox(t, pool, "event_type = '"+cloudevents.TypeProcessPathCreated+"' AND aggregate_id = 'RACE'"); got != 1 {
		t.Fatalf("want exactly 1 ProcessPathCreated outbox row for RACE, got %d", got)
	}
	var rows int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM process_paths WHERE id = 'RACE'").Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("want exactly 1 process_paths row, got %d (err=%v)", rows, err)
	}
}
