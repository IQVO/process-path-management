//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/claudioed/process-path-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/process-path-management/internal/application/usecases"
	"github.com/claudioed/process-path-management/internal/domain/cptschedule"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// activeScheduleNamingInactivePath counts cutoff entries that name a path
// whose status is not ACTIVE: the exact breach of ADR 0010's invariant
// ("no active schedule names an inactive path") that ADR 0026 left as a
// residual race and ADR 0028 closes with row locks.
const activeScheduleNamingInactivePath = `
	SELECT count(*)
	FROM cpt_schedule_cutoffs c
	CROSS JOIN LATERAL unnest(c.eligible_path_ids) AS pid
	JOIN process_paths p ON p.id = pid
	WHERE p.status <> 'ACTIVE'`

// TestCPTSchedule_DefineVsDeactivate_NeverNamesAnInactivePath races, over
// real Postgres, many goroutines that define a schedule listing path P
// against many that deactivate P. Whatever the interleaving, every racer
// must either win or be refused with a business error (409
// path-referenced-by-cpt-schedule on the deactivation side, 422
// ineligible-path-id on the define side), and afterwards no schedule may
// name an inactive path. Repeated so a lost race window has many chances
// to show up.
func TestCPTSchedule_DefineVsDeactivate_NeverNamesAnInactivePath(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	paths := postgres.NewProcessPathRepo(pool)
	schedules := postgres.NewCPTScheduleRepo(pool)
	pub := postgres.NewOutboxPublisher(pool, uuid.NewString)
	uow := postgres.NewUnitOfWork(pool)

	definePath := &usecases.DefinePath{Repo: paths, Publisher: pub, Clock: fixedClock{t: now}, UnitOfWork: uow}
	defineSchedule := &usecases.DefineCPTSchedule{Repo: schedules, ProcessPathRepo: paths, Publisher: pub, Clock: fixedClock{t: now}, UnitOfWork: uow}
	deactivate := &usecases.DeactivatePath{Repo: paths, Publisher: pub, Clock: fixedClock{t: now.Add(time.Minute)}, UnitOfWork: uow, CPTSchedules: schedules}

	const (
		iterations = 50
		definers   = 4
		deactivers = 4
	)
	var definedWins, deactivatedWins int
	for it := 0; it < iterations; it++ {
		pathId := shared.PathId(fmt.Sprintf("RACE-%02d", it))
		if _, err := definePath.Execute(ctx, pathId, "race", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, 2*time.Hour, shared.Eligibility{}); err != nil {
			t.Fatalf("iteration %d: define path: %v", it, err)
		}

		var wg sync.WaitGroup
		start := make(chan struct{})
		errs := make([]error, definers+deactivers)
		wg.Add(definers + deactivers)
		for i := 0; i < definers; i++ {
			go func() {
				defer wg.Done()
				cutoff, err := cptschedule.NewCutoff(fmt.Sprintf("c-%02d-%d", it, i), "15:00", []cptschedule.Weekday{cptschedule.Monday}, "ground", []shared.PathId{pathId})
				if err != nil {
					errs[i] = err
					return
				}
				<-start
				_, errs[i] = defineSchedule.Execute(ctx, shared.SiteId(fmt.Sprintf("site-%02d-%d", it, i)), "America/Sao_Paulo", []cptschedule.Cutoff{cutoff})
			}()
		}
		for i := 0; i < deactivers; i++ {
			go func() {
				defer wg.Done()
				<-start
				errs[definers+i] = deactivate.Execute(ctx, pathId)
			}()
		}
		close(start)
		wg.Wait()

		for i, err := range errs {
			switch {
			case err == nil:
				if i < definers {
					definedWins++
				} else {
					deactivatedWins++
				}
			case i < definers && errors.Is(err, usecases.ErrIneligiblePathId):
			case i >= definers && errors.Is(err, usecases.ErrPathReferencedByCPTSchedule):
			default:
				t.Fatalf("iteration %d racer %d: unexpected error (want nil or the documented business refusal): %v", it, i, err)
			}
		}

		var violations int
		if err := pool.QueryRow(ctx, activeScheduleNamingInactivePath).Scan(&violations); err != nil {
			t.Fatalf("iteration %d: invariant query: %v", it, err)
		}
		if violations != 0 {
			t.Fatalf("iteration %d: %d cutoff(s) name a deactivated path: the CPT-schedule/path race is open (errs=%v)", it, violations, errs)
		}
	}
	t.Logf("%d iterations: define won %d times, deactivate won %d times", iterations, definedWins, deactivatedWins)
}
