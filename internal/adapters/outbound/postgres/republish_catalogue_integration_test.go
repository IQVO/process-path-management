//go:build integration

package postgres_test

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	outboundkafka "github.com/claudioed/process-path-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/process-path-management/internal/application/usecases"
	"github.com/claudioed/process-path-management/internal/domain/cptschedule"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// ADR 0038 over the real UnitOfWork + outbox: the republish enqueues one
// integration-topic row per ACTIVE path and per schedule, in id order, and
// NOTHING on the analytics topic (the projector counts every
// ProcessPathUpdated as a revision).
func TestRepublishCatalogue_EnqueuesOnlyIntegrationRows_ActiveOnly_Sorted(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	paths := postgres.NewProcessPathRepo(pool)
	schedules := postgres.NewCPTScheduleRepo(pool)
	uow := postgres.NewUnitOfWork(pool)
	newId := uuid.NewString
	both := postgres.NewOutboxPublisher(pool, newId, outboundkafka.IntegrationEncoder{}, outboundkafka.NewAnalyticsEncoder(newId))

	define := &usecases.DefinePath{Repo: paths, Publisher: both, Clock: fixedClock{t: now}, UnitOfWork: uow}
	for _, id := range []shared.PathId{"SLAM", "PICK", "PACK"} {
		if _, err := define.Execute(ctx, id, "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, 2*time.Hour, shared.Eligibility{}, shared.Intake{}); err != nil {
			t.Fatalf("define %s: %v", id, err)
		}
	}
	for _, s := range []*cptschedule.CPTSchedule{
		scheduleFor(t, "sp1", cutoffNaming(t, "sp1-1500", "PICK")),
		scheduleFor(t, "ba1", cutoffNaming(t, "ba1-1500", "SLAM")),
	} {
		if err := schedules.Save(ctx, s); err != nil {
			t.Fatalf("save schedule: %v", err)
		}
	}
	deactivate := &usecases.DeactivatePath{Repo: paths, Publisher: both, Clock: fixedClock{t: now}, UnitOfWork: uow, CPTSchedules: schedules}
	if err := deactivate.Execute(ctx, "PACK"); err != nil {
		t.Fatalf("deactivate PACK: %v", err)
	}
	var baseline int64
	if err := pool.QueryRow(ctx, `SELECT coalesce(max(id), 0) FROM outbox_events`).Scan(&baseline); err != nil {
		t.Fatal(err)
	}

	uc := &usecases.RepublishCatalogue{
		Repo: paths, CPTSchedules: schedules, Clock: fixedClock{t: now}, UnitOfWork: uow,
		Publisher: postgres.NewOutboxPublisher(pool, newId, outboundkafka.IntegrationEncoder{}),
	}
	res, err := uc.Execute(ctx)
	if err != nil {
		t.Fatalf("republish: %v", err)
	}
	if res.Paths != 2 || res.Schedules != 2 || res.Omitted != 0 {
		t.Fatalf("result = %+v, want 2 paths, 2 schedules", res)
	}

	rows, err := pool.Query(ctx, `SELECT topic, event_type, aggregate_id FROM outbox_events WHERE id > $1 ORDER BY id`, baseline)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var topic, typ, agg string
		if err := rows.Scan(&topic, &typ, &agg); err != nil {
			t.Fatal(err)
		}
		if topic != outboundkafka.Topic {
			t.Fatalf("republish enqueued a row for %q, want only %q", topic, outboundkafka.Topic)
		}
		got = append(got, typ[strings.LastIndex(typ, ".")+1:]+":"+agg)
	}
	want := []string{"ProcessPathUpdated:PICK", "ProcessPathUpdated:SLAM", "CPTScheduleChanged:ba1", "CPTScheduleChanged:sp1"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("outbox rows = %v, want %v", got, want)
	}
}

// LockAllForShare must really lock: a concurrent revision of a schedule
// (DefineCPTSchedule's UPDATE) blocks until the republishing unit of work
// ends, so its CPTScheduleChanged lands in the outbox AFTER the republished
// snapshot instead of being overtaken by it.
func TestCPTScheduleRepo_LockAllForShare_BlocksConcurrentRevisionUntilCommit(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	repo := postgres.NewCPTScheduleRepo(pool)
	uow := postgres.NewUnitOfWork(pool)
	for _, s := range []*cptschedule.CPTSchedule{
		scheduleFor(t, "sp1", cutoffNaming(t, "sp1-1500", "PICK")),
		scheduleFor(t, "ba1", cutoffNaming(t, "ba1-1500", "PICK")),
	} {
		if err := repo.Save(ctx, s); err != nil {
			t.Fatalf("save: %v", err)
		}
	}
	// Outside a unit of work the answer is every site, ascending.
	sites, err := repo.LockAllForShare(ctx)
	if err != nil || !reflect.DeepEqual(sites, []shared.SiteId{"ba1", "sp1"}) {
		t.Fatalf("LockAllForShare = %v, %v, want [ba1 sp1]", sites, err)
	}

	locked, release := make(chan struct{}), make(chan struct{})
	holderDone := make(chan error, 1)
	go func() {
		holderDone <- uow.Execute(ctx, func(ctx context.Context) error {
			if _, err := repo.LockAllForShare(ctx); err != nil {
				return err
			}
			close(locked)
			<-release
			return nil
		})
	}()
	<-locked

	current, err := repo.FindBySiteID(ctx, "sp1")
	if err != nil || current == nil {
		t.Fatalf("find: %v %v", current, err)
	}
	if _, err := current.Revise("America/Sao_Paulo", []cptschedule.Cutoff{cutoffNaming(t, "sp1-1800", "PICK")}, time.Now().UTC()); err != nil {
		t.Fatalf("revise: %v", err)
	}
	saved := make(chan error, 1)
	go func() { saved <- repo.Save(ctx, current) }()

	select {
	case err := <-saved:
		t.Fatalf("a concurrent Save finished (%v) while the schedules were share-locked", err)
	case <-time.After(750 * time.Millisecond):
	}
	close(release)
	if err := <-holderDone; err != nil {
		t.Fatalf("holder: %v", err)
	}
	select {
	case err := <-saved:
		if err != nil {
			t.Fatalf("Save after the lock released: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Save stayed blocked after the share lock was released")
	}
}
