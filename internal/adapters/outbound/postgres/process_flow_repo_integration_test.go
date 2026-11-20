//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/claudioed/process-path-management/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/process-path-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/process-path-management/internal/application/ports"
	"github.com/claudioed/process-path-management/internal/application/usecases"
	"github.com/claudioed/process-path-management/internal/domain/processflow"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// These tests need Docker (testcontainers, via outboxDB). They are the Postgres
// half of the ProcessFlowRepo contract that memory/process_flow_repo_test.go
// pins for the in-memory adapter, plus the unit-of-work and outbox behaviour
// ADR 0042 relies on.

func flowOf(t *testing.T, id, name string, now time.Time, pairs ...string) *processflow.ProcessFlow {
	t.Helper()
	var stages []processflow.Stage
	for i := 0; i+1 < len(pairs); i += 2 {
		s, err := processflow.NewStage(shared.PathId(pairs[i]), processflow.ProcessType(pairs[i+1]))
		if err != nil {
			t.Fatalf("stage: %v", err)
		}
		stages = append(stages, s)
	}
	f, err := processflow.Define(processflow.FlowId(id), name, stages, now)
	if err != nil {
		t.Fatalf("define flow %s: %v", id, err)
	}
	return f
}

func stageOrder(f *processflow.ProcessFlow) string {
	s := ""
	for _, st := range f.Stages() {
		s += string(st.PathId()) + ":" + string(st.ProcessType()) + ","
	}
	return s
}

func TestProcessFlowRepo_RoundTripKeepsStageOrderAndReplacesWholesale(t *testing.T) {
	pool := outboxDB(t)
	repo := postgres.NewProcessFlowRepo(pool)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)

	f := flowOf(t, "f1", "Outbound", now, "PICK", "PICK", "REBIN", "REBIN", "PACK", "PACK")
	if err := repo.Create(ctx, f); err != nil {
		t.Fatalf("create: %v", err)
	}
	got, err := repo.FindByID(ctx, "f1")
	if err != nil || got == nil {
		t.Fatalf("find: %v %v", got, err)
	}
	if stageOrder(got) != "PICK:PICK,REBIN:REBIN,PACK:PACK," || got.Version() != 1 || got.Name() != "Outbound" || !got.IsActive() {
		t.Fatalf("round trip lost state: %s v%d %q", stageOrder(got), got.Version(), got.Name())
	}
	if !got.CreatedAt().Equal(now) {
		t.Fatalf("createdAt %v, want %v", got.CreatedAt(), now)
	}

	// Revise to a different set and order: the child rows are replaced, not merged.
	st := func(p string, ty processflow.ProcessType) processflow.Stage {
		s, _ := processflow.NewStage(shared.PathId(p), ty)
		return s
	}
	if changed, err := got.Revise("Outbound 2", []processflow.Stage{st("PACK", processflow.ProcessTypePack), st("PICK", processflow.ProcessTypePick)}, now.Add(time.Minute)); err != nil || !changed {
		t.Fatalf("revise: %v %v", changed, err)
	}
	if err := repo.Save(ctx, got); err != nil {
		t.Fatalf("save: %v", err)
	}
	again, _ := repo.FindByID(ctx, "f1")
	if stageOrder(again) != "PACK:PACK,PICK:PICK," || again.Version() != 2 || again.Name() != "Outbound 2" {
		t.Fatalf("wholesale replace failed: %s v%d %q", stageOrder(again), again.Version(), again.Name())
	}

	if missing, err := repo.FindByID(ctx, "ghost"); err != nil || missing != nil {
		t.Fatalf("an unknown id is nil,nil: %v %v", missing, err)
	}
}

func TestProcessFlowRepo_CreateIsInsertOnly_SaveIsVersionGuarded(t *testing.T) {
	pool := outboxDB(t)
	repo := postgres.NewProcessFlowRepo(pool)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)

	if err := repo.Create(ctx, flowOf(t, "f1", "A", now, "PICK", "PICK")); err != nil {
		t.Fatal(err)
	}
	if err := repo.Create(ctx, flowOf(t, "f1", "B", now, "PACK", "PACK")); !errors.Is(err, ports.ErrAlreadyExists) {
		t.Fatalf("second create: got %v, want ErrAlreadyExists", err)
	}
	if kept, _ := repo.FindByID(ctx, "f1"); kept.Name() != "A" || stageOrder(kept) != "PICK:PICK," {
		t.Fatalf("a refused create must change nothing: %q %s", kept.Name(), stageOrder(kept))
	}

	// Two writers load version 1; the first saves version 2, the second's save
	// of ITS version 2 must fail rather than overwrite.
	w1, _ := repo.FindByID(ctx, "f1")
	w2, _ := repo.FindByID(ctx, "f1")
	if _, err := w1.Revise("w1", w1.Stages(), now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := w2.Revise("w2", w2.Stages(), now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := repo.Save(ctx, w1); err != nil {
		t.Fatalf("first writer: %v", err)
	}
	if err := repo.Save(ctx, w2); !errors.Is(err, ports.ErrConcurrentModification) {
		t.Fatalf("second writer: got %v, want ErrConcurrentModification", err)
	}
	if kept, _ := repo.FindByID(ctx, "f1"); kept.Name() != "w1" || kept.Version() != 2 {
		t.Fatalf("the loser must not overwrite the winner: %q v%d", kept.Name(), kept.Version())
	}
	// A save for a flow that does not exist is also a concurrent modification.
	ghost := flowOf(t, "ghost", "G", now, "PICK", "PICK")
	ghost.Retire(now) // version 2, no row at version 1
	if err := repo.Save(ctx, ghost); !errors.Is(err, ports.ErrConcurrentModification) {
		t.Fatalf("save of an unknown flow: got %v", err)
	}
}

func TestProcessFlowRepo_ListPage_ReferencingPath_LockActive(t *testing.T) {
	pool := outboxDB(t)
	repo := postgres.NewProcessFlowRepo(pool)
	uow := postgres.NewUnitOfWork(pool)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)

	for _, f := range []*processflow.ProcessFlow{
		flowOf(t, "d", "D", now, "PICK", "PICK", "PACK", "PACK"),
		flowOf(t, "a", "A", now, "PICK", "PICK"),
		flowOf(t, "c", "C", now, "PACK", "PACK"),
		flowOf(t, "b", "B", now, "PICK", "PICK"),
	} {
		if err := repo.Create(ctx, f); err != nil {
			t.Fatal(err)
		}
	}
	b, _ := repo.FindByID(ctx, "b")
	b.Retire(now.Add(time.Minute))
	if err := repo.Save(ctx, b); err != nil {
		t.Fatal(err)
	}

	page, err := repo.ListPage(ctx, false, "a", 2)
	if err != nil || len(page) != 2 || page[0].ID() != "b" || page[1].ID() != "c" {
		t.Fatalf("page after a (all): %v %v", page, err)
	}
	if len(page[1].Stages()) != 1 || stageOrder(page[1]) != "PACK:PACK," {
		t.Fatalf("a page carries each flow's own stages: %s", stageOrder(page[1]))
	}
	active, err := repo.ListPage(ctx, true, "", 10)
	if err != nil || len(active) != 3 || active[0].ID() != "a" || active[1].ID() != "c" || active[2].ID() != "d" {
		t.Fatalf("active page: %v %v", active, err)
	}
	if empty, err := repo.ListPage(ctx, true, "zzz", 10); err != nil || len(empty) != 0 {
		t.Fatalf("past the end: %v %v", empty, err)
	}

	// Only ACTIVE flows count as referencing, each once, ascending.
	refs, err := repo.ListActiveFlowIDsReferencingPath(ctx, "PICK")
	if err != nil || len(refs) != 2 || refs[0] != "a" || refs[1] != "d" {
		t.Fatalf("PICK is used by active a and d (b is retired): %v %v", refs, err)
	}
	if none, err := repo.ListActiveFlowIDsReferencingPath(ctx, "SLAM"); err != nil || len(none) != 0 {
		t.Fatalf("SLAM: %v %v", none, err)
	}

	// LockActiveForShare inside a unit of work returns the ACTIVE ids ascending.
	var locked []processflow.FlowId
	if err := uow.Execute(ctx, func(ctx context.Context) error {
		var err error
		locked, err = repo.LockActiveForShare(ctx)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if len(locked) != 3 || locked[0] != "a" || locked[1] != "c" || locked[2] != "d" {
		t.Fatalf("locked %v", locked)
	}
}

func TestProcessFlowSchema_RejectsWhatTheDomainWouldNot(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	now := time.Now().UTC()
	if _, err := pool.Exec(ctx, `INSERT INTO process_flows (flow_id, name, status, version, created_at, updated_at) VALUES ('s', 'S', 'ACTIVE', 1, $1, $1)`, now); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO process_flow_stages VALUES ('s', 0, 'PICK', 'WELD')`); err == nil {
		t.Fatal("a process type outside the closed set must violate the check constraint")
	}
	if _, err := pool.Exec(ctx, `INSERT INTO process_flow_stages VALUES ('s', 0, 'PICK', 'PICK')`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO process_flow_stages VALUES ('s', 1, 'PICK', 'PACK')`); err == nil {
		t.Fatal("a path at most once per flow must be enforced by the UNIQUE constraint")
	}
	if _, err := pool.Exec(ctx, `INSERT INTO process_flows (flow_id, name, status, version, created_at, updated_at) VALUES ('t', 'T', 'PAUSED', 1, $1, $1)`, now); err == nil {
		t.Fatal("a status outside ACTIVE|RETIRED must violate the check constraint")
	}
}

// ADR 0042 over the real UnitOfWork and outbox: a define commits the flow and
// its event on BOTH topics together; a path an active flow uses is refused
// with no ProcessPathDeactivated row; retiring the flow lifts the refusal.
func TestProcessFlow_DefineRetireAndDeactivate_OverTheRealOutbox(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	paths := postgres.NewProcessPathRepo(pool)
	flows := postgres.NewProcessFlowRepo(pool)
	newId := uuid.NewString
	pub := postgres.NewOutboxPublisher(pool, newId, outboundkafka.IntegrationEncoder{}, outboundkafka.NewAnalyticsEncoder(newId))
	uow := postgres.NewUnitOfWork(pool)

	definePath := &usecases.DefinePath{Repo: paths, Publisher: pub, Clock: fixedClock{t: now}, UnitOfWork: uow}
	for _, id := range []shared.PathId{"PICK", "PACK"} {
		if _, err := definePath.Execute(ctx, id, "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, 2*time.Hour, shared.Eligibility{}, shared.Intake{}); err != nil {
			t.Fatalf("define %s: %v", id, err)
		}
	}
	defineFlow := &usecases.DefineProcessFlow{Repo: flows, ProcessPathRepo: paths, Publisher: pub, Clock: fixedClock{t: now}, UnitOfWork: uow}
	retire := &usecases.RetireProcessFlow{Repo: flows, Publisher: pub, Clock: fixedClock{t: now.Add(time.Minute)}, UnitOfWork: uow}
	deactivate := &usecases.DeactivatePath{Repo: paths, Publisher: pub, Clock: fixedClock{t: now.Add(time.Minute)}, UnitOfWork: uow, Flows: flows}

	st := flowOf(t, "x", "X", now, "PICK", "PICK", "PACK", "PACK").Stages()
	if _, created, err := defineFlow.Execute(ctx, "f1", "Flow", st); err != nil || !created {
		t.Fatalf("define flow: created=%v err=%v", created, err)
	}
	for _, topic := range []string{outboundkafka.Topic, outboundkafka.AnalyticsTopic} {
		if got := countOutbox(t, pool, "event_type = '"+cloudevents.TypeProcessFlowDefined+"' AND aggregate_id = 'f1' AND topic = '"+topic+"'"); got != 1 {
			t.Fatalf("ProcessFlowDefined on %s: %d rows, want 1", topic, got)
		}
	}
	// An unchanged PUT enqueues nothing.
	if _, created, err := defineFlow.Execute(ctx, "f1", "Flow", st); err != nil || created {
		t.Fatalf("repeat: created=%v err=%v", created, err)
	}
	if got := countOutbox(t, pool, "aggregate_id = 'f1'"); got != 2 {
		t.Fatalf("an unchanged PUT must enqueue nothing: %d rows for f1, want 2", got)
	}

	if err := deactivate.Execute(ctx, "PICK"); !errors.Is(err, usecases.ErrPathInActiveFlow) {
		t.Fatalf("want ErrPathInActiveFlow, got %v", err)
	}
	if got := countOutbox(t, pool, "event_type = '"+cloudevents.TypeProcessPathDeactivated+"' AND aggregate_id = 'PICK'"); got != 0 {
		t.Fatalf("a refused deactivation must enqueue nothing, got %d", got)
	}

	if err := retire.Execute(ctx, "f1"); err != nil {
		t.Fatalf("retire: %v", err)
	}
	if got := countOutbox(t, pool, "event_type = '"+cloudevents.TypeProcessFlowRetired+"' AND aggregate_id = 'f1'"); got != 2 {
		t.Fatalf("ProcessFlowRetired on both topics: %d rows, want 2", got)
	}
	if err := deactivate.Execute(ctx, "PICK"); err != nil {
		t.Fatalf("after the flow was retired: %v", err)
	}
	// A flow can no longer name the deactivated path.
	if _, _, err := defineFlow.Execute(ctx, "f2", "F2", st); !errors.Is(err, usecases.ErrIneligibleStagePath) {
		t.Fatalf("want ErrIneligibleStagePath, got %v", err)
	}
}
