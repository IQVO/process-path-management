package memory_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/process-path-management/internal/adapters/outbound/memory"
	"github.com/claudioed/process-path-management/internal/application/ports"
	"github.com/claudioed/process-path-management/internal/domain/processflow"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// The in-memory half of the ProcessFlowRepo contract; the Postgres half is
// internal/adapters/outbound/postgres/process_flow_repo_integration_test.go.

var t0 = time.Date(2026, 11, 20, 8, 0, 0, 0, time.UTC)

func flowOf(t *testing.T, id string, pairs ...string) *processflow.ProcessFlow {
	t.Helper()
	var stages []processflow.Stage
	for i := 0; i+1 < len(pairs); i += 2 {
		s, err := processflow.NewStage(shared.PathId(pairs[i]), processflow.ProcessType(pairs[i+1]))
		if err != nil {
			t.Fatal(err)
		}
		stages = append(stages, s)
	}
	f, err := processflow.Define(processflow.FlowId(id), id, stages, t0)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestProcessFlowRepo_StoresCopies(t *testing.T) {
	repo, ctx := memory.NewProcessFlowRepo(), context.Background()
	f := flowOf(t, "f", "PICK", "PICK")
	if err := repo.Create(ctx, f); err != nil {
		t.Fatal(err)
	}
	// Mutating the caller's aggregate does not reach the stored one.
	f.Retire(t0)
	stored, _ := repo.FindByID(ctx, "f")
	if !stored.IsActive() || stored.Version() != 1 {
		t.Fatalf("the repo must keep its own copy: %q v%d", stored.Status(), stored.Version())
	}
	// Nor does mutating what FindByID returned.
	stored.Retire(t0)
	again, _ := repo.FindByID(ctx, "f")
	if !again.IsActive() {
		t.Fatal("FindByID must return a copy")
	}
	if none, err := repo.FindByID(ctx, "ghost"); err != nil || none != nil {
		t.Fatalf("unknown id: %v %v", none, err)
	}
}

func TestProcessFlowRepo_CreateIsInsertOnly_SaveIsVersionGuarded(t *testing.T) {
	repo, ctx := memory.NewProcessFlowRepo(), context.Background()
	if err := repo.Create(ctx, flowOf(t, "f", "PICK", "PICK")); err != nil {
		t.Fatal(err)
	}
	if err := repo.Create(ctx, flowOf(t, "f", "PACK", "PACK")); !errors.Is(err, ports.ErrAlreadyExists) {
		t.Fatalf("got %v, want ErrAlreadyExists", err)
	}
	w1, _ := repo.FindByID(ctx, "f")
	w2, _ := repo.FindByID(ctx, "f")
	_, _ = w1.Revise("w1", w1.Stages(), t0)
	_, _ = w2.Revise("w2", w2.Stages(), t0)
	if err := repo.Save(ctx, w1); err != nil {
		t.Fatalf("first writer: %v", err)
	}
	if err := repo.Save(ctx, w2); !errors.Is(err, ports.ErrConcurrentModification) {
		t.Fatalf("second writer: got %v", err)
	}
	if kept, _ := repo.FindByID(ctx, "f"); kept.Name() != "w1" || kept.Version() != 2 {
		t.Fatalf("the loser overwrote the winner: %q v%d", kept.Name(), kept.Version())
	}
	ghost := flowOf(t, "ghost", "PICK", "PICK")
	ghost.Retire(t0)
	if err := repo.Save(ctx, ghost); !errors.Is(err, ports.ErrConcurrentModification) {
		t.Fatalf("an unknown flow: got %v", err)
	}
}

// seededRepo holds d (PICK, PACK), a (PICK), c (PACK) and the retired b (PICK).
func seededRepo(t *testing.T) *memory.ProcessFlowRepo {
	t.Helper()
	repo, ctx := memory.NewProcessFlowRepo(), context.Background()
	for _, f := range []*processflow.ProcessFlow{
		flowOf(t, "d", "PICK", "PICK", "PACK", "PACK"),
		flowOf(t, "a", "PICK", "PICK"),
		flowOf(t, "c", "PACK", "PACK"),
		flowOf(t, "b", "PICK", "PICK"),
	} {
		if err := repo.Create(ctx, f); err != nil {
			t.Fatal(err)
		}
	}
	b, _ := repo.FindByID(ctx, "b")
	b.Retire(t0)
	if err := repo.Save(ctx, b); err != nil {
		t.Fatal(err)
	}
	return repo
}

func flowIDs(fs []*processflow.ProcessFlow) string {
	s := ""
	for _, f := range fs {
		s += string(f.ID())
	}
	return s
}

func TestProcessFlowRepo_ListPage(t *testing.T) {
	repo, ctx := seededRepo(t), context.Background()
	tests := []struct {
		activeOnly bool
		after      processflow.FlowId
		size       int
		want       string
	}{
		{false, "a", 2, "bc"},
		{true, "", 10, "acd"},
		{false, "", 10, "abcd"},
		{true, "zzz", 10, ""},
	}
	for _, tc := range tests {
		page, err := repo.ListPage(ctx, tc.activeOnly, tc.after, tc.size)
		if err != nil || flowIDs(page) != tc.want {
			t.Errorf("ListPage(%v, %q, %d) = %q %v, want %q", tc.activeOnly, tc.after, tc.size, flowIDs(page), err, tc.want)
		}
	}
}

func TestProcessFlowRepo_ReferencingPath_And_LockActive(t *testing.T) {
	repo, ctx := seededRepo(t), context.Background()
	refs, _ := repo.ListActiveFlowIDsReferencingPath(ctx, "PICK")
	if len(refs) != 2 || refs[0] != "a" || refs[1] != "d" {
		t.Fatalf("PICK refs %v (b is retired)", refs)
	}
	if none, _ := repo.ListActiveFlowIDsReferencingPath(ctx, "SLAM"); len(none) != 0 {
		t.Fatalf("SLAM refs %v", none)
	}
	locked, _ := repo.LockActiveForShare(ctx)
	if len(locked) != 3 || locked[0] != "a" || locked[1] != "c" || locked[2] != "d" {
		t.Fatalf("locked %v", locked)
	}
}
