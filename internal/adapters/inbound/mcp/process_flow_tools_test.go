package mcp

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/process-path-management/internal/adapters/outbound/memory"
	"github.com/claudioed/process-path-management/internal/application/usecases"
	"github.com/claudioed/process-path-management/internal/domain/processflow"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// newFlowDeps builds Deps over a real in-memory ProcessFlowRepo seeded with
// real aggregates: ACTIVE "single-unit" (PICK, PACK) and RETIRED "old".
func newFlowDeps(t *testing.T) Deps {
	t.Helper()
	repo := memory.NewProcessFlowRepo()
	at := time.Date(2026, 11, 20, 8, 0, 0, 0, time.UTC)
	mk := func(id string, stages ...processflow.Stage) *processflow.ProcessFlow {
		f, err := processflow.Define(processflow.FlowId(id), id, stages, at)
		if err != nil {
			t.Fatalf("define %s: %v", id, err)
		}
		return f
	}
	stage := func(path shared.PathId, pt processflow.ProcessType) processflow.Stage {
		s, err := processflow.NewStage(path, pt)
		if err != nil {
			t.Fatalf("stage %s: %v", path, err)
		}
		return s
	}
	ctx := context.Background()
	if err := repo.Create(ctx, mk("single-unit", stage("PICK", "PICK"), stage("PACK", "PACK"))); err != nil {
		t.Fatal(err)
	}
	old := mk("old", stage("PICK", "PICK"))
	if err := repo.Create(ctx, old); err != nil {
		t.Fatal(err)
	}
	old.Retire(at.Add(time.Hour))
	if err := repo.Save(ctx, old); err != nil {
		t.Fatal(err)
	}
	return Deps{
		GetProcessFlow:   &usecases.GetProcessFlow{Repo: repo},
		ListProcessFlows: &usecases.ListProcessFlows{Repo: repo},
	}
}

func TestGetProcessFlow_ReturnsStagesInOrder(t *testing.T) {
	out, err := newFlowDeps(t).getProcessFlow(context.Background(), getProcessFlowInput{FlowId: "single-unit"})
	if err != nil {
		t.Fatal(err)
	}
	if out.FlowId != "single-unit" || out.Status != "ACTIVE" || out.Version != 1 {
		t.Fatalf("unexpected dto: %+v", out)
	}
	if len(out.Stages) != 2 || out.Stages[0].PathId != "PICK" || out.Stages[1].PathId != "PACK" || out.Stages[1].ProcessType != "PACK" {
		t.Fatalf("stages out of order: %+v", out.Stages)
	}
}

func TestGetProcessFlow_RetiredIsReadable_UnknownAndEmptyAreErrors(t *testing.T) {
	d := newFlowDeps(t)
	if out, err := d.getProcessFlow(context.Background(), getProcessFlowInput{FlowId: "old"}); err != nil || out.Status != "RETIRED" {
		t.Fatalf("retired flow: %+v %v", out, err)
	}
	if _, err := d.getProcessFlow(context.Background(), getProcessFlowInput{FlowId: "ghost"}); !errors.Is(err, usecases.ErrFlowNotFound) {
		t.Fatalf("unknown flow: got %v, want ErrFlowNotFound", err)
	}
	if _, err := d.getProcessFlow(context.Background(), getProcessFlowInput{}); err == nil {
		t.Fatal("empty flowId must be an error")
	}
}

func TestListProcessFlows_ActiveOnlyByDefault(t *testing.T) {
	d := newFlowDeps(t)
	out, err := d.listProcessFlows(context.Background(), listProcessFlowsInput{})
	if err != nil || len(out.Flows) != 1 || out.Flows[0].FlowId != "single-unit" {
		t.Fatalf("default view: %+v %v", out, err)
	}
	all := false
	out, err = d.listProcessFlows(context.Background(), listProcessFlowsInput{ActiveOnly: &all})
	if err != nil || len(out.Flows) != 2 || out.Flows[0].FlowId != "old" || out.Flows[1].FlowId != "single-unit" {
		t.Fatalf("all view (id order): %+v %v", out, err)
	}
}
