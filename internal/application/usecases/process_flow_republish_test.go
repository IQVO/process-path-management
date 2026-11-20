package usecases_test

import (
	"context"
	"errors"
	"testing"

	"github.com/claudioed/process-path-management/internal/application/usecases"
	"github.com/claudioed/process-path-management/internal/domain/processflow"
)

// republishedFlows runs a republish over three flows: c and a are active (a
// was revised once, so it is at version 2) and b is retired.
func republishedFlows(t *testing.T) (*flowRig, []processflow.ProcessFlowRevised, usecases.RepublishResult, *republishMetrics) {
	t.Helper()
	ctx := context.Background()
	r := newFlowRig(t)
	for _, id := range []processflow.FlowId{"c", "a", "b"} {
		if _, _, err := r.define.Execute(ctx, id, "flow "+string(id), flowStages(t, "PICK", "PICK", "PACK", "PACK")); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := r.define.Execute(ctx, "a", "flow a v2", flowStages(t, "PICK", "PICK")); err != nil {
		t.Fatal(err)
	}
	if err := r.retire.Execute(ctx, "b"); err != nil {
		t.Fatal(err)
	}

	pub := &fakePublisher{}
	m := &republishMetrics{}
	uc := &usecases.RepublishCatalogue{Repo: r.paths, CPTSchedules: newFakeCPTScheduleRepo(), Flows: r.flows, Publisher: pub, Clock: fixedClock{republishNow}, Metrics: m}
	res, err := uc.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var got []processflow.ProcessFlowRevised
	for _, e := range pub.events {
		if rev, ok := e.(processflow.ProcessFlowRevised); ok {
			got = append(got, rev)
		}
	}
	return r, got, res, m
}

// ADR 0042 + ADR 0038: the startup republish enqueues every ACTIVE flow as
// ProcessFlowRevised, ascending by id, and no retired flow.
func TestRepublishCatalogue_RepublishesActiveFlowsAsRevised_Sorted(t *testing.T) {
	_, got, res, m := republishedFlows(t)
	if res.Flows != 2 || res.Omitted != 0 {
		t.Fatalf("result %+v, want 2 flows", res)
	}
	if len(got) != 2 || got[0].FlowId != "a" || got[1].FlowId != "c" {
		t.Fatalf("want ProcessFlowRevised for a then c, got %+v", got)
	}
	if m.published["flow"] != 2 {
		t.Fatalf("metrics %v", m.published)
	}
}

// The payload is the CURRENT state at the CURRENT version, and the republish
// itself changes nothing.
func TestRepublishCatalogue_FlowEventCarriesCurrentStateAndVersion(t *testing.T) {
	r, got, _, _ := republishedFlows(t)
	if got[0].Version != 2 || got[0].Name != "flow a v2" || len(got[0].Stages) != 1 || got[1].Version != 1 {
		t.Fatalf("the republish must carry the CURRENT state and version: %+v", got)
	}
	if !got[0].At.Equal(republishNow) {
		t.Fatalf("the republish stamps the republish time, got %v", got[0].At)
	}
	if f, _ := r.flows.FindByID(context.Background(), "a"); f.Version() != 2 {
		t.Fatalf("republish moved the version to %d", f.Version())
	}
}

func TestRepublishCatalogue_WithoutAFlowRepoRepublishesNoFlows(t *testing.T) {
	r := newFlowRig(t)
	if _, _, err := r.define.Execute(context.Background(), "a", "n", flowStages(t, "PICK", "PICK")); err != nil {
		t.Fatal(err)
	}
	pub := &fakePublisher{}
	m := &republishMetrics{}
	uc := &usecases.RepublishCatalogue{Repo: r.paths, CPTSchedules: newFakeCPTScheduleRepo(), Publisher: pub, Clock: fixedClock{republishNow}, Metrics: m}
	res, err := uc.Execute(context.Background())
	if err != nil || res.Flows != 0 {
		t.Fatalf("res %+v err %v", res, err)
	}
	if _, counted := m.published["flow"]; counted {
		t.Fatalf("no flow kind is reported without a flow repo: %v", m.published)
	}
}

func TestRepublishCatalogue_FlowFailuresAreSurfaced(t *testing.T) {
	ctx := context.Background()
	build := func(t *testing.T) (*flowRig, *usecases.RepublishCatalogue, *republishMetrics) {
		r := newFlowRig(t)
		if _, _, err := r.define.Execute(ctx, "a", "n", flowStages(t, "PICK", "PICK")); err != nil {
			t.Fatal(err)
		}
		m := &republishMetrics{}
		return r, &usecases.RepublishCatalogue{Repo: r.paths, CPTSchedules: newFakeCPTScheduleRepo(), Flows: r.flows, Publisher: &fakePublisher{}, Clock: fixedClock{republishNow}, Metrics: m}, m
	}
	t.Run("lock", func(t *testing.T) {
		r, uc, m := build(t)
		r.flows.lockErr = errBoom
		if _, err := uc.Execute(ctx); !errors.Is(err, errBoom) || m.failed != 1 {
			t.Fatalf("err %v failed %d", err, m.failed)
		}
	})
	t.Run("find", func(t *testing.T) {
		r, uc, m := build(t)
		r.flows.findErr = errBoom
		if _, err := uc.Execute(ctx); !errors.Is(err, errBoom) || m.failed != 1 {
			t.Fatalf("err %v failed %d", err, m.failed)
		}
	})
	t.Run("publish", func(t *testing.T) {
		_, uc, m := build(t)
		uc.Publisher = &erroringPublisher{err: errBoom}
		// the path "PICK" is published first, so the failure surfaces there
		// or on the flow; either way it is counted and returned.
		if _, err := uc.Execute(ctx); !errors.Is(err, errBoom) || m.failed != 1 {
			t.Fatalf("err %v failed %d", err, m.failed)
		}
	})
	t.Run("a flow that vanished or was retired after the lock is skipped", func(t *testing.T) {
		r, uc, _ := build(t)
		uc.Flows = &vanishingFlowRepo{fakeFlowRepo: r.flows}
		res, err := uc.Execute(ctx)
		if err != nil || res.Flows != 0 {
			t.Fatalf("res %+v err %v", res, err)
		}
	})
	t.Run("the cap is exact and the overflow is reported", func(t *testing.T) {
		r, uc, _ := build(t)
		uc.Repo = newFakeRepo()
		for i := 0; i < usecases.MaxRepublishedPerKind+3; i++ {
			id := processflow.FlowId("f" + itoa(10000+i))
			st, _ := processflow.NewStage("PICK", processflow.ProcessTypePick)
			f, _ := processflow.Define(id, "n", []processflow.Stage{st}, republishNow)
			r.flows.flows[id] = f
		}
		res, err := uc.Execute(ctx)
		if err != nil || res.Flows != usecases.MaxRepublishedPerKind || res.Omitted != 4 {
			t.Fatalf("res %+v err %v, want %d flows and 4 omitted", res, err, usecases.MaxRepublishedPerKind)
		}
	})
}

// vanishingFlowRepo locks every id but then finds none of them: a flow that
// disappeared (or stopped being active) between the lock and the read.
type vanishingFlowRepo struct{ *fakeFlowRepo }

func (v *vanishingFlowRepo) FindByID(context.Context, processflow.FlowId) (*processflow.ProcessFlow, error) {
	return nil, nil
}
