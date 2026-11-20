package a2a

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/process-path-management/internal/adapters/outbound/memory"
	"github.com/claudioed/process-path-management/internal/application/ports"
	"github.com/claudioed/process-path-management/internal/application/usecases"
	"github.com/claudioed/process-path-management/internal/domain/processflow"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// flowDeps wires the path reads and the two flow reads over real in-memory
// repositories, seeded with ACTIVE "single-unit" (PICK, PACK) and RETIRED "old".
func flowDeps(t *testing.T) Deps {
	t.Helper()
	repo := memory.NewProcessFlowRepo()
	mk := func(id string, paths ...string) *processflow.ProcessFlow {
		var stages []processflow.Stage
		for _, p := range paths {
			s, err := processflow.NewStage(shared.PathId(p), processflow.ProcessType(p))
			if err != nil {
				t.Fatal(err)
			}
			stages = append(stages, s)
		}
		f, err := processflow.Define(processflow.FlowId(id), id, stages, base)
		if err != nil {
			t.Fatal(err)
		}
		return f
	}
	ctx := context.Background()
	if err := repo.Create(ctx, mk("single-unit", "PICK", "PACK")); err != nil {
		t.Fatal(err)
	}
	old := mk("old", "PICK")
	if err := repo.Create(ctx, old); err != nil {
		t.Fatal(err)
	}
	old.Retire(base.Add(time.Hour))
	if err := repo.Save(ctx, old); err != nil {
		t.Fatal(err)
	}
	d := depsOver(seededRepo(t))
	d.GetProcessFlow = &usecases.GetProcessFlow{Repo: repo}
	d.ListProcessFlows = &usecases.ListProcessFlows{Repo: repo}
	return d
}

func TestWireGetProcessFlowReturnsTheStagesInOrder(t *testing.T) {
	w := newWire(t, flowDeps(t))
	env, text := w.skill(t, skillGetProcessFlow, map[string]any{"flowId": "single-unit"})
	f := env.Data.ProcessFlow
	if env.Problem != nil || f == nil || f.FlowId != "single-unit" || f.Status != "ACTIVE" || f.Version != 1 ||
		len(f.Stages) != 2 || f.Stages[0].PathId != "PICK" || f.Stages[1].PathId != "PACK" || f.Stages[1].ProcessType != "PACK" {
		t.Fatalf("flow = %+v problem = %+v", f, env.Problem)
	}
	if text != "single-unit: active, version 1, stages PICK(PICK) > PACK(PACK)" {
		t.Fatalf("summary = %q", text)
	}
}

func TestWireGetProcessFlowRetiredIsReadable(t *testing.T) {
	w := newWire(t, flowDeps(t))
	env, _ := w.skill(t, skillGetProcessFlow, map[string]any{"flowId": "old"})
	if f := env.Data.ProcessFlow; f == nil || f.Status != "RETIRED" || f.Version != 2 {
		t.Fatalf("retired flow = %+v", f)
	}
}

func TestWireGetProcessFlowProblems(t *testing.T) {
	w := newWire(t, flowDeps(t))
	env, _ := w.skill(t, skillGetProcessFlow, map[string]any{"flowId": "ghost"})
	wantProblem(t, env, 404, "flow-not-found")
	for name, args := range map[string]map[string]any{
		"blank":   {"flowId": "  "},
		"missing": {},
		"unknown": {"flowId": "x", "force": true},
		"typed":   {"flowId": 7},
	} {
		env, _ := w.skill(t, skillGetProcessFlow, args)
		if env.Problem == nil || env.Problem.Status != 400 {
			t.Errorf("%s: want a 400 problem, got %+v", name, env)
		}
	}
}

func TestWireListProcessFlowsActiveOnlyByDefault(t *testing.T) {
	w := newWire(t, flowDeps(t))
	def, text := w.skill(t, skillListProcessFlows, map[string]any{})
	all, _ := w.skill(t, skillListProcessFlows, map[string]any{"activeOnly": false})
	if l := def.Data.ProcessFlows; l == nil || !l.ActiveOnly || len(l.Flows) != 1 || l.Flows[0].FlowId != "single-unit" {
		t.Fatalf("default = %+v", l)
	}
	if text != "1 process flows (active only): single-unit" {
		t.Fatalf("summary = %q", text)
	}
	if l := all.Data.ProcessFlows; l == nil || l.ActiveOnly || len(l.Flows) != 2 || l.Flows[0].FlowId != "old" {
		t.Fatalf("all = %+v", l)
	}
	bad, _ := w.skill(t, skillListProcessFlows, map[string]any{"activeOnlyy": false})
	wantProblem(t, bad, 400, "invalid-argument")
}

// With the flow use cases unwired the two skills are simply not served.
func TestWireFlowSkillsAreUnknownWhenNotWired(t *testing.T) {
	w := newWire(t, depsOver(seededRepo(t)))
	env, _ := w.skill(t, skillGetProcessFlow, map[string]any{"flowId": "single-unit"})
	wantProblem(t, env, 400, "unknown-skill")
}

type failingFlowRepo struct{ ports.ProcessFlowRepo }

func (failingFlowRepo) FindByID(context.Context, processflow.FlowId) (*processflow.ProcessFlow, error) {
	return nil, errBoom
}

func (failingFlowRepo) ListPage(context.Context, bool, processflow.FlowId, int) ([]*processflow.ProcessFlow, error) {
	return nil, errBoom
}

// A failing store is answered as the same opaque 500 as for the path skills:
// the cause (a DSN with a password) never reaches the calling agent.
func TestWireFlowSkillsHideInternalErrors(t *testing.T) {
	d := depsOver(seededRepo(t))
	d.GetProcessFlow = &usecases.GetProcessFlow{Repo: failingFlowRepo{}}
	d.ListProcessFlows = &usecases.ListProcessFlows{Repo: failingFlowRepo{}}
	w := newWire(t, d)
	for skill, args := range map[string]map[string]any{skillGetProcessFlow: {"flowId": "x"}, skillListProcessFlows: {}} {
		env, text := w.skill(t, skill, args)
		wantProblem(t, env, 500, "internal-error")
		for _, s := range []string{env.Problem.Detail, text} {
			if strings.Contains(s, "hunter2") || strings.Contains(s, "10.0.0.7") {
				t.Fatalf("%s leaks the cause: %q", skill, s)
			}
		}
	}
}
