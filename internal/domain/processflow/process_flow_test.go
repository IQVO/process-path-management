package processflow_test

import (
	"errors"
	"testing"
	"time"

	"github.com/claudioed/process-path-management/internal/domain/processflow"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

var (
	t0 = time.Date(2026, 11, 20, 8, 0, 0, 0, time.UTC)
	t1 = t0.Add(time.Hour)
)

func stage(t *testing.T, path string, pt processflow.ProcessType) processflow.Stage {
	t.Helper()
	s, err := processflow.NewStage(shared.PathId(path), pt)
	if err != nil {
		t.Fatalf("NewStage(%q, %q): %v", path, pt, err)
	}
	return s
}

func pickPack(t *testing.T) []processflow.Stage {
	t.Helper()
	return []processflow.Stage{
		stage(t, "PICK", processflow.ProcessTypePick),
		stage(t, "PACK", processflow.ProcessTypePack),
	}
}

func TestNewStage(t *testing.T) {
	for _, pt := range processflow.ProcessTypes() {
		if _, err := processflow.NewStage("P", pt); err != nil {
			t.Errorf("process type %q must be accepted: %v", pt, err)
		}
	}
	if got := len(processflow.ProcessTypes()); got != 8 {
		t.Errorf("the closed set has 8 process types, got %d", got)
	}
	tests := []struct {
		name string
		path shared.PathId
		pt   processflow.ProcessType
		want error
	}{
		{"empty path", "", processflow.ProcessTypePick, processflow.ErrEmptyStagePathId},
		{"unknown type", "P", "WELD", processflow.ErrInvalidProcessType},
		{"lower-case type", "P", "pick", processflow.ErrInvalidProcessType},
		{"empty type", "P", "", processflow.ErrInvalidProcessType},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := processflow.NewStage(tc.path, tc.pt); !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}

func TestDefine(t *testing.T) {
	f, err := processflow.Define("pick-pack", "Pick then pack", pickPack(t), t0)
	if err != nil {
		t.Fatalf("Define: %v", err)
	}
	if f.ID() != "pick-pack" || f.Name() != "Pick then pack" {
		t.Errorf("identity/name not kept: %q %q", f.ID(), f.Name())
	}
	if f.Version() != 1 {
		t.Errorf("a new flow starts at version 1, got %d", f.Version())
	}
	if f.Status() != processflow.StatusActive || !f.IsActive() {
		t.Errorf("a new flow is ACTIVE, got %q", f.Status())
	}
	if !f.CreatedAt().Equal(t0) || !f.UpdatedAt().Equal(t0) {
		t.Errorf("timestamps must be the define time")
	}
	if ids := f.StagePathIds(); len(ids) != 2 || ids[0] != "PICK" || ids[1] != "PACK" {
		t.Errorf("stage order must be kept, got %v", ids)
	}
}

func TestDefine_Invalid(t *testing.T) {
	dup := []processflow.Stage{
		stage(t, "PICK", processflow.ProcessTypePick),
		stage(t, "PICK", processflow.ProcessTypePack),
	}
	tests := []struct {
		name   string
		id     processflow.FlowId
		fname  string
		stages []processflow.Stage
		want   error
	}{
		{"empty id", "", "n", pickPack(t), processflow.ErrEmptyFlowId},
		{"empty name", "f", "", pickPack(t), processflow.ErrEmptyName},
		{"no stages", "f", "n", nil, processflow.ErrNoStages},
		{"duplicate path", "f", "n", dup, processflow.ErrDuplicateStagePath},
		{"zero stage", "f", "n", []processflow.Stage{{}}, processflow.ErrEmptyStagePathId},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := processflow.Define(tc.id, tc.fname, tc.stages, t0); !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}

func TestValidateDefinition(t *testing.T) {
	if err := processflow.ValidateDefinition("n", pickPack(t)); err != nil {
		t.Fatalf("a valid definition is accepted: %v", err)
	}
	if err := processflow.ValidateDefinition("", pickPack(t)); !errors.Is(err, processflow.ErrEmptyName) {
		t.Fatalf("got %v", err)
	}
}

func TestStageAccessors(t *testing.T) {
	s := stage(t, "REBIN-1", processflow.ProcessTypeRebin)
	if s.PathId() != "REBIN-1" || s.ProcessType() != processflow.ProcessTypeRebin {
		t.Fatalf("accessors wrong: %v %v", s.PathId(), s.ProcessType())
	}
}

func TestRevise(t *testing.T) {
	f, _ := processflow.Define("f", "n", pickPack(t), t0)
	reversed := []processflow.Stage{stage(t, "PACK", processflow.ProcessTypePack), stage(t, "PICK", processflow.ProcessTypePick)}
	otherType := []processflow.Stage{stage(t, "PACK", processflow.ProcessTypePack), stage(t, "PICK", processflow.ProcessTypeRelocate)}
	oneStage := []processflow.Stage{stage(t, "PACK", processflow.ProcessTypePack)}

	// Each step applies to the flow the previous one left behind.
	steps := []struct {
		name        string
		newName     string
		stages      []processflow.Stage
		wantChanged bool
		wantVersion int
	}{
		{"nothing changes", "n", pickPack(t), false, 1},
		{"a new name", "renamed", pickPack(t), true, 2},
		{"a different order", "renamed", reversed, true, 3},
		{"a different process type", "renamed", otherType, true, 4},
		{"a different length", "renamed", oneStage, true, 5},
		{"the same again", "renamed", oneStage, false, 5},
	}
	for _, st := range steps {
		changed, err := f.Revise(st.newName, st.stages, t1)
		if err != nil || changed != st.wantChanged || f.Version() != st.wantVersion {
			t.Fatalf("%s: changed=%v err=%v version=%d, want changed=%v version=%d", st.name, changed, err, f.Version(), st.wantChanged, st.wantVersion)
		}
	}
	if !f.UpdatedAt().Equal(t1) {
		t.Errorf("an accepted change stamps updatedAt")
	}
}

func TestRevise_NoChangeKeepsTime(t *testing.T) {
	f, _ := processflow.Define("f", "n", pickPack(t), t0)
	if changed, err := f.Revise("n", pickPack(t), t1); err != nil || changed {
		t.Fatalf("changed=%v err=%v", changed, err)
	}
	if !f.UpdatedAt().Equal(t0) {
		t.Fatalf("updatedAt moved to %v", f.UpdatedAt())
	}
}

func TestRevise_InvalidIsRejectedAndKeepsTheVersion(t *testing.T) {
	f, _ := processflow.Define("f", "n", pickPack(t), t0)
	if _, err := f.Revise("", pickPack(t), t1); !errors.Is(err, processflow.ErrEmptyName) {
		t.Fatalf("got %v", err)
	}
	if f.Version() != 1 {
		t.Fatalf("version moved to %d", f.Version())
	}
}

func TestRetire(t *testing.T) {
	f, _ := processflow.Define("f", "n", pickPack(t), t0)
	if !f.Retire(t1) {
		t.Fatal("retiring an ACTIVE flow changes it")
	}
	if f.Status() != processflow.StatusRetired || f.IsActive() || f.Version() != 2 || !f.UpdatedAt().Equal(t1) {
		t.Fatalf("status %q version %d updatedAt %v", f.Status(), f.Version(), f.UpdatedAt())
	}
	if f.Retire(t1.Add(time.Hour)) {
		t.Fatal("retiring a RETIRED flow changes nothing")
	}
	if f.Version() != 2 || !f.UpdatedAt().Equal(t1) {
		t.Fatalf("a second retire must not move version/time: %d %v", f.Version(), f.UpdatedAt())
	}
	// A retired flow cannot be revised, even by an identical definition.
	if _, err := f.Revise("n", pickPack(t), t1); !errors.Is(err, processflow.ErrFlowRetired) {
		t.Fatalf("got %v, want ErrFlowRetired", err)
	}
}

func TestRehydrate_And_Copies(t *testing.T) {
	stages := pickPack(t)
	f := processflow.Rehydrate("f", "n", stages, processflow.StatusRetired, t0, t1, 7)
	if f.Version() != 7 || f.Status() != processflow.StatusRetired || !f.CreatedAt().Equal(t0) {
		t.Fatalf("rehydrate lost state")
	}
	// Stages() hands out a copy.
	got := f.Stages()
	got[0] = stage(t, "X", processflow.ProcessTypeSlam)
	if f.Stages()[0].PathId() != "PICK" {
		t.Fatal("mutating the returned slice must not change the aggregate")
	}
	// Define copies its input.
	in := pickPack(t)
	g, _ := processflow.Define("g", "n", in, t0)
	in[0] = stage(t, "X", processflow.ProcessTypeSlam)
	if g.Stages()[0].PathId() != "PICK" {
		t.Fatal("Define must copy the stages it is given")
	}
}

func TestEvents_Defined(t *testing.T) {
	f, _ := processflow.Define("f", "Flow", pickPack(t), t0)
	d := f.DefinedEvent(t0)
	if d.EventName() != "ProcessFlowDefined" || !d.OccurredAt().Equal(t0) {
		t.Fatalf("defined: %q %v", d.EventName(), d.OccurredAt())
	}
	if d.FlowId != "f" || d.Name != "Flow" || d.Status != processflow.StatusActive || d.Version != 1 {
		t.Fatalf("defined state: %+v", d.State)
	}
	want := []processflow.StageSnapshot{{PathId: "PICK", ProcessType: "PICK"}, {PathId: "PACK", ProcessType: "PACK"}}
	if len(d.Stages) != 2 || d.Stages[0] != want[0] || d.Stages[1] != want[1] {
		t.Fatalf("defined stages: %+v", d.Stages)
	}
}

func TestEvents_Revised(t *testing.T) {
	f, _ := processflow.Define("f", "Flow", pickPack(t), t0)
	r := f.RevisedEvent(t1)
	if r.EventName() != "ProcessFlowRevised" || !r.OccurredAt().Equal(t1) || r.Version != 1 {
		t.Fatalf("revised: %q %v v%d", r.EventName(), r.OccurredAt(), r.Version)
	}
}

func TestEvents_RetiredCarriesTheFullStateAtTheNewVersion(t *testing.T) {
	f, _ := processflow.Define("f", "Flow", pickPack(t), t0)
	f.Retire(t1)
	x := f.RetiredEvent(t1)
	if x.EventName() != "ProcessFlowRetired" || !x.OccurredAt().Equal(t1) {
		t.Fatalf("retired: %q %v", x.EventName(), x.OccurredAt())
	}
	if x.Status != processflow.StatusRetired || x.Version != 2 || len(x.Stages) != 2 || x.Name != "Flow" {
		t.Fatalf("retired state: %+v", x.State)
	}
}
