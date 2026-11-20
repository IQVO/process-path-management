package usecases_test

import (
	"context"
	"errors"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/claudioed/process-path-management/internal/application/ports"
	"github.com/claudioed/process-path-management/internal/application/usecases"
	"github.com/claudioed/process-path-management/internal/domain/processflow"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// fakeFlowRepo is a small in-memory ports.ProcessFlowRepo that, like the
// real adapters, stores copies and applies the version guard on Save.
type fakeFlowRepo struct {
	mu       sync.Mutex
	flows    map[processflow.FlowId]*processflow.ProcessFlow
	findErr  error
	saveErr  error
	listErr  error
	lockErr  error
	refsErr  error
	saveHook func() // runs inside Save before the guard, to model a racing writer
}

func newFakeFlowRepo() *fakeFlowRepo {
	return &fakeFlowRepo{flows: make(map[processflow.FlowId]*processflow.ProcessFlow)}
}

func copyFlow(f *processflow.ProcessFlow) *processflow.ProcessFlow {
	if f == nil {
		return nil
	}
	return processflow.Rehydrate(f.ID(), f.Name(), f.Stages(), f.Status(), f.CreatedAt(), f.UpdatedAt(), f.Version())
}

func (r *fakeFlowRepo) Create(_ context.Context, f *processflow.ProcessFlow) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.saveErr != nil {
		return r.saveErr
	}
	if _, ok := r.flows[f.ID()]; ok {
		return ports.ErrAlreadyExists
	}
	r.flows[f.ID()] = copyFlow(f)
	return nil
}

func (r *fakeFlowRepo) Save(_ context.Context, f *processflow.ProcessFlow) error {
	if r.saveHook != nil {
		r.saveHook()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.saveErr != nil {
		return r.saveErr
	}
	if stored, ok := r.flows[f.ID()]; !ok || stored.Version() != f.Version()-1 {
		return ports.ErrConcurrentModification
	}
	r.flows[f.ID()] = copyFlow(f)
	return nil
}

func (r *fakeFlowRepo) FindByID(_ context.Context, id processflow.FlowId) (*processflow.ProcessFlow, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.findErr != nil {
		return nil, r.findErr
	}
	return copyFlow(r.flows[id]), nil
}

func (r *fakeFlowRepo) ListPage(_ context.Context, activeOnly bool, after processflow.FlowId, size int) ([]*processflow.ProcessFlow, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.listErr != nil {
		return nil, r.listErr
	}
	var ids []processflow.FlowId
	for id, f := range r.flows {
		if id > after && (!activeOnly || f.IsActive()) {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	if len(ids) > size {
		ids = ids[:size]
	}
	out := make([]*processflow.ProcessFlow, 0, len(ids))
	for _, id := range ids {
		out = append(out, copyFlow(r.flows[id]))
	}
	return out, nil
}

func (r *fakeFlowRepo) ListActiveFlowIDsReferencingPath(_ context.Context, id shared.PathId) ([]processflow.FlowId, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.refsErr != nil {
		return nil, r.refsErr
	}
	var out []processflow.FlowId
	for fid, f := range r.flows {
		if !f.IsActive() {
			continue
		}
		for _, p := range f.StagePathIds() {
			if p == id {
				out = append(out, fid)
				break
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

func (r *fakeFlowRepo) LockActiveForShare(_ context.Context) ([]processflow.FlowId, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lockErr != nil {
		return nil, r.lockErr
	}
	var out []processflow.FlowId
	for id, f := range r.flows {
		if f.IsActive() {
			out = append(out, id)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

var errBoom = errors.New("boom")

func flowStages(t *testing.T, pairs ...string) []processflow.Stage {
	t.Helper()
	var out []processflow.Stage
	for i := 0; i+1 < len(pairs); i += 2 {
		s, err := processflow.NewStage(shared.PathId(pairs[i]), processflow.ProcessType(pairs[i+1]))
		if err != nil {
			t.Fatalf("NewStage: %v", err)
		}
		out = append(out, s)
	}
	return out
}

type flowRig struct {
	paths  *fakeRepo
	flows  *fakeFlowRepo
	pub    *fakePublisher
	define *usecases.DefineProcessFlow
	retire *usecases.RetireProcessFlow
}

func newFlowRig(t *testing.T) *flowRig {
	t.Helper()
	r := &flowRig{paths: newFakeRepo(), flows: newFakeFlowRepo(), pub: &fakePublisher{}}
	clock := fixedClock{time.Date(2026, 11, 20, 8, 0, 0, 0, time.UTC)}
	r.define = &usecases.DefineProcessFlow{Repo: r.flows, ProcessPathRepo: r.paths, Publisher: r.pub, Clock: clock}
	r.retire = &usecases.RetireProcessFlow{Repo: r.flows, Publisher: r.pub, Clock: clock}
	seedActivePath(t, r.paths, "PICK")
	seedActivePath(t, r.paths, "REBIN")
	seedActivePath(t, r.paths, "PACK")
	return r
}

func (r *flowRig) lastEvent(t *testing.T) shared.DomainEvent {
	t.Helper()
	e := r.pub.last()
	if e == nil {
		t.Fatal("no event was published")
	}
	return e
}

func TestDefineProcessFlow_First_CreatesAtVersion1AndPublishesDefined(t *testing.T) {
	r := newFlowRig(t)
	f, created, err := r.define.Execute(context.Background(), "f1", "Pick pack", flowStages(t, "PICK", "PICK", "PACK", "PACK"))
	if err != nil || !created {
		t.Fatalf("created=%v err=%v", created, err)
	}
	if f.Version() != 1 || !f.IsActive() {
		t.Fatalf("version %d status %q", f.Version(), f.Status())
	}
	ev, ok := r.lastEvent(t).(processflow.ProcessFlowDefined)
	if !ok {
		t.Fatalf("want ProcessFlowDefined, got %T", r.lastEvent(t))
	}
	if ev.FlowId != "f1" || ev.Version != 1 || len(ev.Stages) != 2 {
		t.Fatalf("event %+v", ev)
	}
}

func TestDefineProcessFlow_Revise_BumpsVersionAndPublishesRevised(t *testing.T) {
	r := newFlowRig(t)
	ctx := context.Background()
	if _, _, err := r.define.Execute(ctx, "f1", "Pick pack", flowStages(t, "PICK", "PICK", "PACK", "PACK")); err != nil {
		t.Fatal(err)
	}
	f, created, err := r.define.Execute(ctx, "f1", "Pick rebin pack", flowStages(t, "PICK", "PICK", "REBIN", "REBIN", "PACK", "PACK"))
	if err != nil || created {
		t.Fatalf("created=%v err=%v", created, err)
	}
	if f.Version() != 2 {
		t.Fatalf("version %d, want 2", f.Version())
	}
	ev, ok := r.lastEvent(t).(processflow.ProcessFlowRevised)
	if !ok || ev.Version != 2 || len(ev.Stages) != 3 || ev.Name != "Pick rebin pack" {
		t.Fatalf("event %T %+v", r.lastEvent(t), ev)
	}
	stored, _ := r.flows.FindByID(ctx, "f1")
	if stored.Version() != 2 {
		t.Fatalf("stored version %d", stored.Version())
	}
}

func TestDefineProcessFlow_NothingChanges_NoEventSameVersion(t *testing.T) {
	r := newFlowRig(t)
	ctx := context.Background()
	st := flowStages(t, "PICK", "PICK", "PACK", "PACK")
	if _, _, err := r.define.Execute(ctx, "f1", "n", st); err != nil {
		t.Fatal(err)
	}
	f, created, err := r.define.Execute(ctx, "f1", "n", st)
	if err != nil || created {
		t.Fatalf("created=%v err=%v", created, err)
	}
	if f.Version() != 1 || r.pub.count() != 1 {
		t.Fatalf("version %d, events %d: an unchanged PUT raises nothing", f.Version(), r.pub.count())
	}
}

func TestDefineProcessFlow_Rejections_PublishNothingAndChangeNothing(t *testing.T) {
	ctx := context.Background()
	two := flowStages(t, "PICK", "PICK", "PACK", "PACK")
	tests := []struct {
		name   string
		id     processflow.FlowId
		fname  string
		stages []processflow.Stage
		want   error
	}{
		{"empty id", "", "n", two, processflow.ErrEmptyFlowId},
		{"empty name", "f", "", two, processflow.ErrEmptyName},
		{"no stages", "f", "n", nil, processflow.ErrNoStages},
		{"duplicate path", "f", "n", flowStages(t, "PICK", "PICK", "PICK", "PACK"), processflow.ErrDuplicateStagePath},
		{"unknown path", "f", "n", flowStages(t, "PICK", "PICK", "GHOST", "PACK"), usecases.ErrIneligibleStagePath},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := newFlowRig(t)
			_, _, err := r.define.Execute(ctx, tc.id, tc.fname, tc.stages)
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
			if r.pub.count() != 0 {
				t.Fatalf("a rejected define must publish nothing, got %d", r.pub.count())
			}
			if got, _ := r.flows.FindByID(ctx, "f"); got != nil {
				t.Fatalf("a rejected define must store nothing")
			}
		})
	}
}

func TestDefineProcessFlow_DeactivatedPath_IsIneligibleAndNamed(t *testing.T) {
	r := newFlowRig(t)
	deact := &usecases.DeactivatePath{Repo: r.paths, Publisher: &fakePublisher{}, Clock: fixedClock{time.Now()}}
	if err := deact.Execute(context.Background(), "REBIN"); err != nil {
		t.Fatal(err)
	}
	_, _, err := r.define.Execute(context.Background(), "f", "n", flowStages(t, "PICK", "PICK", "REBIN", "REBIN"))
	if !errors.Is(err, usecases.ErrIneligibleStagePath) || !strings.Contains(err.Error(), "REBIN") {
		t.Fatalf("got %v, want ErrIneligibleStagePath naming REBIN", err)
	}
}

func TestDefineProcessFlow_LocksStagePathsSortedInsideTheUnitOfWork(t *testing.T) {
	r := newFlowRig(t)
	r.define.UnitOfWork = recordingUoW{repo: r.paths}
	if _, _, err := r.define.Execute(context.Background(), "f", "n", flowStages(t, "PACK", "PACK", "PICK", "PICK")); err != nil {
		t.Fatal(err)
	}
	log := strings.Join(r.paths.callLog(), " ")
	if !strings.Contains(log, "uow-begin for-share:PACK,PICK uow-end") {
		t.Fatalf("paths must be share-locked ascending inside the unit of work, got %q", log)
	}
}

func TestDefineProcessFlow_RetiredFlow_CannotBeRevised(t *testing.T) {
	r := newFlowRig(t)
	ctx := context.Background()
	st := flowStages(t, "PICK", "PICK")
	if _, _, err := r.define.Execute(ctx, "f", "n", st); err != nil {
		t.Fatal(err)
	}
	if err := r.retire.Execute(ctx, "f"); err != nil {
		t.Fatal(err)
	}
	before := r.pub.count()
	for _, name := range []string{"n", "other"} {
		_, _, err := r.define.Execute(ctx, "f", name, st)
		if !errors.Is(err, processflow.ErrFlowRetired) {
			t.Fatalf("name %q: got %v, want ErrFlowRetired", name, err)
		}
	}
	if r.pub.count() != before {
		t.Fatalf("a rejected revise must publish nothing")
	}
}

func TestDefineProcessFlow_ConcurrentRevise_IsAConcurrentModification(t *testing.T) {
	r := newFlowRig(t)
	ctx := context.Background()
	if _, _, err := r.define.Execute(ctx, "f", "n", flowStages(t, "PICK", "PICK")); err != nil {
		t.Fatal(err)
	}
	// A racing writer commits version 2 between this caller's load and Save.
	r.flows.saveHook = func() {
		r.flows.mu.Lock()
		r.flows.flows["f"] = processflow.Rehydrate("f", "racer", flowStages(t, "PACK", "PACK"), processflow.StatusActive, time.Now(), time.Now(), 2)
		r.flows.mu.Unlock()
	}
	_, _, err := r.define.Execute(ctx, "f", "mine", flowStages(t, "REBIN", "REBIN"))
	if !errors.Is(err, ports.ErrConcurrentModification) {
		t.Fatalf("got %v, want ErrConcurrentModification", err)
	}
	if r.pub.count() != 1 {
		t.Fatalf("the loser must publish nothing, got %d events", r.pub.count())
	}
}

func TestDefineProcessFlow_ConcurrentFirstDefine_LoserSeesConcurrentModification(t *testing.T) {
	r := newFlowRig(t)
	// The flow appears between FindByID (nil) and Create.
	racing := &createRaceRepo{fakeFlowRepo: r.flows}
	r.define.Repo = racing
	_, _, err := r.define.Execute(context.Background(), "f", "n", flowStages(t, "PICK", "PICK"))
	if !errors.Is(err, ports.ErrConcurrentModification) {
		t.Fatalf("got %v, want ErrConcurrentModification", err)
	}
	if r.pub.count() != 0 {
		t.Fatalf("the loser must publish nothing")
	}
}

type createRaceRepo struct{ *fakeFlowRepo }

func (c *createRaceRepo) Create(ctx context.Context, f *processflow.ProcessFlow) error {
	_ = c.fakeFlowRepo.Create(ctx, f) // the winner
	return c.fakeFlowRepo.Create(ctx, f)
}

func TestDefineProcessFlow_RepoAndPublisherFailuresPropagate(t *testing.T) {
	ctx := context.Background()
	st := flowStages(t, "PICK", "PICK")
	t.Run("find", func(t *testing.T) {
		r := newFlowRig(t)
		r.flows.findErr = errBoom
		if _, _, err := r.define.Execute(ctx, "f", "n", st); !errors.Is(err, errBoom) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("create", func(t *testing.T) {
		r := newFlowRig(t)
		r.flows.saveErr = errBoom
		if _, _, err := r.define.Execute(ctx, "f", "n", st); !errors.Is(err, errBoom) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("lock paths", func(t *testing.T) {
		r := newFlowRig(t)
		r.define.ProcessPathRepo = &erroringRepo{fakeRepo: r.paths, findErr: errBoom}
		if _, _, err := r.define.Execute(ctx, "f", "n", st); !errors.Is(err, errBoom) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("publish", func(t *testing.T) {
		r := newFlowRig(t)
		r.define.Publisher = &erroringPublisher{err: errBoom}
		if _, _, err := r.define.Execute(ctx, "f", "n", st); !errors.Is(err, errBoom) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("save on revise", func(t *testing.T) {
		r := newFlowRig(t)
		if _, _, err := r.define.Execute(ctx, "f", "n", st); err != nil {
			t.Fatal(err)
		}
		r.flows.saveErr = errBoom
		if _, _, err := r.define.Execute(ctx, "f", "m", st); !errors.Is(err, errBoom) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("publish on revise", func(t *testing.T) {
		r := newFlowRig(t)
		if _, _, err := r.define.Execute(ctx, "f", "n", st); err != nil {
			t.Fatal(err)
		}
		r.define.Publisher = &erroringPublisher{err: errBoom}
		if _, _, err := r.define.Execute(ctx, "f", "m", st); !errors.Is(err, errBoom) {
			t.Fatalf("got %v", err)
		}
	})
}

func TestRetireProcessFlow(t *testing.T) {
	ctx := context.Background()
	r := newFlowRig(t)
	if _, _, err := r.define.Execute(ctx, "f", "n", flowStages(t, "PICK", "PICK", "PACK", "PACK")); err != nil {
		t.Fatal(err)
	}
	if err := r.retire.Execute(ctx, "f"); err != nil {
		t.Fatal(err)
	}
	ev, ok := r.lastEvent(t).(processflow.ProcessFlowRetired)
	if !ok || ev.Status != processflow.StatusRetired || ev.Version != 2 || len(ev.Stages) != 2 || ev.Name != "n" {
		t.Fatalf("want the full state RETIRED at version 2, got %T %+v", r.lastEvent(t), ev)
	}
	// Retiring again is an idempotent success: no event, no version move.
	n := r.pub.count()
	if err := r.retire.Execute(ctx, "f"); err != nil {
		t.Fatalf("second retire: %v", err)
	}
	if r.pub.count() != n {
		t.Fatalf("a second retire must publish nothing")
	}
	if stored, _ := r.flows.FindByID(ctx, "f"); stored.Version() != 2 {
		t.Fatalf("version %d, want 2", stored.Version())
	}
	// Unknown flow.
	if err := r.retire.Execute(ctx, "ghost"); !errors.Is(err, usecases.ErrFlowNotFound) {
		t.Fatalf("got %v, want ErrFlowNotFound", err)
	}
}

func TestRetireProcessFlow_Failures(t *testing.T) {
	ctx := context.Background()
	r := newFlowRig(t)
	if _, _, err := r.define.Execute(ctx, "f", "n", flowStages(t, "PICK", "PICK")); err != nil {
		t.Fatal(err)
	}
	r.flows.findErr = errBoom
	if err := r.retire.Execute(ctx, "f"); !errors.Is(err, errBoom) {
		t.Fatalf("find: %v", err)
	}
	r.flows.findErr = nil
	r.flows.saveErr = errBoom
	if err := r.retire.Execute(ctx, "f"); !errors.Is(err, errBoom) {
		t.Fatalf("save: %v", err)
	}
	r.flows.saveErr = nil
	r.retire.Publisher = &erroringPublisher{err: errBoom}
	if err := r.retire.Execute(ctx, "f"); !errors.Is(err, errBoom) {
		t.Fatalf("publish: %v", err)
	}
}

// seededFlows defines flows b, a, c (in that order) and retires b.
func seededFlows(t *testing.T) *flowRig {
	t.Helper()
	r := newFlowRig(t)
	for _, id := range []processflow.FlowId{"b", "a", "c"} {
		if _, _, err := r.define.Execute(context.Background(), id, "n", flowStages(t, "PICK", "PICK")); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.retire.Execute(context.Background(), "b"); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestGetProcessFlow(t *testing.T) {
	ctx := context.Background()
	r := seededFlows(t)
	get := &usecases.GetProcessFlow{Repo: r.flows}

	if _, err := get.Execute(ctx, "ghost"); !errors.Is(err, usecases.ErrFlowNotFound) {
		t.Fatalf("got %v", err)
	}
	if f, err := get.Execute(ctx, "b"); err != nil || f.IsActive() {
		t.Fatalf("a retired flow is still readable: %v %v", f, err)
	}
	r.flows.findErr = errBoom
	if _, err := get.Execute(ctx, "a"); !errors.Is(err, errBoom) {
		t.Fatalf("get find error: %v", err)
	}
}

func flowIDs(fs []*processflow.ProcessFlow) []processflow.FlowId {
	out := make([]processflow.FlowId, len(fs))
	for i, f := range fs {
		out[i] = f.ID()
	}
	return out
}

func TestListProcessFlows(t *testing.T) {
	ctx := context.Background()
	r := seededFlows(t)
	list := &usecases.ListProcessFlows{Repo: r.flows}

	tests := []struct {
		name       string
		activeOnly bool
		want       []processflow.FlowId
	}{
		{"active only", true, []processflow.FlowId{"a", "c"}},
		{"all", false, []processflow.FlowId{"a", "b", "c"}},
	}
	for _, tc := range tests {
		got, err := list.Execute(ctx, tc.activeOnly)
		if err != nil || !slices.Equal(flowIDs(got), tc.want) {
			t.Fatalf("%s: %v %v, want %v", tc.name, flowIDs(got), err, tc.want)
		}
	}
	page, err := list.ExecutePage(ctx, false, "a", 1)
	if err != nil || !slices.Equal(flowIDs(page), []processflow.FlowId{"b"}) {
		t.Fatalf("page after a: %v %v", flowIDs(page), err)
	}
	r.flows.listErr = errBoom
	if _, err := list.Execute(ctx, true); !errors.Is(err, errBoom) {
		t.Fatalf("list error: %v", err)
	}
}

func TestListProcessFlows_Execute_WalksEveryPage(t *testing.T) {
	ctx := context.Background()
	repo := newFakeFlowRepo()
	const n = 1203 // more than two pages of 500
	for i := 0; i < n; i++ {
		id := processflow.FlowId("f" + strings.Repeat("0", 5-len(itoa(i))) + itoa(i))
		st, _ := processflow.NewStage("PICK", processflow.ProcessTypePick)
		f, _ := processflow.Define(id, "n", []processflow.Stage{st}, time.Now())
		repo.flows[id] = f
	}
	got, err := (&usecases.ListProcessFlows{Repo: repo}).Execute(ctx, true)
	if err != nil || len(got) != n {
		t.Fatalf("got %d flows (%v), want %d", len(got), err, n)
	}
	for i := 1; i < len(got); i++ {
		if got[i-1].ID() >= got[i].ID() {
			t.Fatalf("not in id order at %d", i)
		}
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for ; i > 0; i /= 10 {
		b = append([]byte{byte('0' + i%10)}, b...)
	}
	return string(b)
}

// ADR 0042: a path an ACTIVE flow uses cannot be deactivated; the refusal
// names the flows; retiring (or revising away) the flow lifts it.
func TestDeactivatePath_UsedByActiveFlows_IsRefusedNamingTheFlows(t *testing.T) {
	ctx := context.Background()
	r := newFlowRig(t)
	for _, id := range []processflow.FlowId{"zeta", "alpha"} {
		if _, _, err := r.define.Execute(ctx, id, "n", flowStages(t, "PICK", "PICK", "PACK", "PACK")); err != nil {
			t.Fatal(err)
		}
	}
	pub := &fakePublisher{}
	uc := &usecases.DeactivatePath{Repo: r.paths, Publisher: pub, Clock: fixedClock{time.Now()}, Flows: r.flows}

	err := uc.Execute(ctx, "PICK")
	if !errors.Is(err, usecases.ErrPathInActiveFlow) {
		t.Fatalf("got %v, want ErrPathInActiveFlow", err)
	}
	if !strings.Contains(err.Error(), "[alpha zeta]") {
		t.Fatalf("the error must name the flows in order, got %q", err.Error())
	}
	if pub.count() != 0 {
		t.Fatalf("a refused deactivation publishes nothing")
	}
	if p, _ := r.paths.FindByID(ctx, "PICK"); !p.IsActive() {
		t.Fatalf("the path must stay Active")
	}

	// A path no flow uses is deactivated as before.
	if err := uc.Execute(ctx, "REBIN"); err != nil {
		t.Fatalf("unused path: %v", err)
	}

	// Retire one flow: still refused by the other; retire both: allowed.
	if err := r.retire.Execute(ctx, "alpha"); err != nil {
		t.Fatal(err)
	}
	if err := uc.Execute(ctx, "PICK"); !errors.Is(err, usecases.ErrPathInActiveFlow) || !strings.Contains(err.Error(), "[zeta]") {
		t.Fatalf("one active flow remains: %v", err)
	}
	if err := r.retire.Execute(ctx, "zeta"); err != nil {
		t.Fatal(err)
	}
	if err := uc.Execute(ctx, "PICK"); err != nil {
		t.Fatalf("no ACTIVE flow uses the path any more: %v", err)
	}
}

func TestDeactivatePath_FlowLookupFailurePropagates(t *testing.T) {
	r := newFlowRig(t)
	r.flows.refsErr = errBoom
	uc := &usecases.DeactivatePath{Repo: r.paths, Publisher: &fakePublisher{}, Clock: fixedClock{time.Now()}, Flows: r.flows}
	if err := uc.Execute(context.Background(), "PICK"); !errors.Is(err, errBoom) {
		t.Fatalf("got %v", err)
	}
}
