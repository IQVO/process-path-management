package a2a

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/a2aproject/a2a-go/v2/a2asrv"

	"github.com/claudioed/process-path-management/internal/adapters/outbound/memory"
	"github.com/claudioed/process-path-management/internal/application/ports"
	"github.com/claudioed/process-path-management/internal/application/usecases"
	"github.com/claudioed/process-path-management/internal/domain/processpath"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

var base = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

// seededRepo holds one active path (PICK) and one deactivated path (REBIN),
// defined through the real aggregate.
func seededRepo(t *testing.T) *memory.ProcessPathRepo {
	t.Helper()
	repo := memory.NewProcessPathRepo()
	ctx := context.Background()

	pick, err := processpath.Define("PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, 2*time.Hour, shared.Eligibility{}, shared.Intake{}, base)
	if err != nil {
		t.Fatalf("define PICK: %v", err)
	}
	if err := repo.Save(ctx, pick); err != nil {
		t.Fatalf("save PICK: %v", err)
	}

	rebin, err := processpath.Define("REBIN", "rebin", false, []shared.Capability{"rebin"}, shared.DestinationLocationRoleUnset, 2*time.Hour, shared.Eligibility{}, shared.Intake{}, base)
	if err != nil {
		t.Fatalf("define REBIN: %v", err)
	}
	rebin.Deactivate(base.Add(time.Hour))
	if err := repo.Save(ctx, rebin); err != nil {
		t.Fatalf("save REBIN: %v", err)
	}
	return repo
}

func depsOver(repo ports.ProcessPathRepo) Deps {
	return Deps{GetPath: &usecases.GetPath{Repo: repo}, ListPaths: &usecases.ListPaths{Repo: repo}}
}

// ---- the wire: JSON-RPC over HTTP, driven by the SDK's own client ----------

// wireEnvelope is a TEST-LOCAL copy of the answer contract. If the adapter's
// envelope drifts, this suite fails instead of silently following it.
type wireEnvelope struct {
	Skill string `json:"skill"`
	Data  struct {
		ProcessPath  *wirePath `json:"processPath"`
		ProcessPaths *struct {
			ActiveOnly bool       `json:"activeOnly"`
			Paths      []wirePath `json:"paths"`
		} `json:"processPaths"`
	} `json:"data"`
	Problem *struct {
		Type   string `json:"type"`
		Title  string `json:"title"`
		Detail string `json:"detail"`
		Status int    `json:"status"`
	} `json:"problem"`
}

type wirePath struct {
	PathId               string   `json:"pathId"`
	MatchPrefix          string   `json:"matchPrefix"`
	Direct               bool     `json:"direct"`
	RequiredCapabilities []string `json:"requiredCapabilities"`
	Status               string   `json:"status"`
	Active               bool     `json:"active"`
	CreatedAt            string   `json:"createdAt"`
	CycleTimeP95         string   `json:"cycleTimeP95"`
	Intake               []string `json:"intake"`
}

type wire struct {
	client *a2aclient.Client
}

func newWire(t *testing.T, deps Deps) *wire {
	t.Helper()
	srv := httptest.NewServer(Handler(NewExecutor(deps)))
	t.Cleanup(srv.Close)
	card := Card(Config{AgentURL: srv.URL + "/", Version: "test"})
	client, err := a2aclient.NewFromCard(context.Background(), card)
	if err != nil {
		t.Fatalf("NewFromCard: %v", err)
	}
	return &wire{client: client}
}

// call sends one message part and returns the decoded envelope and the
// one-line text summary of the agent's reply.
func (w *wire) call(t *testing.T, part *a2a.Part) (wireEnvelope, string) {
	t.Helper()
	resp, err := w.client.SendMessage(context.Background(), &a2a.SendMessageRequest{Message: a2a.NewMessage(a2a.MessageRoleUser, part)})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	msg, ok := resp.(*a2a.Message)
	if !ok || len(msg.Parts) < 2 {
		t.Fatalf("reply = %T, want a message with a data and a text part", resp)
	}
	data, ok := msg.Parts[0].Content.(a2a.Data)
	if !ok {
		t.Fatalf("first reply part is %T, want data", msg.Parts[0].Content)
	}
	raw, _ := json.Marshal(data.Value)
	var env wireEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("reply is not the skill envelope: %v (%s)", err, raw)
	}
	text, _ := msg.Parts[1].Content.(a2a.Text)
	return env, string(text)
}

func (w *wire) skill(t *testing.T, skill string, args map[string]any) (wireEnvelope, string) {
	t.Helper()
	return w.call(t, a2a.NewDataPart(map[string]any{"skill": skill, "arguments": args}))
}

func wantProblem(t *testing.T, env wireEnvelope, status int, slug string) {
	t.Helper()
	if env.Problem == nil {
		t.Fatalf("expected a %d problem, got data: %+v", status, env.Data)
	}
	if env.Problem.Status != status || env.Problem.Type != problemTypeBase+"/"+slug {
		t.Fatalf("problem = %+v, want %d %s", env.Problem, status, slug)
	}
}

func TestWireGetProcessPathReturnsTheFullDefinition(t *testing.T) {
	w := newWire(t, depsOver(seededRepo(t)))

	env, text := w.skill(t, skillGetProcessPath, map[string]any{"pathId": "PICK"})
	p := env.Data.ProcessPath
	if env.Problem != nil || p == nil {
		t.Fatalf("envelope = %+v", env)
	}
	if p.PathId != "PICK" || p.MatchPrefix != "pick" || !p.Direct || !p.Active || p.Status != "ACTIVE" ||
		len(p.RequiredCapabilities) != 1 || p.RequiredCapabilities[0] != "pick" ||
		p.CycleTimeP95 != "2h0m0s" || p.CreatedAt != "2026-01-01T12:00:00Z" || p.Intake == nil || len(p.Intake) != 0 {
		t.Fatalf("path = %+v", p)
	}
	if text != "PICK: active, direct, matchPrefix pick, requires pick" {
		t.Fatalf("summary = %q", text)
	}
}

func TestWireGetProcessPathReadsDeactivatedPathsToo(t *testing.T) {
	w := newWire(t, depsOver(seededRepo(t)))

	env, text := w.skill(t, skillGetProcessPath, map[string]any{"pathId": "REBIN"})
	p := env.Data.ProcessPath
	if env.Problem != nil || p == nil || p.Active || p.Status != "DEACTIVATED" || p.Direct {
		t.Fatalf("envelope = %+v", env)
	}
	if text != "REBIN: deactivated, not direct, matchPrefix rebin, requires rebin" {
		t.Fatalf("summary = %q", text)
	}
}

func TestWireListProcessPathsDefaultsToActiveOnly(t *testing.T) {
	w := newWire(t, depsOver(seededRepo(t)))

	for name, args := range map[string]map[string]any{"omitted": {}, "explicit true": {"activeOnly": true}} {
		t.Run(name, func(t *testing.T) {
			env, text := w.skill(t, skillListProcessPaths, args)
			l := env.Data.ProcessPaths
			if env.Problem != nil || l == nil || !l.ActiveOnly || len(l.Paths) != 1 || l.Paths[0].PathId != "PICK" {
				t.Fatalf("envelope = %+v", env)
			}
			if text != "1 process paths (active only): PICK" {
				t.Fatalf("summary = %q", text)
			}
		})
	}
}

func TestWireListProcessPathsIncludesDeactivatedOnRequest(t *testing.T) {
	w := newWire(t, depsOver(seededRepo(t)))

	env, text := w.skill(t, skillListProcessPaths, map[string]any{"activeOnly": false})
	l := env.Data.ProcessPaths
	if env.Problem != nil || l == nil || l.ActiveOnly || len(l.Paths) != 2 {
		t.Fatalf("envelope = %+v", env)
	}
	if !strings.HasPrefix(text, "2 process paths (including deactivated): ") {
		t.Fatalf("summary = %q", text)
	}
}

func TestWireEmptyCatalogueIsAnEmptyListNotAProblem(t *testing.T) {
	w := newWire(t, depsOver(memory.NewProcessPathRepo()))

	env, _ := w.skill(t, skillListProcessPaths, map[string]any{})
	if env.Problem != nil || env.Data.ProcessPaths == nil || env.Data.ProcessPaths.Paths == nil || len(env.Data.ProcessPaths.Paths) != 0 {
		t.Fatalf("envelope = %+v, want an empty paths array", env)
	}
}

// An unknown path is a 404 answered in-band with the same type and status the
// REST adapter gives GET /process-paths/{pathId}.
func TestWireUnknownPathIsAnsweredAs404NotRaised(t *testing.T) {
	w := newWire(t, depsOver(seededRepo(t)))

	env, text := w.skill(t, skillGetProcessPath, map[string]any{"pathId": "GHOST"})
	wantProblem(t, env, 404, "path-not-found")
	if env.Problem.Detail != `no process path exists with pathId "GHOST"` {
		t.Fatalf("detail = %q", env.Problem.Detail)
	}
	// The shared use case error text carries an internal "usecases:" prefix;
	// the adapter owns the answer text and must not leak it.
	for _, s := range []string{env.Problem.Detail, env.Problem.Title, text} {
		if strings.Contains(s, "usecases") {
			t.Fatalf("an answer leaks the internal error text: %q", s)
		}
	}
}

// failingRepo makes every read fail, standing in for a database outage.
type failingRepo struct{ ports.ProcessPathRepo }

var errBoom = errors.New("pgx: connection refused to 10.0.0.7:5432 password=hunter2")

func (failingRepo) FindByID(context.Context, shared.PathId) (*processpath.ProcessPath, error) {
	return nil, errBoom
}
func (failingRepo) ListActive(context.Context) ([]*processpath.ProcessPath, error) {
	return nil, errBoom
}
func (failingRepo) ListAll(context.Context) ([]*processpath.ProcessPath, error) {
	return nil, errBoom
}
func (failingRepo) ListPage(context.Context, bool, shared.PathId, int) ([]*processpath.ProcessPath, error) {
	return nil, errBoom
}

// An unexpected failure is answered as a 500 problem, never a JSON-RPC error,
// and its raw text (which may carry infrastructure detail) is not echoed.
func TestWireUnexpectedFailureIs500AndDoesNotLeakTheCause(t *testing.T) {
	w := newWire(t, depsOver(failingRepo{memory.NewProcessPathRepo()}))

	cases := map[string]struct {
		skill string
		args  map[string]any
	}{
		"get":  {skillGetProcessPath, map[string]any{"pathId": "PICK"}},
		"list": {skillListProcessPaths, map[string]any{}},
		"all":  {skillListProcessPaths, map[string]any{"activeOnly": false}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			env, text := w.skill(t, tc.skill, tc.args)
			wantProblem(t, env, 500, "internal-error")
			for _, s := range []string{env.Problem.Detail, env.Problem.Title, text} {
				if strings.Contains(s, "hunter2") || strings.Contains(s, "10.0.0.7") {
					t.Fatalf("a 500 answer leaks the underlying error: %q", s)
				}
			}
		})
	}
}

func TestWireArgumentValidation(t *testing.T) {
	w := newWire(t, depsOver(seededRepo(t)))
	cases := map[string]struct {
		skill string
		args  map[string]any
	}{
		"get: missing pathId":       {skillGetProcessPath, map[string]any{}},
		"get: blank pathId":         {skillGetProcessPath, map[string]any{"pathId": "   "}},
		"get: pathId wrong type":    {skillGetProcessPath, map[string]any{"pathId": 7}},
		"get: unknown argument":     {skillGetProcessPath, map[string]any{"pathId": "PICK", "force": true}},
		"get: typo'd argument":      {skillGetProcessPath, map[string]any{"pathID": "PICK", "pathIdd": "PICK"}},
		"list: unknown argument":    {skillListProcessPaths, map[string]any{"activeOnlyy": false}},
		"list: activeOnly a string": {skillListProcessPaths, map[string]any{"activeOnly": "false"}},
		"list: wrong-skill arg":     {skillListProcessPaths, map[string]any{"pathId": "PICK"}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			env, _ := w.skill(t, tc.skill, tc.args)
			wantProblem(t, env, 400, "invalid-argument")
		})
	}
}

func TestWireRequestShapeProblems(t *testing.T) {
	w := newWire(t, depsOver(seededRepo(t)))

	env, _ := w.call(t, a2a.NewTextPart("not json at all"))
	wantProblem(t, env, 400, "malformed-skill-request")

	env, _ = w.call(t, a2a.NewDataPart(map[string]any{"arguments": map[string]any{}}))
	wantProblem(t, env, 400, "malformed-skill-request")

	env, _ = w.call(t, a2a.NewDataPart(map[string]any{"skill": "list_process_paths", "arguments": map[string]any{}, "extra": 1}))
	wantProblem(t, env, 400, "malformed-skill-request")

	// Trailing data after the JSON document is refused too.
	env, _ = w.call(t, a2a.NewTextPart(`{"skill":"list_process_paths","arguments":{}} {"skill":"get_process_path"}`))
	wantProblem(t, env, 400, "malformed-skill-request")

	env, _ = w.skill(t, "launch_rocket", map[string]any{})
	wantProblem(t, env, 400, "unknown-skill")

	// A text part holding the same JSON is accepted (documented contract).
	env, _ = w.call(t, a2a.NewTextPart(`{"skill":"get_process_path","arguments":{"pathId":"PICK"}}`))
	if env.Problem != nil || env.Data.ProcessPath == nil || env.Data.ProcessPath.PathId != "PICK" {
		t.Fatalf("a text part holding the skill JSON must be accepted, got %+v", env)
	}
}

// The write skills of the REST surface are not offered here: asking for one is
// an unknown skill, answered in-band, and nothing is written.
func TestWireOffersNoWriteSkill(t *testing.T) {
	repo := seededRepo(t)
	w := newWire(t, depsOver(repo))

	for _, skill := range []string{"define_path", "revise_path", "deactivate_path", "relocate_process_path", "define_cpt_schedule"} {
		env, _ := w.skill(t, skill, map[string]any{"pathId": "NEW", "matchPrefix": "new"})
		wantProblem(t, env, 400, "unknown-skill")
	}
	if p, err := repo.FindByID(context.Background(), "NEW"); err != nil || p != nil {
		t.Fatalf("a write skill name must not write anything: %v %v", p, err)
	}
}

// TestCardIsServedAndDialable proves discovery end to end: the card handler
// serves the card at the well-known path and a client built from the FETCHED
// card reaches the agent.
func TestCardIsServedAndDialable(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle("/", Handler(NewExecutor(depsOver(seededRepo(t)))))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	mux.Handle(a2asrv.WellKnownAgentCardPath, a2asrv.NewStaticAgentCardHandler(Card(Config{AgentURL: srv.URL + "/", Version: "test"})))

	res, err := http.Get(srv.URL + a2asrv.WellKnownAgentCardPath)
	if err != nil || res.StatusCode != http.StatusOK {
		t.Fatalf("card fetch = %v / %v", res, err)
	}
	defer func() { _ = res.Body.Close() }()
	var card a2a.AgentCard
	if err := json.NewDecoder(res.Body).Decode(&card); err != nil {
		t.Fatal(err)
	}
	client, err := a2aclient.NewFromCard(context.Background(), &card)
	if err != nil {
		t.Fatal(err)
	}
	w := &wire{client: client}
	env, _ := w.skill(t, skillGetProcessPath, map[string]any{"pathId": "PICK"})
	if env.Data.ProcessPath == nil {
		t.Fatalf("a client built from the served card could not call the agent: %+v", env)
	}
}
