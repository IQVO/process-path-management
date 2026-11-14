package main_test

// Step definitions for the documentation-derived rule features
// (features/*_rules.feature). They extend the existing vocabulary with:
//
//   - strict setup requests and a few generic response assertions;
//   - keyset-page helpers for GET /process-paths (items, nextCursor);
//   - a readiness rig: the real Readiness gate over controllable Postgres and
//     Kafka dependencies, served by the production router;
//   - a fault rig: the production router over decorators of the in-memory
//     repositories that can fail the way the real adapters do (database down,
//     a concurrent writer winning the version-guarded save, a lost insert race);
//   - hand-built CloudEvents fed to the analytics consumer, and a failing
//     report store behind the real reports router.
//
// Nothing here reaches a sibling bounded context: this service never calls
// one (AGENTS.md), so every fact is local and declarative.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"

	"github.com/cucumber/godog"

	inboundhttp "github.com/claudioed/process-path-management/internal/adapters/inbound/http"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/memory"
	"github.com/claudioed/process-path-management/internal/analytics/report"
	"github.com/claudioed/process-path-management/internal/application/ports"
	"github.com/claudioed/process-path-management/internal/application/usecases"
	"github.com/claudioed/process-path-management/internal/domain/cptschedule"
	"github.com/claudioed/process-path-management/internal/domain/processpath"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// rules is the per-scenario state of the rule steps; it embeds the wave-2
// state so it can reach the analytics pipeline the wave-2 steps start.
type rules struct {
	*wave2

	faults *faultRig
	ready  *readyRig
	sent   map[string][]byte // hand-built analytics messages by CloudEvents id
}

func (r *rules) reset() {
	r.faults = nil
	r.ready = nil
	r.sent = map[string][]byte{}
}

// --- strict setup and generic assertions -------------------------------------

// clientHasSent performs a setup request that must succeed, so a broken
// fixture never looks like a failing assertion.
func (r *rules) clientHasSent(method, path string, body *godog.DocString) error {
	if err := r.send(method, path, body.Content); err != nil {
		return err
	}
	if r.lastStatus < 200 || r.lastStatus > 299 {
		return fmt.Errorf("setup %s %s: got %d: %s", method, path, r.lastStatus, string(r.lastBody))
	}
	return nil
}

func (r *rules) responseFieldIsPresent(path string) error {
	obj, err := r.decodeLastObject()
	if err != nil {
		return err
	}
	if _, ok := lookupField(obj, path); !ok {
		return fmt.Errorf("field %q is absent in %s, want it present", path, string(r.lastBody))
	}
	return nil
}

func (r *rules) problemDetailIs(want string) error {
	detail, err := r.problemDetail()
	if err != nil {
		return err
	}
	if detail != want {
		return fmt.Errorf("problem detail is %q, want %q", detail, want)
	}
	return nil
}

// --- keyset pages of GET /process-paths ---------------------------------------

func (r *rules) pathListRequested(query string) error {
	path := "/process-paths"
	if query != "" {
		path += "?" + query
	}
	return r.do(http.MethodGet, path, nil)
}

func (r *rules) pathListRequestedWithCursor(cursor string) error {
	return r.do(http.MethodGet, "/process-paths?cursor="+cursor, nil)
}

func (r *rules) nextCursor() (string, error) {
	obj, err := r.decodeLastObject()
	if err != nil {
		return "", err
	}
	cursor, _ := obj["nextCursor"].(string)
	return cursor, nil
}

func (r *rules) followNextCursor(query string) error {
	cursor, err := r.nextCursor()
	if err != nil {
		return err
	}
	if cursor == "" {
		return errors.New("the last page has no nextCursor to follow")
	}
	extra := "cursor=" + cursor
	if query != "" {
		extra = query + "&" + extra
	}
	return r.pathListRequested(extra)
}

func (r *rules) pageListsPathIds(csv string) error {
	items, err := r.decodeLastItems()
	if err != nil {
		return err
	}
	ids := make([]string, len(items))
	for i, item := range items {
		obj, _ := item.(map[string]any)
		ids[i], _ = obj["pathId"].(string)
	}
	if got := strings.Join(ids, ","); got != csv {
		return fmt.Errorf("page lists path ids %q, want %q", got, csv)
	}
	return nil
}

func (r *rules) pageNextCursor(want bool) error {
	cursor, err := r.nextCursor()
	if err != nil {
		return err
	}
	if (cursor != "") != want {
		return fmt.Errorf("nextCursor present = %v, want %v: %s", cursor != "", want, string(r.lastBody))
	}
	return nil
}

// --- the readiness rig ----------------------------------------------------------

// readinessTTL is how long the rig's gate reuses a dependency verdict. The
// scenarios sleep for twice that to let a verdict expire.
const readinessTTL = 250 * time.Millisecond

// readyRig is the real Readiness gate over two dependencies whose reachability
// a scenario controls.
type readyRig struct {
	gate *inboundhttp.Readiness
	mu   sync.Mutex
	down map[string]bool
}

func (rig *readyRig) set(name string, down bool) {
	rig.mu.Lock()
	defer rig.mu.Unlock()
	rig.down[name] = down
}

func (rig *readyRig) check(name string) inboundhttp.Dependency {
	return inboundhttp.Dependency{Name: name, Check: func(context.Context) error {
		rig.mu.Lock()
		defer rig.mu.Unlock()
		if rig.down[name] {
			return fmt.Errorf("%s is unreachable", name)
		}
		return nil
	}}
}

// swapServer replaces the scenario's server (closing the old one).
func (r *rules) swapServer(handler http.Handler) {
	r.server.Close()
	r.server = httptest.NewServer(handler)
}

func (r *rules) readinessGateOverDependencies() error {
	rig := &readyRig{down: map[string]bool{}}
	rig.gate = inboundhttp.NewReadiness(readinessTTL, time.Second, rig.check("postgres"), rig.check("kafka"))
	r.ready = rig
	r.swapServer(inboundhttp.NewRouter(&inboundhttp.Server{Readiness: rig.gate}, nil, ""))
	return nil
}

func (r *rules) requireReadyRig() (*readyRig, error) {
	if r.ready == nil {
		return nil, errors.New("no readiness gate: add the Given step first")
	}
	return r.ready, nil
}

func (r *rules) dependenciesUnreachable(csv string) error {
	rig, err := r.requireReadyRig()
	if err != nil {
		return err
	}
	for _, name := range splitCapabilities(csv) {
		rig.set(name, true)
	}
	return nil
}

func (r *rules) dependencyReachableAgain(name string) error {
	rig, err := r.requireReadyRig()
	if err != nil {
		return err
	}
	rig.set(name, false)
	return nil
}

func (r *rules) sigtermReceived() error {
	rig, err := r.requireReadyRig()
	if err != nil {
		return err
	}
	rig.gate.SetNotReady()
	return nil
}

func (r *rules) readinessProbed() error { return r.do(http.MethodGet, "/readyz", nil) }
func (r *rules) livenessProbed() error  { return r.do(http.MethodGet, "/healthz", nil) }

func (r *rules) verdictExpires() error {
	time.Sleep(2 * readinessTTL)
	return nil
}

// --- the fault rig ------------------------------------------------------------------

var errDatabaseDown = errors.New("connection refused: the database is unreachable")

// faultRig is the state the repository decorators consult.
type faultRig struct {
	mu               sync.Mutex
	dbDown           bool
	failPathSave     error                    // one shot
	failScheduleSave error                    // one shot
	raceWinner       *processpath.ProcessPath // committed by "another operator" during the next Create
}

func (f *faultRig) down() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.dbDown {
		return errDatabaseDown
	}
	return nil
}

func (f *faultRig) takePathSaveFault() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	err := f.failPathSave
	f.failPathSave = nil
	return err
}

func (f *faultRig) takeScheduleSaveFault() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	err := f.failScheduleSave
	f.failScheduleSave = nil
	return err
}

func (f *faultRig) takeRaceWinner() *processpath.ProcessPath {
	f.mu.Lock()
	defer f.mu.Unlock()
	w := f.raceWinner
	f.raceWinner = nil
	return w
}

// faultyPathRepo decorates the in-memory path repo. Unlike the bare double it
// hands out COPIES, as a database does: a use case that mutates a loaded
// aggregate and then fails to save it must leave the stored row untouched.
type faultyPathRepo struct {
	*memory.ProcessPathRepo
	rig *faultRig
}

func copyOfPath(p *processpath.ProcessPath) *processpath.ProcessPath {
	if p == nil {
		return nil
	}
	return processpath.Rehydrate(p.ID(), p.MatchPrefix(), p.Direct(), p.RequiredCapabilities(), p.DestinationLocationRole(),
		p.CycleTimeP95(), p.Eligibility(), p.Intake(), p.Status(), p.CreatedAt(), p.UpdatedAt(), p.Version())
}

func (f *faultyPathRepo) Create(ctx context.Context, p *processpath.ProcessPath) error {
	if err := f.rig.down(); err != nil {
		return err
	}
	if winner := f.rig.takeRaceWinner(); winner != nil {
		// Another operator's define commits between this caller's duplicate
		// check and its insert: the insert-only Create loses.
		if err := f.ProcessPathRepo.Create(ctx, winner); err != nil {
			return err
		}
		return ports.ErrAlreadyExists
	}
	return f.ProcessPathRepo.Create(ctx, p)
}

func (f *faultyPathRepo) Save(ctx context.Context, p *processpath.ProcessPath) error {
	if err := f.rig.down(); err != nil {
		return err
	}
	if err := f.rig.takePathSaveFault(); err != nil {
		return err
	}
	return f.ProcessPathRepo.Save(ctx, p)
}

func (f *faultyPathRepo) FindByID(ctx context.Context, id shared.PathId) (*processpath.ProcessPath, error) {
	if err := f.rig.down(); err != nil {
		return nil, err
	}
	p, err := f.ProcessPathRepo.FindByID(ctx, id)
	return copyOfPath(p), err
}

func (f *faultyPathRepo) FindByIDForUpdate(ctx context.Context, id shared.PathId) (*processpath.ProcessPath, error) {
	return f.FindByID(ctx, id)
}

func (f *faultyPathRepo) ListPage(ctx context.Context, activeOnly bool, after shared.PathId, size int) ([]*processpath.ProcessPath, error) {
	if err := f.rig.down(); err != nil {
		return nil, err
	}
	return f.ProcessPathRepo.ListPage(ctx, activeOnly, after, size)
}

func (f *faultyPathRepo) LockByIDsForShare(ctx context.Context, ids []shared.PathId) ([]*processpath.ProcessPath, error) {
	if err := f.rig.down(); err != nil {
		return nil, err
	}
	return f.ProcessPathRepo.LockByIDsForShare(ctx, ids)
}

// faultyScheduleRepo decorates the in-memory schedule repo.
type faultyScheduleRepo struct {
	*memory.CPTScheduleRepo
	rig *faultRig
}

func (f *faultyScheduleRepo) Save(ctx context.Context, s *cptschedule.CPTSchedule) error {
	if err := f.rig.down(); err != nil {
		return err
	}
	if err := f.rig.takeScheduleSaveFault(); err != nil {
		return err
	}
	return f.CPTScheduleRepo.Save(ctx, s)
}

func (f *faultyScheduleRepo) FindBySiteID(ctx context.Context, siteId shared.SiteId) (*cptschedule.CPTSchedule, error) {
	if err := f.rig.down(); err != nil {
		return nil, err
	}
	return f.CPTScheduleRepo.FindBySiteID(ctx, siteId)
}

func (f *faultyScheduleRepo) ListSiteIDsReferencingPath(ctx context.Context, id shared.PathId) ([]shared.SiteId, error) {
	if err := f.rig.down(); err != nil {
		return nil, err
	}
	return f.CPTScheduleRepo.ListSiteIDsReferencingPath(ctx, id)
}

// serviceWhoseStoresCanFail rebuilds the production router over the faulty
// decorators, sharing the scenario's recording publisher and clock.
func (r *rules) serviceWhoseStoresCanFail() error {
	rig := &faultRig{}
	repo := &faultyPathRepo{ProcessPathRepo: memory.NewProcessPathRepo(), rig: rig}
	schedules := &faultyScheduleRepo{CPTScheduleRepo: memory.NewCPTScheduleRepo(), rig: rig}
	pub, clock := r.events, r.clock
	s := &inboundhttp.Server{
		DefinePath:        &usecases.DefinePath{Repo: repo, Publisher: pub, Clock: clock},
		RevisePath:        &usecases.RevisePath{Repo: repo, Publisher: pub, Clock: clock},
		DeactivatePath:    &usecases.DeactivatePath{Repo: repo, Publisher: pub, Clock: clock, CPTSchedules: schedules},
		GetPath:           &usecases.GetPath{Repo: repo},
		ListPaths:         &usecases.ListPaths{Repo: repo},
		DefineCPTSchedule: &usecases.DefineCPTSchedule{Repo: schedules, ProcessPathRepo: repo, Publisher: pub, Clock: clock},
		GetCPTSchedule:    &usecases.GetCPTSchedule{Repo: schedules},
	}
	r.faults = rig
	r.swapServer(inboundhttp.NewRouter(s, nil, ""))
	return nil
}

func (r *rules) requireFaults() (*faultRig, error) {
	if r.faults == nil {
		return nil, errors.New("no faulty stores: add the Given step first")
	}
	return r.faults, nil
}

func (r *rules) setDatabase(down bool) error {
	rig, err := r.requireFaults()
	if err != nil {
		return err
	}
	rig.mu.Lock()
	defer rig.mu.Unlock()
	rig.dbDown = down
	return nil
}

func (r *rules) databaseUnreachable() error { return r.setDatabase(true) }
func (r *rules) databaseBack() error        { return r.setDatabase(false) }

func (r *rules) concurrentWriterWinsNextPathSave() error {
	rig, err := r.requireFaults()
	if err != nil {
		return err
	}
	rig.mu.Lock()
	defer rig.mu.Unlock()
	rig.failPathSave = fmt.Errorf("optimistic save lost: %w", ports.ErrConcurrentModification)
	return nil
}

func (r *rules) concurrentWriterWinsNextScheduleSave() error {
	rig, err := r.requireFaults()
	if err != nil {
		return err
	}
	rig.mu.Lock()
	defer rig.mu.Unlock()
	rig.failScheduleSave = fmt.Errorf("optimistic save lost: %w", ports.ErrConcurrentModification)
	return nil
}

func (r *rules) anotherOperatorWinsTheDefine(pathId, matchPrefix string) error {
	rig, err := r.requireFaults()
	if err != nil {
		return err
	}
	winner, err := processpath.Define(shared.PathId(pathId), matchPrefix, true, []shared.Capability{"pick"},
		shared.DestinationLocationRoleUnset, 2*time.Hour, shared.Eligibility{}, shared.Intake{}, r.clock.Now())
	if err != nil {
		return fmt.Errorf("build the winning define: %w", err)
	}
	rig.mu.Lock()
	defer rig.mu.Unlock()
	rig.raceWinner = winner
	return nil
}

// --- analytics: hand-built CloudEvents and a failing report store ------------------

// deliverCloudEvent hands the consumer a CloudEvents 1.0 message of the given
// full type, id, subject and time attribute, written exactly as given (so a
// time with a UTC offset reaches the projector as the producer wrote it).
func (r *rules) deliverCloudEvent(eventType, id, subject, at string) error {
	p, err := r.requirePipeline()
	if err != nil {
		return err
	}
	raw, err := json.Marshal(map[string]any{
		"specversion": "1.0", "id": id, "source": "/warehouse/process-path-management",
		"type": eventType, "subject": subject, "time": at,
		"datacontenttype": "application/json", "data": map[string]any{"path_id": subject},
	})
	if err != nil {
		return err
	}
	r.sent[id] = raw
	p.lastErr = p.consumer.HandleMessage(context.Background(), raw)
	return nil
}

func (r *rules) redeliverCloudEvent(id string) error {
	p, err := r.requirePipeline()
	if err != nil {
		return err
	}
	raw, ok := r.sent[id]
	if !ok {
		return fmt.Errorf("no CloudEvent with id %q was delivered", id)
	}
	p.lastErr = p.consumer.HandleMessage(context.Background(), raw)
	return nil
}

func (r *rules) projectorAcceptedWithoutError() error {
	p, err := r.requirePipeline()
	if err != nil {
		return err
	}
	if p.lastErr != nil {
		return fmt.Errorf("the projector returned %w, want the message skipped or applied without error", p.lastErr)
	}
	return nil
}

func (r *rules) reportRequestedWithQuery(query string) error {
	return r.getReport("/reports/catalogue-growth?" + query)
}

// failingReportStore is an analytical database that cannot be queried.
type failingReportStore struct{}

func (failingReportStore) Query(context.Context, report.ReportQuery) (report.CatalogueReport, error) {
	return report.CatalogueReport{}, errors.New("analytics database: statement timeout")
}

func (failingReportStore) FreshnessLag(context.Context) (time.Duration, error) {
	return 0, errors.New("analytics database: statement timeout")
}

func (r *rules) reportStoreUnavailable() error {
	p, err := r.requirePipeline()
	if err != nil {
		return err
	}
	p.server.Close()
	p.server = httptest.NewServer(inboundhttp.NewReportsRouter(&inboundhttp.ReportsHandlers{Store: failingReportStore{}}, nil))
	return nil
}

// --- registration ------------------------------------------------------------------

func registerRulesSteps(sc *godog.ScenarioContext, w2 *wave2) {
	r := &rules{wave2: w2}

	sc.Before(func(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
		r.reset()
		return ctx, nil
	})

	sc.Step(`^the client has successfully sent (POST|PUT) "([^"]*)" with body:$`, r.clientHasSent)
	sc.Step(`^the response field "([^"]*)" is present$`, r.responseFieldIsPresent)
	sc.Step(`^the response problem detail is "([^"]*)"$`, r.problemDetailIs)

	sc.Step(`^the process path list is requested with "([^"]*)"$`, r.pathListRequested)
	sc.Step(`^the process path list is requested with the cursor "([^"]*)"$`, r.pathListRequestedWithCursor)
	sc.Step(`^the next page is requested with "([^"]*)"$`, r.followNextCursor)
	sc.Step(`^the page lists path ids "([^"]*)"$`, r.pageListsPathIds)
	sc.Step(`^the page has a next cursor$`, func() error { return r.pageNextCursor(true) })
	sc.Step(`^the page has no next cursor$`, func() error { return r.pageNextCursor(false) })

	sc.Step(`^a service whose Postgres and Kafka dependencies are reachable$`, r.readinessGateOverDependencies)
	sc.Step(`^the dependencies "([^"]*)" are unreachable$`, r.dependenciesUnreachable)
	sc.Step(`^the "([^"]*)" dependency is reachable again$`, r.dependencyReachableAgain)
	sc.Step(`^the service receives SIGTERM$`, r.sigtermReceived)
	sc.Step(`^the readiness probe is queried$`, r.readinessProbed)
	sc.Step(`^the liveness probe is queried$`, r.livenessProbed)
	sc.Step(`^the readiness verdict has expired$`, r.verdictExpires)

	sc.Step(`^a service whose stores can be made to fail$`, r.serviceWhoseStoresCanFail)
	sc.Step(`^the database becomes unreachable$`, r.databaseUnreachable)
	sc.Step(`^the database is reachable again$`, r.databaseBack)
	sc.Step(`^a concurrent writer commits first, so the next save of a process path is refused$`, r.concurrentWriterWinsNextPathSave)
	sc.Step(`^a concurrent writer commits first, so the next save of a CPT schedule is refused$`, r.concurrentWriterWinsNextScheduleSave)
	sc.Step(`^another operator's define of "([^"]*)" with matchPrefix "([^"]*)" commits between the duplicate check and the insert$`, r.anotherOperatorWinsTheDefine)

	sc.Step(`^the analytics topic delivers a CloudEvent of type "([^"]*)" with id "([^"]*)" for "([^"]*)" at "([^"]*)"$`, r.deliverCloudEvent)
	sc.Step(`^the analytics topic redelivers the CloudEvent with id "([^"]*)"$`, r.redeliverCloudEvent)
	sc.Step(`^the projector accepted the message without error$`, r.projectorAcceptedWithoutError)
	sc.Step(`^the catalogue growth report is requested with the query "([^"]*)"$`, r.reportRequestedWithQuery)
	sc.Step(`^the report store is unavailable$`, r.reportStoreUnavailable)
}
