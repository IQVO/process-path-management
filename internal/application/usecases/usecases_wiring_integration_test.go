//go:build integration

// Package usecases_test proves the process-path write use cases against a
// REAL Postgres (testcontainers): the real Postgres repos, the real
// UnitOfWork, and a recording publisher, wired exactly like the composition
// root in cmd/pathmgmt. These are integration tests in the fleet's sense:
// they execute the real cross-component contracts (define idempotency,
// version-guarded revise, the CPT-schedule deactivation guard, the atomic
// Publish-inside-UoW bracket) against real infrastructure, with no
// in-memory repo fakes anywhere in the path.
//
// The package boots its own throwaway Postgres via testcontainers in
// TestMain: one container for the whole package, migrated once into a
// template database, one private database per test. Never an external
// DATABASE_URL, never t.Skip.
package usecases_test

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/claudioed/process-path-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/process-path-management/internal/application/usecases"
	"github.com/claudioed/process-path-management/internal/domain/cptschedule"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// One Postgres container serves the whole package (containers are slow to
// boot). TestMain starts it, applies every migration ONCE into a template
// database, and each test then gets its own database cloned from that
// template (CREATE DATABASE ... TEMPLATE, a file-level copy: milliseconds).
// Isolation is therefore total — no TRUNCATE bookkeeping, no dependence on
// test order, and tests that assert on global state still start pristine.
//
// Never an external DATABASE_URL, never t.Skip.
const wiringTemplateDB = "usecases_wiring_template"

var (
	wiringBaseURL string // connection URL of the container's default database
	wiringDBSeq   uint64 // guarded by the Go test runner's serial execution
)

func TestMain(m *testing.M) {
	os.Exit(wiringRunTests(m))
}

func wiringRunTests(m *testing.M) int {
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("pathmgmt"),
		tcpostgres.WithUsername("pathmgmt"),
		tcpostgres.WithPassword("pathmgmt"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(90*time.Second),
		),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start postgres container: %v\n", err)
		return 1
	}
	defer func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			fmt.Fprintf(os.Stderr, "terminate postgres container: %v\n", err)
		}
	}()

	wiringBaseURL, err = container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintf(os.Stderr, "postgres connection string: %v\n", err)
		return 1
	}

	// Migrate a template database once; every test clones it.
	if err := wiringCreateDatabase(ctx, wiringTemplateDB); err != nil {
		fmt.Fprintf(os.Stderr, "create template database: %v\n", err)
		return 1
	}
	if err := postgres.RunMigrations(wiringWithDB(wiringBaseURL, wiringTemplateDB), wiringMigrationsDir()); err != nil {
		fmt.Fprintf(os.Stderr, "migrate template: %v\n", err)
		return 1
	}

	return m.Run()
}

// wiringMigratedDB hands the test a connection URL to its own private
// database, cloned from the migrated template. Cloning is a file-level
// copy, so it costs milliseconds and the test's writes never leak into
// another test.
func wiringMigratedDB(t *testing.T) string {
	t.Helper()
	name := fmt.Sprintf("usecases_wiring_%d", wiringDBSeq+1)
	conn, err := pgx.Connect(context.Background(), wiringBaseURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	if _, err := conn.Exec(context.Background(), fmt.Sprintf(
		"CREATE DATABASE %q WITH TEMPLATE %q", name, wiringTemplateDB)); err != nil {
		t.Fatalf("clone database: %v", err)
	}
	wiringDBSeq++
	return wiringWithDB(wiringBaseURL, name)
}

// wiringCreateDatabase creates an empty database inside the shared
// container.
func wiringCreateDatabase(ctx context.Context, name string) error {
	conn, err := pgx.Connect(ctx, wiringBaseURL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %q", name)); err != nil {
		return fmt.Errorf("create database %s: %w", name, err)
	}
	return nil
}

// wiringWithDB rewrites the path of a connection URL to the named database.
func wiringWithDB(baseURL, name string) string {
	u, err := url.Parse(baseURL)
	if err != nil {
		panic(err)
	}
	u.Path = "/" + name
	return u.String()
}

// wiringMigrationsDir resolves /migrations relative to this test file, so
// the suite works regardless of the directory go test is invoked from
// (same convention as the postgres package's own integration tests).
func wiringMigrationsDir() string {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		panic("unable to resolve test file path")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "migrations")
}

// wiringClock is the ports.Clock the use cases already accept; a fixed now
// keeps the persisted timestamps deterministic.
type wiringClock struct{ now time.Time }

func (c wiringClock) Now() time.Time { return c.now }

// wiringPublisher records every event the use cases publish so tests can
// assert on the event stream, standing in for the composition root's
// OutboxPublisher (whose transactional bracket the postgres package's own
// integration suite proves).
type wiringPublisher struct {
	mu     sync.Mutex
	events []shared.DomainEvent
}

func (p *wiringPublisher) Publish(_ context.Context, event shared.DomainEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, event)
	return nil
}

// names returns the EventName of every published event, in order.
func (p *wiringPublisher) names() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, 0, len(p.events))
	for _, e := range p.events {
		out = append(out, e.EventName())
	}
	return out
}

// wiredUsecases is the real adapter stack over a private migrated
// database, wired exactly like cmd/pathmgmt's composition root: real
// Postgres repos, the real UnitOfWork, and a recording publisher the tests
// assert on. No in-memory repo fakes.
type wiredUsecases struct {
	define       *usecases.DefinePath
	revise       *usecases.RevisePath
	deactivate   *usecases.DeactivatePath
	defineSched  *usecases.DefineCPTSchedule
	getSched     *usecases.GetCPTSchedule
	getPath      *usecases.GetPath
	listPaths    *usecases.ListPaths
	pathRepo     *postgres.ProcessPathRepo
	cptRepo      *postgres.CPTScheduleRepo
	publisher    *wiringPublisher
	wall         time.Time
	cycleTimeP95 time.Duration
}

// newWiredUsecases builds the stack on its own private database clone.
func newWiredUsecases(t *testing.T) *wiredUsecases {
	t.Helper()
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, wiringMigratedDB(t))
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)

	wall := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	publisher := &wiringPublisher{}
	clock := wiringClock{now: wall}
	uow := postgres.NewUnitOfWork(pool)

	pathRepo := postgres.NewProcessPathRepo(pool)
	cptRepo := postgres.NewCPTScheduleRepo(pool)

	return &wiredUsecases{
		define: &usecases.DefinePath{
			Repo: pathRepo, Publisher: publisher, Clock: clock, UnitOfWork: uow,
		},
		revise: &usecases.RevisePath{
			Repo: pathRepo, Publisher: publisher, Clock: clock, UnitOfWork: uow,
		},
		deactivate: &usecases.DeactivatePath{
			Repo: pathRepo, Publisher: publisher, Clock: clock,
			UnitOfWork: uow, CPTSchedules: cptRepo,
		},
		defineSched: &usecases.DefineCPTSchedule{
			Repo: cptRepo, ProcessPathRepo: pathRepo,
			Publisher: publisher, Clock: clock, UnitOfWork: uow,
		},
		getSched:     &usecases.GetCPTSchedule{Repo: cptRepo},
		getPath:      &usecases.GetPath{Repo: pathRepo},
		listPaths:    &usecases.ListPaths{Repo: pathRepo},
		pathRepo:     pathRepo,
		cptRepo:      cptRepo,
		publisher:    publisher,
		wall:         wall,
		cycleTimeP95: 90 * time.Minute,
	}
}

// definePath drives the real DefinePath use case and fails the test on any
// error.
func (w *wiredUsecases) definePath(t *testing.T, id, matchPrefix string, capabilities ...shared.Capability) {
	t.Helper()
	if _, err := w.define.Execute(context.Background(), shared.PathId(id), matchPrefix, true,
		capabilities, shared.DestinationLocationRoleUnset, w.cycleTimeP95, shared.Eligibility{}); err != nil {
		t.Fatalf("define %s: %v", id, err)
	}
}

// mustCutoff builds a Cutoff referencing the given paths or fails the test.
func mustCutoff(t *testing.T, cptId string, eligible ...shared.PathId) cptschedule.Cutoff {
	t.Helper()
	c, err := cptschedule.NewCutoff(cptId, "15:00",
		[]cptschedule.Weekday{cptschedule.Monday, cptschedule.Tuesday}, "ground", eligible)
	if err != nil {
		t.Fatalf("cutoff %s: %v", cptId, err)
	}
	return c
}

// TestUsecases_ProcessPathLifecycleRoundTrip drives the aggregate's whole
// lifecycle — define -> revise -> deactivate -> read model — through the
// real Postgres repos and the real UnitOfWork, asserting both the persisted
// state and the published event stream after every step.
func TestUsecases_ProcessPathLifecycleRoundTrip(t *testing.T) {
	w := newWiredUsecases(t)
	ctx := context.Background()

	// Define two paths; both persist at version 1.
	w.definePath(t, "PICK", "pick", "pick")
	w.definePath(t, "REBIN", "rebin", "rebin")
	for _, id := range []shared.PathId{"PICK", "REBIN"} {
		p, err := w.getPath.Execute(ctx, id)
		if err != nil {
			t.Fatalf("get %s after define: %v", id, err)
		}
		if p.Version() != 1 || !p.IsActive() {
			t.Fatalf("%s must persist ACTIVE at version 1, got %s v%d", id, p.Status(), p.Version())
		}
	}

	// Revise PICK's capabilities; the persisted row must advance to
	// version 2 carrying the new capability set (the optimistic-concurrency
	// bump the repo's own suite proves in isolation).
	w.revise.Clock = wiringClock{now: w.wall.Add(time.Hour)}
	if _, err := w.revise.Execute(ctx, "PICK", "pick", []shared.Capability{"pick", "hazmat"},
		w.cycleTimeP95, shared.Eligibility{}); err != nil {
		t.Fatalf("revise PICK: %v", err)
	}
	revised, err := w.getPath.Execute(ctx, "PICK")
	if err != nil {
		t.Fatalf("get PICK after revise: %v", err)
	}
	if revised.Version() != 2 {
		t.Fatalf("revise must bump the persisted version to 2, got %d", revised.Version())
	}
	if caps := revised.RequiredCapabilities(); len(caps) != 2 || caps[1] != "hazmat" {
		t.Fatalf("revise must persist the new capability set, got %v", caps)
	}
	if !revised.UpdatedAt().After(revised.CreatedAt()) {
		t.Fatalf("revise must move UpdatedAt, got created %v updated %v",
			revised.CreatedAt(), revised.UpdatedAt())
	}

	// Deactivate REBIN; the row survives as a closed historical record.
	if err := w.deactivate.Execute(ctx, "REBIN"); err != nil {
		t.Fatalf("deactivate REBIN: %v", err)
	}
	closed, err := w.getPath.Execute(ctx, "REBIN")
	if err != nil {
		t.Fatalf("get REBIN after deactivate: %v", err)
	}
	if closed.IsActive() {
		t.Fatal("REBIN must persist DEACTIVATED")
	}

	// The read models agree: active-only view hides REBIN, the audit view
	// keeps it.
	active, err := w.listPaths.Execute(ctx, true)
	if err != nil {
		t.Fatalf("list active: %v", err)
	}
	if len(active) != 1 || active[0].ID() != "PICK" {
		t.Fatalf("active-only view must be exactly [PICK], got %d paths", len(active))
	}
	all, err := w.listPaths.Execute(ctx, false)
	if err != nil {
		t.Fatalf("list all: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("audit view must keep the deactivated path, got %d paths", len(all))
	}

	// The published stream tells the same story, in order.
	want := []string{
		"ProcessPathCreated", "ProcessPathCreated",
		"ProcessPathUpdated", "ProcessPathDeactivated",
	}
	if got := w.publisher.names(); !equalStrings(got, want) {
		t.Fatalf("published events %v, want %v", got, want)
	}
}

// TestUsecases_DefinePathRejectsDuplicateId proves a path's identity is
// permanent against the real store: re-defining an existing PathId is
// refused end-to-end (nothing re-published, nothing overwritten), whether
// the existing path is active or already deactivated.
func TestUsecases_DefinePathRejectsDuplicateId(t *testing.T) {
	w := newWiredUsecases(t)
	ctx := context.Background()

	w.definePath(t, "PACK", "pack", "pack")
	_, err := w.define.Execute(ctx, "PACK", "pack-v2", true,
		[]shared.Capability{"pack"}, shared.DestinationLocationRoleUnset,
		w.cycleTimeP95, shared.Eligibility{})
	if !errors.Is(err, usecases.ErrPathAlreadyExists) {
		t.Fatalf("re-define must fail with ErrPathAlreadyExists, got %v", err)
	}

	// The refusal published nothing and overwrote nothing.
	if got := w.publisher.names(); len(got) != 1 || got[0] != "ProcessPathCreated" {
		t.Fatalf("duplicate define must not publish, got %v", got)
	}
	p, findErr := w.getPath.Execute(ctx, "PACK")
	if findErr != nil || p.MatchPrefix() != "pack" || p.Version() != 1 {
		t.Fatalf("duplicate define must not touch the stored row, got %v v%d (err %v)",
			p.MatchPrefix(), p.Version(), findErr)
	}

	// Even a deactivated id stays taken: deactivation is terminal, the id
	// is never recyclable.
	if err := w.deactivate.Execute(ctx, "PACK"); err != nil {
		t.Fatalf("deactivate PACK: %v", err)
	}
	if _, err := w.define.Execute(ctx, "PACK", "pack-v3", true,
		[]shared.Capability{"pack"}, shared.DestinationLocationRoleUnset,
		w.cycleTimeP95, shared.Eligibility{}); !errors.Is(err, usecases.ErrPathAlreadyExists) {
		t.Fatalf("re-define of a deactivated id must fail with ErrPathAlreadyExists, got %v", err)
	}
}

// TestUsecases_CPTScheduleGuardsDeactivation proves the ADR 0026/0028
// cross-aggregate invariant through the real store: DeactivatePath is
// refused while any site's CPT schedule still lists the path, and succeeds
// once the schedule is revised to stop referencing it — with the schedule
// change and the deactivation each publishing exactly one event.
func TestUsecases_CPTScheduleGuardsDeactivation(t *testing.T) {
	w := newWiredUsecases(t)
	ctx := context.Background()

	w.definePath(t, "SLAM", "slam", "slam")
	w.definePath(t, "SLAM2", "slam2", "slam")

	// A schedule for site sp1 whose only cutoff routes to SLAM.
	cutoff := mustCutoff(t, "sp1-1500", "SLAM")
	if _, err := w.defineSched.Execute(ctx, "sp1", "UTC", []cptschedule.Cutoff{cutoff}); err != nil {
		t.Fatalf("define schedule: %v", err)
	}

	// Deactivation is refused while the schedule references SLAM.
	err := w.deactivate.Execute(ctx, "SLAM")
	if !errors.Is(err, usecases.ErrPathReferencedByCPTSchedule) {
		t.Fatalf("deactivate of a referenced path must fail with ErrPathReferencedByCPTSchedule, got %v", err)
	}
	if still, findErr := w.getPath.Execute(ctx, "SLAM"); findErr != nil || !still.IsActive() {
		t.Fatal("the refused deactivation must leave SLAM active")
	}

	// Revise the schedule to route to SLAM2 instead; now SLAM deactivates.
	revised := mustCutoff(t, "sp1-1500", "SLAM2")
	w.defineSched.Clock = wiringClock{now: w.wall.Add(2 * time.Hour)}
	if _, err := w.defineSched.Execute(ctx, "sp1", "UTC", []cptschedule.Cutoff{revised}); err != nil {
		t.Fatalf("revise schedule: %v", err)
	}
	if err := w.deactivate.Execute(ctx, "SLAM"); err != nil {
		t.Fatalf("deactivate after schedule revision: %v", err)
	}

	// The read model reflects the revised schedule through the real repo.
	sched, err := w.getSched.Execute(ctx, "sp1")
	if err != nil {
		t.Fatalf("get schedule: %v", err)
	}
	cutoffs := sched.Cutoffs()
	if len(cutoffs) != 1 || len(cutoffs[0].EligiblePathIds()) != 1 || cutoffs[0].EligiblePathIds()[0] != "SLAM2" {
		t.Fatalf("schedule must now route to SLAM2, got %+v", cutoffs)
	}

	want := []string{
		"ProcessPathCreated", "ProcessPathCreated", // SLAM, SLAM2
		"CPTScheduleChanged", // define
		"CPTScheduleChanged", // revise
		"ProcessPathDeactivated",
	}
	if got := w.publisher.names(); !equalStrings(got, want) {
		t.Fatalf("published events %v, want %v", got, want)
	}
}

// equalStrings compares two slices element-wise.
func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
