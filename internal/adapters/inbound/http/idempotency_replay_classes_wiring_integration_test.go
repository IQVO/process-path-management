//go:build integration

package http_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	inboundhttp "github.com/claudioed/process-path-management/internal/adapters/inbound/http"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/process-path-management/internal/pgtx"
)

// The store half of the replay-class test pair (ADR 0043): the production
// middleware over a real Postgres — the key row lives in the request's own
// transaction, the handler behind a savepoint (ADR 0011, design A).
//
// ONE container is booted per test binary (the reference test asks for a
// clean store once per catalogue entry); every newReplayOps call gets its
// own FRESH DATABASE with the migrations applied — the exact recipe of the
// throwaway Postgres the other suites of this package boot per test
// (idempotencyDB), minus one container per test. The container is reaped by
// testcontainers' ryuk when the binary exits. (A template-clone variant was
// tried first and the template came out empty on some CI runs while
// RunMigrations returned nil; per-database migrations are the boring,
// already-proven path.)
var (
	replayContainerOnce sync.Once
	replayContainer     *tcpostgres.PostgresContainer
	replayContainerErr  error
	replayDBSeq         int
)

// replayDatabaseURL rewrites the container's base connection string to name
// db in the URL PATH (postgres://user:***@host:port/db?...), not as a
// `database=` query parameter: the path form selects the same database for
// every driver in play here (pgx's pools, golang-migrate's).
func replayDatabaseURL(ctx context.Context, db string) (string, error) {
	base, err := replayContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return "", err
	}
	slash := strings.LastIndex(base, "/")
	question := strings.Index(base, "?")
	if slash < 0 || question < slash {
		return "", fmt.Errorf("unexpected connection string %q", base)
	}
	return base[:slash+1] + db + base[question:], nil
}

func replayStore(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	replayContainerOnce.Do(func() {
		replayContainer, replayContainerErr = tcpostgres.Run(ctx, "postgres:16-alpine",
			tcpostgres.WithDatabase("pathmgmt"),
			tcpostgres.WithUsername("pathmgmt"),
			tcpostgres.WithPassword("pathmgmt"),
			tcpostgres.BasicWaitStrategies(),
		)
	})
	if replayContainerErr != nil {
		t.Fatalf("the shared replay Postgres: %v", replayContainerErr)
	}

	replayDBSeq++
	db := fmt.Sprintf("pathmgmt_replay_%d", replayDBSeq)
	url, err := replayContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	admin, err := postgres.NewPool(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+db); err != nil {
		t.Fatalf("create the clean store: %v", err)
	}
	durl, err := replayDatabaseURL(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	// The idempotencyDB recipe, verbatim: every migration, twice (the second
	// run must be a no-op).
	for range 2 {
		if err := postgres.RunMigrations(durl, "../../../../migrations"); err != nil {
			t.Fatalf("migrations for the clean store %s: %v", db, err)
		}
	}
	pool, err := postgres.NewPool(ctx, durl)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	// A broken store must fail HERE with a clear message, not as 48 cryptic
	// per-entry failures.
	if _, err := pool.Exec(ctx, "SELECT 1 FROM idempotency_keys LIMIT 1"); err != nil {
		t.Fatalf("the clean store %s has no idempotency_keys after its migrations: %v", db, err)
	}
	return pool
}

// newReplayOps is the ONLY file of the pair that knows the store: the six
// operations of replayOps over one clean store. It names no driver beyond
// the container helper; the pool arrives by type inference.
func newReplayOps(t *testing.T) replayOps {
	t.Helper()
	pool := replayStore(t)
	if _, err := pool.Exec(context.Background(), `CREATE TABLE replay_probe (tag text NOT NULL)`); err != nil {
		t.Fatalf("create the probe table: %v", err)
	}
	count := func(t *testing.T, query string, args ...any) int {
		t.Helper()
		var n int
		if err := pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		return n
	}
	return replayOps{
		Catalogue: func() []problemEntry {
			var out []problemEntry
			for _, p := range inboundhttp.ProblemTypesForTest() {
				out = append(out, problemEntry{Slug: p.Slug, Status: p.Status, Class: p.Class})
			}
			return out
		},
		Middleware: func(_ *testing.T, next http.Handler) http.Handler {
			return inboundhttp.RequireIdempotencyKey(pool)(next)
		},
		Refuse: func(w http.ResponseWriter, r *http.Request, e problemEntry) {
			inboundhttp.RefuseForTest(w, r, e.Slug, e.Status)
		},
		Write: func(ctx context.Context, tag string) error {
			tx, ok := pgtx.TxFrom(ctx)
			if !ok {
				return fmt.Errorf("no transaction bound: the middleware did not join the request")
			}
			_, err := tx.Exec(ctx, `INSERT INTO replay_probe (tag) VALUES ($1)`, tag)
			return err
		},
		Committed: func(t *testing.T, tag string) int {
			return count(t, `SELECT count(*) FROM replay_probe WHERE tag = $1`, tag)
		},
		KeyRows: func(t *testing.T, key string) int {
			return count(t, `SELECT count(*) FROM idempotency_keys WHERE key = $1`, key)
		},
	}
}

// The OUTCOME branch (ADR 0043). No problem type of this service declares
// OUTCOME — no use case commits state and then refuses — but the middleware
// honours the class, so a future OUTCOME entry is safe: this probe commits
// one write and refuses 409 with class OUTCOME. The write commits WITH the
// stored answer (one transaction), a replay records nothing new, and an
// OUTCOME whose writes died with the transaction (the savepoint cannot be
// released) is a 500 that stores nothing.
func TestOutcomeBranchCommitsTheWritesWithTheStoredAnswerAndAReplayAddsNothing(t *testing.T) {
	pool := replayStore(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `CREATE TABLE outcome_probe (tag text NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	mw := inboundhttp.RequireIdempotencyKey(pool)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		tx, ok := pgtx.TxFrom(r.Context())
		if !ok {
			t.Error("the middleware did not bind a transaction into the request context")
			return
		}
		if _, err := tx.Exec(r.Context(), `INSERT INTO outcome_probe (tag) VALUES ('committed')`); err != nil {
			t.Error(err)
		}
		inboundhttp.ProbeRefuseForTest(w, r, http.StatusConflict, "OUTCOME")
	}))
	count := func(sql string) int {
		var n int
		if err := pool.QueryRow(ctx, sql).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	req := func(key string) *http.Request {
		req := httptest.NewRequest(http.MethodPost, "/process-paths", strings.NewReader(`{}`))
		req.Header.Set(inboundhttp.IdempotencyKeyHeader, key)
		return req
	}

	first := httptest.NewRecorder()
	mw.ServeHTTP(first, req("k-outcome"))
	if first.Code != http.StatusConflict {
		t.Fatalf("first = %d %s, want 409", first.Code, first.Body.String())
	}
	if n := count(`SELECT count(*) FROM outcome_probe`); n != 1 {
		t.Fatalf("committed writes after the refusal = %d, want 1: an OUTCOME refusal commits what the command did", n)
	}
	if n := count(`SELECT count(*) FROM idempotency_keys WHERE key = 'k-outcome'`); n != 1 {
		t.Fatalf("key rows = %d, want 1: the answer is stored in the same transaction as the writes", n)
	}
	for i := 0; i < 3; i++ {
		rep := httptest.NewRecorder()
		mw.ServeHTTP(rep, req("k-outcome"))
		if rep.Code != http.StatusConflict || rep.Header().Get(inboundhttp.IdempotencyReplayedHeader) != "true" || rep.Body.String() != first.Body.String() {
			t.Fatalf("replay %d = %d replayed=%q, want the stored 409", i, rep.Code, rep.Header().Get(inboundhttp.IdempotencyReplayedHeader))
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("handler ran %d times, want 1", calls.Load())
	}
	if n := count(`SELECT count(*) FROM outcome_probe`); n != 1 {
		t.Fatalf("committed writes after 3 replays = %d, want 1: a replay records nothing new", n)
	}
}

// What a concurrent duplicate sees when the first request ends RETRYABLE
// (ADR 0043): its INSERT blocks on the first transaction's key row until
// that transaction resolves. The rollback removes the row, so the blocked
// insert SUCCEEDS and the duplicate executes the work fresh — no replay
// header, nothing of the released refusal is observable.
func TestConcurrentDuplicateWhileTheFirstEndsRetryableExecutesFresh(t *testing.T) {
	ops := newReplayOps(t)
	entered := make(chan struct{})
	var calls atomic.Int64
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			close(entered)
			time.Sleep(150 * time.Millisecond) // hold the key row's transaction open
			ops.Refuse(w, r, problemEntry{Slug: "concurrent-modification", Status: http.StatusConflict, Class: "RETRYABLE"})
			return
		}
		if err := ops.Write(r.Context(), "won"); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusCreated)
	})
	mw := ops.Middleware(t, handler)
	doPost := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, route, strings.NewReader(`{"n":1}`))
		req.Header.Set(idempotencyKeyHeader, "k-dup")
		rec := httptest.NewRecorder()
		mw.ServeHTTP(rec, req)
		return rec
	}

	var first, second *httptest.ResponseRecorder
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		first = doPost()
	}()
	<-entered // the first request holds the key row's transaction open
	wg.Add(1)
	go func() {
		defer wg.Done()
		second = doPost()
	}()
	wg.Wait()

	if first.Code != http.StatusConflict {
		t.Fatalf("the first (retiring) request = %d %s, want the RETRYABLE 409", first.Code, first.Body.String())
	}
	if second.Code != http.StatusCreated {
		t.Fatalf("the concurrent duplicate = %d %s, want a fresh 201: the released key must not block it", second.Code, second.Body.String())
	}
	if replayed := second.Header().Get(idempotencyReplayedHeader); replayed != "" {
		t.Fatalf("the concurrent duplicate carries %s: %q, want none: it executed the work, it was not answered from the store", idempotencyReplayedHeader, replayed)
	}
	if calls.Load() != 2 {
		t.Fatalf("handler ran %d times, want 2: both requests executed", calls.Load())
	}
	if got := ops.Committed(t, "won"); got != 1 {
		t.Fatalf("committed writes = %d, want 1", got)
	}
	if got := ops.KeyRows(t, "k-dup"); got != 1 {
		t.Fatalf("key rows after the race = %d, want 1: the duplicate's 201 is stored, the refusal was never stored", got)
	}
	third := doPost()
	if third.Code != http.StatusCreated || third.Header().Get(idempotencyReplayedHeader) != "true" || calls.Load() != 2 {
		t.Fatalf("third request = %d replayed=%q calls=%d, want the stored 201 replayed", third.Code, third.Header().Get(idempotencyReplayedHeader), calls.Load())
	}
}
