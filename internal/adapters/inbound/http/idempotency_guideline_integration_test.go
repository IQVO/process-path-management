//go:build integration

// REST API guideline conformance of the Idempotency-Key middleware against a
// real Postgres 16 (testcontainers), through the real chi middleware: the key
// is GLOBAL (looked up by key alone, compared by method + path + body hash),
// a malformed key is 400 idempotency-key-invalid, a 5xx is never stored, and
// PUT/DELETE need no key.
package http_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	inboundhttp "github.com/claudioed/process-path-management/internal/adapters/inbound/http"
)

// stubRouter mounts two POST routes (and one that always answers 500) behind
// the REAL RequireIdempotencyKey middleware; `calls` counts handler executions
// so a replay (handler not run) is told apart from a re-execution.
func stubRouter(pool *pgxpool.Pool, calls *atomic.Int32) http.Handler {
	r := chi.NewRouter()
	ok := func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}
	boom := func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}
	g := r.With(inboundhttp.RequireIdempotencyKey(pool))
	g.Post("/a", ok)
	g.Post("/b", ok)
	g.Patch("/a", ok)
	g.Post("/boom", boom)
	return r
}

func doKeyed(h http.Handler, method, path, key, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if key != "" {
		req.Header.Set(inboundhttp.IdempotencyKeyHeader, key)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func problemSlug(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var p struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("decode problem: %v (body: %s)", err, rec.Body.String())
	}
	return p.Type[strings.LastIndex(p.Type, "/")+1:]
}

// TestIdempotency_KeyIsGlobal_ReuseOnAnotherRouteOrMethodIs409: the stored
// record is found by the key alone, so the same key on another path or with
// another method is NOT a fresh key but 409 idempotency-key-reused, and the
// handler never runs for it.
func TestIdempotency_KeyIsGlobal_ReuseOnAnotherRouteOrMethodIs409(t *testing.T) {
	pool := idempotencyDB(t)
	var calls atomic.Int32
	h := stubRouter(pool, &calls)

	if rec := doKeyed(h, http.MethodPost, "/a", "key-global-1", `{"n":1}`); rec.Code != http.StatusCreated {
		t.Fatalf("first call = %d, want 201", rec.Code)
	}
	for name, c := range map[string]struct{ method, path string }{
		"another path":   {http.MethodPost, "/b"},
		"another method": {http.MethodPatch, "/a"},
	} {
		rec := doKeyed(h, c.method, c.path, "key-global-1", `{"n":1}`)
		if rec.Code != http.StatusConflict || problemSlug(t, rec) != "idempotency-key-reused" {
			t.Fatalf("%s: = %d %s, want 409 idempotency-key-reused", name, rec.Code, rec.Body.String())
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler ran %d times, want 1 (reuse must never execute)", got)
	}
	// The identical request still replays.
	rec := doKeyed(h, http.MethodPost, "/a", "key-global-1", `{"n":1}`)
	if rec.Code != http.StatusCreated || rec.Header().Get("Idempotency-Replayed") != "true" {
		t.Fatalf("identical retry = %d replayed=%q, want 201 replayed=true", rec.Code, rec.Header().Get("Idempotency-Replayed"))
	}
}

// TestIdempotency_ServerError_IsNotStoredAndRetryReExecutes: a 5xx is not a
// deterministic answer (guideline 2.5), so no key row is kept and the retry
// with the same key runs the handler again.
func TestIdempotency_ServerError_IsNotStoredAndRetryReExecutes(t *testing.T) {
	pool := idempotencyDB(t)
	var calls atomic.Int32
	h := stubRouter(pool, &calls)

	for i := 1; i <= 2; i++ {
		rec := doKeyed(h, http.MethodPost, "/boom", "key-5xx-1", `{}`)
		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("attempt %d = %d, want 500", i, rec.Code)
		}
		if rec.Header().Get("Idempotency-Replayed") != "" {
			t.Fatalf("attempt %d: a re-execution must not carry Idempotency-Replayed", i)
		}
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("handler ran %d times, want 2 (the 5xx must not be replayed)", got)
	}
	var n int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM idempotency_keys WHERE key = $1", "key-5xx-1").Scan(&n); err != nil {
		t.Fatalf("count idempotency_keys: %v", err)
	}
	if n != 0 {
		t.Fatalf("idempotency_keys rows for a 5xx = %d, want 0", n)
	}
}

// TestIdempotency_MalformedKey_Is400Invalid: more than 128 characters, a
// control character or a non-ASCII byte is idempotency-key-invalid (a missing
// key stays idempotency-key-required), and nothing is recorded.
func TestIdempotency_MalformedKey_Is400Invalid(t *testing.T) {
	pool := idempotencyDB(t)
	var calls atomic.Int32
	h := stubRouter(pool, &calls)

	for name, key := range map[string]string{
		"129 characters": strings.Repeat("k", 129),
		"control char":   "key\x01bad",
		"non-ascii":      "chave-ção",
	} {
		rec := doKeyed(h, http.MethodPost, "/a", key, `{}`)
		if rec.Code != http.StatusBadRequest || problemSlug(t, rec) != "idempotency-key-invalid" {
			t.Fatalf("%s: = %d %s, want 400 idempotency-key-invalid", name, rec.Code, rec.Body.String())
		}
	}
	if rec := doKeyed(h, http.MethodPost, "/a", strings.Repeat("k", 128), `{}`); rec.Code != http.StatusCreated {
		t.Fatalf("a 128-character key = %d, want 201", rec.Code)
	}
	if rec := doKeyed(h, http.MethodPost, "/a", "", `{}`); rec.Code != http.StatusBadRequest || problemSlug(t, rec) != "idempotency-key-required" {
		t.Fatalf("missing key = %d %s, want 400 idempotency-key-required", rec.Code, rec.Body.String())
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler ran %d times, want 1 (only the valid key executes)", got)
	}
}

// TestIdempotency_PutAndDeleteTakeNoKey: PUT and DELETE are idempotent by
// method (guideline 2.1) and sit outside the middleware; a key sent to them
// is accepted and ignored, never recorded.
func TestIdempotency_PutAndDeleteTakeNoKey(t *testing.T) {
	pool := idempotencyDB(t)
	router := newIdempotencyRouter(t, pool)

	if rec := doKeyed(router, http.MethodPost, "/process-paths", "key-put-setup", validDefinePathBody); rec.Code != http.StatusCreated {
		t.Fatalf("setup = %d (%s)", rec.Code, rec.Body.String())
	}
	revise := `{"matchPrefix":"pick-b","requiredCapabilities":["pick"],"cycleTimeP95":"3h"}`
	for i := 0; i < 2; i++ {
		if rec := doKeyed(router, http.MethodPut, "/process-paths/PICK-1", "", revise); rec.Code != http.StatusOK {
			t.Fatalf("PUT without a key (#%d) = %d (%s), want 200", i+1, rec.Code, rec.Body.String())
		}
	}
	if rec := doKeyed(router, http.MethodPut, "/process-paths/PICK-1", "key-ignored", revise); rec.Code != http.StatusOK {
		t.Fatalf("PUT with a key = %d, want 200", rec.Code)
	}
	for i := 0; i < 2; i++ {
		if rec := doKeyed(router, http.MethodDelete, "/process-paths/PICK-1", "", ""); rec.Code != http.StatusNoContent {
			t.Fatalf("DELETE without a key (#%d) = %d, want 204", i+1, rec.Code)
		}
	}
	var n int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM idempotency_keys WHERE key = 'key-ignored'").Scan(&n); err != nil {
		t.Fatalf("count idempotency_keys: %v", err)
	}
	if n != 0 {
		t.Fatalf("a key sent to PUT was recorded (%d rows), want it ignored", n)
	}
}
