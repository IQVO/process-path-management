//go:build integration

// A replay must never answer from an EXPIRED idempotency_keys row (the
// ADR 0043 addendum of 2026-11-22). The ADR 0018 sweeper purges expired
// rows hourly, but until the age guard existed nothing stopped the replay
// read from answering between a row's expiry and the next sweep. The
// replay read now carries the same TTL the sweeper uses (one constant,
// internal/idempotency.KeyTTL), so an expired key EXECUTES: the
// middleware takes the key over — deletes the expired row and inserts a
// fresh one in one transaction — and runs the real handler. These tests
// drive the REAL chi router over a REAL Postgres; the row is aged with a
// direct UPDATE because no request-path clock can be faked from outside.
package http_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	inboundhttp "github.com/claudioed/process-path-management/internal/adapters/inbound/http"
)

// ageKeyPastTTL moves a key row's created_at 30 hours back — past the 24h
// TTL — without touching anything else, which is exactly the state a row
// is in between its expiry and the sweeper's next pass.
func ageKeyPastTTL(t *testing.T, pool *pgxpool.Pool, key string) {
	t.Helper()
	if _, err := pool.Exec(context.Background(),
		`UPDATE idempotency_keys SET created_at = now() - interval '30 hours' WHERE key = $1`, key); err != nil {
		t.Fatalf("age key row %s: %v", key, err)
	}
}

// keyRowState reads the stored outcome and the row's age for assertions.
func keyRowState(t *testing.T, pool *pgxpool.Pool, key string) (statusCode int, createdAt time.Time) {
	t.Helper()
	if err := pool.QueryRow(context.Background(),
		`SELECT status_code, created_at FROM idempotency_keys WHERE key = $1`, key).
		Scan(&statusCode, &createdAt); err != nil {
		t.Fatalf("read key row %s: %v", key, err)
	}
	return statusCode, createdAt
}

// TestReplay_AgeGuard_ExpiredKeyExecutesInsteadOfAnswering is the core
// red-first proof: the same key + same body, replayed after the stored
// answer has expired but BEFORE the sweeper's next pass, must execute the
// handler again — the real truth for the current state — instead of
// replaying the stale stored answer.
func TestReplay_AgeGuard_ExpiredKeyExecutesInsteadOfAnswering(t *testing.T) {
	pool := idempotencyDB(t)
	router := newIdempotencyRouter(t, pool)

	post := func(key string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/process-paths", strings.NewReader(validDefinePathBody))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(inboundhttp.IdempotencyKeyHeader, key)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec
	}

	// A real define first, so the stored answer is the real 201.
	first := post("k-age-1")
	if first.Code != http.StatusCreated {
		t.Fatalf("first define = %d (%s), want 201", first.Code, first.Body.String())
	}

	// Age the row past the TTL: expired, not yet swept.
	ageKeyPastTTL(t, pool, "k-age-1")

	// Same key, same body. The expired 201 must NOT come back: the
	// request executes and DefinePath answers the truth for the state —
	// 409 path-already-exists (a FINAL refusal, ADR 0043) — stored fresh.
	retry := post("k-age-1")
	if retry.Code != http.StatusConflict {
		t.Fatalf("retry under an expired key = %d (%s), want 409 path-already-exists (the handler re-executed)", retry.Code, retry.Body.String())
	}
	if body := retry.Body.String(); !strings.Contains(body, "path-already-exists") {
		t.Fatalf("retry body = %s, want path-already-exists", body)
	}
	if got := retry.Header().Get(inboundhttp.IdempotencyReplayedHeader); got != "" {
		t.Fatalf("retry under an expired key carries %s=%q, want no replay header (the request executed)", inboundhttp.IdempotencyReplayedHeader, got)
	}

	// The takeover must leave a LIVE row holding the new answer, not the
	// expired one: the row is refreshed (created_at is now) and carries
	// the 409 a later retry will replay.
	stored, createdAt := keyRowState(t, pool, "k-age-1")
	if stored != http.StatusConflict {
		t.Fatalf("stored status under the taken-over key = %d, want 409", stored)
	}
	if age := time.Since(createdAt); age > time.Minute {
		t.Fatalf("taken-over key row is %s old, want a fresh row (created now)", age)
	}
	if n := countProcessPathRows(t, pool); n != 1 {
		t.Fatalf("process_paths rows = %d, want 1 (the retry executed but created nothing new)", n)
	}

	// A retry of THAT answer — the row is live now — replays normally:
	// the guard releases only expired rows, never live ones.
	again := post("k-age-1")
	if again.Code != http.StatusConflict || again.Header().Get(inboundhttp.IdempotencyReplayedHeader) != "true" {
		t.Fatalf("retry under the live taken-over key = %d replayed=%q, want the stored 409 replayed", again.Code, again.Header().Get(inboundhttp.IdempotencyReplayedHeader))
	}
	if again.Body.String() != retry.Body.String() {
		t.Fatalf("replayed body differs:\nfirst:  %s\nretry:  %s", retry.Body.String(), again.Body.String())
	}
}

// TestReplay_AgeGuard_ExpiredKeyWithAnotherBodyIsNotReuse proves the
// other half of "an expired key executes": a row that no longer owns its
// key cannot answer 409 idempotency-key-reused either, so a genuinely
// different request under the expired key creates its path.
func TestReplay_AgeGuard_ExpiredKeyWithAnotherBodyIsNotReuse(t *testing.T) {
	pool := idempotencyDB(t)
	router := newIdempotencyRouter(t, pool)

	post := func(key, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/process-paths", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(inboundhttp.IdempotencyKeyHeader, key)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec
	}

	if rec := post("k-age-2", validDefinePathBody); rec.Code != http.StatusCreated {
		t.Fatalf("first define = %d (%s), want 201", rec.Code, rec.Body.String())
	}
	ageKeyPastTTL(t, pool, "k-age-2")

	otherBody := `{"pathId":"AGE-2","matchPrefix":"age","direct":true,"requiredCapabilities":["pick"],"cycleTimeP95":"1h"}`
	rec := post("k-age-2", otherBody)
	if rec.Code != http.StatusCreated {
		t.Fatalf("a different body under an expired key = %d (%s), want 201 (a new request, not idempotency-key-reused)", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get(inboundhttp.IdempotencyReplayedHeader); got != "" {
		t.Fatalf("a different body under an expired key carries %s=%q, want no replay header (the request executed)", inboundhttp.IdempotencyReplayedHeader, got)
	}
	if n := countProcessPathRows(t, pool); n != 2 {
		t.Fatalf("process_paths rows = %d, want 2 (both defines executed)", n)
	}
}
