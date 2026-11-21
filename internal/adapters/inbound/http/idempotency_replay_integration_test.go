//go:build integration

// Router-level proof of the replay classes through the REAL chi router and
// a REAL Postgres (ADR 0043 on top of ADR 0011): the concurrent race this
// service can actually produce on its one keyed route (POST /process-paths)
// is two different keys defining the same caller-chosen PathId. The loser
// answers 409 path-already-exists — a FINAL refusal (a taken PathId is
// taken forever: deactivation keeps the row, there is no reactivate use
// case and no delete), so its key row IS stored and a retry under the
// loser's key replays it. The fleet's `concurrent-modification` RETRYABLE
// loser is not reachable on this route (the natural-key race maps to
// ErrAlreadyExists, not to the optimistic-lock conflict); the middleware's
// release of it is proved over a real Postgres by the reference test
// (idempotency_replay_classes_integration_test.go) and by
// TestConcurrentDuplicateWhileTheFirstEndsRetryableExecutesFresh.
package http_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	inboundhttp "github.com/claudioed/process-path-management/internal/adapters/inbound/http"
)

// TestReplayClasses_RealRouterRace_LosersFinal409IsStoredAndReplayed drives
// the real router: two concurrent defines of the same PathId under two
// different keys. Exactly one 201 is created; the loser's 409
// path-already-exists (FINAL) is stored under the loser's key and replayed
// byte-identically on retry; no second process_paths row ever appears.
func TestReplayClasses_RealRouterRace_LosersFinal409IsStoredAndReplayed(t *testing.T) {
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

	var recs [2]*httptest.ResponseRecorder
	keys := [2]string{"k-race-a", "k-race-b"}
	var wg sync.WaitGroup
	for i := range recs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			recs[i] = post(keys[i])
		}(i)
	}
	wg.Wait()

	won, lost := -1, -1
	for i, rec := range recs {
		switch rec.Code {
		case http.StatusCreated:
			if won >= 0 {
				t.Fatalf("two 201s: the race must have exactly one winner")
			}
			won = i
		case http.StatusConflict:
			if lost >= 0 {
				t.Fatalf("two 409s: the race must have exactly one loser")
			}
			lost = i
		default:
			t.Fatalf("racer %d answered %d (%s), want 201 or 409", i, rec.Code, rec.Body.String())
		}
	}
	if !strings.Contains(recs[lost].Body.String(), "path-already-exists") {
		t.Fatalf("the loser's 409 is %s, want path-already-exists (the natural-key race maps to ErrAlreadyExists)", recs[lost].Body.String())
	}
	if got := countProcessPathRows(t, pool); got != 1 {
		t.Fatalf("process_paths rows after the race = %d, want 1", got)
	}

	// The loser's FINAL refusal is stored under its own key and replays.
	loserKey := keys[lost]
	var stored int
	if err := pool.QueryRow(context.Background(),
		`SELECT status_code FROM idempotency_keys WHERE key = $1`, loserKey).Scan(&stored); err != nil {
		t.Fatalf("the loser's key row: %v (a FINAL refusal must be stored)", err)
	}
	if stored != http.StatusConflict {
		t.Fatalf("stored status = %d, want 409", stored)
	}
	retry := post(loserKey)
	if retry.Code != http.StatusConflict || retry.Header().Get(inboundhttp.IdempotencyReplayedHeader) != "true" {
		t.Fatalf("retry under the loser's key = %d replayed=%q, want the stored 409 replayed", retry.Code, retry.Header().Get(inboundhttp.IdempotencyReplayedHeader))
	}
	if retry.Body.String() != recs[lost].Body.String() {
		t.Fatalf("replayed body differs:\nfirst:  %s\nretry:  %s", recs[lost].Body.String(), retry.Body.String())
	}
	if got := countProcessPathRows(t, pool); got != 1 {
		t.Fatalf("process_paths rows after the replay = %d, want 1", got)
	}

	// The winner's 201 is stored and replays under the winner's key too.
	winnerKey := keys[won]
	again := post(winnerKey)
	if again.Code != http.StatusCreated || again.Header().Get(inboundhttp.IdempotencyReplayedHeader) != "true" {
		t.Fatalf("retry under the winner's key = %d replayed=%q, want the stored 201 replayed", again.Code, again.Header().Get(inboundhttp.IdempotencyReplayedHeader))
	}
}

// TestReplayClasses_RealRouter_MalformedBodyIsFinalAndStored is the FINAL
// case a real handler of this service answers behind the
// middleware: a body that is not valid JSON is refused 400
// malformed-request-body (the request's own bytes alone) and replayed.
func TestReplayClasses_RealRouter_MalformedBodyIsFinalAndStored(t *testing.T) {
	pool := idempotencyDB(t)
	router := newIdempotencyRouter(t, pool)

	post := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/process-paths", strings.NewReader(`{"pathId":`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(inboundhttp.IdempotencyKeyHeader, "k-malformed")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec
	}

	first := post()
	if first.Code != http.StatusBadRequest || !strings.Contains(first.Body.String(), "malformed-request-body") {
		t.Fatalf("first = %d %s, want 400 malformed-request-body", first.Code, first.Body.String())
	}
	retry := post()
	if retry.Code != http.StatusBadRequest || retry.Header().Get(inboundhttp.IdempotencyReplayedHeader) != "true" || retry.Body.String() != first.Body.String() {
		t.Fatalf("retry = %d replayed=%q, want the stored 400 replayed byte-identically", retry.Code, retry.Header().Get(inboundhttp.IdempotencyReplayedHeader))
	}
	if got := countProcessPathRows(t, pool); got != 0 {
		t.Fatalf("process_paths rows = %d, want 0", got)
	}
}
