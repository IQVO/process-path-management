//go:build integration

package http_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	inboundhttp "github.com/claudioed/process-path-management/internal/adapters/inbound/http"
)

// TestNUL_IsAClientErrorAndDoesNotConsumeTheKey is the real-Postgres proof of
// the NUL guard (ADR 0037). A U+0000 in a text field made the Postgres text
// parameter abort the transaction (SQLSTATE 22021, then 25P02) and surfaced as
// a 500 on the real runtime; the in-memory adapters never noticed. It is now a
// 400 malformed-request-body (404 in the path) decided BEFORE the
// Idempotency-Key middleware records anything, so the key stays free for the
// corrected request.
func TestNUL_IsAClientErrorAndDoesNotConsumeTheKey(t *testing.T) {
	pool := idempotencyDB(t)
	router := newIdempotencyRouter(t, pool)

	post := func(body, key string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/process-paths", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(inboundhttp.IdempotencyKeyHeader, key)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec
	}
	countKeys := func() int {
		var n int
		if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM idempotency_keys WHERE key = $1", "key-nul-1").Scan(&n); err != nil {
			t.Fatalf("count idempotency_keys: %v", err)
		}
		return n
	}

	for name, body := range map[string]string{
		"escaped NUL": `{"pathId":"PI\u0000CK","matchPrefix":"pick","direct":true,"requiredCapabilities":["pick"],"cycleTimeP95":"2h"}`,
		"raw NUL":     "{\"pathId\":\"PI\x00CK\",\"matchPrefix\":\"pick\",\"direct\":true,\"requiredCapabilities\":[\"pick\"],\"cycleTimeP95\":\"2h\"}",
	} {
		rec := post(body, "key-nul-1")
		if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "malformed-request-body") {
			t.Fatalf("%s: status = %d, want 400 malformed-request-body (body: %s)", name, rec.Code, rec.Body.String())
		}
		if n := countKeys(); n != 0 {
			t.Fatalf("%s: idempotency_keys rows for the key = %d, want 0 (the rejected request must not consume the key)", name, n)
		}
	}

	// A NUL in the path is a plain 404, never a 500.
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/process-paths/PI%00CK", http.NoBody))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("NUL in the path: status = %d, want 404 (body: %s)", rec.Code, rec.Body.String())
	}

	// The key is still free: the corrected request under the same key succeeds.
	if rec := post(validDefinePathBody, "key-nul-1"); rec.Code != http.StatusCreated {
		t.Fatalf("corrected request under the same key: status = %d, want 201 (body: %s)", rec.Code, rec.Body.String())
	}
	if got := countProcessPathRows(t, pool); got != 1 {
		t.Fatalf("process_paths rows = %d, want 1", got)
	}
}
