//go:build integration

package http_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestListPaths_Postgres_WalksEveryPage_NoDuplicateNoSkip is the real-router,
// real-Postgres proof of the keyset paging (REST guideline section 3): more
// rows than `limit`, ids differing only by case and numeric suffix, and the
// audit view next to the active-only default. Walking the pages must yield
// exactly the unpaged order, each id once, for every page size.
func TestListPaths_Postgres_WalksEveryPage_NoDuplicateNoSkip(t *testing.T) {
	pool := idempotencyDB(t)
	router := newIdempotencyRouter(t, pool)

	ids := []string{"PACK", "PICK", "PICK-1", "PICK-10", "PICK-2", "REBIN", "SLAM", "pick", "stow"}
	for i, id := range ids {
		body := fmt.Sprintf(`{"pathId":%q,"matchPrefix":"p","direct":true,"requiredCapabilities":["cap"],"cycleTimeP95":"2h"}`, id)
		if rec := doKeyed(router, http.MethodPost, "/process-paths", fmt.Sprintf("seed-%d", i), body); rec.Code != http.StatusCreated {
			t.Fatalf("seed %s = %d: %s", id, rec.Code, rec.Body.String())
		}
	}

	want := walkPaths(t, router, "limit=500")
	if len(want) != len(ids) {
		t.Fatalf("unpaged list has %d rows, want %d: %v", len(want), len(ids), want)
	}
	seen := map[string]bool{}
	for _, id := range want {
		if seen[id] {
			t.Fatalf("unpaged list repeats %s: %v", id, want)
		}
		seen[id] = true
	}
	for _, limit := range []int{1, 2, 3, 4, len(ids) - 1, len(ids), len(ids) + 1} {
		if got := walkPaths(t, router, fmt.Sprintf("limit=%d", limit)); strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("limit=%d walk = %v, want %v", limit, got, want)
		}
	}

	for _, id := range []string{"PICK-2", "SLAM"} {
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, httptest.NewRequest(http.MethodDelete, "/process-paths/"+id, nil))
		if rr.Code != http.StatusNoContent {
			t.Fatalf("deactivate %s = %d", id, rr.Code)
		}
	}
	if got := walkPaths(t, router, "limit=3"); len(got) != len(ids)-2 {
		t.Fatalf("active-only walk = %v, want %d rows", got, len(ids)-2)
	}
	if got := walkPaths(t, router, "all=true&limit=3"); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("all=true walk = %v, want %v", got, want)
	}
}
