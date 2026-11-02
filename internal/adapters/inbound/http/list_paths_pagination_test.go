package http_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

// pathsPageBody is the GET /process-paths answer: {items, nextCursor}.
type pathsPageBody struct {
	Items []struct {
		PathID string `json:"pathId"`
		Status string `json:"status"`
	} `json:"items"`
	NextCursor string `json:"nextCursor"`
}

func getJSON(t *testing.T, router http.Handler, target string) (int, []byte) {
	t.Helper()
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, target, nil))
	return rr.Code, rr.Body.Bytes()
}

func definePathIDs(t *testing.T, router http.Handler, ids ...string) {
	t.Helper()
	for _, id := range ids {
		body := fmt.Sprintf(`{"pathId":%q,"matchPrefix":"p","direct":true,"requiredCapabilities":["cap"],"cycleTimeP95":"2h"}`, id)
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/process-paths", bytes.NewBufferString(body)))
		if rr.Code != http.StatusCreated {
			t.Fatalf("setup define %s: want 201, got %d: %s", id, rr.Code, rr.Body.String())
		}
	}
}

// walkPaths follows nextCursor from `first` until it is absent and returns
// every pathId in the order served; the filters are repeated on every page.
func walkPaths(t *testing.T, router http.Handler, query string) []string {
	t.Helper()
	var ids []string
	cursor := ""
	for pages := 0; ; pages++ {
		if pages > 100 {
			t.Fatalf("paging did not terminate after %d pages", pages)
		}
		target := "/process-paths?" + query
		if cursor != "" {
			target += "&cursor=" + url.QueryEscape(cursor)
		}
		code, body := getJSON(t, router, target)
		if code != http.StatusOK {
			t.Fatalf("GET %s = %d: %s", target, code, body)
		}
		var pg pathsPageBody
		if err := json.Unmarshal(body, &pg); err != nil {
			t.Fatalf("decode page: %v: %s", err, body)
		}
		for _, it := range pg.Items {
			ids = append(ids, it.PathID)
		}
		if pg.NextCursor == "" {
			return ids
		}
		cursor = pg.NextCursor
	}
}

// TestListPaths_WalksEveryPage_NoDuplicateNoSkip: more rows than `limit`, ids
// that differ only by case/suffix, and the audit view (all=true) next to the
// default active-only view. Walking the pages must return exactly the
// unpaged order, each id once.
func TestListPaths_WalksEveryPage_NoDuplicateNoSkip(t *testing.T) {
	router := newTestServer(t)
	all := []string{"PACK", "PICK", "PICK-1", "PICK-10", "PICK-2", "REBIN", "SLAM", "pick", "stow"}
	definePathIDs(t, router, all...)

	want := walkPaths(t, router, "limit=500")
	if len(want) != len(all) {
		t.Fatalf("unpaged list has %d rows, want %d: %v", len(want), len(all), want)
	}
	for _, limit := range []int{1, 2, 3, 4, len(all) - 1, len(all), len(all) + 1} {
		got := walkPaths(t, router, fmt.Sprintf("limit=%d", limit))
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("limit=%d walk = %v, want %v", limit, got, want)
		}
	}

	// Deactivate two: the default view skips them on every page, all=true keeps them.
	for _, id := range []string{"PICK-2", "SLAM"} {
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, httptest.NewRequest(http.MethodDelete, "/process-paths/"+id, nil))
		if rr.Code != http.StatusNoContent {
			t.Fatalf("deactivate %s = %d", id, rr.Code)
		}
	}
	if got := walkPaths(t, router, "limit=3"); len(got) != len(all)-2 {
		t.Fatalf("active-only walk = %v, want %d rows", got, len(all)-2)
	}
	if got := walkPaths(t, router, "all=true&limit=3"); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("all=true walk = %v, want %v", got, want)
	}
}

func TestListPaths_LastPageHasNoNextCursor_AndEmptyIsAnEmptyPage(t *testing.T) {
	router := newTestServer(t)
	code, body := getJSON(t, router, "/process-paths")
	if code != http.StatusOK || string(bytes.TrimSpace(body)) != `{"items":[]}` {
		t.Fatalf("empty catalogue = %d %s, want 200 {\"items\":[]}", code, body)
	}
	definePathIDs(t, router, "PICK", "PACK")
	_, body = getJSON(t, router, "/process-paths?limit=2")
	var pg pathsPageBody
	if err := json.Unmarshal(body, &pg); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(pg.Items) != 2 || pg.NextCursor != "" {
		t.Fatalf("exactly-full last page = %+v, want 2 items and no nextCursor", pg)
	}
}

// TestListPaths_RejectBadLimitCursorAndRetiredParams pins the 400s of a paged
// collection (REST guideline 3.4, 4.2): bad limit, bad cursor, and every
// retired paging parameter (`after`, `offset`, `nextToken`, `from`) or any
// other undeclared name fails loudly instead of reading page one forever.
func TestListPaths_RejectBadLimitCursorAndRetiredParams(t *testing.T) {
	router := newTestServer(t)
	definePathIDs(t, router, "PICK")
	cases := []struct{ query, slug string }{
		{"limit=0", "invalid-limit"},
		{"limit=501", "invalid-limit"},
		{"limit=-1", "invalid-limit"},
		{"limit=abc", "invalid-limit"},
		{"cursor=%25%25%25", "invalid-cursor"},
		{"cursor=bm90LWpzb24", "invalid-cursor"},                                   // base64url, but not a position
		{"cursor=" + url.QueryEscape("eyJzIjoiYSIsImkiOiJiIn0"), "invalid-cursor"}, // sort != id: not issued by this API
		{"after=PICK", "unknown-query-parameter"},
		{"offset=10", "unknown-query-parameter"},
		{"nextToken=x", "unknown-query-parameter"},
		{"from=2026-01-01", "unknown-query-parameter"},
		{"page=2", "unknown-query-parameter"},
		{"all=true&after=PICK", "unknown-query-parameter"},
	}
	for _, c := range cases {
		code, body := getJSON(t, router, "/process-paths?"+c.query)
		if code != http.StatusBadRequest {
			t.Errorf("GET ?%s = %d, want 400: %s", c.query, code, body)
			continue
		}
		var p struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(body, &p); err != nil || !bytes.HasSuffix([]byte(p.Type), []byte("/"+c.slug)) {
			t.Errorf("GET ?%s type = %q, want .../%s", c.query, p.Type, c.slug)
		}
	}
}
