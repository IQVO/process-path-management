package http_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func doJSON(t *testing.T, router http.Handler, method, target, body string) (int, []byte) {
	t.Helper()
	var rd *bytes.Buffer
	if body != "" {
		rd = bytes.NewBufferString(body)
	} else {
		rd = &bytes.Buffer{}
	}
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, httptest.NewRequest(method, target, rd))
	return rr.Code, rr.Body.Bytes()
}

func putFlowBody(name string, stages ...string) string {
	var st []string
	for _, s := range stages {
		p, ty, _ := strings.Cut(s, ":")
		st = append(st, fmt.Sprintf(`{"pathId":%q,"processType":%q}`, p, ty))
	}
	return fmt.Sprintf(`{"name":%q,"stages":[%s]}`, name, strings.Join(st, ","))
}

func flowProblemSlug(t *testing.T, body []byte) string {
	t.Helper()
	var p struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(body, &p); err != nil {
		t.Fatalf("decode problem: %v: %s", err, body)
	}
	return p.Type[strings.LastIndex(p.Type, "/")+1:]
}

func TestPutProcessFlow_FirstIs201_LaterIs200_NoChangeKeepsVersion(t *testing.T) {
	router := newTestServer(t)
	definePathIDs(t, router, "PICK", "PACK")

	code, body := doJSON(t, router, http.MethodPut, "/process-flows/f1", putFlowBody("Flow", "PICK:PICK", "PACK:PACK"))
	if code != http.StatusCreated {
		t.Fatalf("first PUT: want 201, got %d: %s", code, body)
	}
	var resp struct {
		FlowId  string `json:"flowId"`
		Status  string `json:"status"`
		Version int    `json:"version"`
		Stages  []struct {
			PathId      string `json:"pathId"`
			ProcessType string `json:"processType"`
		} `json:"stages"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.FlowId != "f1" || resp.Status != "ACTIVE" || resp.Version != 1 || len(resp.Stages) != 2 || resp.Stages[1].PathId != "PACK" {
		t.Fatalf("response %+v", resp)
	}

	code, body = doJSON(t, router, http.MethodPut, "/process-flows/f1", putFlowBody("Flow", "PICK:PICK", "PACK:PACK"))
	if code != http.StatusOK || !strings.Contains(string(body), `"version":1`) {
		t.Fatalf("same PUT: want 200 at version 1, got %d: %s", code, body)
	}
	code, body = doJSON(t, router, http.MethodPut, "/process-flows/f1", putFlowBody("Flow", "PICK:PICK"))
	if code != http.StatusOK || !strings.Contains(string(body), `"version":2`) {
		t.Fatalf("revise: want 200 at version 2, got %d: %s", code, body)
	}
}

func TestPutProcessFlow_Rejections(t *testing.T) {
	tests := []struct {
		name   string
		body   string
		status int
		slug   string
	}{
		{"not json", `{`, http.StatusBadRequest, "malformed-request-body"},
		{"unknown path", putFlowBody("F", "GHOST:PICK"), http.StatusUnprocessableEntity, "ineligible-stage-path"},
		{"empty name", putFlowBody("", "PICK:PICK"), http.StatusUnprocessableEntity, "empty-flow-name"},
		{"no stages", `{"name":"F","stages":[]}`, http.StatusUnprocessableEntity, "no-stages"},
		{"absent stages", `{"name":"F"}`, http.StatusUnprocessableEntity, "no-stages"},
		{"bad process type", putFlowBody("F", "PICK:WELD"), http.StatusUnprocessableEntity, "invalid-process-type"},
		{"duplicate path", putFlowBody("F", "PICK:PICK", "PICK:PACK"), http.StatusUnprocessableEntity, "duplicate-stage-path"},
		{"null stage", `{"name":"F","stages":[null]}`, http.StatusUnprocessableEntity, "empty-stage-path-id"},
		{"empty stage path", putFlowBody("F", ":PICK"), http.StatusUnprocessableEntity, "empty-stage-path-id"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			router := newTestServer(t)
			definePathIDs(t, router, "PICK")
			code, body := doJSON(t, router, http.MethodPut, "/process-flows/f1", tc.body)
			if code != tc.status || flowProblemSlug(t, body) != tc.slug {
				t.Fatalf("got %d %s, want %d %s", code, body, tc.status, tc.slug)
			}
			if code, _ := doJSON(t, router, http.MethodGet, "/process-flows/f1", ""); code != http.StatusNotFound {
				t.Fatalf("a rejected PUT must create nothing, GET = %d", code)
			}
		})
	}
}

func TestProcessFlow_GetRetireAndTheRetiredRevise(t *testing.T) {
	router := newTestServer(t)
	definePathIDs(t, router, "PICK")
	if code, body := doJSON(t, router, http.MethodGet, "/process-flows/ghost", ""); code != http.StatusNotFound || flowProblemSlug(t, body) != "flow-not-found" {
		t.Fatalf("GET unknown: %d %s", code, body)
	}
	if code, body := doJSON(t, router, http.MethodDelete, "/process-flows/ghost", ""); code != http.StatusNotFound || flowProblemSlug(t, body) != "flow-not-found" {
		t.Fatalf("DELETE unknown: %d %s", code, body)
	}
	doJSON(t, router, http.MethodPut, "/process-flows/f1", putFlowBody("F", "PICK:PICK"))
	for i := 0; i < 2; i++ { // the second DELETE is an idempotent 204 as well
		if code, body := doJSON(t, router, http.MethodDelete, "/process-flows/f1", ""); code != http.StatusNoContent {
			t.Fatalf("DELETE #%d: %d %s", i+1, code, body)
		}
	}
	code, body := doJSON(t, router, http.MethodGet, "/process-flows/f1", "")
	if code != http.StatusOK || !strings.Contains(string(body), `"status":"RETIRED"`) || !strings.Contains(string(body), `"version":2`) {
		t.Fatalf("GET retired: %d %s", code, body)
	}
	code, body = doJSON(t, router, http.MethodPut, "/process-flows/f1", putFlowBody("F", "PICK:PICK"))
	if code != http.StatusUnprocessableEntity || flowProblemSlug(t, body) != "flow-retired" {
		t.Fatalf("PUT retired: %d %s", code, body)
	}
}

func TestDeactivatePath_InActiveFlow_Is409NamingTheFlows(t *testing.T) {
	router := newTestServer(t)
	definePathIDs(t, router, "PICK")
	doJSON(t, router, http.MethodPut, "/process-flows/beta", putFlowBody("B", "PICK:PICK"))
	doJSON(t, router, http.MethodPut, "/process-flows/alpha", putFlowBody("A", "PICK:PICK"))
	code, body := doJSON(t, router, http.MethodDelete, "/process-paths/PICK", "")
	if code != http.StatusConflict || flowProblemSlug(t, body) != "path-in-active-flow" || !strings.Contains(string(body), "[alpha beta]") {
		t.Fatalf("got %d %s", code, body)
	}
	doJSON(t, router, http.MethodDelete, "/process-flows/alpha", "")
	doJSON(t, router, http.MethodDelete, "/process-flows/beta", "")
	if code, body := doJSON(t, router, http.MethodDelete, "/process-paths/PICK", ""); code != http.StatusNoContent {
		t.Fatalf("after retiring both flows: %d %s", code, body)
	}
}

type flowsPageBody struct {
	Items []struct {
		FlowId string `json:"flowId"`
	} `json:"items"`
	NextCursor string `json:"nextCursor"`
}

func TestListProcessFlows_WalksEveryPage_AndHonoursAll(t *testing.T) {
	router := newTestServer(t)
	definePathIDs(t, router, "PICK")
	for _, id := range []string{"e", "b", "d", "a", "c"} {
		doJSON(t, router, http.MethodPut, "/process-flows/"+id, putFlowBody(id, "PICK:PICK"))
	}
	doJSON(t, router, http.MethodDelete, "/process-flows/c", "")

	walk := func(query string) string {
		var ids []string
		cursor := ""
		for pages := 0; pages < 20; pages++ {
			target := "/process-flows?limit=2&" + query
			if cursor != "" {
				target += "&cursor=" + url.QueryEscape(cursor)
			}
			code, body := doJSON(t, router, http.MethodGet, target, "")
			if code != http.StatusOK {
				t.Fatalf("GET %s = %d: %s", target, code, body)
			}
			var pg flowsPageBody
			if err := json.Unmarshal(body, &pg); err != nil {
				t.Fatal(err)
			}
			for _, it := range pg.Items {
				ids = append(ids, it.FlowId)
			}
			if pg.NextCursor == "" {
				return strings.Join(ids, "")
			}
			cursor = pg.NextCursor
		}
		t.Fatal("paging did not terminate")
		return ""
	}
	if got := walk("all=false"); got != "abde" {
		t.Fatalf("active flows %q, want abde", got)
	}
	if got := walk("all=true"); got != "abcde" {
		t.Fatalf("all flows %q, want abcde", got)
	}
}

func TestListProcessFlows_RejectsBadParameters(t *testing.T) {
	router := newTestServer(t)
	tests := []struct{ query, slug string }{
		{"all=null", "invalid-query-parameter"},
		{"limit=0", "invalid-limit"},
		{"cursor=nope", "invalid-cursor"},
		{"after=x", "unknown-query-parameter"},
		{"offset=1", "unknown-query-parameter"},
	}
	for _, tc := range tests {
		code, body := doJSON(t, router, http.MethodGet, "/process-flows?"+tc.query, "")
		if code != http.StatusBadRequest || flowProblemSlug(t, body) != tc.slug {
			t.Errorf("?%s: got %d %s, want 400 %s", tc.query, code, body, tc.slug)
		}
	}
	// An empty collection is an empty page, never null.
	code, body := doJSON(t, router, http.MethodGet, "/process-flows", "")
	if code != http.StatusOK || !strings.Contains(string(body), `"items":[]`) {
		t.Fatalf("empty list: %d %s", code, body)
	}
}
