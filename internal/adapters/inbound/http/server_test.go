package http_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	inboundhttp "github.com/claudioed/process-path-management/internal/adapters/inbound/http"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/memory"
	"github.com/claudioed/process-path-management/internal/application/usecases"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time { return c.t }

// logOnlyPublisher discards every event -- handler tests assert on HTTP
// responses, not on what got published (that is covered by the use-case
// and kafka-publisher test suites).
type logOnlyPublisher struct{}

func (logOnlyPublisher) Publish(context.Context, shared.DomainEvent) error { return nil }

// newTestServer wires a real router over in-memory adapters -- no
// mocking of the HTTP layer itself, matching the fleet's own
// httptest-over-real-router convention for handler tests.
func newTestServer(t *testing.T) http.Handler {
	t.Helper()
	repo := memory.NewProcessPathRepo()
	scheduleRepo := memory.NewCPTScheduleRepo()
	clock := fixedClock{time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)}
	pub := &logOnlyPublisher{}

	server := &inboundhttp.Server{
		DefinePath:        &usecases.DefinePath{Repo: repo, Publisher: pub, Clock: clock},
		RevisePath:        &usecases.RevisePath{Repo: repo, Publisher: pub, Clock: clock},
		DeactivatePath:    &usecases.DeactivatePath{Repo: repo, Publisher: pub, Clock: clock},
		GetPath:           &usecases.GetPath{Repo: repo},
		ListPaths:         &usecases.ListPaths{Repo: repo},
		DefineCPTSchedule: &usecases.DefineCPTSchedule{Repo: scheduleRepo, ProcessPathRepo: repo, Publisher: pub, Clock: clock},
		GetCPTSchedule:    &usecases.GetCPTSchedule{Repo: scheduleRepo},
	}
	return inboundhttp.NewRouter(server, nil, "")
}

func TestHealthz_ReturnsOK(t *testing.T) {
	router := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rr.Code)
	}
}

func TestDefinePath_ValidRequest_Returns201(t *testing.T) {
	router := newTestServer(t)
	body := `{"pathId":"PICK","matchPrefix":"pick","direct":true,"requiredCapabilities":["pick"],"cycleTimeP95":"2h"}`
	req := httptest.NewRequest(http.MethodPost, "/process-paths", bytes.NewBufferString(body))
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusCreated {
		t.Fatalf("want 201, got %d: %s", rr.Code, rr.Body.String())
	}
	var resp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp["pathId"] != "PICK" || resp["status"] != "ACTIVE" {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestDefinePath_InvalidMatchPrefix_Returns422WithProblemDetails(t *testing.T) {
	router := newTestServer(t)
	body := `{"pathId":"PICK","matchPrefix":"","direct":true,"requiredCapabilities":["pick"],"cycleTimeP95":"2h"}`
	req := httptest.NewRequest(http.MethodPost, "/process-paths", bytes.NewBufferString(body))
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("want 422, got %d: %s", rr.Code, rr.Body.String())
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("want application/problem+json, got %s", ct)
	}
}

func TestDefinePath_EmptyPathId_Returns422WithProblemDetails(t *testing.T) {
	router := newTestServer(t)
	// A path defined with an empty id would be unreachable through
	// /process-paths/{pathId} (chi never matches an empty segment), so
	// the aggregate rejects it up front instead of creating a resource
	// no one can read back.
	body := `{"pathId":"","matchPrefix":"pick","direct":true,"requiredCapabilities":["pick"],"cycleTimeP95":"2h"}`
	req := httptest.NewRequest(http.MethodPost, "/process-paths", bytes.NewBufferString(body))
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("want 422, got %d: %s", rr.Code, rr.Body.String())
	}
	var problem struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &problem); err != nil {
		t.Fatalf("unmarshal problem: %v", err)
	}
	if want := "https://errors.process-path-management.warehouse-systems.dev/empty-path-id"; problem.Type != want {
		t.Fatalf("want type %s, got %s", want, problem.Type)
	}
}

func TestListPaths_NonBooleanAll_Returns400WithProblemDetails(t *testing.T) {
	router := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/process-paths?all=null", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d: %s", rr.Code, rr.Body.String())
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("want application/problem+json, got %s", ct)
	}
}

// The published contract types `all` as a boolean: only "true"/"false" are
// valid. strconv.ParseBool's lenient spellings must be rejected (the
// Schemathesis contract job found ?all=F answered 200).
func TestListPaths_LenientBooleanSpellings_Return400(t *testing.T) {
	for _, v := range []string{"F", "T", "1", "0", "t", "f", "TRUE", "False"} {
		t.Run(v, func(t *testing.T) {
			router := newTestServer(t)
			rr := httptest.NewRecorder()
			router.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/process-paths?all="+v, nil))
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("?all=%s: want 400, got %d", v, rr.Code)
			}
		})
	}
}

func TestListPaths_AllFalse_StillActiveOnly(t *testing.T) {
	router := newTestServer(t)
	body := `{"pathId":"PICK","matchPrefix":"pick","direct":true,"requiredCapabilities":["pick"],"cycleTimeP95":"2h"}`
	defineReq := httptest.NewRequest(http.MethodPost, "/process-paths", bytes.NewBufferString(body))
	defineRR := httptest.NewRecorder()
	router.ServeHTTP(defineRR, defineReq)
	if defineRR.Code != http.StatusCreated {
		t.Fatalf("setup: want 201, got %d", defineRR.Code)
	}

	req := httptest.NewRequest(http.MethodGet, "/process-paths?all=false", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	var paths []map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &paths); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(paths) != 1 {
		t.Fatalf("want 1 path with ?all=false, got %d", len(paths))
	}
}

func TestRouter_UnroutablePath_Returns404ProblemDetails(t *testing.T) {
	router := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/process-paths/", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("want 404, got %d: %s", rr.Code, rr.Body.String())
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("want application/problem+json, got %s", ct)
	}
}

func TestDefinePath_DuplicateId_Returns409(t *testing.T) {
	router := newTestServer(t)
	body := `{"pathId":"PICK","matchPrefix":"pick","direct":true,"requiredCapabilities":["pick"],"cycleTimeP95":"2h"}`

	req1 := httptest.NewRequest(http.MethodPost, "/process-paths", bytes.NewBufferString(body))
	rr1 := httptest.NewRecorder()
	router.ServeHTTP(rr1, req1)
	if rr1.Code != http.StatusCreated {
		t.Fatalf("setup: want 201, got %d", rr1.Code)
	}

	req2 := httptest.NewRequest(http.MethodPost, "/process-paths", bytes.NewBufferString(body))
	rr2 := httptest.NewRecorder()
	router.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusConflict {
		t.Fatalf("want 409, got %d: %s", rr2.Code, rr2.Body.String())
	}
}

func TestGetPath_Missing_Returns404(t *testing.T) {
	router := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/process-paths/MISSING", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("want 404, got %d", rr.Code)
	}
}

func TestListPaths_ActiveOnlyByDefault_ExcludesDeactivated(t *testing.T) {
	router := newTestServer(t)
	define := func(id string) {
		body := `{"pathId":"` + id + `","matchPrefix":"` + id + `","direct":true,"requiredCapabilities":["cap"],"cycleTimeP95":"2h"}`
		req := httptest.NewRequest(http.MethodPost, "/process-paths", bytes.NewBufferString(body))
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
		if rr.Code != http.StatusCreated {
			t.Fatalf("setup define %s: want 201, got %d: %s", id, rr.Code, rr.Body.String())
		}
	}
	define("pick")
	define("pack")

	deleteReq := httptest.NewRequest(http.MethodDelete, "/process-paths/pack", nil)
	deleteRR := httptest.NewRecorder()
	router.ServeHTTP(deleteRR, deleteReq)
	if deleteRR.Code != http.StatusNoContent {
		t.Fatalf("setup deactivate: want 204, got %d", deleteRR.Code)
	}

	listReq := httptest.NewRequest(http.MethodGet, "/process-paths", nil)
	listRR := httptest.NewRecorder()
	router.ServeHTTP(listRR, listReq)
	var paths []map[string]any
	if err := json.Unmarshal(listRR.Body.Bytes(), &paths); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(paths) != 1 {
		t.Fatalf("want 1 active path, got %d: %+v", len(paths), paths)
	}

	allReq := httptest.NewRequest(http.MethodGet, "/process-paths?all=true", nil)
	allRR := httptest.NewRecorder()
	router.ServeHTTP(allRR, allReq)
	var allPaths []map[string]any
	if err := json.Unmarshal(allRR.Body.Bytes(), &allPaths); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(allPaths) != 2 {
		t.Fatalf("want 2 total paths with ?all=true, got %d", len(allPaths))
	}
}

func TestRevisePath_ValidRequest_Returns200WithUpdatedFields(t *testing.T) {
	router := newTestServer(t)
	defineBody := `{"pathId":"PICK","matchPrefix":"pick","direct":true,"requiredCapabilities":["pick"],"cycleTimeP95":"2h"}`
	defineReq := httptest.NewRequest(http.MethodPost, "/process-paths", bytes.NewBufferString(defineBody))
	defineRR := httptest.NewRecorder()
	router.ServeHTTP(defineRR, defineReq)
	if defineRR.Code != http.StatusCreated {
		t.Fatalf("setup: want 201, got %d", defineRR.Code)
	}

	reviseBody := `{"matchPrefix":"pick-zone-a","requiredCapabilities":["pick","hazmat"],"cycleTimeP95":"3h"}`
	reviseReq := httptest.NewRequest(http.MethodPut, "/process-paths/PICK", bytes.NewBufferString(reviseBody))
	reviseRR := httptest.NewRecorder()
	router.ServeHTTP(reviseRR, reviseReq)
	if reviseRR.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", reviseRR.Code, reviseRR.Body.String())
	}
	var resp map[string]any
	if err := json.Unmarshal(reviseRR.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp["matchPrefix"] != "pick-zone-a" {
		t.Fatalf("unexpected matchPrefix: %+v", resp)
	}
}

func TestDeactivatePath_ValidRequest_Returns204(t *testing.T) {
	router := newTestServer(t)
	defineBody := `{"pathId":"PICK","matchPrefix":"pick","direct":true,"requiredCapabilities":["pick"],"cycleTimeP95":"2h"}`
	defineReq := httptest.NewRequest(http.MethodPost, "/process-paths", bytes.NewBufferString(defineBody))
	defineRR := httptest.NewRecorder()
	router.ServeHTTP(defineRR, defineReq)
	if defineRR.Code != http.StatusCreated {
		t.Fatalf("setup: want 201, got %d", defineRR.Code)
	}

	deleteReq := httptest.NewRequest(http.MethodDelete, "/process-paths/PICK", nil)
	deleteRR := httptest.NewRecorder()
	router.ServeHTTP(deleteRR, deleteReq)
	if deleteRR.Code != http.StatusNoContent {
		t.Fatalf("want 204, got %d: %s", deleteRR.Code, deleteRR.Body.String())
	}
}
