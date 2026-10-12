package http_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestDefineCPTSchedule_EmptySiteIdSegment_IsRouteNotFound pins the
// openapi.yaml contract for PUT /sites/{siteId}/cpt-schedule: "404 -- No route
// matches this request path (e.g. an empty siteId path segment)". chi matches
// "/sites//cpt-schedule" with siteId == "", so without a guard the handler
// stored (and tried to publish an event for) a schedule with an empty site id.
func TestDefineCPTSchedule_EmptySiteIdSegment_IsRouteNotFound(t *testing.T) {
	router := newTestServer(t)

	define := `{"pathId":"PICK","matchPrefix":"pick","direct":true,"requiredCapabilities":["pick"],"cycleTimeP95":"2h"}`
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/process-paths", bytes.NewBufferString(define)))
	if rr.Code != http.StatusCreated {
		t.Fatalf("setup define: want 201, got %d: %s", rr.Code, rr.Body.String())
	}

	body := `{"timezone":"America/Sao_Paulo","cutoffs":[{"cptId":"x-1500","localTime":"15:00","daysOfWeek":["Mon"],"shipMethod":"ground","eligiblePathIds":["PICK"]}]}`
	rr = httptest.NewRecorder()
	router.ServeHTTP(rr, httptest.NewRequest(http.MethodPut, "/sites//cpt-schedule", bytes.NewBufferString(body)))

	if rr.Code != http.StatusNotFound {
		t.Fatalf("want 404, got %d: %s", rr.Code, rr.Body.String())
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("content type = %q, want application/problem+json", ct)
	}
	var problem map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &problem); err != nil {
		t.Fatalf("unmarshal problem: %v", err)
	}
	if typ, _ := problem["type"].(string); !strings.HasSuffix(typ, "/route-not-found") {
		t.Fatalf("problem type = %q, want suffix /route-not-found", typ)
	}

	// Nothing may have been stored under the empty site id.
	rr = httptest.NewRecorder()
	router.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/sites//cpt-schedule", nil))
	if rr.Code != http.StatusNotFound {
		t.Fatalf("GET after rejected PUT: want 404, got %d: %s", rr.Code, rr.Body.String())
	}
}
