package http_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
)

// ADR 0034: intake on POST (optional, absent = []), on PUT (optional,
// absent/null = keep, [] = clear) and always on the response.

func serve(t *testing.T, router http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var req *http.Request
	if body == "" {
		req = httptest.NewRequest(method, path, nil)
	} else {
		req = httptest.NewRequest(method, path, bytes.NewBufferString(body))
	}
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	return rr
}

func intakeOf(t *testing.T, rr *httptest.ResponseRecorder) []string {
	t.Helper()
	var resp struct {
		Intake *[]string `json:"intake"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v (%s)", err, rr.Body.String())
	}
	if resp.Intake == nil {
		t.Fatalf("response has no intake array: %s", rr.Body.String())
	}
	return *resp.Intake
}

const pickNoIntake = `{"pathId":"PICK","matchPrefix":"pick","direct":true,"requiredCapabilities":["pick"],"cycleTimeP95":"2h"}`

func TestDefinePath_WithoutIntake_RespondsWithAnEmptyIntakeArray(t *testing.T) {
	router := newTestServer(t)
	rr := serve(t, router, http.MethodPost, "/process-paths", pickNoIntake)
	if rr.Code != http.StatusCreated {
		t.Fatalf("want 201, got %d: %s", rr.Code, rr.Body.String())
	}
	if got := intakeOf(t, rr); len(got) != 0 {
		t.Fatalf("intake = %v, want []", got)
	}
	if !strings.Contains(rr.Body.String(), `"intake":[]`) {
		t.Fatalf("intake must encode as [], never null: %s", rr.Body.String())
	}
}

func TestDefinePath_WithIntake_IsStoredAndReadBack(t *testing.T) {
	router := newTestServer(t)
	body := `{"pathId":"PICK","matchPrefix":"pick","direct":true,"requiredCapabilities":["pick"],"cycleTimeP95":"2h","intake":["CUSTOMER_ORDER"]}`
	if rr := serve(t, router, http.MethodPost, "/process-paths", body); rr.Code != http.StatusCreated {
		t.Fatalf("want 201, got %d: %s", rr.Code, rr.Body.String())
	}
	rr := serve(t, router, http.MethodGet, "/process-paths/PICK", "")
	if got := intakeOf(t, rr); !slices.Equal(got, []string{"CUSTOMER_ORDER"}) {
		t.Fatalf("GET intake = %v, want [CUSTOMER_ORDER]", got)
	}
}

func TestDefinePath_UnknownOrDuplicateIntake_Returns422(t *testing.T) {
	for _, intake := range []string{`["ORDERS"]`, `["CUSTOMER_ORDER","CUSTOMER_ORDER"]`, `["customer_order"]`} {
		router := newTestServer(t)
		body := `{"pathId":"PICK","matchPrefix":"pick","direct":true,"requiredCapabilities":["pick"],"cycleTimeP95":"2h","intake":` + intake + `}`
		rr := serve(t, router, http.MethodPost, "/process-paths", body)
		if rr.Code != http.StatusUnprocessableEntity {
			t.Fatalf("intake %s: want 422, got %d: %s", intake, rr.Code, rr.Body.String())
		}
		if !strings.Contains(rr.Body.String(), "/invalid-intake") {
			t.Fatalf("intake %s: want the invalid-intake problem type, got %s", intake, rr.Body.String())
		}
	}
}

func TestDefinePath_NullIntakeElement_Returns400(t *testing.T) {
	router := newTestServer(t)
	body := `{"pathId":"PICK","matchPrefix":"pick","direct":true,"requiredCapabilities":["pick"],"cycleTimeP95":"2h","intake":[null]}`
	if rr := serve(t, router, http.MethodPost, "/process-paths", body); rr.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d: %s", rr.Code, rr.Body.String())
	}
}

func TestRevisePath_IntakeAbsentOrNull_KeepsIt_EmptyClearsIt(t *testing.T) {
	router := newTestServer(t)
	body := `{"pathId":"PICK","matchPrefix":"pick","direct":true,"requiredCapabilities":["pick"],"cycleTimeP95":"2h","intake":["CUSTOMER_ORDER"]}`
	if rr := serve(t, router, http.MethodPost, "/process-paths", body); rr.Code != http.StatusCreated {
		t.Fatalf("setup: want 201, got %d: %s", rr.Code, rr.Body.String())
	}
	for _, revise := range []string{
		`{"matchPrefix":"pick","requiredCapabilities":["pick"],"cycleTimeP95":"3h"}`,
		`{"matchPrefix":"pick","requiredCapabilities":["pick"],"cycleTimeP95":"4h","intake":null}`,
	} {
		rr := serve(t, router, http.MethodPut, "/process-paths/PICK", revise)
		if rr.Code != http.StatusOK {
			t.Fatalf("want 200, got %d: %s", rr.Code, rr.Body.String())
		}
		if got := intakeOf(t, rr); !slices.Equal(got, []string{"CUSTOMER_ORDER"}) {
			t.Fatalf("revise %s: intake = %v, want it kept as [CUSTOMER_ORDER]", revise, got)
		}
	}
	rr := serve(t, router, http.MethodPut, "/process-paths/PICK", `{"matchPrefix":"pick","requiredCapabilities":["pick"],"cycleTimeP95":"4h","intake":[]}`)
	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", rr.Code, rr.Body.String())
	}
	if got := intakeOf(t, rr); len(got) != 0 {
		t.Fatalf("intake = %v, want [] after clearing", got)
	}
}

func TestRevisePath_InvalidIntake_Returns422(t *testing.T) {
	router := newTestServer(t)
	if rr := serve(t, router, http.MethodPost, "/process-paths", pickNoIntake); rr.Code != http.StatusCreated {
		t.Fatalf("setup: want 201, got %d", rr.Code)
	}
	rr := serve(t, router, http.MethodPut, "/process-paths/PICK", `{"matchPrefix":"pick","requiredCapabilities":["pick"],"cycleTimeP95":"2h","intake":["NOPE"]}`)
	if rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("want 422, got %d: %s", rr.Code, rr.Body.String())
	}
}
