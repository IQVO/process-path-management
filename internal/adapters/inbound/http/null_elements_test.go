package http_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
)

// The published contract types every string-array element as a non-null
// `string`. encoding/json silently turns a JSON null element into "" for a
// []string, so a body like {"requiredProductAttributes":[null]} used to be
// ACCEPTED (200/201) although it violates the schema — found by the
// Schemathesis contract job's negative_data_rejection check (stateful
// phase, `became null`). The string arrays the domain does NOT already
// reject on an empty element (requiredCapabilities and the two
// eligibility attribute sets) must reject a null element with 400. The CPT
// arrays (daysOfWeek, eligiblePathIds) already answer 422 for the "" a null
// decodes to, so they are unchanged.

func TestRequestStringArrays_NullElement_Returns400(t *testing.T) {
	define := func(router http.Handler) {
		t.Helper()
		body := `{"pathId":"PICK","matchPrefix":"pick","direct":true,"requiredCapabilities":["pick"],"cycleTimeP95":"2h"}`
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/process-paths", bytes.NewBufferString(body)))
		if rr.Code != http.StatusCreated {
			t.Fatalf("setup: want 201, got %d: %s", rr.Code, rr.Body.String())
		}
	}

	cases := []struct {
		name   string
		method string
		url    string
		body   string
	}{
		{"define requiredCapabilities", http.MethodPost, "/process-paths",
			`{"pathId":"P1","matchPrefix":"p1","requiredCapabilities":[null],"cycleTimeP95":"2h"}`},
		{"define eligibility.requiredProductAttributes", http.MethodPost, "/process-paths",
			`{"pathId":"P1","matchPrefix":"p1","requiredCapabilities":["pick"],"cycleTimeP95":"2h","eligibility":{"requiredProductAttributes":[null]}}`},
		{"define eligibility.excludedProductAttributes", http.MethodPost, "/process-paths",
			`{"pathId":"P1","matchPrefix":"p1","requiredCapabilities":["pick"],"cycleTimeP95":"2h","eligibility":{"excludedProductAttributes":["hazmat",null]}}`},
		{"revise requiredCapabilities", http.MethodPut, "/process-paths/PICK",
			`{"matchPrefix":"pick","requiredCapabilities":["pick",null],"cycleTimeP95":"2h"}`},
		{"revise eligibility.requiredProductAttributes", http.MethodPut, "/process-paths/PICK",
			`{"matchPrefix":"pick","requiredCapabilities":["pick"],"cycleTimeP95":"2h","eligibility":{"requiredProductAttributes":[null,null]}}`},
		{"revise eligibility.excludedProductAttributes", http.MethodPut, "/process-paths/PICK",
			`{"matchPrefix":"pick","requiredCapabilities":["pick"],"cycleTimeP95":"2h","eligibility":{"excludedProductAttributes":[null]}}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			router := newTestServer(t)
			define(router)
			rr := httptest.NewRecorder()
			router.ServeHTTP(rr, httptest.NewRequest(tc.method, tc.url, bytes.NewBufferString(tc.body)))
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("null array element: want 400, got %d: %s", rr.Code, rr.Body.String())
			}
			if ct := rr.Header().Get("Content-Type"); ct != "application/problem+json" {
				t.Fatalf("want application/problem+json, got %s", ct)
			}
		})
	}
}

// An absent array, an empty array and ordinary string elements keep their
// existing behaviour (the strict decoding only rejects null elements).
func TestRequestStringArrays_NonNullElements_StillAccepted(t *testing.T) {
	router := newTestServer(t)
	body := `{"pathId":"PICK","matchPrefix":"pick","requiredCapabilities":["pick"],"cycleTimeP95":"2h","eligibility":{"requiredProductAttributes":["giftWrap"],"excludedProductAttributes":[]}}`
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/process-paths", bytes.NewBufferString(body)))
	if rr.Code != http.StatusCreated {
		t.Fatalf("want 201, got %d: %s", rr.Code, rr.Body.String())
	}
}
