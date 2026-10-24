package http_test

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

// ADR 0038: the POST /process-paths request examples in apis/openapi.yaml
// are real requests. Each one is posted through the real router and must be
// accepted (201), and must say the same thing as the entry of the same path
// id in catalogue/paths.json, so the documented example cannot drift from
// the declared catalogue.

var openapiSpecPath = filepath.Join("..", "..", "..", "..", "apis", "openapi.yaml")
var declaredCataloguePath = filepath.Join("..", "..", "..", "..", "catalogue", "paths.json")

type definePathExamples struct {
	Paths map[string]map[string]struct {
		RequestBody struct {
			Content map[string]struct {
				Examples map[string]struct {
					Value map[string]any `yaml:"value"`
				} `yaml:"examples"`
			} `yaml:"content"`
		} `yaml:"requestBody"`
	} `yaml:"paths"`
}

func loadDefinePathExamples(t *testing.T) map[string]map[string]any {
	t.Helper()
	raw, err := os.ReadFile(openapiSpecPath)
	if err != nil {
		t.Fatalf("read %s: %v", openapiSpecPath, err)
	}
	var spec definePathExamples
	if err := yaml.Unmarshal(raw, &spec); err != nil {
		t.Fatalf("parse %s: %v", openapiSpecPath, err)
	}
	out := map[string]map[string]any{}
	for name, ex := range spec.Paths["/process-paths"]["post"].RequestBody.Content["application/json"].Examples {
		out[name] = ex.Value
	}
	return out
}

func declaredEntries(t *testing.T) map[string]map[string]any {
	t.Helper()
	raw, err := os.ReadFile(declaredCataloguePath)
	if err != nil {
		t.Fatalf("read %s: %v", declaredCataloguePath, err)
	}
	var doc struct {
		Paths []map[string]any `json:"paths"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("decode %s: %v", declaredCataloguePath, err)
	}
	out := map[string]map[string]any{}
	for _, e := range doc.Paths {
		out[e["pathId"].(string)] = e
	}
	return out
}

// normalise makes the YAML example and the JSON entry comparable: the cycle
// time is compared as a duration, and empty collections are the same as absent.
func normalise(t *testing.T, in map[string]any) map[string]any {
	t.Helper()
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	d, err := time.ParseDuration(out["cycleTimeP95"].(string))
	if err != nil {
		t.Fatalf("cycleTimeP95: %v", err)
	}
	out["cycleTimeP95"] = d.String()
	return out
}

func TestOpenAPI_DefinePathExamplesAreAcceptedAndMatchTheDeclaredCatalogue(t *testing.T) {
	examples := loadDefinePathExamples(t)
	want := map[string]string{"fullCase": "FULL_CASE", "crossDock": "CROSS_DOCK", "breakCase": "BREAK_CASE"}
	if len(examples) != len(want) {
		t.Fatalf("POST /process-paths declares %d examples, want %d (%v)", len(examples), len(want), want)
	}
	declared := declaredEntries(t)
	for name, id := range want {
		ex, ok := examples[name]
		if !ok {
			t.Fatalf("POST /process-paths has no example %q", name)
		}
		router := newTestServer(t)
		body, err := json.Marshal(ex)
		if err != nil {
			t.Fatalf("marshal example %s: %v", name, err)
		}
		rr := serve(t, router, http.MethodPost, "/process-paths", string(body))
		if rr.Code != http.StatusCreated {
			t.Fatalf("example %s: want 201, got %d: %s", name, rr.Code, rr.Body.String())
		}
		var resp map[string]any
		if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
			t.Fatalf("example %s: response: %v", name, err)
		}
		if resp["pathId"] != id || resp["status"] != "ACTIVE" {
			t.Fatalf("example %s: response = %v", name, resp)
		}
		if !reflect.DeepEqual(normalise(t, ex), normalise(t, declared[id])) {
			t.Fatalf("example %s differs from catalogue/paths.json\n example: %v\n declared: %v", name, ex, declared[id])
		}
	}
}
