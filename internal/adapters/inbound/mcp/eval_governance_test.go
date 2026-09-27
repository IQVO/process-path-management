// E1 — schema & metadata evals for the MCP tool surface.
//
// These evals go one level deeper than the existing transport tests: they
// prove every advertised tool's input schema is a resolvable, constraining
// JSON Schema (the thing an LLM host feeds a model), that every parameter
// carries a description, and that the advertised surface matches a golden
// registry shared across the fleet so tool names stay globally unique
// across the eight warehouse-systems servers. They run as a plain
// `go test` inside the existing CI test job — no new infrastructure.
package mcp_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/google/jsonschema-go/jsonschema"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	inboundmcp "github.com/claudioed/process-path-management/internal/adapters/inbound/mcp"
)

// schemaOf extracts and parses the InputSchema of a wire-listed tool into
// the SDK's jsonschema type. Over Streamable HTTP the schema arrives as raw
// JSON exactly as a model host would see it.
func schemaOf(t *testing.T, raw any) *jsonschema.Schema {
	t.Helper()
	encoded, err := json.Marshal(raw)
	if err != nil {
		t.Fatalf("marshal input schema: %v", err)
	}
	var s jsonschema.Schema
	if err := json.Unmarshal(encoded, &s); err != nil {
		t.Fatalf("input schema is not valid JSON Schema (%v): %s", err, encoded)
	}
	return &s
}

// hasType reports whether a property declares the given JSON type, via
// either the single-type or multi-type form.
func hasType(s *jsonschema.Schema, typ string) bool {
	if s.Type == typ {
		return true
	}
	for _, t := range s.Types {
		if t == typ {
			return true
		}
	}
	return false
}

// validInstanceFor builds a schema-shaped arguments object from a tool's
// properties: strings become "probe", integers 1, numbers 1.5, booleans
// true, arrays and objects their empty forms. Good enough to prove the
// schema accepts what it declares.
func validInstanceFor(s *jsonschema.Schema) map[string]any {
	instance := map[string]any{}
	for name, prop := range s.Properties {
		switch {
		case hasType(prop, "string"):
			instance[name] = "probe"
		case hasType(prop, "integer"):
			instance[name] = json.Number("1")
		case hasType(prop, "number"):
			instance[name] = json.Number("1.5")
		case hasType(prop, "boolean"):
			instance[name] = true
		case hasType(prop, "array"):
			instance[name] = []any{}
		case hasType(prop, "object"):
			instance[name] = map[string]any{}
		default:
			instance[name] = nil
		}
	}
	return instance
}

// TestEval_InputSchemasResolveAndConstrain proves, per advertised tool:
// (1) the input schema resolves (structurally valid, no dangling refs),
// (2) a schema-shaped arguments object validates cleanly, and
// (3) a wrong-typed value for a declared property is REJECTED — i.e. the
// schema genuinely constrains what a model may send, not just decorates it.
func TestEval_InputSchemasResolveAndConstrain(t *testing.T) {
	for _, tool := range wireTools(t) {
		t.Run(tool.Name, func(t *testing.T) {
			s := schemaOf(t, tool.InputSchema)
			if !hasType(s, "object") {
				t.Fatalf("input schema type = %q, want object", s.Type)
			}
			resolved, err := s.Resolve(nil)
			if err != nil {
				t.Fatalf("input schema does not resolve: %v", err)
			}

			valid := validInstanceFor(s)
			if err := resolved.Validate(valid); err != nil {
				t.Fatalf("schema rejects its own shape of arguments (%v): %v", valid, err)
			}

			// Flip the first property of each declared kind to a wrong
			// Go type; the schema must reject it. (float64 for strings,
			// not json.Number — the validator type-checks Go kinds, and
			// json.Number is a string kind. And a string probe for
			// booleans: list_process_paths declares only a boolean, so a
			// string-only probe would never exercise its schema.)
			probedString, probedBool := false, false
			for name, prop := range s.Properties {
				var wrong any
				switch {
				case hasType(prop, "string") && !probedString:
					wrong, probedString = float64(42), true
				case hasType(prop, "boolean") && !probedBool:
					wrong, probedBool = "not-a-boolean", true
				default:
					continue
				}
				if err := resolved.Validate(map[string]any{name: wrong}); err == nil {
					t.Fatalf("schema accepts a wrong-typed %q — it does not constrain model input", name)
				}
			}
		})
	}
}

// TestEval_ParametersAreDescribed asserts every property of every tool
// schema carries a non-empty description: these descriptions are the model
// UI, and a missing one silently degrades tool selection.
func TestEval_ParametersAreDescribed(t *testing.T) {
	for _, tool := range wireTools(t) {
		s := schemaOf(t, tool.InputSchema)
		for name, prop := range s.Properties {
			if strings.TrimSpace(prop.Description) == "" {
				t.Errorf("tool %q: parameter %q has no description", tool.Name, name)
			}
		}
		for _, name := range s.Required {
			if _, ok := s.Properties[name]; !ok {
				t.Errorf("tool %q: required parameter %q is not declared in properties", tool.Name, name)
			}
		}
	}
}

// TestEval_ToolRegistryMatchesGolden pins the exact advertised surface
// (names + write-intent annotations) to testdata/tool_registry.golden. Any
// addition, removal, or annotation change must be a conscious golden-file
// update reviewed against the charter's curation rules. The golden pins
// the DEFAULT-deps surface (the deps cmd/mcp always wires); the
// conditionally registered report tool is pinned separately below.
func TestEval_ToolRegistryMatchesGolden(t *testing.T) {
	var got []string
	for _, tool := range wireTools(t) {
		destructive := false
		if tool.Annotations != nil && tool.Annotations.DestructiveHint != nil {
			destructive = *tool.Annotations.DestructiveHint
		}
		readOnly := false
		if tool.Annotations != nil {
			readOnly = tool.Annotations.ReadOnlyHint
		}
		got = append(got, fmt.Sprintf("%s\t%t\t%t", tool.Name, readOnly, destructive))
	}
	sort.Strings(got)

	goldenPath := filepath.Join("testdata", "tool_registry.golden")
	wantBytes, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("read golden registry: %v", err)
	}
	want := strings.Split(strings.TrimRight(string(wantBytes), "\n"), "\n")

	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("advertised tool registry drifted from %s:\n want:\n%s\n got:\n%s",
			goldenPath, strings.Join(want, "\n"), strings.Join(got, "\n"))
	}
}

// fleetRepos is the fixed set of warehouse-systems MCP servers the fleet
// snapshot may name. Anything else in the snapshot is a typo or a stale
// entry and fails the eval.
var fleetRepos = map[string]bool{
	"facility-layout":         true,
	"fulfillment-execution":   true,
	"inventory-storage":       true,
	"labor-performance":       true,
	"order-management":        true,
	"process-path-management": true,
	"wes-work-planning":       true,
	"workforce-management":    true,
}

// TestEval_FleetToolNamesAreGloballyUnique checks this server's tools
// against testdata/fleet_tool_snapshot.golden — the federated registry of
// every tool every fleet MCP server exposes (kept identical in all eight
// repos; each rollout PR appends its repo's section). A model host mounts
// several of these servers together, so a tool name must be globally
// unique, and this repo's registry must be fully represented in the
// snapshot.
func TestEval_FleetToolNamesAreGloballyUnique(t *testing.T) {
	snapshotBytes, err := os.ReadFile(filepath.Join("testdata", "fleet_tool_snapshot.golden"))
	if err != nil {
		t.Fatalf("read fleet snapshot: %v", err)
	}

	owner := map[string]string{} // tool name -> repo that owns it
	for lineNum, line := range strings.Split(strings.TrimRight(string(snapshotBytes), "\n"), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		parts := strings.Split(line, "\t")
		if len(parts) != 2 {
			t.Fatalf("fleet snapshot line %d is not <repo>\\t<tool>: %q", lineNum+1, line)
		}
		repo, tool := parts[0], parts[1]
		if !fleetRepos[repo] {
			t.Fatalf("fleet snapshot names unknown repo %q (line %d)", repo, lineNum+1)
		}
		if other, clash := owner[tool]; clash {
			t.Fatalf("tool %q is exposed by both %s and %s — tool names must be globally unique across the fleet", tool, other, repo)
		}
		owner[tool] = repo
	}

	for _, tool := range wireTools(t) {
		if repo, ok := owner[tool.Name]; !ok {
			t.Errorf("tool %q is advertised but missing from fleet_tool_snapshot.golden — append it under process-path-management", tool.Name)
		} else if repo != "process-path-management" {
			t.Errorf("tool %q is advertised here but the fleet snapshot credits %s", tool.Name, repo)
		}
	}
}

// TestEval_ConditionalToolsArePinned documents the surface beyond the
// golden registry: two tools register only when their dependency is
// wired. get_catalogue_growth_report appears exactly when Deps.Reports is
// set (REPORTS_BASE_URL in cmd/mcp) and is read-only with a described
// schema; get_cpt_schedule disappears when Deps.GetCPTSchedule is nil
// without disturbing the other tools. Pinned so a deployment difference
// is a visible contract, not a surprise.
func TestEval_ConditionalToolsArePinned(t *testing.T) {
	byName := func(t *testing.T, deps inboundmcp.Deps) map[string]*sdk.Tool {
		t.Helper()
		tools := map[string]*sdk.Tool{}
		res, err := wireSession(t, deps).ListTools(t.Context(), nil)
		if err != nil {
			t.Fatalf("list tools: %v", err)
		}
		for _, tool := range res.Tools {
			tools[tool.Name] = tool
		}
		return tools
	}

	withReports := byName(t, evalDepsWithReports())
	tool, ok := withReports["get_catalogue_growth_report"]
	if !ok {
		t.Fatal("get_catalogue_growth_report must be advertised when a reports client is wired")
	}
	if tool.Annotations == nil || !tool.Annotations.ReadOnlyHint {
		t.Error("get_catalogue_growth_report must be annotated read-only")
	}
	s := schemaOf(t, tool.InputSchema)
	for _, name := range []string{"from", "to"} {
		prop, ok := s.Properties[name]
		if !ok {
			t.Errorf("get_catalogue_growth_report schema does not declare %q", name)
			continue
		}
		if strings.TrimSpace(prop.Description) == "" {
			t.Errorf("get_catalogue_growth_report: parameter %q has no description", name)
		}
	}

	withoutSchedule := byName(t, evalDepsWithoutCPTSchedule())
	if _, ok := withoutSchedule["get_cpt_schedule"]; ok {
		t.Error("get_cpt_schedule must not be advertised when Deps.GetCPTSchedule is nil")
	}
	for _, name := range []string{"get_process_path", "list_process_paths"} {
		if _, ok := withoutSchedule[name]; !ok {
			t.Errorf("tool %q must stay advertised when only the CPT schedule dependency is nil", name)
		}
	}
}

// wireTools lists the advertised tools over the real Streamable HTTP
// handler, so every eval sees exactly what a model host sees after a JSON
// round-trip (not the in-process Go values).
func wireTools(t *testing.T) []*sdk.Tool {
	t.Helper()
	sess := wireSession(t, newEvalDeps())
	res, err := sess.ListTools(t.Context(), nil)
	if err != nil {
		t.Fatalf("list tools: %v", err)
	}
	return res.Tools
}
