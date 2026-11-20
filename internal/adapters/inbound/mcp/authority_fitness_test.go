// Agentic Standard v2 (AS-1): every MCP tool declares exactly one authority
// level. A tool that forgets, or that claims a write level while annotated
// read-only (or vice versa), fails the build.
package mcp_test

import (
	"fmt"
	"testing"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

// authorityKey is the pinned tools/list _meta key (contracts G2).
const authorityKey = "com.warehouse/authority"

// authorityOf returns the level a tool declares and whether it is a valid
// level string (L1..L4). L4 is a valid LEVEL; this context grants no L4
// (the MCP charter is read-only, ADR 0006/0041), which the exact-level
// assertions below pin.
func authorityOf(tool *sdk.Tool) (string, bool) {
	level, ok := tool.Meta[authorityKey].(string)
	if !ok {
		return "", false
	}
	switch level {
	case "L1", "L2", "L3", "L4":
		return level, true
	}
	return level, false
}

// authorityViolation returns why tool breaks the authority rule, or "".
func authorityViolation(tool *sdk.Tool) string {
	level, ok := authorityOf(tool)
	if !ok {
		return fmt.Sprintf("must declare exactly one authority level L1..L4 in Meta[%q], got %v", authorityKey, tool.Meta[authorityKey])
	}
	readOnly := tool.Annotations != nil && tool.Annotations.ReadOnlyHint
	switch {
	case !readOnly && level != "L3":
		return fmt.Sprintf("has ReadOnlyHint=false so it must be L3, got %s", level)
	case readOnly && level != "L1" && level != "L2":
		return fmt.Sprintf("is read-only so it must be L1 or L2, got %s", level)
	}
	return ""
}

// TestEveryAgentToolDeclaresAuthority (MCP half; the A2A half lives in the
// a2a package under the same name) lists EVERY tool — including both
// conditionally registered ones (get_cpt_schedule, get_catalogue_growth_report)
// — over the real Streamable HTTP handler and asserts: exactly one valid
// level per tool, every read-only tool L1 or L2, and the exact level the
// ADR-0041 table pins (every tool here is L1: the charter is read-only).
func TestEveryAgentToolDeclaresAuthority(t *testing.T) {
	deps := newEvalDeps()
	deps.Reports = &fakeReportsClient{}
	sess := wireSession(t, deps)
	res, err := sess.ListTools(t.Context(), nil)
	if err != nil {
		t.Fatalf("list tools: %v", err)
	}
	if len(res.Tools) == 0 {
		t.Fatal("no tools listed; the sensor is looking at nothing")
	}
	want := map[string]string{
		"get_process_path":            "L1",
		"list_process_paths":          "L1",
		"get_cpt_schedule":            "L1",
		"get_catalogue_growth_report": "L1",
	}
	if len(res.Tools) != len(want) {
		t.Fatalf("listed %d tools, want %d (%v)", len(res.Tools), len(want), toolNames(res.Tools))
	}
	for _, tool := range res.Tools {
		if v := authorityViolation(tool); v != "" {
			t.Errorf("tool %q %s", tool.Name, v)
		}
		if tool.Meta[authorityKey] != want[tool.Name] {
			t.Errorf("tool %q authority = %q, want %q (ADR 0041 table)", tool.Name, tool.Meta[authorityKey], want[tool.Name])
		}
	}
}

func toolNames(tools []*sdk.Tool) []string {
	names := make([]string, 0, len(tools))
	for _, tool := range tools {
		names = append(names, tool.Name)
	}
	return names
}

// TestAuthoritySensorFailsOnBadTools proves the sensor can fail: a missing
// declaration, a bad value, a write level on a read-only tool and a read
// level on a write tool are all rejected, and well-formed tools pass.
func TestAuthoritySensorFailsOnBadTools(t *testing.T) {
	ro := &sdk.ToolAnnotations{ReadOnlyHint: true}
	rw := &sdk.ToolAnnotations{ReadOnlyHint: false}
	cases := []struct {
		name string
		tool *sdk.Tool
		bad  bool
	}{
		{"read L1", &sdk.Tool{Annotations: ro, Meta: sdk.Meta{authorityKey: "L1"}}, false},
		{"read L2", &sdk.Tool{Annotations: ro, Meta: sdk.Meta{authorityKey: "L2"}}, false},
		{"write L3", &sdk.Tool{Annotations: rw, Meta: sdk.Meta{authorityKey: "L3"}}, false},
		{"missing", &sdk.Tool{Annotations: ro}, true},
		{"unknown level", &sdk.Tool{Annotations: ro, Meta: sdk.Meta{authorityKey: "L5"}}, true},
		{"non-string", &sdk.Tool{Annotations: ro, Meta: sdk.Meta{authorityKey: 1}}, true},
		{"write L1", &sdk.Tool{Annotations: rw, Meta: sdk.Meta{authorityKey: "L1"}}, true},
		{"read L3", &sdk.Tool{Annotations: ro, Meta: sdk.Meta{authorityKey: "L3"}}, true},
		{"no annotations is a write", &sdk.Tool{Meta: sdk.Meta{authorityKey: "L1"}}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := authorityViolation(tc.tool) != ""; got != tc.bad {
				t.Fatalf("violation=%v, want %v", got, tc.bad)
			}
		})
	}
}
