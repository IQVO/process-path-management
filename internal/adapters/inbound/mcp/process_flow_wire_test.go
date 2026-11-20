package mcp_test

import (
	"context"
	"testing"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/claudioed/process-path-management/internal/adapters/outbound/memory"
	"github.com/claudioed/process-path-management/internal/application/usecases"
	"github.com/claudioed/process-path-management/internal/domain/processflow"
)

// Over the real wire (ADR 0042): both flow tools are advertised only when
// their use case is wired, both L1 and read-only, and a call answers the
// stages in order.
func TestFlowToolsOverTheWire(t *testing.T) {
	without, err := wireSession(t, newEvalDeps()).ListTools(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tool := range without.Tools {
		if tool.Name == "get_process_flow" || tool.Name == "list_process_flows" {
			t.Fatalf("%s must not be advertised when its use case is nil", tool.Name)
		}
	}

	deps := newEvalDeps()
	repo := seedWireFlow(t)
	deps.GetProcessFlow = &usecases.GetProcessFlow{Repo: repo}
	deps.ListProcessFlows = &usecases.ListProcessFlows{Repo: repo}
	sess := wireSession(t, deps)
	assertFlowToolsAdvertised(t, sess)
	assertFlowToolCalls(t, sess)
}

func seedWireFlow(t *testing.T) *memory.ProcessFlowRepo {
	t.Helper()
	repo := memory.NewProcessFlowRepo()
	pick, _ := processflow.NewStage("PICK", "PICK")
	pack, _ := processflow.NewStage("PACK", "PACK")
	f, err := processflow.Define("single-unit", "single-unit", []processflow.Stage{pick, pack}, time.Date(2026, 11, 20, 8, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Create(context.Background(), f); err != nil {
		t.Fatal(err)
	}
	return repo
}

func assertFlowToolsAdvertised(t *testing.T, sess *sdk.ClientSession) {
	t.Helper()
	res, err := sess.ListTools(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, tool := range res.Tools {
		if tool.Name != "get_process_flow" && tool.Name != "list_process_flows" {
			continue
		}
		seen[tool.Name] = true
		if v := authorityViolation(tool); v != "" {
			t.Errorf("tool %q %s", tool.Name, v)
		}
		if tool.Meta[authorityKey] != "L1" || tool.Annotations == nil || !tool.Annotations.ReadOnlyHint {
			t.Errorf("tool %q must be L1 and read-only: %v %+v", tool.Name, tool.Meta, tool.Annotations)
		}
	}
	if len(seen) != 2 {
		t.Fatalf("flow tools advertised: %v", seen)
	}
}

func assertFlowToolCalls(t *testing.T, sess *sdk.ClientSession) {
	t.Helper()
	call, err := sess.CallTool(t.Context(), &sdk.CallToolParams{Name: "get_process_flow", Arguments: map[string]any{"flowId": "single-unit"}})
	if err != nil || call.IsError {
		t.Fatalf("call: %v %+v", err, call)
	}
	stages, _ := call.StructuredContent.(map[string]any)["stages"].([]any)
	if len(stages) != 2 {
		t.Fatalf("structured content: %+v", call.StructuredContent)
	}
	miss, err := sess.CallTool(t.Context(), &sdk.CallToolParams{Name: "get_process_flow", Arguments: map[string]any{"flowId": "ghost"}})
	if err != nil || !miss.IsError {
		t.Fatalf("an unknown flow must be a tool-level error, got %v %+v", err, miss)
	}
}
