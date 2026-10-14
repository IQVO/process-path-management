package mcp_test

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	inboundmcp "github.com/claudioed/process-path-management/internal/adapters/inbound/mcp"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/memory"
	"github.com/claudioed/process-path-management/internal/application/usecases"
	"github.com/claudioed/process-path-management/internal/domain/processpath"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

var base = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

// seed builds an in-memory repo with one active path (PICK) and one
// deactivated path (REBIN).
func seed(t *testing.T) *memory.ProcessPathRepo {
	t.Helper()
	repo := memory.NewProcessPathRepo()

	pick, err := processpath.Define(shared.PathId("PICK"), "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, 2*time.Hour, shared.Eligibility{}, shared.Intake{}, base)
	if err != nil {
		t.Fatalf("define PICK: %v", err)
	}
	if err := repo.Save(context.Background(), pick); err != nil {
		t.Fatalf("save PICK: %v", err)
	}

	rebin, err := processpath.Define(shared.PathId("REBIN"), "rebin", false, []shared.Capability{"rebin"}, shared.DestinationLocationRoleUnset, 2*time.Hour, shared.Eligibility{}, shared.Intake{}, base)
	if err != nil {
		t.Fatalf("define REBIN: %v", err)
	}
	rebin.Deactivate(base.Add(time.Hour))
	if err := repo.Save(context.Background(), rebin); err != nil {
		t.Fatalf("save REBIN: %v", err)
	}

	return repo
}

// newServer builds a real MCP HTTP server over the seeded repo, and
// returns its httptest URL.
func newServer(t *testing.T) string {
	t.Helper()
	repo := seed(t)
	scheduleRepo := memory.NewCPTScheduleRepo()
	deps := inboundmcp.Deps{
		GetPath:        &usecases.GetPath{Repo: repo},
		ListPaths:      &usecases.ListPaths{Repo: repo},
		GetCPTSchedule: &usecases.GetCPTSchedule{Repo: scheduleRepo},
	}
	server := inboundmcp.NewServer(deps)
	httpSrv := httptest.NewServer(inboundmcp.Handler(server))
	t.Cleanup(httpSrv.Close)
	return httpSrv.URL
}

func connect(t *testing.T, url string) *sdk.ClientSession {
	t.Helper()
	client := sdk.NewClient(&sdk.Implementation{Name: "test-client", Version: "0.0.1"}, nil)
	transport := &sdk.StreamableClientTransport{Endpoint: url}
	session, err := client.Connect(context.Background(), transport, nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return session
}

func TestServer_ToolsListAndCall(t *testing.T) {
	url := newServer(t)
	session := connect(t, url)
	ctx := context.Background()

	tools, err := session.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("list tools: %v", err)
	}
	want := map[string]bool{"get_process_path": false, "list_process_paths": false, "get_cpt_schedule": false}
	for _, tool := range tools.Tools {
		if _, ok := want[tool.Name]; ok {
			want[tool.Name] = true
		}
		if tool.Annotations == nil || !tool.Annotations.ReadOnlyHint {
			t.Errorf("tool %q must be annotated ReadOnlyHint=true", tool.Name)
		}
	}
	for name, seen := range want {
		if !seen {
			t.Errorf("tool %q not advertised", name)
		}
	}

	res, err := session.CallTool(ctx, &sdk.CallToolParams{
		Name:      "get_process_path",
		Arguments: map[string]any{"pathId": "PICK"},
	})
	if err != nil {
		t.Fatalf("call tool: %v", err)
	}
	if res.IsError {
		t.Fatalf("tool returned error: %+v", res.Content)
	}
	matchPrefix, ok := res.StructuredContent.(map[string]any)["matchPrefix"]
	if !ok || matchPrefix != "pick" {
		t.Fatalf("unexpected structured content: %+v", res.StructuredContent)
	}
}

func TestServer_GetProcessPathNotFoundIsToolError(t *testing.T) {
	url := newServer(t)
	session := connect(t, url)
	res, err := session.CallTool(context.Background(), &sdk.CallToolParams{
		Name:      "get_process_path",
		Arguments: map[string]any{"pathId": "MISSING"},
	})
	if err != nil {
		t.Fatalf("call tool transport error: %v", err)
	}
	if !res.IsError {
		t.Fatal("expected tool-level error for an unknown pathId")
	}
}

func TestServer_ListProcessPathsDefaultsActiveOnly(t *testing.T) {
	url := newServer(t)
	session := connect(t, url)
	res, err := session.CallTool(context.Background(), &sdk.CallToolParams{
		Name:      "list_process_paths",
		Arguments: map[string]any{},
	})
	if err != nil {
		t.Fatalf("call tool: %v", err)
	}
	if res.IsError {
		t.Fatalf("tool returned error: %+v", res.Content)
	}
	paths, ok := res.StructuredContent.(map[string]any)["paths"].([]any)
	if !ok {
		t.Fatalf("no paths array in structured content: %+v", res.StructuredContent)
	}
	if len(paths) != 1 {
		t.Fatalf("paths = %d, want 1 (active only, default)", len(paths))
	}
}

func TestServer_ListProcessPathsIncludingDeactivated(t *testing.T) {
	url := newServer(t)
	session := connect(t, url)
	res, err := session.CallTool(context.Background(), &sdk.CallToolParams{
		Name:      "list_process_paths",
		Arguments: map[string]any{"activeOnly": false},
	})
	if err != nil {
		t.Fatalf("call tool: %v", err)
	}
	if res.IsError {
		t.Fatalf("tool returned error: %+v", res.Content)
	}
	paths, ok := res.StructuredContent.(map[string]any)["paths"].([]any)
	if !ok {
		t.Fatalf("no paths array in structured content: %+v", res.StructuredContent)
	}
	if len(paths) != 2 {
		t.Fatalf("paths = %d, want 2 (activeOnly=false)", len(paths))
	}
}
