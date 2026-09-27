// Shared harness for the MCP eval suites (E1-E3): one place that builds a
// real Streamable HTTP server over in-memory adapters and connects a real
// SDK client to it, so schema evals, wire conformance evals, and the
// Gherkin behavioral evals all exercise exactly the surface a model host
// would.
package mcp_test

import (
	"context"
	"net/http/httptest"
	"testing"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	inboundmcp "github.com/claudioed/process-path-management/internal/adapters/inbound/mcp"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/memory"
	"github.com/claudioed/process-path-management/internal/application/usecases"
	"github.com/claudioed/process-path-management/internal/domain/cptschedule"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// evalHarness is a fully wired MCP server over in-memory repos plus the
// client session talking to it, with the knobs the evals assert on.
type evalHarness struct {
	session         *sdk.ClientSession
	server          *httptest.Server
	reports         *fakeReportsClient
	lastCallResult  *sdk.CallToolResult
	lastCallErr     error
	lastCallContent string
}

// newEvalDeps builds the default tool surface over empty in-memory repos —
// the same deps cmd/mcp always wires (GetPath, ListPaths, GetCPTSchedule;
// Reports only when REPORTS_BASE_URL is set). This is the pinned
// default-deps surface of tool_registry.golden and the fleet snapshot.
func newEvalDeps() inboundmcp.Deps {
	repo := memory.NewProcessPathRepo()
	scheduleRepo := memory.NewCPTScheduleRepo()
	return inboundmcp.Deps{
		GetPath:        &usecases.GetPath{Repo: repo},
		ListPaths:      &usecases.ListPaths{Repo: repo},
		GetCPTSchedule: &usecases.GetCPTSchedule{Repo: scheduleRepo},
	}
}

// newEvalHarness seeds the canonical eval state (the same contract
// server_test.go's seed provides: PICK active + REBIN deactivated, plus a
// CPT schedule for site sp1 and a canned catalogue-growth report via a
// fake reports client so the conditionally registered report tool is
// exercisable) over a real Streamable HTTP server, and connects a client
// session to it.
func newEvalHarness(t *testing.T) *evalHarness {
	t.Helper()

	h := &evalHarness{}

	repo := seed(t)
	scheduleRepo := memory.NewCPTScheduleRepo()
	cutoff, err := cptschedule.NewCutoff("sp1-1500", "15:00", []cptschedule.Weekday{cptschedule.Monday}, "ground", []shared.PathId{"PICK"})
	if err != nil {
		t.Fatalf("seed cpt cutoff: %v", err)
	}
	schedule, err := cptschedule.Define("sp1", "America/Sao_Paulo", []cptschedule.Cutoff{cutoff}, base)
	if err != nil {
		t.Fatalf("seed cpt schedule: %v", err)
	}
	if err := scheduleRepo.Save(context.Background(), schedule); err != nil {
		t.Fatalf("save cpt schedule: %v", err)
	}

	h.reports = &fakeReportsClient{
		report: inboundmcp.CatalogueGrowthReportView{
			Rows: []inboundmcp.CatalogueGrowthRowView{
				{DayBucket: "2026-09-01T00:00:00Z", PathsDefined: 3, PathsRevised: 1},
			},
		},
	}

	server := inboundmcp.NewServer(inboundmcp.Deps{
		GetPath:        &usecases.GetPath{Repo: repo},
		ListPaths:      &usecases.ListPaths{Repo: repo},
		GetCPTSchedule: &usecases.GetCPTSchedule{Repo: scheduleRepo},
		Reports:        h.reports,
	})
	h.server = httptest.NewServer(inboundmcp.Handler(server))
	t.Cleanup(h.server.Close)

	client := sdk.NewClient(&sdk.Implementation{Name: "eval-client", Version: "0.0.1"}, nil)
	transport := &sdk.StreamableClientTransport{Endpoint: h.server.URL}
	session, err := client.Connect(context.Background(), transport, nil)
	if err != nil {
		t.Fatalf("eval harness connect: %v", err)
	}
	h.session = session
	t.Cleanup(func() { _ = session.Close() })
	return h
}

// evalDepsWithReports is the default deps plus a fake reports client —
// the surface a deployment with REPORTS_BASE_URL set advertises.
func evalDepsWithReports() inboundmcp.Deps {
	deps := newEvalDeps()
	deps.Reports = &fakeReportsClient{}
	return deps
}

// evalDepsWithoutCPTSchedule is the default deps minus the CPT schedule
// read port — the conditional GetCPTSchedule=nil surface.
func evalDepsWithoutCPTSchedule() inboundmcp.Deps {
	deps := newEvalDeps()
	deps.GetCPTSchedule = nil
	return deps
}

// wireSession builds a real Streamable HTTP server over the given deps and
// connects a client session to it, for evals that do not need seeded
// state.
func wireSession(t *testing.T, deps inboundmcp.Deps) *sdk.ClientSession {
	t.Helper()
	server := inboundmcp.NewServer(deps)
	httpSrv := httptest.NewServer(inboundmcp.Handler(server))
	t.Cleanup(httpSrv.Close)

	client := sdk.NewClient(&sdk.Implementation{Name: "eval-client", Version: "0.0.1"}, nil)
	transport := &sdk.StreamableClientTransport{Endpoint: httpSrv.URL}
	session, err := client.Connect(context.Background(), transport, nil)
	if err != nil {
		t.Fatalf("wire session connect: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return session
}

// callTool invokes a tool and records the result for the Then steps.
func (h *evalHarness) callTool(ctx context.Context, name string, args map[string]any) error {
	res, err := h.session.CallTool(ctx, &sdk.CallToolParams{Name: name, Arguments: args})
	h.lastCallResult, h.lastCallErr = res, err
	h.lastCallContent = ""
	if res != nil {
		for _, c := range res.Content {
			if text, ok := c.(*sdk.TextContent); ok {
				h.lastCallContent += text.Text
			}
		}
	}
	return err
}
