// Package a2a is the inbound Agent2Agent adapter: it exposes this bounded
// context to the fleet's agent ecosystem as a third driving adapter over the
// same application-layer read use cases the HTTP and MCP adapters use. It is
// built on the official A2A Go SDK (a2aproject/a2a-go) and speaks JSON-RPC 2.0
// over HTTP, with the Agent Card served at the well-known path.
//
// Per ADR 0039, A2A is the DELEGATION channel between bounded contexts: an
// agent client sends a goal-shaped skill request as a structured message part
// and this agent answers from its own state through its own use cases. It is
// not a data-sync channel: facts between contexts still travel exclusively as
// CloudEvents on Kafka (ADR 0016), published through the transactional outbox
// (ADR 0003).
//
// This agent is READ-ONLY, exactly like the MCP adapter (ADR 0006): both
// skills read the process-path catalogue. This package never writes, never
// calls another bounded context (AGENTS.md: no REST, no MCP, no gRPC, no live
// lookup, in either direction) and contains no outbound A2A client.
//
// Like the MCP adapter, this package depends inward on the application layer
// (use cases) and the domain only, never on an outbound adapter. The
// composition root (cmd/a2a) wires the concrete repository into the use cases.
// Domain structs never leak across the message boundary.
package a2a

import (
	"context"
	"iter"
	"net/http"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"

	"github.com/claudioed/process-path-management/internal/application/usecases"
)

// Config carries the deployment-specific identity of this agent: the absolute
// URL clients use to reach its JSON-RPC endpoint (advertised on the Agent
// Card) and the card version. It is supplied by the composition root from
// A2A_AGENT_URL; the card MUST advertise a URL the fleet's agent clients can
// actually dial (in-cluster: http://<release>-a2a.<ns>.svc.cluster.local:8093/).
type Config struct {
	AgentURL string
	Version  string
}

// Deps is everything the A2A skills need, injected by the composition root.
// It carries the SAME read use cases the HTTP and MCP adapters use; the adapter
// never constructs an outbound adapter itself, and never writes.
type Deps struct {
	// GetPath is the existing read use case, reused unchanged: the full
	// definition of one process path, active or deactivated.
	GetPath *usecases.GetPath
	// ListPaths is the existing read use case, reused unchanged: the
	// catalogue, active-only by default or every path including deactivated.
	ListPaths *usecases.ListPaths
}

// Card builds this agent's Agent Card (ADR 0039). The surface is two curated,
// goal-shaped READ skills derived from the MCP tools get_process_path and
// list_process_paths (ADR 0006): this context's MCP charter is entirely
// read-only, so the agent is read-only too and never widens what the context
// exposes. The card declares no security schemes on purpose: this surface is
// unauthenticated by the same fleet-wide decision as REST and MCP (ADR 0005);
// schemes are added here, not in a redesign, if that decision is ever
// reversed.
func Card(cfg Config) *a2a.AgentCard {
	return &a2a.AgentCard{
		Name:        "process-path-management",
		Description: "Process Path Management agent for the Warehouse Execution System: the read-only source of truth for the fleet's process-path catalogue. It answers what a process path is (its matchPrefix routing rule, whether it is direct, the capabilities required to work it, cycle time, eligibility and which demand sources may route onto it) and which paths currently exist. It never changes the catalogue, and it does not decide dispatch, routing or assignment.",
		Version:     cfg.Version,
		Provider: &a2a.AgentProvider{
			Org: "IQVO",
			URL: "https://github.com/IQVO/process-path-management",
		},
		SupportedInterfaces: []*a2a.AgentInterface{
			a2a.NewAgentInterface(cfg.AgentURL, a2a.TransportProtocolJSONRPC),
		},
		DefaultInputModes:  []string{"application/json", "text/plain"},
		DefaultOutputModes: []string{"application/json", "text/plain"},
		Capabilities:       a2a.AgentCapabilities{Streaming: false},
		Skills: []a2a.AgentSkill{
			{
				ID:          skillGetProcessPath,
				Name:        "Look up one process path",
				Description: "Read the full definition of one process path by its canonical pathId (e.g. PICK, PACK, REBIN, SLAM): matchPrefix, direct, requiredCapabilities, cycleTimeP95, eligibility, intake (the demand sources allowed to route work onto it; [] means only an explicit path id reaches it), status and timestamps. Works for deactivated paths too. Answers a 404 problem when no path has that id. Persists nothing.",
				Tags:        []string{"process-path", "catalogue", "read"},
				InputModes:  []string{"application/json"},
				Examples: []string{
					`{"skill":"get_process_path","arguments":{"pathId":"PICK"}}`,
				},
			},
			{
				ID:          skillListProcessPaths,
				Name:        "List the process-path catalogue",
				Description: "Read the process-path catalogue: every ACTIVE path by default, or, with activeOnly=false, deactivated paths too (the audit view). Each entry carries the same fields as get_process_path. The catalogue is small and bounded. Persists nothing.",
				Tags:        []string{"process-path", "catalogue", "read"},
				InputModes:  []string{"application/json"},
				Examples: []string{
					`{"skill":"list_process_paths","arguments":{}}`,
					`{"skill":"list_process_paths","arguments":{"activeOnly":false}}`,
				},
			},
		},
	}
}

// NewExecutor returns the AgentExecutor that maps an incoming message to a
// skill and answers with a single agent Message (a message-only agent in A2A
// terms): both curated skills are transactional request/response reads, so
// there is no long-running Task lifecycle to track in this first A2A surface.
// The response carries the caller's contextId (and taskId when the client
// referenced one) so a delegating saga can group its interactions.
func NewExecutor(deps Deps) a2asrv.AgentExecutor {
	return a2asrv.AgentExecutorFunc(func(ctx context.Context, ec *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			yield(answer(ctx, deps, ec), nil)
		}
	})
}

// Handler wires the executor into the transport-agnostic A2A request handler
// and wraps it in the JSON-RPC 2.0 HTTP handler, the only transport this
// agent ships (ADR 0039), mirroring ADR 0006's "Streamable HTTP only"
// minimalism for MCP.
func Handler(executor a2asrv.AgentExecutor) http.Handler {
	return a2asrv.NewJSONRPCHandler(a2asrv.NewHandler(executor))
}
