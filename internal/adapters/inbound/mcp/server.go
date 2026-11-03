package mcp

import (
	"net/http"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// NewServer builds the MCP server for this bounded context with its
// read-only tools registered.
func NewServer(deps Deps) *mcp.Server {
	server := mcp.NewServer(
		&mcp.Implementation{Name: "process-path-management-mcp", Version: "1.0.0"},
		&mcp.ServerOptions{
			Instructions: "Read-only access to the process-path-management catalogue: a path's canonical identity, its matchPrefix routing rule, whether it is direct, and the capabilities required to work it. This is the fleet's Open Host Service / published-language source for process paths.",
		},
	)

	deps.registerTools(server)

	return server
}

// Handler returns the Streamable HTTP handler for the MCP server. Per
// the fleet's current convention (the static-bearer auth layer was
// removed fleet-wide, see this repo's ADR 0005), the server is mounted
// unauthenticated.
func Handler(server *mcp.Server) http.Handler {
	// Stateless (Agentic Standard v2, AS-4): no Mcp-Session-Id is issued,
	// so the server also answers the 2026-07-28 revision's per-request
	// handshake (server/discover) while legacy initialize clients keep
	// working. Nothing here relies on cross-call session state.
	return mcp.NewStreamableHTTPHandler(
		func(*http.Request) *mcp.Server { return server },
		&mcp.StreamableHTTPOptions{Stateless: true},
	)
}
