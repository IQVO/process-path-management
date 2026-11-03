package mcp

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// mimeTypeJSON is the MIME type of every resource this server serves.
const mimeTypeJSON = "application/json"

// registerResources adds the scoped read-model resource (Agentic Standard
// v2, AS-3). Per the charter, resources are bounded-context contracts tied
// to a decision, not bulk dumps: the path-definition resource answers
// "what exactly IS this process path?" for one canonical id — the same
// GetPath read model the get_process_path tool uses, loadable by a client
// without a tool call. The catalogue itself stays behind the
// list_process_paths tool; this URI always returns exactly one bounded
// definition.
func (d Deps) registerResources(server *mcp.Server) {
	const pathTemplate = "warehouse://process-path-management/paths/{pathId}"
	server.AddResourceTemplate(&mcp.ResourceTemplate{
		URITemplate: pathTemplate,
		Name:        "process path definition",
		Description: "The full definition of one process path by its canonical id: matchPrefix, direct, requiredCapabilities, cycleTimeP95, eligibility, intake, status and timestamps. Deactivated paths are readable too; an unknown id is an error, never an empty success.",
		MIMEType:    mimeTypeJSON,
	}, func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		pathId, err := parsePathURI(req.Params.URI)
		if err != nil {
			return nil, err
		}
		p, err := d.GetPath.Execute(ctx, shared.PathId(pathId))
		if err != nil {
			return nil, err
		}
		return pathResourceResult(req.Params.URI, toProcessPathDTO(p))
	})
}

func pathResourceResult(uri string, dto processPathDTO) (*mcp.ReadResourceResult, error) {
	body, err := json.Marshal(dto)
	if err != nil {
		return nil, err
	}
	return &mcp.ReadResourceResult{
		Contents: []*mcp.ResourceContents{{
			URI:      uri,
			MIMEType: mimeTypeJSON,
			Text:     string(body),
		}},
	}, nil
}

// parsePathURI extracts the PathId from a
// warehouse://process-path-management/paths/{pathId} URI. The segment is
// model-supplied and therefore untrusted; a malformed URI or an empty
// segment is rejected rather than silently defaulted. Hand-rolled (no
// net/url) so a path id containing a colon or percent sign cannot smuggle
// a different scheme past the prefix check.
func parsePathURI(uri string) (string, error) {
	const prefix = "warehouse://process-path-management/paths/"
	if len(uri) < len(prefix) || uri[:len(prefix)] != prefix {
		return "", fmt.Errorf("resource uri %q must start with %q", uri, prefix)
	}
	id := uri[len(prefix):]
	if id == "" || id != trimAllSlashes(id) {
		return "", fmt.Errorf("resource uri %q must be warehouse://process-path-management/paths/{pathId} with a single non-empty pathId segment", uri)
	}
	return id, nil
}

func trimAllSlashes(s string) string {
	for len(s) > 0 && (s[0] == '/' || s[len(s)-1] == '/') {
		if s[0] == '/' {
			s = s[1:]
		} else {
			s = s[:len(s)-1]
		}
	}
	return s
}
