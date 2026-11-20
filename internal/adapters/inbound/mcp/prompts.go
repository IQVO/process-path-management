package mcp

import (
	"context"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// reviewPathDefinitionSOP is the operational standard-operating-procedure
// the review_path_definition prompt hands to the model (Agentic Standard
// v2, AS-3). Per the charter, prompts encode how to interpret results,
// when to escalate, and what "done" means — they standardise agent
// behaviour across clients rather than leaving procedure implicit. The
// prompt names tools but never calls them; nothing here writes (this
// context's MCP charter is read-only, ADR 0006).
const reviewPathDefinitionSOP = `You are reviewing a process-path catalogue entry for correctness and operational safety. Use only the read surfaces of process-path-management; never assume state. This context is the fleet's single source of truth for WHAT a path is and WHICH capabilities it requires — it never assigns or dispatches work, and the MCP surface is read-only: catalogue corrections are a human action on the REST API.

Procedure:
1. Read the path's definition (resource warehouse://process-path-management/paths/<pathId>, or the get_process_path tool). It carries matchPrefix, direct, requiredCapabilities, cycleTimeP95, eligibility, intake (the demand sources allowed to route work onto it; [] means only an explicit path id reaches it) and status.
2. Check internal consistency: matchPrefix must not be empty; requiredCapabilities should name capabilities the workforce can actually hold; a deactivated path (status DEACTIVATED) must not be the routing target for an active demand source — an intake list on a deactivated path is a finding, not an error to fix yourself.
3. When siteId is given, read the site's CPT schedule (get_cpt_schedule): every cutoff's eligiblePathIds must reference a path id that exists (get_process_path answers that) — a cutoff naming an unknown or deactivated path is a finding.
4. For context, list_process_paths with activeOnly=false shows the audit view (including deactivated paths); use it to spot near-duplicate matchPrefixes that could capture each other's demand.

Interpretation:
- A path whose matchPrefix is a prefix of another ACTIVE path's matchPrefix can silently capture its demand — report both, do not decide which one wins.
- An eligibility or intake mismatch between what the definition says and what operations reports is a data-quality finding for the catalogue owner.

Escalate to a human when: the definition needs correcting, a path should be (de)activated, or a CPT cutoff references a missing path. Those are REST writes (this MCP surface cannot and must not make them).

Done means: you have reported the path's definition, listed every internal-consistency finding with the exact field and value, flagged any CPT cutoff referencing a missing or deactivated path, and named the human action each finding needs.`

// registerPrompts adds the workflow prompt (the operator SOP).
func (d Deps) registerPrompts(server *mcp.Server) {
	server.AddPrompt(&mcp.Prompt{
		Name:        "review_path_definition",
		Description: "Standard operating procedure for reviewing one process-path catalogue entry: read the definition, check matchPrefix/capabilities/intake consistency, verify CPT cutoffs reference existing paths, and report findings for human action.",
		Arguments: []*mcp.PromptArgument{
			{Name: "pathId", Description: "The canonical process path id to review, e.g. PICK, PACK, REBIN, SLAM.", Required: true},
			{Name: "siteId", Description: "Optional site id: when given, the site's CPT schedule is also checked against the path (its cutoffs' eligiblePathIds must reference existing paths).", Required: false},
		},
	}, func(ctx context.Context, req *mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
		return &mcp.GetPromptResult{
			Description: "How to review a process-path definition: read it, check consistency, verify CPT references, escalate corrections to a human.",
			Messages: []*mcp.PromptMessage{{
				Role:    "user",
				Content: &mcp.TextContent{Text: reviewPathDefinitionSOP},
			}},
		}, nil
	})
}
