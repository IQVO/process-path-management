---
id: 0041-agent-authority-levels-and-complete-mcp-surface
slug: /adr/0041-agent-authority-levels-and-complete-mcp-surface
title: 0041. Agent authority levels and complete MCP surface
sidebar_label: 0041. Agent authority and complete MCP surface
description: "ADR 0041: every MCP tool and A2A skill declares one authority level (all L1 here: the charter is read-only), the MCP server is stateless so it also answers the 2026-07-28 revision, and the server gains exactly one resource template and one prompt."
---

# 0041. Agent authority levels and complete MCP surface

## Status

Accepted (2026-11-02). Applies the fleet decision "Agentic Standard v2"
(`docs/architecture/agentic-standard.md` and the fleet rule
`agents/fleet/agent-authority.md` in IQVO/warehouse-docs) to this
context. Extends [ADR 0006](./0006-mcp-server-second-inbound-adapter.md)
(MCP charter, read-only) and [ADR 0039](./0039-a2a-inbound-adapter.md)
(A2A skills); changes nothing in
[ADR 0003](./0003-transactional-outbox.md) or the CloudEvents envelope
([ADR 0016](./0016-cloudevents-mandatory-event-envelope.md)).

## Context

An agent (or a model host mounting several fleet MCP servers) could see
WHAT each tool does — the MCP `ReadOnlyHint` annotation and the A2A
`read` tag — but not HOW MUCH AUTHORITY a call represents: a pure read,
a computed recommendation, or a state change a human must own. The fleet
decision "Agentic Standard v2" (AS-1) fixes one vocabulary for that:
authority levels `L1`..`L4`, declared where the tool is registered.

Separately, the fleet's MCP servers built the Streamable HTTP handler
with its default (stateful) options, which advertises only the 2025-11-25
revision and older, so a client speaking the 2026-07-28 per-request
handshake (`server/discover`) was refused. And this server's surface was
tools-only: no resource and no prompt (AS-3 calls that an incomplete
agent surface).

## Decision

### Authority levels, declared where the tool is registered

Authority levels are the strings `L1`..`L4`. In this context:

| Surface | Name | Level | Why |
|---|---|---|---|
| MCP | `get_process_path` | `L1` | reads facts |
| MCP | `list_process_paths` | `L1` | reads facts |
| MCP | `get_cpt_schedule` | `L1` | reads facts (registered only when the schedule read port is wired) |
| MCP | `get_catalogue_growth_report` | `L1` | reads facts via the reports REST service (registered only when `REPORTS_BASE_URL` is set) |
| A2A | `get_process_path` | `L1` | reads facts; same level as the tool it fronts |
| A2A | `list_process_paths` | `L1` | reads facts; same level as the tool it fronts |

- **MCP** declares it in the tool's `Meta` under `com.warehouse/authority`
  (`Meta: authorityMeta(authorityL1)` with the pinned helper and consts in
  `internal/adapters/inbound/mcp/tools.go`); it is returned verbatim in
  `tools/list` `_meta`.
- **A2A** declares it as the skill tag `authority:L<n>` on the Agent Card
  (a write skill would keep its `write` tag; there is none here).
- **L4 is not granted here.** The MCP charter is read-only
  ([ADR 0006](./0006-mcp-server-second-inbound-adapter.md)): every tool is
  L1, there is no L2 (no computed recommendation) and no L3/L4 (no write).
  Catalogue changes stay on the REST API, a human action.

Two sensors enforce it. The static go/ast fitness sensor
`internal/architecture/mcp_authority_fitness_test.go` (copied verbatim
from the harness template; `TestEveryMCPToolDeclaresAuthority`,
`TestMCPWriteToolsAreAtLeastL3`, `TestA2ASkillsDeclareAuthority`) fails
the build on a tool without a declaration, and the runtime-wired
`TestEveryAgentToolDeclaresAuthority` (in both the `mcp` and `a2a`
packages) asserts the exact levels over the real Streamable HTTP handler.
The golden registry
(`internal/adapters/inbound/mcp/testdata/tool_registry.golden`) gains an
authority column: `name, readOnly, destructive, authority`.

### Stateless Streamable HTTP handler (AS-4)

`mcp.Handler` builds the handler with
`&mcp.StreamableHTTPOptions{Stateless: true}`: no `Mcp-Session-Id` is
issued, and the server answers the 2026-07-28 revision's per-request
handshake (`server/discover` advertises it) while a legacy `initialize`
still answers 200. Nothing in this server relies on cross-call session
state. Raw-POST tests (`stateless_test.go`) pin the wire facts; reverting
to the default options makes them fail with `-32022` (verified).

### One resource, one prompt (AS-3)

- **Resource template** `warehouse://process-path-management/paths/{pathId}`
  — the bounded path-definition read model (the same `GetPath` use case
  `get_process_path` uses), JSON, one definition per read, an unknown id
  is an error. Not a table dump: the catalogue list stays behind the
  `list_process_paths` tool.
- **Prompt** `review_path_definition` — the operator SOP for auditing one
  catalogue entry: read the definition, check matchPrefix/capabilities/
  intake consistency, verify a site's CPT cutoffs reference existing
  paths, and escalate corrections to the REST surface. Arguments are
  typed (`pathId` required, `siteId` optional); the prompt names tools but
  never calls them and performs no writes.

Both are L1 by nature and carry no tool `Meta`. The eval conformance
tests that pinned "no resources, no prompts" now pin the new surface:
exactly one template and exactly one prompt.

## Consequences

- `tools/list` carries `com.warehouse/authority` in every tool's `_meta`;
  a host can gate calls on it. Additive: names, arguments and results are
  unchanged, unknown arguments stay rejected.
- The server keeps working for every existing client (the SDK client
  negotiates a legacy revision; the integration suite passes unchanged).
- Adding a tool now means declaring its level in the same PR the sensor
  lands (fleet rollout rule); adding a write tool would need L3 and a
  charter amendment.
- The conditional tools (`get_cpt_schedule`, `get_catalogue_growth_report`)
  declare their level exactly like the always-registered ones; the golden
  registry pins the full surface including them.
