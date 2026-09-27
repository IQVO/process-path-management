---
id: governance-charter
title: MCP Governance Charter
sidebar_label: MCP Governance Charter
description: "The estate-wide rules every warehouse-systems MCP server follows — tool curation, naming, annotations, security posture, audit, and the review gate. Federated: global standards, domain-owned servers."
---

# MCP Governance Charter

This charter is the **federated computational governance** for MCP across
warehouse-systems: one set of global standards, enforced the same way in every
repository, while each bounded context owns its own server. It is the MCP
counterpart to the platform's existing quality gate and its ADR discipline.
`fulfillment-execution` is the reference implementation; this repository —
`process-path-management`, whose own adapter decision is
[ADR-0006](../adr/0006-mcp-server-second-inbound-adapter.md) — copies the
federated charter and enforces it locally, as every fleet context does.

Keywords **MUST**, **SHOULD**, **MAY** are used per RFC 2119.

## 1. Architecture rules (non-negotiable)

1. An MCP server **MUST** be an inbound adapter at
   `internal/adapters/inbound/mcp/`, depending inward on `application` only.
2. Tool handlers **MUST** call existing application-layer **use cases**. They
   **MUST NOT** touch the domain layer directly, run SQL, or duplicate use-case
   logic. No MCP type may appear in `internal/domain/**` or
   `internal/application/**` — the arch-go fitness tests
   (`internal/architecture/fitness_test.go`: "mcp adapter depends only on
   application and domain") enforce the dependency rule.
3. Each server **MUST** ship as a separate `cmd/mcp` binary reusing the service's
   existing composition wiring.
4. Transport **MUST** be Streamable HTTP. stdio builds **MUST NOT** be shipped.
5. The official Go SDK (`github.com/modelcontextprotocol/go-sdk`) **MUST** be
   used and **MUST** be version-pinned in `go.mod`. Deprecated MCP features
   (`roots`, `sampling`, `logging`; SEP-2577) **MUST NOT** be used — prefer tool
   parameters, resource URIs, and configuration.

## 2. Tool curation — the surface is a product

The single most important rule: **expose intent-level tools, not one tool per
REST endpoint.** Tools are designed around decisions an agent makes.

1. A tool **MUST** map to an outcome an agent wants (`get_process_path`),
   not to a transport route (`get_paths_id`).
2. A server **SHOULD** expose **no more than 8 tools**. A PR that pushes a server
   over that count **MUST** carry an explicit justification in its description
   and be approved by a second reviewer.
3. Bulk read access **MUST NOT** be exposed as a tool. Large read models are
   **resources**, scoped to a decision (see §5). (The catalogue is small and
   bounded, so `list_process_paths` is an intent-level read, not a bulk dump.)

## 3. Naming conventions

| Element | Convention | Example |
| --- | --- | --- |
| Tool name | `snake_case`, `verb_noun`, intent-level | `get_process_path` |
| Resource URI | `<kind>://<context>/<scope>` | `process-path://pathmgmt/PICK` |
| Prompt name | `snake_case`, names the SOP | `triage_stale_paths` |

`<context>` is the bounded-context short name (`pathmgmt`).

## 4. Tool annotations (mandatory)

Every tool **MUST** declare annotations so a host can reason about risk before
letting a model call it:

1. A **read** tool **MUST** be annotated read-only (no state change).
2. A **write** tool **MUST** be annotated destructive. This server is
   **entirely read-only** (ADR-0006) — it exposes no write tool at all, so the
   destructive branch never applies here today.
3. Annotations and descriptions are treated as **untrusted** across servers; a
   host **MUST NOT** rely on another server's annotations for its own safety
   decisions. (Within our own trusted servers they are authoritative.)
4. Descriptions **MUST** state what the tool does and its side effects plainly —
   the description is read by the model and is part of the safety surface.

## 5. Resources — scoped context contracts

1. A resource **MUST** be scoped to a decision, backed by an existing read model
   / projection. It **MUST NOT** dump an entire table, config, or log.
2. Resources are read-only. Anything that changes state is a write **tool**, not
   a resource.

This server currently exposes **no resources and no resource templates** — the
E2 evals pin the empty discovery lists so that stays a visible contract.

## 6. Prompts — operational SOPs

Prompts **SHOULD** encode operational discipline the model should follow. This
server currently exposes **no prompts**; the E2 evals pin the empty
`prompts/list` result. Adding the first prompt means updating
`eval_conformance_test.go` alongside it.

## 7. Security & authorization (current posture: unauthenticated, in-cluster)

The fleet's static-bearer-key posture was rolled out and then **removed**; in
this repository that removal is recorded by
[ADR-0005](../adr/0005-remove-rest-auth.md). Today:

1. `cmd/mcp` serves the Streamable HTTP handler **with no authentication**;
   it reads no `MCP_READ_KEY` / `MCP_READWRITE_KEY`, and no tool checks a
   scope.
2. No secret, token, or key **MAY** appear in any log line.
3. Servers **MUST** remain reachable only in-cluster (a `ClusterIP` Service);
   a server **MUST NOT** be exposed to public/end-user traffic. Re-introducing
   an identity layer is a new ADR, not a revert.
4. When a tool must call another service, the server **MUST NOT** forward a
   client-supplied credential (confused-deputy prevention). In this repository
   the only upstream hop is the report tool's call to the
   `pathmgmt-reports` REST (ADR-0007), which carries no credential.

## 8. Guardrails (regardless of auth)

1. Every tool handler **MUST** validate its inputs defensively — the caller is
   a model, arguments are untrusted.
2. Write tools **MUST** be rate-limited. (Not applicable today: this adapter
   is read-only.)
3. Domain errors **MUST** surface as clean structured tool errors. Here a
   not-found id is deliberately a **tool-level error** (matching this
   service's REST 404 semantics), never a zero-value success.

## 9. Auditability

Every tool call **MUST** emit an audit record with, at minimum:

- `tool` name,
- `outcome` (success / error),
- timestamp and trace id.

Audit records **MUST** carry the OpenTelemetry trace id so a call links to its
span. The adapter **MUST** be instrumented with the platform's existing OTel
setup — satisfied here today by the per-call span (`mcp.tool <name>`,
`internal/adapters/inbound/mcp/tools.go`); a dedicated audit record and
invocation/denial counters per call are not yet implemented (see Status).

## 10. Quality gate (same bar as the rest of the service)

1. Tool handlers **MUST** be unit-tested (table-driven, in-memory adapters) to
   the platform's ≥90% coverage bar, plus at least one transport-level test.
2. The MCP adapter **MUST** pass `make check` (fmt, vet, build, lint, test) and
   the arch-go fitness tests.
3. **Phase-6 governance gate:** a plain `go test` that boots the real
   server and asserts the tool-count budget (§2.2), the naming convention,
   mandatory annotations and non-empty descriptions. Not yet implemented
   in this repository as a separate `governance_test.go` — the E1 evals
   below already enforce annotations, descriptions, and the pinned
   surface; the mechanical count budget is the remaining gap.
4. **Eval gate (E1–E3):** the tool surface **MUST** pass the eval suites in
   `internal/adapters/inbound/mcp/eval_*_test.go` and `evalsuite_test.go`,
   all plain `go test`s inside the CI `test` job (ported from the
   inventory-storage pilot, adapted to this repo's read-only surface):
   - **E1 — schema & metadata** (`eval_governance_test.go`): every
     advertised tool's input schema resolves as a JSON Schema, accepts a
     schema-shaped arguments object, and REJECTS wrong-typed values (it
     constrains model input, not just decorates it); every parameter
     carries a non-empty description and every required parameter is
     declared; the advertised surface matches
     `testdata/tool_registry.golden`; and this repo's tools are present,
     correctly credited, and globally unique in
     `testdata/fleet_tool_snapshot.golden` (the federated registry kept
     identical across all fleet repos — a model host mounts several of
     these servers together, so tool names MUST NOT collide). The golden
     pins the **default-deps surface** (the deps `cmd/mcp` always wires);
     the two conditionally registered tools (`get_catalogue_growth_report`
     when a reports client is wired / `REPORTS_BASE_URL` is set,
     `get_cpt_schedule` when the schedule read port is wired) are pinned
     separately by `TestEval_ConditionalToolsArePinned`.
   - **E2 — wire conformance** (`eval_conformance_test.go`): over the real
     Streamable HTTP handler — initialize handshake carries server info
     and non-empty instructions; unknown tools, wrong-typed arguments,
     unknown extra arguments, unknown resources and prompts are rejected;
     resource templates, resources, and prompts list as **empty** (this
     server exposes none — pinned, not assumed); a closed session fails
     loudly.
   - **E3 — behavioral evals** (`evalsuite_test.go` +
     `testdata/features/mcp_tools.feature`): Gherkin scenarios driving
     `tools/call` with model-realistic arguments (stray keys, wrong types,
     unknown ids) against seeded state, pinning structured results. This
     adapter is read-only, so there are no side-effect steps to pin;
     instead every tool's happy path AND error/edge path are pinned,
     including the deactivated-path read and the strict-boolean flag.

### Pinned behavioral contracts the evals found

- Typed tool schemas are **strict** (`additionalProperties: false`, the
  SDK default): stray model-generated argument keys are rejected with a
  validation error, not silently ignored.
- A **deactivated** path is hidden from `list_process_paths` by default
  (`activeOnly` defaults to true) but still returned — flagged
  `status=DEACTIVATED`, `active=false` — by `get_process_path` for its
  canonical id. Deactivation is a lifecycle change, not a delete.
- Not-found is a **tool-level error** whose message mentions "not found"
  (the use case's `ErrPathNotFound` / `ErrCPTScheduleNotFound` surface
  unchanged); unlike inventory-storage's zero-usable convention, this
  server never answers an unknown id with a zero-value success.
- A boolean parameter (`activeOnly`) given a string value (`"yes"`) is
  **rejected without coercion** — no truthy string interpretation.
- `cycleTimeP95` crosses the wire as Go's duration string form (`2h0m0s`),
  not an RFC 3339 duration — pinned as the visible contract.

### Status in this repository

`process-path-management-mcp` exposes 3 tools by default
(`get_process_path`, `list_process_paths`, `get_cpt_schedule`) plus
`get_catalogue_growth_report` when `REPORTS_BASE_URL` is set, **no
resources and no prompts**. Each tool call gets an OTel span
(`mcp.tool <name>`). Not yet implemented here: a dedicated audit record per
call (§9), and MCP-specific invocation/denial counters (§9). This charter
was ported from the fleet's federated charter (inventory-storage's copy)
in the same change that added the E1–E3 eval gate.

## 11. Changing this charter

This charter is versioned with the docs. A change to a global standard **MUST**
be proposed as a PR and, because it binds every fleet context, **SHOULD** be
recorded as an ADR when it changes an architecturally significant rule.
