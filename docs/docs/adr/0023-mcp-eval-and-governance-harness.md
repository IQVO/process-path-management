---
id: 0023-mcp-eval-and-governance-harness
slug: /adr/0023-mcp-eval-and-governance-harness
title: 0023. The MCP eval and governance harness
sidebar_label: 0023. MCP eval & governance
description: ADR 0023 — eval_*_test.go suites (E1 schema, E2 wire conformance, E3 behavioral), testdata/*.golden snapshots, and docs/docs/mcp/governance-charter.md gate every MCP tool change, run by CI's evals-tests job.
---

# 0023. The MCP eval and governance harness

## Status

Accepted — codifies the harness that already ships, retroactively.

## Context

The MCP server (ADR 0006) exposes this context's read surface to AI
agents. Its tool schemas and wire behaviour are a contract: an agent
(or a fleet tool snapshot) that memorized a tool's shape breaks
silently when it changes. The repo ships three eval layers in
`internal/adapters/inbound/mcp` — `eval_conformance_test.go` (E1:
declared schemas), `eval_harness_test.go`/`evalsuite_test.go` (E2:
golden wire snapshots under `testdata/`), `eval_governance_test.go`
(E3: behavioural rules) — plus `docs/docs/mcp/governance-charter.md`,
the human-written charter those tests enforce. CI runs them as the
dedicated `evals-tests` job.

## Decision

Keep the harness gating: every tool schema/behaviour change must update
the golden files deliberately (`go test ./internal/adapters/inbound/mcp/
... -run '^TestEval|^TestMCPEvalSuite'`), the charter is the source of
truth the E3 tests cite, and new tools join the suites from their first
commit.

## Consequences

Agent-visible drift is caught at PR time instead of downstream; a
reviewer can diff golden files to see exactly what an agent would
observe changing. The cost is maintaining snapshots alongside schema
changes — mechanical, and the tests print the regeneration path on
mismatch.
