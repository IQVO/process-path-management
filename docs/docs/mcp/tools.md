---
id: tools
title: MCP tools
sidebar_label: MCP tools
description: Every tool the process-path-management MCP server (cmd/mcp) exposes, with its input fields, output shape, annotations and registration conditions.
---

# MCP tools

`cmd/mcp` is a separate binary that serves the
[Model Context Protocol](https://modelcontextprotocol.io) over **Streamable
HTTP only**, on `MCP_ADDR` (default `:8090`). The handler is mounted at `/`,
`/mcp` and `/mcp/`; `GET /healthz` answers `{"status":"ok"}` for the probes.
None of these routes are in `apis/openapi.yaml`. The server is
unauthenticated, like the rest of the fleet
([ADR 0005](../adr/0005-remove-rest-auth.md)), and identifies itself as
`process-path-management-mcp` version `1.0.0`
(`internal/adapters/inbound/mcp/server.go`).

**Every tool is read-only.** There is no write tool: paths and CPT
schedules change only through the REST API. The rules for adding a tool are
in the [Governance charter](./governance-charter.md)
([ADR 0006](../adr/0006-mcp-server-second-inbound-adapter.md),
[ADR 0023](../adr/0023-mcp-eval-and-governance-harness.md)).

## Tool list

The schemas below are the server's own `tools/list` answer, captured from
`cmd/mcp` running in memory with `REPORTS_BASE_URL` set. Every input schema
has `additionalProperties: false`, so unknown arguments are rejected.

| Tool | Annotations | Registered when | Use case / backend |
| --- | --- | --- | --- |
| `get_process_path` | `readOnlyHint: true`, `idempotentHint: false` | always | `GetPath` over the OLTP store |
| `list_process_paths` | `readOnlyHint: true`, `idempotentHint: false` | always | `ListPaths` over the OLTP store |
| `get_cpt_schedule` | `readOnlyHint: true`, `idempotentHint: false` | whenever the schedule read port is wired, which `cmd/mcp` always does | `GetCPTSchedule` over the OLTP store |
| `get_catalogue_growth_report` | `readOnlyHint: true`, `idempotentHint: false` | only when `REPORTS_BASE_URL` is set | HTTP `GET /reports/catalogue-growth` on `pathmgmt-reports` (10 s client timeout) |

Each call runs inside an OTel span `mcp.tool <name>` with attribute
`mcp.tool.name`; a failing call marks the span `Error`.

### `get_process_path`

| Input | Type | Required | Meaning |
| --- | --- | --- | --- |
| `pathId` | string | yes | Canonical process path id, e.g. `PICK`, `PACK`, `REBIN`, `SLAM`. Empty → error `pathId is required`. |

Returns one path in any status: `pathId`, `matchPrefix`, `direct`,
`requiredCapabilities[]`, `destinationLocationRole` (omitted when unset),
`status` (`ACTIVE`/`DEACTIVATED`), `active` (boolean), `createdAt`,
`updatedAt` (RFC 3339), `cycleTimeP95` (Go duration string such as
`2h0m0s`), `eligibility` (`maxUnitsPerLine`, `requiredProductAttributes`,
`excludedProductAttributes`, `nonSortable`, each omitted when unset).
An unknown id is a **tool error** (`usecases: process path not found`), not
an empty success.

### `list_process_paths`

| Input | Type | Required | Meaning |
| --- | --- | --- | --- |
| `activeOnly` | boolean or null | no | `true` or omitted: only Active paths. `false`: include deactivated ones. |

Returns `{"paths": [...]}` with the same per-path shape as
`get_process_path`.

### `get_cpt_schedule`

| Input | Type | Required | Meaning |
| --- | --- | --- | --- |
| `siteId` | string | yes | Site id, e.g. `sp1`. Empty → error `siteId is required`. |

Returns `siteId`, `timezone` (IANA), `createdAt`, `updatedAt` and
`cutoffs[]`, each with `cptId`, `localTime` (`HH:MM`), `daysOfWeek[]`
(`Mon`…`Sun`), `shipMethod`, `eligiblePathIds[]`. A site with no schedule is
a tool error.

### `get_catalogue_growth_report`

| Input | Type | Required in schema | Meaning |
| --- | --- | --- | --- |
| `from` | string | yes | Window start, inclusive, RFC 3339. |
| `to` | string | yes | Window end, exclusive, RFC 3339. |
| `granularity` | string | **yes** | Only `day` is supported. |

Returns `{"rows": [...]}`, each row with `dayBucket`, `pathsDefined`,
`pathsRevised`, `pathsDeactivated`: exactly the body of the reports API
([Runbook](../operations/runbook.md#reports-api-pathmgmt-reports)).

The handler treats `granularity` as optional (it forwards it only when
non-empty, and the reports API defaults to `day`), but the field has no
`omitempty` tag, so the SDK-generated input schema lists it as required and
a schema-validating client must send it. Missing `from`/`to` → error
`from and to are required (RFC3339)`; a non-2xx from the reports service →
`reports client: unexpected status <code>`.

The reports client also implements a freshness call
(`GET /reports/catalogue-growth/freshness`), but no tool exposes it.

## Talking to the server by hand

```bash
go run ./cmd/mcp      # or port-forward svc/<release>-mcp 8090

curl -si localhost:8090/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"curl","version":"0"}}}'
# 200, text/event-stream, header Mcp-Session-Id: <id>

curl -s localhost:8090/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -H 'Mcp-Session-Id: <id>' -H 'Mcp-Protocol-Version: 2025-06-18' \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"list_process_paths","arguments":{"activeOnly":false}}}'
```

Sessions live in the process's memory, so keep `mcp.replicaCount` at 1 (the
chart has no HPA for MCP and the Service has no session affinity).

## Who calls it

`warehouse-ops-agent` has a client for `get_process_path` and
`list_process_paths`, constructed but unused today. No sibling calls
`get_cpt_schedule` or `get_catalogue_growth_report`. See
[Integration](../ecosystem/integration.md).
