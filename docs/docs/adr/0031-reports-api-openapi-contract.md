---
id: 0031-reports-api-openapi-contract
slug: /adr/0031-reports-api-openapi-contract
title: 0031. The reports API has its own OpenAPI contract
sidebar_label: 0031. Reports API OpenAPI contract
description: ADR 0031 — every route of cmd/pathmgmt-reports is specified in apis/openapi-reports.yaml, linted by Spectral, rendered into the docs site as its own API reference, and contract-tested in Go against the real router (fleet gap fix D18).
---

# 0031. The reports API has its own OpenAPI contract

## Status

Accepted.

## Context

`cmd/pathmgmt-reports` (ADR 0007) serves three routes:
`GET /reports/catalogue-growth`, `GET /reports/catalogue-growth/freshness`
and `GET /healthz`. warehouse-console's context-reports screen and this
service's own MCP tool `get_catalogue_growth_report` call them. They were
described only in prose on the runbook page: no machine-readable contract,
no lint, no generated reference, and no test that would fail when the
handlers and the description drift apart. `apis/openapi.yaml` describes the
OLTP `cmd/pathmgmt` API, which is a different binary on a different port
against a different database, so adding the reports routes to it would
describe a server that does not exist.

The fleet's 2026-10-12 gap review (D18) decided that every `*-reports`
binary's routes go into `apis/openapi-reports.yaml`, the convention
labor-performance started.

## Decision

- `apis/openapi-reports.yaml` (OpenAPI 3.0.3) specifies the three routes,
  their query parameters (`from`, `to` required RFC 3339; `granularity`
  optional, `day` only), the `CatalogueGrowthReport`, `CatalogueGrowthRow`,
  `Freshness` and `Health` bodies, and the RFC 7807 `Problem` body of the
  400 `invalid-report-query` and 500 `report-store-error` answers. The
  window is half-open `[from, to)` on the day bucket, as the Postgres query
  implements it. This is a description of existing behaviour: no route,
  parameter or body changed.
- The `api-lint` CI job and `make api-lint` run Spectral on it with the
  same ruleset as `apis/openapi.yaml`.
- The docs site generates a second API reference from it
  (`docs/docs/api-reference/rest-reports/`, plugin config
  `pathmgmtReports`); the `docs-api-drift` job regenerates both references
  with `npm run clean-api-docs:all && npm run gen-api-docs:all` and fails on
  any diff or untracked page.
- `internal/adapters/inbound/http/reports_contract_test.go` keeps the spec
  honest: `TestReportsContract_RoutesMatchSpec` walks the real chi router
  and fails when a served route is not specified or a specified route is
  not served; `TestReportsContract_ResponsesMatchSpec` drives every
  documented status (200, 400, 500) of every operation and fails on an
  undeclared status or media type, a missing required property or an
  undocumented one (including the first report row).
- Schemathesis (ADR 0025) stays on `apis/openapi.yaml` only: the reports
  binary needs a Postgres read model to answer, and the Go contract test
  covers its small surface without one.

## Consequences

**Easier**

- Consumers of the reports API (the console, the MCP tool, other agents)
  have a generated reference and a spec they can generate clients from.
- A change to a reports handler or DTO that is not reflected in the spec
  fails `go test` in CI.

**Harder**

- Two specs to keep in step with two routers; the contract test and the
  drift job make forgetting one visible.
- The contract test checks property presence, not value types or formats;
  Schemathesis-level property-based coverage of the reports API is not part
  of this decision.
