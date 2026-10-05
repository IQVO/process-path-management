---
id: 0020-rfc-7807-problem-details
slug: /adr/0020-rfc-7807-problem-details
title: 0020. Adopting RFC 7807 Problem Details for all HTTP error responses
sidebar_label: 0020. RFC 7807 problem details
description: ADR 0020 — every non-2xx HTTP response is an application/problem+json document under https://errors.process-path-management.warehouse-systems.dev/, one stable type per error category.
---

# 0020. Adopting RFC 7807 Problem Details for all HTTP error responses

## Status

Accepted — codifies behaviour that already ships, retroactively.

## Context

REST error responses need a stable, machine-readable shape so the MFE
and scripted callers can branch on an error category rather than parse
prose. The fleet standard (workforce-management ADR 0005 is the
reference) is RFC 7807 Problem Details. This service has implemented it
since the REST adapter's first release: every non-2xx response is
`application/problem+json` with `type` under
`https://errors.process-path-management.warehouse-systems.dev/<slug>`,
generated from the single `problemCatalog` in
`internal/adapters/inbound/http/errors.go`.

## Decision

Keep RFC 7807 as the contract for every error response: one catalog
entry (slug + title) per error category, `statusFor` mapping each typed
error to its status code, `detail` carrying the dynamic message. New
error categories are added to the catalog, never ad-hoc JSON.

## Consequences

Clients can switch on the `type` URI's last segment (e.g.
`concurrent-modification` from ADR 0017) without parsing `detail`; the
OpenAPI spec documents one `ProblemDetails` schema for every error
response. The type URIs are identifiers, not resolvable pages — they
need no hosting.
