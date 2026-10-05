---
id: 0005-remove-rest-auth
slug: /adr/0005-remove-rest-auth
title: 0005. Removing the REST auth layer
sidebar_label: 0005. Remove REST auth
description: ADR 0005 — this context removes the static-bearer-key REST auth layer adopted in ADR 0004, superseding it. Every REST route (including /process-paths*) is unauthenticated again.
---

# 0005. Removing the REST auth layer

## Status

Accepted — 2026-09-09. Supersedes
[ADR 0004 — Adopting the fleet REST identity standard](0004-rest-auth-adoption.md).

## Context

ADR 0004 adopted the fleet-wide static bearer key standard
(warehouse-ops-agent ADR 0005): every `/process-paths*` route required an
`Authorization: Bearer <key>` with a read or read-write scope, enforced by
`internal/adapters/inbound/auth` and wired through the composition root
and Helm chart.

That fleet-wide REST auth posture is being rolled back. This service is
the transactional-outbox REFERENCE implementation (ADR 0003) for the
fleet, and the outbox design is unaffected by this change — only the auth
layer is removed.

## Decision

Remove the REST auth layer entirely:

- Delete `internal/adapters/inbound/auth` (middleware, static key
  authenticator, mode parsing).
- `internal/adapters/inbound/http`'s router mounts every
  `/process-paths*` route unauthenticated, same as `/healthz` always was.
- The composition root (`cmd/pathmgmt/main.go`) no longer constructs an
  authenticator or reads `AUTH_MODE` / `API_READ_KEY` / `API_READWRITE_KEY`.
- The Helm chart (`charts/process-path-management`) no longer renders an
  `auth:` values block, the `<release>-auth` Secret, or `AUTH_MODE` in the
  ConfigMap/Deployment env.
- `apis/openapi.yaml` no longer declares `components.securitySchemes.bearerAuth`
  or any `security:` key. `apis/asyncapi.yaml` had no auth-related content
  to begin with.
- This service had no MCP adapter at the time of this record
  (`internal/adapters/inbound/mcp/` landed later, ADR 0006); that
  adapter was born unauthenticated under this decision, so there was no
  MCP-specific unauthenticate step to apply then and none is needed now.
- No outbound REST clients or peer API-key logic exist in
  `internal/adapters/outbound/` for this service (the one HTTP client
  this service holds today — the MCP report tool calling this context's
  OWN pathmgmt-reports service, ADR 0007 — is same-context and carries
  no auth either), so nothing there changes either.

## Consequences

Every `/process-paths*` route is reachable with no credential, matching
this service's pre-ADR-0004 posture. Operators, warehouse-console, and the
e2e harness no longer need to carry `API_READ_KEY` / `API_READWRITE_KEY`
for this service. ADR 0004 is left in place for historical record but is
superseded by this decision.
