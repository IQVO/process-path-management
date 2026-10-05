---
id: 0004-rest-auth-adoption
slug: /adr/0004-rest-auth-adoption
title: 0004. Adopting the fleet REST identity standard (static bearer keys, read/read-write scopes)
sidebar_label: 0004. REST auth adoption
description: "ADR 0004 — this context adopts the fleet-wide decision recorded in warehouse-ops-agent ADR 0005: every REST route except /healthz requires a static bearer key carrying a read or read-write scope, rolled out through AUTH_MODE=log before enforce."
---

# 0004. Adopting the fleet REST identity standard

> **Superseded by [ADR 0005 — Removing the REST auth layer](0005-remove-rest-auth.md)
> (2026-09-09).** The auth layer this ADR describes has been removed;
> every REST route is unauthenticated again. Left below for historical
> record.

## Status

Superseded by [ADR 0005](0005-remove-rest-auth.md) (2026-09-09).
Originally accepted 2026-09-07. Adopts the fleet-wide decision in
[warehouse-ops-agent ADR 0005 — Fleet REST identity: static bearer keys
with read/read-write scopes, no IdP](https://github.com/claudioed/warehouse-ops-agent/blob/develop/docs/docs/adr/0005-rest-identity-static-bearer-scopes.md).
Context, alternatives and consequences are recorded there and not repeated
here.

## What this context does

The fleet template lives, byte-for-byte apart from the import path, in
`internal/adapters/inbound/auth`. `NewRouter` mounts its `Middleware` on a
chi `Group` containing every `/process-paths*` route; `/healthz` stays
outside it so probes never need a credential. `GET`/`HEAD`/`OPTIONS`
require the `read` scope, `POST`/`PUT`/`DELETE` require `read-write`;
failures are RFC 7807 problem details under this service's existing
`https://errors.process-path-management.warehouse-systems.dev/` base
(401 `unauthenticated` with `WWW-Authenticate`, 403 `insufficient-scope`).
Keys come from `API_READ_KEY` / `API_READWRITE_KEY` (falling back to
`MCP_READ_KEY` / `MCP_READWRITE_KEY`); `AUTH_MODE=enforce|log|off`
defaults to `enforce` when any key is set and to `off` — with a WARN — when
none is, so local runs and the existing handler tests are unchanged. The
Helm chart exposes `auth.mode`, `auth.readKey`, `auth.readWriteKey` and
`auth.existingSecret`. At the time of this record this service had no
MCP adapter and no reports binary (both landed later — ADR 0006's MCP
server and ADR 0007's analytical data product — and inherited this
decision's unauthenticated posture until the fleet-wide revert below);
it is a pure resource server whose only callers are operators,
warehouse-console and the e2e harness.
