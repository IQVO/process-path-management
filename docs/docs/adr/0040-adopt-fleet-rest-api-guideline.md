---
id: 0040-adopt-fleet-rest-api-guideline
slug: /adr/0040-adopt-fleet-rest-api-guideline
title: 0040. Adopt the fleet REST API guideline
sidebar_label: 0040. Fleet REST API guideline
description: "ADR 0040: process-path-management adopts the fleet REST API guideline. GET /process-paths becomes a keyset-paged collection ({items,nextCursor}, limit and cursor), Idempotency-Key reuse is 409 instead of 422, replays carry Idempotency-Replayed, malformed keys are 400 idempotency-key-invalid. Amends ADR 0011."
---

# 0040. Adopt the fleet REST API guideline

## Status

Accepted (2026-11-01). Amends [ADR 0011](./0011-idempotency-key-middleware.md)
(key reuse is `409`, not `422`). Part of the fleet-wide REST cutover: it is
deployed together with the other services' cutover PRs, because the key
contract and the list envelope are breaking for clients.

## Context

The 2026-10-30 portfolio assessment found two spellings of the replay header,
key reuse answered `422` in most services and `409` in one, verb-style command
URLs and unpaginated lists. The fleet standard removes each divergence and a
Spectral ruleset (`.spectral.yaml` + `.spectral/functions/restGuideline.js`,
rules R1-R8) enforces it in the `api-lint` job with `--fail-severity=warn`.

This service had no verb routes and already keyed its only `POST`
(`/process-paths`), so two things changed: the idempotency contract details, and
`GET /process-paths`, which returned a bare array.

## Decision

1. **Routes.** None renamed. `PUT /process-paths/{pathId}`,
   `DELETE /process-paths/{pathId}` and `PUT /sites/{siteId}/cpt-schedule` are
   idempotent by method and take no middleware.
2. **Key contract.** The key is global (looked up by key alone, compared by
   method + path + SHA-256 of the body). Same key and same request replays the
   stored answer with `Idempotency-Replayed: true`. Same key and a different
   request is `409` `idempotency-key-reused`. A missing key is `400
   idempotency-key-required`; a key that is not 1-128 printable ASCII
   characters is `400 idempotency-key-invalid`. `Idempotent-Replayed` is not
   used.
3. **Pagination.** `GET /process-paths` returns `{items,nextCursor}` and takes
   `limit` (1-500, default 100) and an opaque `cursor`, plus the existing `all`
   filter. Paging is keyset on `pathId`, in the Postgres and in-memory
   repositories, with a stable order. Any other query parameter is `400
   unknown-query-parameter`, so a caller still sending `after` or `offset`
   fails loudly instead of silently getting page one.
4. **Exemption.** `GET /sites/{siteId}/cpt-schedule` is a composite singleton
   (one schedule per site), declared `x-pagination-exempt: view`.
5. **Other adapters.** The A2A skill and the MCP tool keep the unpaged
   `ListPaths` read use case. They are not REST, and their output contract is
   unchanged.

## Consequences

- Breaking for REST clients: reuse is `409`, and `GET /process-paths` is an
  envelope, not an array. Callers that parse the array (the ops agent and
  work planning are the known candidates) change in their own PRs in the same
  set.
- Existing stored keys keep working: the lookup is by key alone, so a retry of
  the same request replays; only a retry whose body changed gets `409`.
- The cursor is opaque. Clients must pass back `nextCursor` untouched and stop
  when it is absent (the last page).
