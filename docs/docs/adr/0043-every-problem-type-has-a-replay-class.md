---
id: 0043-every-problem-type-has-a-replay-class
slug: /adr/0043-every-problem-type-has-a-replay-class
title: "0043. Every problem type has a replay class; a retryable refusal is not stored"
sidebar_label: "0043. Replay classes"
description: "ADR 0043: every RFC 7807 problem type this service can answer declares exactly one idempotency replay class (FINAL, OUTCOME, RETRYABLE) on its catalogue entry. A retried request is answered from the idempotency store only when the first answer is final for that request; a RETRYABLE refusal (concurrent-modification, not-found of a caller-defined id, a referenced path not known yet, a 5xx) rolls the whole request transaction back, key row included, so the same key executes again."
---

# 0043. Every problem type has a replay class; a retryable refusal is not stored

## Status

Accepted (2026-11-20). Amends [ADR 0011](./0011-idempotency-key-middleware.md) (the
transactional Idempotency-Key middleware) and the guideline contract of
[ADR 0040](./0040-adopt-fleet-rest-api-guideline.md). Implements the fleet decision of
2026-11-18 (`warehouse-harness-template` `.claude/rules/fleet/idempotency-and-outbox.md`,
standard: `docs/idempotency-replay-classes.md`). Wire slugs, titles and statuses are
unchanged; only which refusals are stored changes.

## Context

ADR 0011 stores and replays every answer below 500: a 4xx refusal is replayed for the
24-hour window of the key. That is wrong for refusals whose cause is state that can
change: a `409 concurrent-modification` (the optimistic-lock loser, ADR 0017), a
`404 path-not-found` for an id the CALLER chose (another request can define that path a
moment later), a `422 ineligible-path-id` (the referenced path is not defined yet).
A client that did the right thing — fixed the state and retried the same key — was
answered with the stale refusal pinned by the key row for the window.

The fleet decision (owner, 2026-11-18): a retried request is answered from the store
only when the first answer is FINAL for that request. Each problem type gets exactly one
class, declared on its catalogue entry:

- **FINAL** — the answer is a function of the request alone. Stored and replayed.
- **OUTCOME** — the command COMMITTED state and then answered an error that must be
  remembered. Stored, and its writes commit with the stored answer.
- **RETRYABLE** — nothing committed and the answer depends on state that can change.
  Not stored: the request transaction rolls back, key row included, and a retry
  executes again.

2xx is stored; 5xx and panics are never stored; a replay carries
`Idempotency-Replayed: true`. The middleware reads the class and holds no list of slugs
and no status ranges beyond 2xx and 5xx. A problem type without a class fails a unit
test (`TestEveryProblemTypeDeclaresAReplayClass`); at runtime a refusal that declares no
class is released like RETRYABLE — the safe fallback: a released refusal costs one more
execution, a wrongly stored one pins a stale refusal for the window.

Releasing a RETRYABLE key means only that the same key may be tried again by whoever
sent it: a different body under the same key afterwards is a NEW request, not
`idempotency-key-reused`. No consumer-side retry behaviour is added by this ADR.

## The store design this rides on (ADR 0011, unchanged)

Design A, the savepoint design: the middleware opens the request's own transaction,
inserts the `idempotency_keys` row with `ON CONFLICT DO NOTHING`, runs the handler
behind `SAVEPOINT handler_run` with the transaction bound into the request context
(`internal/pgtx`; `postgres.UnitOfWork` joins it), then updates the row with the
captured answer and commits. Releasing a RETRYABLE key is exactly the rollback the 5xx
branch already did: `tx.Rollback` discards the key row AND the handler's writes. An
OUTCOME refusal needs no new mechanism either — a 4xx whose savepoint was released
already commits its writes with the key row — but the failed-release path (a statement
died inside the handler) now refuses to store an OUTCOME whose writes are gone.

What a concurrent duplicate sees when the first request ends RETRYABLE: the second
request's `INSERT ... ON CONFLICT` blocks on the first transaction's key row until it
resolves; the rollback removes the row, so the blocked insert SUCCEEDS and the duplicate
executes the work fresh (no replay header). It never observes the released refusal.

## Decision

Every problem type of this service is classified below (the table is held equal to the
catalogue by `TestTheADRClassTableMatchesTheCatalogue`). No problem type of this service
is OUTCOME: no use case of this service commits state and then refuses — the refusals
are pre-checks (validation, natural-key existence, reference checks) that write nothing,
and the two writers that could look like one (the NUL-path and route 404s) answer before
the store is touched. The middleware nevertheless honours the class, so a future OUTCOME
entry is safe (proved by probe in `TestOutcomeBranchCommitsTheWritesWithTheStoredAnswer`).

### The class table

| Slug | Status | Class | Reason |
|---|---|---|---|
| `path-not-found` | 404 | RETRYABLE | the PathId is CALLER-supplied, not minted here; another request can define it later (the 404 rule: FINAL only if this service mints the ids and nothing is ever deleted) |
| `cpt-schedule-not-found` | 404 | RETRYABLE | the schedule is created by another request (`PUT /sites/{siteId}/cpt-schedule`); a retry after it exists can succeed |
| `flow-not-found` | 404 | RETRYABLE | the FlowId is caller-supplied and flows are delivered by other requests; nothing here mints flow ids |
| `path-already-exists` | 409 | FINAL | a taken PathId is taken forever: deactivation keeps the row (the slug's own text: "active or deactivated"), there is no reactivate use case and no delete — a retry can never succeed, so the stored 409 is stable |
| `path-referenced-by-cpt-schedule` | 409 | RETRYABLE | the referencing schedule can be revised first (another request), then the deactivation succeeds |
| `path-in-active-flow` | 409 | RETRYABLE | the active flow can be revised or retired first, then the deactivation succeeds |
| `concurrent-modification` | 409 | RETRYABLE | the optimistic-lock loser (ADR 0017): nothing committed, the answer depends on the winner's write |
| `path-deactivated` | 422 | FINAL | deactivation is terminal (no reactivate use case): the answer can never change on retry |
| `flow-retired` | 422 | FINAL | retirement is terminal (PUT on a retired flow refuses): the answer can never change on retry |
| `ineligible-path-id` | 422 | RETRYABLE | the referenced path must be Active in this service's own store and may not be defined yet (a referenced fact not known yet) |
| `ineligible-stage-path` | 422 | RETRYABLE | same cause as ineligible-path-id: a stage's pathId may reference a path not defined yet |
| `empty-path-id` | 422 | FINAL | the request's own field |
| `empty-match-prefix` | 422 | FINAL | the request's own field |
| `match-prefix-not-lowercase` | 422 | FINAL | the request's own field |
| `no-required-capabilities` | 422 | FINAL | the request's own field |
| `invalid-destination-location-role` | 422 | FINAL | the request's own field |
| `invalid-cycle-time-p95` | 422 | FINAL | the request's own field |
| `invalid-intake` | 422 | FINAL | the request's own field |
| `empty-timezone` | 422 | FINAL | the request's own field |
| `invalid-timezone` | 422 | FINAL | the request's own field |
| `no-cutoffs` | 422 | FINAL | the request's own field |
| `empty-cpt-id` | 422 | FINAL | the request's own field |
| `duplicate-cpt-id` | 422 | FINAL | validated from the submitted schedule alone (cptschedule.go: cptId unique within the request's own cutoffs) |
| `empty-local-time` | 422 | FINAL | the request's own field |
| `invalid-local-time` | 422 | FINAL | the request's own field |
| `no-days-of-week` | 422 | FINAL | the request's own field |
| `invalid-day-of-week` | 422 | FINAL | the request's own field |
| `empty-ship-method` | 422 | FINAL | the request's own field |
| `no-eligible-path-ids` | 422 | FINAL | the request's own field |
| `empty-flow-id` | 422 | FINAL | the request's own field |
| `empty-flow-name` | 422 | FINAL | the request's own field |
| `no-stages` | 422 | FINAL | the request's own field |
| `empty-stage-path-id` | 422 | FINAL | the request's own field |
| `invalid-process-type` | 422 | FINAL | the request's own field |
| `duplicate-stage-path` | 422 | FINAL | a pathId may appear at most once in the submitted flow |
| `idempotency-key-required` | 400 | FINAL | answered before the store is touched; the request alone |
| `idempotency-key-invalid` | 400 | FINAL | answered before the store is touched; the request alone |
| `idempotency-key-reused` | 409 | FINAL | the store's own answer for a different request; nothing was written for this one |
| `malformed-request-body` | 400 | FINAL | the request's own bytes alone decide |
| `route-not-found` | 404 | FINAL | answered before the middleware; the request path alone |
| `invalid-limit` | 400 | FINAL | the query alone (a GET: never stored) |
| `invalid-cursor` | 400 | FINAL | the query alone (a GET: never stored) |
| `unknown-query-parameter` | 400 | FINAL | the query alone (a GET: never stored) |
| `invalid-query-parameter` | 400 | FINAL | the query alone (a GET: never stored) |
| `invalid-report-query` | 400 | FINAL | the reports query alone (a read-only binary, never behind the middleware) |
| `dependency-unavailable` | 503 | RETRYABLE | a 5xx is never stored |
| `report-store-error` | 500 | RETRYABLE | a 5xx is never stored |
| `internal-error` | 500 | RETRYABLE | a 5xx is never stored |

### Judgement calls (most consequential first)

1. **`path-already-exists` is FINAL, not RETRYABLE.** Strictly the answer depends on
   another request's write, but the fleet's unsure-rule exists to avoid PINNING an
   answer a retry could pass. Here no retry can ever pass: a deactivated path still
   blocks re-defining the id (there is no reactivate use case and no delete in the
   store), so the 409 is stable forever. A concurrent duplicate that loses the
   natural-key race gets this same FINAL 409, stored and replayed — the race loser of
   THIS service is not the fleet's `concurrent-modification` loser.
2. **`path-not-found`, `cpt-schedule-not-found`, `flow-not-found` are RETRYABLE.** This
   service does not mint these ids (PathId, SiteId, FlowId are caller-supplied) and the
   facts are delivered by other requests, so a retry can pass. When unsure, RETRYABLE.
3. **No OUTCOME entries.** No use case commits and then refuses; the savepoint design
   would support it, and the middleware branch is probe-tested, but nothing declares it
   today.
4. **The middleware's own answers (`idempotency-key-required/invalid/reused`) are
   classified FINAL** even though they are answered outside the store: the catalogue is
   the one place every problem type of the package is classified, and the class
   documents the intent (the request alone / the store's own answer).

## Consequences

- A retry under a key whose first answer was RETRYABLE executes the work again; the
  same key with a DIFFERENT body is a new request, not `idempotency-key-reused`.
- A stored FINAL refusal is unchanged: byte-identical replay for the window, same
  slugs, titles and statuses on the wire.
- `apis/openapi.yaml`'s Idempotency-Key descriptions now say which refusals are
  replayed and which release the key (descriptions only; oasdiff breaking gate green).
- The reference test of `warehouse-harness-template`
  (`templates/idempotency/replay_classes_test.go.tmpl`, rendered as
  `idempotency_replay_classes_integration_test.go` with the wiring in
  `idempotency_replay_classes_wiring_integration_test.go`) proves every entry's class
  against the real middleware over a real Postgres, per class.
- Retention is unchanged and enforced: the sweeper (ADR 0018, 24h TTL) purges expired
  `idempotency_keys` rows.
