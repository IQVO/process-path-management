---
id: 0028-close-path-cpt-schedule-race-with-row-locks
slug: /adr/0028-close-path-cpt-schedule-race-with-row-locks
title: 0028. Close the path / CPT-schedule race with row locks
sidebar_label: 0028. Row locks for the path / CPT-schedule race
description: ADR 0028 — DefineCPTSchedule locks the referenced process-path rows FOR SHARE and DeactivatePath locks the path row FOR UPDATE, both inside their unit of work, so a schedule can never commit naming a path that is deactivated; closes the residual race recorded in ADR 0026.
---

# 0028. Close the path / CPT-schedule race with row locks

## Status

Accepted. Extends ADR 0026 (it keeps its 409 behaviour unchanged and removes
the residual race that ADR recorded); supersedes nothing.

## Context

ADR 0010 requires every `eligiblePathIds` entry of every cutoff in a CPT
schedule to name an **Active** `ProcessPath`. ADR 0026 keeps that true after
the write by making `DeactivatePath` refuse (409
`path-referenced-by-cpt-schedule`) while a schedule lists the path. It also
recorded a residual race it did not close: the Active-path check in
`DefineCPTSchedule` and the reference check in `DeactivatePath` each run in
their own READ COMMITTED transaction and neither locks the other aggregate's
rows. Interleaved, both pass:

1. `DefineCPTSchedule` reads path P as ACTIVE.
2. `DeactivatePath(P)` finds no schedule listing P (the schedule is not yet
   committed) and commits the deactivation.
3. `DefineCPTSchedule` commits a schedule that lists the now-deactivated P.

The result is a schedule naming an inactive path. ADR 0026 judged closing it
"out of proportion". The architect has since decided the opposite (2026-10-06
audit decisions): both aggregates live in the same Postgres, so enforcing a
cross-aggregate invariant correctly is cheap, and "almost always true" is not
an invariant.

## Decision

Keep ADR 0026's reject-with-409 and close the race inside one Postgres
transaction on each side, with row locks on the `process_paths` rows. No
migration, no new column, no new event, no REST/MCP/Kafka contract change.

- **Port.** The locking lives behind `ports.ProcessPathRepo`, never in the
  domain: `LockByIDsForShare(ctx, ids)` returns the locked paths (unknown ids
  are absent) and `FindByIDForUpdate(ctx, id)` returns one locked path. The
  Postgres adapter implements them with `SELECT ... FOR SHARE` and `SELECT ...
  FOR UPDATE`; the in-memory adapter (no transactions) just reads.
- **`DefineCPTSchedule`** now runs entirely inside its unit of work. First it
  collects the distinct `eligiblePathIds` across all cutoffs, **sorts them by
  id** and takes `FOR SHARE` on all of them in one `ORDER BY id` statement,
  then requires each to be present and Active (`ErrIneligiblePathId`, 422,
  unchanged), and only then loads, validates and saves the schedule and
  publishes `CPTScheduleChanged`. The share locks are held until commit.
- **`DeactivatePath`** keeps a cheap unlocked pre-check (unknown path: 404;
  already deactivated: 204 no-op, no unit of work, no event; deactivation is
  terminal, so that answer cannot go stale). For an Active path it opens its
  unit of work, re-reads the row `FOR UPDATE`, re-checks it is still Active,
  and only then looks for referencing schedules
  (`ListSiteIDsReferencingPath`), mutates, saves and publishes. 409
  `path-referenced-by-cpt-schedule` is unchanged.
- **Why it is race-free.** `FOR SHARE` and `FOR UPDATE` conflict, so for one
  path the two commands serialise. If the define holds the share lock first,
  the deactivation waits for its commit, then (READ COMMITTED, a new statement)
  sees the committed schedule and refuses with 409. If the deactivation holds
  the update lock first, the define waits for its commit and re-reads the row
  as deactivated and refuses with 422 `ineligible-path-id`. Either way no
  committed schedule names an inactive path.
- **Deadlocks.** Several defines hold `FOR SHARE` on overlapping paths at once
  without conflict. A define takes all its locks in ascending id order
  (the use case sorts, and the adapter's `ORDER BY id` takes the locks in
  that order); a deactivation takes exactly one lock. No lock cycle can form.
  Two defines of the same site still resolve through the version guard (ADR
  0017), as before.
- **Side effect, intended.** Concurrent deactivations of one path now
  serialise on the row lock: the loser sees it already deactivated and returns
  the idempotent 204, instead of `409 concurrent-modification`.

## Consequences

**Easier**

- ADR 0010's Active-path invariant now holds at all times across both
  aggregates, not merely at each write's check time.
- Proven by a testcontainers Postgres race test (many goroutines defining
  schedules that list path P while others deactivate P, 50 iterations,
  invariant "no cutoff names a non-ACTIVE path" checked after each), by
  lock-semantics integration tests (a share lock blocks a deactivation's
  `FOR UPDATE`, and a held `FOR UPDATE` blocks a define, which then sees the
  path as deactivated), and by use-case unit tests.

**Harder**

- A schedule define for a path being deactivated now waits for the other
  transaction instead of racing past it. Both transactions are short and the
  catalogue is operator-edited, so the wait is bounded by the statement
  timeout (ADR 0014); the loser receives a normal 409 or 422.
- `DefineCPTSchedule` reads the schedule inside the transaction, so a failed
  define holds its path share locks until rollback. Same transaction length as
  before for the write path; slightly longer for a rejected request.
- Any future writer that makes a schedule name a path, or that deactivates a
  path, must take the same locks, in the same order. The two port methods are
  the single place that does.
