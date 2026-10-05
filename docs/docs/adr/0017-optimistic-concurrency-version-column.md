---
id: 0017-optimistic-concurrency-version-column
slug: /adr/0017-optimistic-concurrency-version-column
title: 0017. Optimistic concurrency (version column) on ProcessPath and CPTSchedule
sidebar_label: 0017. Optimistic concurrency (version column)
description: ADR 0017 — a version column on both mutable aggregates closes the blind-overwrite lost-update race in ProcessPathRepo.Save and CPTScheduleRepo.Save; a mismatch surfaces as 409 concurrent-modification.
---

# 0017. Optimistic concurrency (version column) on ProcessPath and CPTSchedule

## Status

Accepted — implemented in the same change that introduced this record.
Mirrors the fleet's reference pattern (workforce-management ADR 0021, and
inventory-storage's repos before it), adapted to this service's two
mutable aggregates.

## Context

Every write use case in this service is read-modify-write:
`FindByID`/`FindBySiteID` → mutate the in-memory aggregate → `Save`.
Both `ProcessPathRepo.Save` and `CPTScheduleRepo.Save` were blind
full-column upserts (`INSERT ... ON CONFLICT DO UPDATE` with no guard).
Two callers that load the same row, each apply their own mutation, and
Save one after the other: the second Save silently overwrites every
column the first Save touched — the operator revising a path's
capabilities at the same moment another operator revises its cycle time
is a realistic double-writer, and today neither is told anything went
wrong. DeactivatePath racing RevisePath on the same path is the same
hazard. CPTSchedule is equally mutable (every `PUT
/sites/{siteId}/cpt-schedule` is a full read-modify-write of the
site's row).

The immutable-after-creation facts (pathId, direct,
destinationLocationRole) are not the hazard — the revisable columns
(matchPrefix, requiredCapabilities, cycleTimeP95, eligibility, status,
and the schedule's timezone/cutoffs) are.

## Decision

Add a `version INTEGER NOT NULL DEFAULT 1` column to `process_paths`
and `cpt_schedules` (migration `0007_version`), and make both repos'
`Save` a version-guarded upsert:

```sql
INSERT INTO process_paths (..., version) VALUES (..., 1)
ON CONFLICT (id) DO UPDATE
  SET ..., version = process_paths.version + 1
WHERE process_paths.version = $loaded_version
```

`RowsAffected() == 0` means the row exists but its version no longer
matches what the caller loaded → return the new
`ports.ErrConcurrentModification` sentinel
(`internal/application/ports/errors.go`). A fresh INSERT always
affects exactly one row and starts at version 1.

- **Domain**: each aggregate gains an unexported `version int` and a
  `Version() int` accessor — inert infrastructure metadata, exactly
  like the aggregate's own id; no domain logic branches on it.
  `Define` starts at 1; `Rehydrate` takes the row's version as its
  last parameter.
- **HTTP**: `ports.ErrConcurrentModification` maps to `409 Conflict`
  with its own RFC 7807 category `concurrent-modification`, distinct
  from the natural-key `path-already-exists` 409, so a client can
  tell "re-fetch and retry" apart from a domain-rule rejection. It is
  a possible response of `PUT /process-paths/{pathId}` and `PUT
  /sites/{siteId}/cpt-schedule` (the two mutating PUTs); POST and
  DELETE cannot hit it on a single-writer flow.
- **Use cases are unchanged**: the version flows through
  transparently (Rehydrate populates it, Save reads it back off the
  aggregate).

Callers do not get an automatic retry: the correct client reaction to
`409 concurrent-modification` is to re-fetch and re-apply the change
against the current state — distinct from the Idempotency-Key
middleware's concern (ADR 0011), which deduplicates retries of the
SAME request, not lost updates between DIFFERENT requests.

## Consequences

**Positive**

- The lost-update race on both mutable aggregates is closed, and
  proven closed by real two-goroutine concurrent tests against
  testcontainers Postgres (`version_integration_test.go`):
  exactly one winner, exactly one version increment, exactly one of
  the two mutations visible on reload.
- Single-writer flows are unaffected: version 1 rows Save cleanly,
  and every version the guard compares came from the row itself.

**Negative / accepted**

- A concurrent edit now surfaces as a `409` the MFE/operator must
  handle by reloading; this repo implements no auto-merge.
- One more column, one migration, one more error category to
  document.
- The version is per-row monotonic, not a global sequence: it
  guarantees detection of a stale write, not ordering between
  different aggregates.

## Alternatives considered

- **`SELECT ... FOR UPDATE` row locks**: would require holding a
  transaction across the read AND the caller's in-memory mutation,
  which does not fit this codebase's load-then-later-Save use-case
  shape.
- **Last-writer-wins with `updated_at` compare**: same idea with a
  strictly weaker token (clock skew, truncation); a dedicated integer
  is exact.
