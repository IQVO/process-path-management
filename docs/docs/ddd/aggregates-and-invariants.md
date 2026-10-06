---
title: Aggregates and Invariants
sidebar_label: Aggregates and Invariants
description: The ProcessPath and CPTSchedule aggregates and their invariants — lower-case matchPrefix, non-empty requiredCapabilities, positive cycleTimeP95, terminal/idempotent deactivation, and per-site CPT cutoffs that reference Active paths.
---

# Aggregates and Invariants

## Verdict: Generic Subdomain

Process Path Management is a **Generic Subdomain**, the same classification
as `facility-layout`. A process-path catalogue is a well-understood,
industry-common concern — it is not a competitive differentiator the way
`fulfillment-execution`'s Pick/Pack/SLAM task lifecycle or
`inventory-storage`'s chaotic-storage inventory truth are. What makes it
worth extracting into its own bounded context is exactly the same argument
`facility-layout` was built on: **no single existing context owns it, and
several need it identically** — `fulfillment-execution`,
`wes-work-planning`, and `workforce-management` all reference the same
`PathId` identity and the same capability requirements, but none of them
should be the single source of truth for what a path IS.

| Context | Classification | Reasoning |
| --- | --- | --- |
| `fulfillment-execution` | Core | The Pick/Pack/SLAM task lifecycle; throughput and accuracy at scale. |
| `wes-work-planning` | Core | The conductor — waveless release and flow balance. |
| `workforce-management` | Supporting | Labor & workforce allocation. |
| `order-management` | Generic/Supporting | Order intake, allocation, and the customer promise. |
| `labor-performance` | Supporting | Actual-vs-standard performance scoring. |
| `facility-layout` | Generic | Physical warehouse structure, extracted once rather than duplicated. |
| **`process-path-management`** | **Generic** | **The process-path catalogue — extracted once rather than duplicated across its consumers.** |

## The ProcessPath aggregate

The first of this domain's two aggregate roots. Fields: `PathId`
(immutable identity), `MatchPrefix` (revisable), `Direct` (immutable),
`RequiredCapabilities` (revisable), `DestinationLocationRole` (optional,
immutable — [ADR 0009](/docs/adr/0009-destination-location-role-on-process-path)),
`CycleTimeP95` and `Eligibility` (revisable —
[ADR 0010](/docs/adr/0010-fulfillment-capability-contract)), `Status`
(Active/Deactivated), `CreatedAt`/`UpdatedAt`.

### Invariant 1 — non-empty, lower-case matchPrefix

`MatchPrefix` must be non-empty and lower-case. Enforced identically at
`Define` and at `Revise` time (`validate` is a single shared function, not
duplicated logic that could drift). Rejected with a typed sentinel
(`ErrEmptyMatchPrefix` / `ErrMatchPrefixNotLowercase`) rather than silently
coerced — persisted data is exactly what was validated, never a
silently-lower-cased value the caller didn't actually send.

```go
if matchPrefix == "" {
    return ErrEmptyMatchPrefix
}
if matchPrefix != strings.ToLower(matchPrefix) {
    return ErrMatchPrefixNotLowercase
}
```

### Invariant 2 — non-empty requiredCapabilities

`RequiredCapabilities` must contain at least one capability. A process path
with zero required capabilities is not a meaningful business fact — every
real path (PICK, PACK, SLAM, REBIN) requires at least the capability named
after itself. Enforced by the same shared `validate` function as Invariant
1, at both `Define` and `Revise` time.

### Invariant 3 — positive cycleTimeP95, recognised destination role

The same shared `validate` function also rejects a `CycleTimeP95` that is
not strictly positive (`ErrInvalidCycleTime`) and a non-empty
`DestinationLocationRole` outside `Drop`/`WorkCenter`/`Shipping`
(`shared.ErrInvalidDestinationLocationRole`). `Eligibility` has no
invariant of its own: every combination, including the empty value, is a
valid declaration.

### Invariant 4 — deactivation is terminal and idempotent

Once a `ProcessPath` transitions to `Deactivated`, it is a closed historical
record:

- **Terminal.** `Revise` on a Deactivated path returns `ErrPathDeactivated`
  — there is no path back to Active. A deactivated path's identity is
  permanent (see `ErrPathAlreadyExists`'s own doc comment: re-using a
  deactivated id is refused, never silently reopened).
- **Idempotent.** Calling `Deactivate` on an already-deactivated path is a
  no-op success, not an error — matching this fleet's established
  "duplicate/redelivered command is a no-op, not an error" convention (see
  `fulfillment-execution`'s `WorkPool.Complete`). This means a retried
  operator action or a redelivered command never surfaces a spurious error,
  and — enforced one layer up, in the `DeactivatePath` use case — never
  republishes `ProcessPathDeactivated` a second time either.

```go
func (p *ProcessPath) Deactivate(now time.Time) {
    if p.status == StatusDeactivated {
        return
    }
    p.status = StatusDeactivated
    p.updatedAt = now
}
```

## Why deactivation carries no position on in-flight work

This service takes no position on work already assigned to a path in a
downstream context at the moment it is deactivated — that is each
consumer's own operational concern, the same "read model, not a command"
posture `workforce-management`'s ADR-0002 established for
`PathUnderstaffed`. `ProcessPathDeactivated` tells consumers to stop
accepting NEW work against this path; it is not a command to cancel
anything already in flight.

## Revision is a real no-op, not a spurious event

`Revise` returns a `changed` boolean. When the caller's request is
identical to the path's current `MatchPrefix`, `RequiredCapabilities`,
`CycleTimeP95` and `Eligibility`, `changed` is `false` and `RevisePath`'s
use case does not republish `ProcessPathUpdated` — consumers never have to
diff two identical payloads to notice nothing changed.

## The CPTSchedule aggregate

The second aggregate root, one per site (identity `SiteId`), introduced by
[ADR 0010](/docs/adr/0010-fulfillment-capability-contract). Fields:
`Timezone` (IANA zone), `Cutoffs`, `CreatedAt`/`UpdatedAt`. Each `Cutoff`
entity carries `cptId`, `localTime`, `daysOfWeek`, `shipMethod` and
`eligiblePathIds`.

Invariants enforced in the domain (`internal/domain/cptschedule`):

- `timezone` is non-empty and a recognised IANA zone
  (`ErrEmptyTimezone`, `ErrInvalidTimezone`).
- At least one cutoff (`ErrNoCutoffs`), and `cptId` is unique within the
  schedule (`ErrDuplicateCptId`).
- Each cutoff has a non-empty `cptId`, an `HH:MM` 24-hour `localTime`,
  non-empty `daysOfWeek` drawn from `Mon`..`Sun`, a non-empty
  `shipMethod`, and non-empty `eligiblePathIds`.

One cross-aggregate invariant lives in the `DefineCPTSchedule` use case,
not the domain, because it needs the `ProcessPathRepo`: every
`eligiblePathIds` entry must reference an **Active** process path in this
service's own store (`ErrIneligiblePathId`, 422).

The same rule is kept true from the other side by the `DeactivatePath` use
case ([ADR 0026](/docs/adr/0026-reject-deactivation-of-paths-in-cpt-schedules)):
a path that any site's schedule still lists in a cutoff's `eligiblePathIds`
cannot be deactivated. The command is refused with
`ErrPathReferencedByCPTSchedule` (409 `path-referenced-by-cpt-schedule`,
naming the sites) and the operator revises those schedules first.
Nothing is pruned automatically and no `CPTScheduleChanged` is raised by a
deactivation.

**Decided 2026-10-06 (ADR 0028):** the invariant holds under concurrency
too. `DefineCPTSchedule` locks every path it lists `FOR SHARE` (sorted by
id, one statement) inside its unit of work before writing, and
`DeactivatePath` reads the path row `FOR UPDATE` before looking for
referencing schedules. The two serialise per path: a deactivation either
sees the committed schedule (409) or commits first and the define is
refused (422 `ineligible-path-id`). The lock lives behind
`ports.ProcessPathRepo`, never in the domain.

A schedule is revised **wholesale** (`PUT /sites/{siteId}/cpt-schedule`
replaces timezone and cutoffs together). `Revise` returns `changed`; an
identical re-submission raises nothing, and a real change publishes a
`CPTScheduleChanged` carrying the full schedule snapshot.
