---
id: 0026-reject-deactivation-of-paths-in-cpt-schedules
slug: /adr/0026-reject-deactivation-of-paths-in-cpt-schedules
title: 0026. Refuse to deactivate a process path that a CPT schedule still lists
sidebar_label: 0026. Reject deactivation of scheduled paths
description: ADR 0026 — DELETE /process-paths/{pathId} answers 409 path-referenced-by-cpt-schedule while any site's CPT schedule lists the path in a cutoff's eligiblePathIds, keeping ADR 0010's Active-path invariant true after the write instead of only at write time.
---

# 0026. Refuse to deactivate a process path that a CPT schedule still lists

## Status

Accepted. Extends ADR 0010 (the CPT schedule's Active-path invariant);
supersedes nothing.

## Context

ADR 0010 requires every `eligiblePathIds` entry of every cutoff in a
site's `CPTSchedule` to name an **Active** `ProcessPath` in this
service's own store, "checked at write time". It says nothing about the
other direction: what happens to a schedule when a path it lists is
deactivated later. Before this ADR `DeactivatePath` touched only the
`ProcessPath` aggregate, so a deactivated path stayed listed in the
schedule, `CPTScheduleChanged` was not re-published, and consumers (for
example order-management's promise engine) kept reading a cutoff whose
eligible path the same service had just announced as retired. The next
`PUT` of that schedule would have been rejected with
`ineligible-path-id`, but nothing forced that `PUT` to happen.

ADR 0010, ADR 0001 and ADR 0003 are silent on this, so the choice below
is a new decision, not an implementation of an existing one. The options
weighed:

- **Reject the deactivation while a schedule lists the path (chosen).**
  The invariant stays true at all times. The operator sees exactly which
  sites to revise and does so with the existing `PUT`, which already
  publishes the authoritative `CPTScheduleChanged` snapshot.
- **Deactivate and prune the path from every schedule, publishing
  `CPTScheduleChanged` per affected site.** Never blocks the operator,
  but one `DELETE` would silently rewrite other aggregates, could leave a
  cutoff with an empty `eligiblePathIds` (which the aggregate forbids),
  and would have to invent what to do then (drop the cutoff? reject
  anyway?) — a product rule nobody has specified.
- **Leave it as is and document the gap.** Cheapest, but the published
  schedule contradicts the published catalogue until someone happens to
  re-submit it.

## Decision

`usecases.DeactivatePath` refuses to deactivate a path that any site's
CPT schedule still lists:

- A new port method `ports.CPTScheduleRepo.ListSiteIDsReferencingPath`
  returns the sites (ascending, each once) with at least one cutoff whose
  `eligiblePathIds` contains the path. Postgres:
  `SELECT DISTINCT schedule_site_id FROM cpt_schedule_cutoffs WHERE $1 =
  ANY (eligible_path_ids)`. No migration is needed.
- `DeactivatePath` runs the check inside its unit of work, before the
  aggregate is mutated and before anything is saved or published. If a
  schedule lists the path it returns `usecases.ErrPathReferencedByCPTSchedule`
  (wrapped with the site ids) and writes nothing: the path stays `ACTIVE`,
  no outbox row is enqueued.
- The HTTP adapter maps it to **409** with the RFC 7807 type
  `path-referenced-by-cpt-schedule` (ADR 0020). `DELETE
  /process-paths/{pathId}` therefore gains a documented 409 response in
  `apis/openapi.yaml`; the success path, the 404 and the idempotent
  "already deactivated" 204 are unchanged. This is additive for clients:
  they already have to handle the other documented 409 on this resource
  family.
- Deactivating a path no schedule lists, deactivating an unknown path
  (404) and re-deactivating an already deactivated path (204, no event)
  behave exactly as before.
- To retire a scheduled path the operator first `PUT`s the site's
  schedule without it (which publishes `CPTScheduleChanged`), then
  deactivates the path. Nothing is ever pruned automatically.

The service still never calls another bounded context for this; the check
reads only this service's own tables.

## Consequences

**Easier**

- The catalogue and the schedules this service publishes can no longer
  disagree about a path being retired, so consumers' Active-path
  assumption from ADR 0010 holds after the write too.
- The error names the sites to fix; no hunting through schedules.

**Harder**

- Retiring a path is now a two-step operation when it is scheduled. An
  operator (or the console) that issued a bare `DELETE` before gets a 409
  and must revise the schedule first. `process-path-mfe` shows the
  problem detail; no UI change is needed for the error to surface.
- The check and the deactivation share one READ COMMITTED transaction and
  neither locks the other aggregate's rows. A schedule written at the same
  instant as the deactivation of a path it lists can still commit, leaving
  one schedule that lists a deactivated path — exactly the pre-ADR state
  for that one schedule, and the next `PUT` of it is rejected by ADR
  0010's write-time check. Closing the window would need cross-aggregate
  row locking (`SELECT ... FOR SHARE` on the referenced paths in
  `DefineCPTSchedule`), which was judged out of proportion for a
  rarely-edited configuration catalogue. Recorded, not hidden.
- If the product later wants "deactivate and prune" instead, that is a new
  ADR that supersedes this one and must specify what happens to a cutoff
  left without eligible paths.
