---
title: Ubiquitous Language
sidebar_label: Ubiquitous Language
sidebar_position: 2
description: ProcessPath, PathId, Capability, MatchPrefix, Direct, DestinationLocationRole, CycleTimeP95, Eligibility, Status, CPTSchedule, Cutoff — each term mapped to the code identifier that implements it.
---

# Ubiquitous Language

The definitions below are pulled directly from
`internal/domain/processpath/process_path.go`,
`internal/domain/cptschedule/cpt_schedule.go`,
`internal/domain/cptschedule/events.go`,
`internal/domain/shared/shared.go`,
`internal/domain/shared/eligibility.go` and
`internal/domain/shared/events.go` — not reinvented for this page. Part of
the [DDD artifact pack](./ddd-artifacts.md).

## Glossary → code

| Term | Code identifier | Kind | Wire name (REST / Kafka) | Note |
| --- | --- | --- | --- | --- |
| Process Path | `processpath.ProcessPath` | Aggregate root | `processPathResponse` DTO / `ProcessPathData` | |
| Path Id | `shared.PathId` | Identity (string type) | `pathId` / `path_id` | Same identity as fulfillment-execution `task.Type`, wes-work-planning `WorkPool.PathId`, workforce-management `PathPlan.PathId`. |
| Match Prefix | `ProcessPath.matchPrefix` (`MatchPrefix()`) | Attribute | `matchPrefix` / `match_prefix` | |
| Direct | `ProcessPath.direct` (`Direct()`) | Attribute | `direct` / `direct` | Omitted from the Kafka payload when `false` (`omitempty`). |
| Capability | `shared.Capability` | Value (string type) | `requiredCapabilities[]` / `required_capabilities[]` | The domain term is **required capabilities**; the type is the single `Capability`. |
| Destination Location Role | `shared.DestinationLocationRole` | Value (closed string enum) | `destinationLocationRole` / `destination_location_role` | Constants `DestinationLocationRoleUnset`, `...Drop`, `...WorkCenter`, `...Shipping`. |
| Cycle Time p95 | `ProcessPath.cycleTimeP95` (`time.Duration`) | Attribute | `cycleTimeP95` / `cycle_time_p95` | Go duration string on both wires (`2h0m0s`). |
| Eligibility | `shared.Eligibility` | Value object | `eligibility` / `eligibility` | Fields `maxUnitsPerLine`, `requiredProductAttributes`, `excludedProductAttributes`, `nonSortable`. |
| Status | `processpath.Status` | Enumeration | `status` | `StatusActive = "ACTIVE"`, `StatusDeactivated = "DEACTIVATED"`. Not carried on Kafka — the event type conveys it. |
| Define / Revise / Deactivate | `processpath.Define`, `(*ProcessPath).Revise`, `(*ProcessPath).Deactivate` | Factory / commands | `POST`, `PUT`, `DELETE /process-paths…` | Use cases `usecases.DefinePath`, `RevisePath`, `DeactivatePath`. |
| CPT Schedule | `cptschedule.CPTSchedule` | Aggregate root | `cptScheduleResponse` DTO / `CPTScheduleData` | One per site. |
| Site Id | `shared.SiteId` | Identity (string type) | `siteId` / `site_id` | facility-layout vocabulary, never validated against it. |
| Cutoff | `cptschedule.Cutoff` | Entity (local identity `cptId`) | `cutoffs[]` | Built by `cptschedule.NewCutoff`. |
| CPT Id | `Cutoff.cptId` | Local identity | `cptId` / `cpt_id` | Unique within a schedule. |
| Local Time | `Cutoff.localTime` | Attribute | `localTime` / `local_time` | Strict `HH:MM` 24-hour. |
| Day of Week | `cptschedule.Weekday` | Enumeration | `daysOfWeek[]` / `days_of_week[]` | `Mon`..`Sun`. |
| Ship Method | `Cutoff.shipMethod` | Attribute | `shipMethod` / `ship_method` | Free-form label. |
| Eligible Path Ids | `Cutoff.eligiblePathIds` | Reference by id | `eligiblePathIds` / `eligible_path_ids` | Must name Active paths (use-case check). |
| Cutoff Snapshot | `cptschedule.CutoffSnapshot` | Event payload part | `CutoffData` | Code-only name; the domain says "cutoff". |
| Process Path Created / Updated / Deactivated | `shared.ProcessPathCreated`, `shared.ProcessPathUpdated`, `shared.ProcessPathDeactivated` | Domain events | `com.warehouse.wes.process-path-management.processpath.<Name>` | |
| CPT Schedule Changed | `cptschedule.CPTScheduleChanged` | Domain event | `com.warehouse.wes.process-path-management.cptschedule.CPTScheduleChanged` | Full snapshot, raised on define AND revise. |
| Catalogue Growth report | `report.CatalogueReport`, `report.Row` | Read model | `GET /reports/catalogue-growth` | Analytics side, not part of the OLTP model. |

**Terms whose code name differs:** "required capabilities" →
`shared.Capability` slice; "deactivate" is a soft delete exposed as HTTP
`DELETE`; "cutoff" in events is `CutoffSnapshot` / `CutoffData`;
"revise a CPT schedule" has no separate use case — `DefineCPTSchedule`
both defines and revises.

## ProcessPath

> `ProcessPath` implements the operator-configurable definition of one
> process path (its canonical identity, the path_id-family match rule
> downstream consumers use, and the capabilities a station/associate must
> hold to work it).

The aggregate root. Identity (`PathId`) is immutable once constructed;
`MatchPrefix`, `RequiredCapabilities`, `CycleTimeP95` and `Eligibility` may
be revised while Active via `Revise`; `Direct` and
`DestinationLocationRole` are immutable (they describe structural facts
about the path's routing shape, not operational parameters operators
tune).

This aggregate replaces what was, before this service existed, a static
YAML file (`warehouse-infra/config/process-paths/sortable-fc.yaml`)
loaded once at boot by three other services.

## PathId

> `PathId` is the canonical identity of a process path (e.g. `"PICK"`,
> `"PACK"`, `"REBIN"`, `"SLAM"`). It is the SAME identity
> `fulfillment-execution`'s `task.Type`, `wes-work-planning`'s
> `WorkPool.PathId`, and `workforce-management`'s `PathPlan.PathId` all
> reference — this service is the one place that identity is DEFINED, not
> just consumed.

Kept as a plain string type (not an enum) because the valid set is
operator-configurable, not compiled in.

## Capability

> `Capability` is a named qualification a station/associate must hold to
> work a process path (e.g. `"pick"`, `"pack"`, `"hazmat"`) — the exact
> same vocabulary `workforce-management`'s `Certification` and
> `fulfillment-execution`'s `Station.Capability` already use.

This service does not invent a new capability vocabulary; it is the
authoritative SOURCE for which capabilities a given path requires.

## MatchPrefix

The lower-case prefix downstream consumers match a caller-supplied id
against: `id == matchPrefix` OR `id` starts with `matchPrefix + "-"` —
never a bare substring match without the separator (a hypothetical
`"picking-station"` must not match `"pick"`). Validated lower-case at
construction time (`ErrMatchPrefixNotLowercase`) — not lower-cased for the
caller — so persisted data is exactly what was validated.

## Direct

A structural fact about the path's routing shape (reserved for a future
multi-hop topology, not a day-to-day operational parameter). Immutable
once set at `Define` time — never revisable via `Revise`.

## DestinationLocationRole

An **optional** declaration of which facility-layout `LocationRole` a
completed task on this path is destined for: `Drop`, `WorkCenter` or
`Shipping`, or unset (the zero value). Declarative routing intent only —
this service never calls facility-layout to validate it; the closed value
set is kept in sync by convention
(`shared.ErrInvalidDestinationLocationRole` for an unknown value).
Immutable once set at `Define` time. See
[ADR 0009](/docs/adr/0009-destination-location-role-on-process-path).

## CycleTimeP95

> The operator-declared p95 end-to-end cycle time from release into the
> path to manifest (ADR 0010) — a declared standard, not a measured value.

Required and strictly positive (`ErrInvalidCycleTime` otherwise),
revisable while Active. See
[ADR 0010](/docs/adr/0010-fulfillment-capability-contract).

## Eligibility

A value object declaring the rules a unit of work must satisfy to be
routed to a path: `maxUnitsPerLine` (unset means unbounded; `1` declares a
singles path), `requiredProductAttributes`, `excludedProductAttributes`,
and `nonSortable`. Every field is optional and the zero value is fully
permissive. Compared with `Eligibility.Equal` for no-op detection.

## CPTSchedule, Cutoff, SiteId

> A site-scoped, recurring Critical Pull Time (CPT) schedule. A CPT is a
> property of a departure, not of a path — several paths feed the same
> truck, and a slow path simply cannot make the later ones — so the
> schedule is modelled once per site rather than duplicated on every path.

The second aggregate root, identified by `SiteId`. It holds an IANA
`timezone` and one or more **Cutoffs**, each with a `cptId` (unique within
the schedule), a `localTime` (`HH:MM`), `daysOfWeek` (`Mon`..`Sun`), a
`shipMethod`, and the `eligiblePathIds` that can make it. Revised
wholesale, never partially.

## Status (Active / Deactivated)

> `Status` is the activation lifecycle of a ProcessPath. There is no
> "draft" state — a path is live the instant it is defined.

Two values: `ACTIVE` and `DEACTIVATED`. Deactivation is a one-way,
idempotent transition — see the
[Aggregate Design Canvas](./aggregate-design-canvas.md) for the full
lifecycle.
