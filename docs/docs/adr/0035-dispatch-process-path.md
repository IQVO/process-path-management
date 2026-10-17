---
id: 0035-dispatch-process-path
slug: /adr/0035-dispatch-process-path
title: "0035. A DISPATCH process path carries the transfer dispatch leg"
sidebar_label: "0035. DISPATCH process path"
description: "ADR 0035 — the catalogue gains a DISPATCH path (matchPrefix dispatch, direct, required capability dispatch, intake STOCK_TRANSFER) so the transfer dispatch leg that network-inventory-planning releases can become a fulfillment-execution task. The path id is the fulfillment-execution task type, so it is DISPATCH, not TRANSFER_DISPATCH. The entry is declared in catalogue/paths.json and applied by scripts/seed_catalogue.py. Companion to fulfillment-execution ADR-0036 and ADR 0033/0034 of this service. No schema change, no new event."
---

# 0035. A DISPATCH process path carries the transfer dispatch leg

## Status

**Accepted** (2026-10-16). Companion to fulfillment-execution
[ADR-0036 "Transfer task types and facts"](https://github.com/IQVO/fulfillment-execution/tree/develop/docs/docs/adr),
which requires a `dispatch-*` path family to be declared in the catalogue
before transfer dispatch work can be released. Follows
[ADR 0033](./0033-relocate-process-path.md) (the same precedent: the path is
catalogue data under the existing `ProcessPath` model) and uses the closed
intake vocabulary of [ADR 0034](./0034-process-path-intake.md)
(`STOCK_TRANSFER` is already in it). No schema change, no migration, no new
event, no REST or MCP change.

## Context

network-inventory-planning releases the **dispatch leg** of an inter-warehouse
stock transfer as a `WorkDemandReleased` that names a path id. wes-work-planning
forwards it, and fulfillment-execution builds the task from this catalogue:

- It resolves the released path id against its copy of the catalogue. A path
  definition matches an id when `id == matchPrefix` or `id` starts with
  `matchPrefix + "-"`, and the longest prefix wins.
- It then sets the task type from the **path definition's id**
  (`task.Type(pathDef.Id)` in `consume_work_released.go`) and takes the task's
  required capabilities from the same definition.

The catalogue declared PICK, PACK, REBIN, SLAM and RELOCATE only. A dispatch
demand therefore matched nothing and was dead-lettered by the consumer.

An earlier guess for the entry was path id `TRANSFER_DISPATCH` with prefix
`transfer-dispatch`. That is wrong for a reason that is easy to miss: because
the path id becomes the fulfillment-execution task type, the id has to be the
task type that fulfillment-execution's ADR-0036 names for the leg, `DISPATCH`.
The e2e saga already runs with the path id `dispatch-transfer`
(`NIP_TRANSFER_DISPATCH_PATH_ID`) and a station capability `dispatch`, which a
`dispatch` family matches.

## Decision

Define one Active process path, through the same `DefinePath` use case,
outbox and `ProcessPathCreated` CloudEvent as every other path:

| Field | Value | Why |
| --- | --- | --- |
| `pathId` | `DISPATCH` | The canonical id, and therefore the fulfillment-execution task type (`task.Type(pathDef.Id)`). It is `DISPATCH`, not `TRANSFER_DISPATCH`: a different id would create a task type nobody handles. |
| `matchPrefix` | `dispatch` | The family key. network-inventory-planning releases `dispatch-transfer`, which starts with `dispatch-`, so it resolves to this path. No existing prefix (`pick`, `pack`, `rebin`, `slam`, `relocate`, ADR 0008's families) is a prefix of it, and it is not a prefix of theirs, so the longest-prefix rule cannot confuse it. |
| `direct` | `true` | A released dispatch demand becomes one execution task, the same shape as the other paths. |
| `requiredCapabilities` | `["dispatch"]` | One capability named after the family, the convention of every other path. Only stations and associates holding `dispatch` can claim the task, and workforce-management plans headcount against it. |
| `cycleTimeP95` | `2h` | The fleet's flat default (ADR 0010 of this service). Operators revise it with `PUT` when they measure it. |
| `eligibility` | `{}` (permissive) | Eligibility describes the unit of work a path can take, not who may route onto it. |
| `intake` | `["STOCK_TRANSFER"]` | [ADR 0034](./0034-process-path-intake.md): only stock-transfer legs route onto DISPATCH. order-management considers only paths whose intake contains `CUSTOMER_ORDER`, so DISPATCH is never chosen for a customer order line, whatever its cycle time. PICK's intake is unchanged. |
| `destinationLocationRole` | unset | The entry mirrors RELOCATE and PICK: no destination role. |

The entry is declared in `catalogue/paths.json`, in the request-body shape of
`POST /process-paths`, and `scripts/seed_catalogue.py` converges it with the
convergence table of [ADR 0033](./0033-relocate-process-path.md): `POST` when
absent, nothing when identical, `PUT` when a revisable field differs, an error
for an immutable difference, and never a resurrection of a deactivated path.
Nothing at boot reads the file.

This ADR declares the dispatch leg only. It does not declare an arrival
family, and it adds nothing to the intake vocabulary.

## Consequences

**Easier**

- network-inventory-planning's dispatch leg becomes releasable and
  claimable through the existing planner, router and executor. Capability
  gating, labour planning and telemetry apply with no new machinery.
- A new environment gets the path from the same idempotent command as
  RELOCATE (`make seed-catalogue`).
- No consumer code changes. Every consumer already decodes
  `ProcessPathCreated`, and the wire shape is pinned by a golden test.

**Harder, and who must set what**

- **The seed must run before the first dispatch leg is released.** An
  environment that skips it has no DISPATCH path, and the demand is
  dead-lettered by fulfillment-execution (fail-closed: refused, not routed
  elsewhere). Consumers apply the event live, so no restart is needed.
- **network-inventory-planning's configured path id must be
  `dispatch-transfer`** (or any other `dispatch` or `dispatch-*` id). That is
  a configuration change in warehouse-infra, delivered by a separate PR; this
  repository does not change it. Any earlier instruction to `POST` a
  `TRANSFER_DISPATCH` / `transfer-dispatch` path should not be followed.
- **Stations that execute the leg need the capability `dispatch`.**
  Without it nobody can claim a DISPATCH task.
- The path id `DISPATCH` is effectively permanent: a deactivated id is never
  resurrected (ADR 0033), so a wrong id could only be replaced by a new one.
  That is why it is settled here before the first environment seeds it.
