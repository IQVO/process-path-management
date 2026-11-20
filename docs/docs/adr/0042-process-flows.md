---
id: 0042-process-flows
slug: /adr/0042-process-flows
title: "0042. Process flows: the ordered stages work passes through"
sidebar_label: "0042. Process flows"
description: "ADR 0042: a new ProcessFlow aggregate declares the ordered sequence of process-path stages a workload passes through (for example PICK, REBIN, PACK, SLAM), each stage naming an existing path and its process type; full-state ProcessFlowDefined, ProcessFlowRevised and ProcessFlowRetired events carry a version; a path an active flow uses cannot be deactivated. warehouse-planning reads the flows instead of declaring its own."
---

# 0042. Process flows: the ordered stages work passes through

## Status

Accepted (2026-11-18). Extends [ADR 0001](./0001-process-path-management-bounded-context.md)
(what this context owns) next to [ADR 0034](./0034-process-path-intake.md) (which demand may
enter a path) and follows [ADR 0026](./0026-reject-deactivation-of-paths-in-cpt-schedules.md)
(refusing a deactivation that would break a reference). Consumer: warehouse-planning ADR 0025.
Additive: existing paths, routes and published types are unchanged. Implemented after
warehouse-planning's ADR 0022 to ADR 0024.

## Context

A `ProcessPath` here is one stage of work (`PICK`, `PACK`, `REBIN`, `SLAM`) with its routing
metadata. Which stage feeds which is not modelled: ADR 0034's `intake` says that PACK, REBIN and
SLAM are fed by PICK's output, not in which order a workload visits them. warehouse-planning
needs that order to compose a path's end-to-end capacity (the slowest stage binds), and today
declares its own sequences with `POST /process-paths` in that context. The floor's topology is
then owned by a planning context, under a name (`ProcessPath`) that means a stage here and a
sequence there.

This context was extracted so that the fleet has one definition of its paths. The order of
stages is part of that definition.

## Decision

1. **`ProcessFlow` aggregate.** Identity: `flow_id` (same id rules as a path id). Fields:
   `name`, ordered `stages` (at least one), `status` (`ACTIVE` | `RETIRED`), `version`
   (starts at 1, +1 per accepted change). A stage is `{path_id, process_type}`:
   - `path_id` names an existing ACTIVE `ProcessPath` (validated against this context's own
     catalogue, no lookup elsewhere);
   - `process_type` is one of the closed set `PICK`, `REBIN`, `PACK`, `SLAM`, `DISPATCH`,
     `ARRIVAL`, `RELOCATE`, `REPLENISH` (the task types fulfillment-execution publishes);
   - a `path_id` appears at most once in a flow.
   A command that changes nothing raises no event and keeps the version.
2. **Routes.** `PUT /process-flows/{flowId}` (define or revise, idempotent),
   `GET /process-flows/{flowId}`, `GET /process-flows` (keyset page, ADR 0040),
   `DELETE /process-flows/{flowId}` (retire; a retired flow cannot be revised). Read-only MCP
   tools and A2A skills follow the authority levels of ADR 0041 (`L1`).
3. **Events, full state per flow.** `ProcessFlowDefined`, `ProcessFlowRevised` and
   `ProcessFlowRetired`, type `com.warehouse.wes.process-path-management.processflow.<Event>`,
   on `warehouse.process-path-management.events` and the analytics topic, through the outbox.
   `data`: `flow_id`, `name`, `stages` (`[{path_id, process_type}]`, in order), `status`,
   `version`. Key and `subject` = `flow_id`, so one flow's events stay in order. Consumers
   apply a message only when its `version` is greater than the stored one. The startup
   republish of ADR 0038 republishes every ACTIVE flow as `ProcessFlowRevised` with its current
   state, the same way it republishes paths.
4. **Referential rule.** Deactivating a path that an ACTIVE flow uses is refused with
   `409 path-in-active-flow`, naming the flows (the ADR 0026 precedent). Until a flow exists,
   no deactivation behaves differently.
5. **Hard rules hold.** No call to any sibling; the only consumer stays the projector of this
   context's analytics topic.

## Consequences

- The floor's topology has one owner; warehouse-planning, and any later context, read flows
  instead of declaring them.
- A new aggregate, table, three published types (catalogue rows in ADR 0016's type list,
  AsyncAPI on both channels), four routes and the generated REST reference.
- Path deactivation gains a refusal, but only for paths an active flow uses.

## Alternatives rejected

- **An ordered `next_path_id` on each path.** A path can belong to several flows (PICK feeds
  both a REBIN flow and a direct-to-PACK flow); one successor field cannot express that.
- **Derive the order from `intake`.** `intake` names demand sources, not an order of stages.
- **Leave sequences in warehouse-planning.** The topology stays in a planning context and
  drifts from the catalogue it references.
