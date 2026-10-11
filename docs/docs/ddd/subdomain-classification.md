---
id: subdomain-classification
title: Subdomain classification
sidebar_label: Subdomain classification
description: Why process-path-management is a Generic subdomain in the wes tier, and how its neighbours are classified according to the fleet table.
---

# Subdomain classification

**Process Path Management is a Generic subdomain.** It publishes under the
`wes` CloudEvents segment (`Subdomain = "wes"` in
`internal/adapters/kafka/cloudevents/cloudevents.go`), so every event `type`
starts with `com.warehouse.wes.process-path-management.`. The tier (`wes`)
and the classification (Generic) are independent: the tier groups contexts by
the layer they run in, the classification by how much competitive advantage
they carry.

This matches [ADR 0001](../adr/0001-process-path-management-bounded-context.md),
the [Core Domain Chart](./core-domain-chart.md) (point at complexity 0.58,
differentiation 0.22) and the fleet table in warehouse-docs.

## Why Generic

| Question | Answer from this codebase |
| --- | --- |
| Does it decide anything that makes the warehouse faster or cheaper than a competitor's? | No. It declares what a path is (`matchPrefix`, capabilities, cycle time, eligibility) and when trucks leave (CPT cutoffs). Dispatch, release, staffing and the order promise are decided by its consumers. |
| Is the model well understood in the industry? | Yes. Process-path / labour-path catalogues and carrier cutoff tables are standard configuration in commercial WMS/WES products. |
| Why build it instead of keeping a file? | Several contexts need the same definition and none is its natural owner. It replaced a static YAML file that three services loaded at boot, with no owner, audit trail or way to change it without redeploying consumers ([ADR 0002](../adr/0002-yaml-to-kafka-cutover.md)). |
| Is it complex? | Moderately: two aggregates (`ProcessPath`, `CPTSchedule`), one cross-aggregate rule enforced with row locks ([ADR 0028](../adr/0028-close-path-cpt-schedule-race-with-row-locks.md)), four event types. Most of the weight is infrastructure (outbox, idempotency keys, optimistic concurrency, analytics projector). Complexity does not make a subdomain Core. |
| Why not Supporting? | Supporting means fleet-specific logic that is still not a differentiator. The catalogue's rules are generic configuration rules; its fleet-specific part is only vocabulary (capability names, the `matchPrefix` convention). |

### What would change the verdict

If CPT feasibility (can this order make this truck through this path?) moved
here from `order-management`, the context would start making a promise
decision and would move up the differentiation axis. That would need a new
ADR and a re-classification in warehouse-docs.

## Neighbours

Classifications copied from warehouse-docs
`docs/strategic-design/subdomain-classification.md` (branch `main`, read
2026-10-09). Do not edit them here; change them there.

| Context | Relationship to this service | Classification | CloudEvents tier |
| --- | --- | --- | --- |
| `fulfillment-execution` | Consumes `warehouse.process-path-management.events` | Core | `wes` |
| `wes-work-planning` | Consumes the events topic | Core | `wes` |
| `workforce-management` | Consumes the events topic | Supporting | `wes` |
| `order-management` | Consumes the events topic, including `CPTScheduleChanged` | Generic/Supporting | `wes` |
| `network-fulfillment` | Consumes the events topic, including `CPTScheduleChanged` | Supporting | `wes` |
| `warehouse-ops-agent` | Has an MCP client for this service, unused | Supporting | `wes` (publishes no events) |
| `facility-layout` | Separate Ways: `DestinationLocationRole` and `SiteId` are copied by convention | Generic | `wms` |
| `product-master` | Owns the handling vocabulary behind eligibility product attributes; not consumed | Supporting | `wms` |

`facility-layout` and `process-path-management` are the fleet's two Generic
contexts. Both exist so that no Core or Supporting context has to own a
concern several of them need, and neither calls a sibling.

See also: [Bounded Context Canvas](./bounded-context-canvas.md),
[Context Map](../ecosystem/context-map.md),
[Integration](../ecosystem/integration.md).
