---
paths:
  - "cmd/**"
  - "internal/**"
  - "apis/**"
  - "features/**"
  - "migrations/**"
---

# Strategic classification, consumers and scope boundaries

Moved out of `CLAUDE.md`; still authoritative.

> This is the **ninth** bounded-context Go service in the warehouse-systems
> fleet (after order-management, inventory-storage, wes-work-planning,
> workforce-management, fulfillment-execution, facility-layout,
> warehouse-ops-agent, labor-performance). See ADR 0001 for the full
> decision record.

The fleet's **operator-configurable process-path catalogue**: a path's
canonical identity (`PathId`, e.g. `PICK`, `PACK`, `REBIN`, `SLAM`), the
`matchPrefix` rule downstream consumers use to resolve a caller-supplied id
to a path family, whether it is `Direct`, and the capabilities a
station/associate must hold to work it. Before this service existed, that
definition lived in a static YAML file
(`warehouse-infra/config/process-paths/sortable-fc.yaml`) loaded once at
boot by `fulfillment-execution`, `wes-work-planning`, and
`workforce-management` — a published language with no single owner, no
audit trail, and no way to revise without a coordinated redeploy of all
three consumers. This service replaces that file as the single source of
truth.

**Generic Subdomain**, the same bucket as `facility-layout` — well
understood, not a competitive differentiator, but needed identically by
multiple existing contexts, so it is extracted rather than duplicated or
left as an unowned static file (ADR 0001).

This service is the **Open Host Service / Published Language SOURCE** for
`warehouse.process-path-management.events`, with `fulfillment-execution`
(Core), `wes-work-planning` (Core), and `workforce-management`
(Supporting) as its **Conformist** consumers. It has **zero inbound
dependency** — no inbound Kafka consumer, no synchronous REST dependency
in either direction. It never calls into any other service, synchronously
or otherwise; every change propagates exclusively via Kafka.

**Consumers (live):** the three above replaced their static YAML catalogue
with this topic on 2026-09-06 (ADR 0002); `order-management` also
consumes it for `cycle_time_p95`/`eligibility` and `CPTScheduleChanged`
(ADR 0010), and `network-fulfillment` consumes it (ProcessPath* +
`CPTScheduleChanged`) into its `processpathcache`. `warehouse-ops-agent`
has an MCP client for this service wired but not yet used by a use case;
`warehouse-console` mounts its `web/` remote and reads the
`pathmgmt-reports` catalogue-growth report. See
`docs/docs/ecosystem/context-map.md`, and verify against the sibling
repos' `origin/develop` before relying on it.

## What this service deliberately does not own

- Does not decide dispatch, routing, or task assignment — defines WHAT a
  path is and WHICH capabilities it requires; never claims, assigns, or
  completes work. That is `fulfillment-execution`'s job.
- Does not call any other context — REST, MCP or otherwise — ever.
- Consumes no other context's topic (its only consumer reads its own
  analytics topic).
