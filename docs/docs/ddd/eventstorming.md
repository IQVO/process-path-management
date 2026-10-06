---
title: EventStorming
sidebar_label: EventStorming
sidebar_position: 7
description: Design-level EventStorming of process-path-management in ddd-crew cheat-sheet notation — the path lifecycle, the CPT schedule and the analytics projection — with every sticky traced to code and hotspots taken from real known gaps.
---

# EventStorming

Design-level [EventStorming](https://github.com/ddd-crew/eventstorming-glossary-cheat-sheet)
of this context's three processes, reconstructed from the code rather
than from a workshop. Part of the [DDD artifact pack](./ddd-artifacts.md).
Read each board left to right: an **actor** issues a **command**, an
**aggregate** decides, a **domain event** records the fact, a **policy**
reacts ("whenever ... then ..."), **read models** inform the next
decision, and **external systems** sit on the edges. **Hotspots** are real
open issues found in the code or recorded in ADRs, not invented ones.

## Legend

```mermaid
flowchart LR
    A["Actor"]:::actor
    C["Command"]:::command
    AG["Aggregate"]:::aggregate
    E["Domain event"]:::event
    P["Policy"]:::policy
    R["Read model"]:::readmodel
    X["External system"]:::external
    H["Hotspot"]:::hotspot
    A --> C --> AG --> E --> P
    R -.-> A
    X --> C
    H -.- AG

    classDef actor fill:#fef9c3,stroke:#a16207,color:#000,font-size:11px
    classDef command fill:#4aa3df,stroke:#1f6391,color:#000
    classDef aggregate fill:#f7d84a,stroke:#9a7d0a,color:#000
    classDef event fill:#f6a04d,stroke:#a04000,color:#000
    classDef policy fill:#c39bd3,stroke:#6c3483,color:#000
    classDef readmodel fill:#7dcea0,stroke:#1e8449,color:#000
    classDef external fill:#f1948a,stroke:#922b21,color:#000
    classDef hotspot fill:#e74c3c,stroke:#7b241c,color:#fff
```

Source: ddd-crew EventStorming cheat-sheet colours as fixed by the fleet
documentation brief.

## Process 1 — define, revise and deactivate a process path

```mermaid
flowchart LR
    OP["Operator"]:::actor
    RM0["Process path list - GET /process-paths"]:::readmodel
    C1["DefinePath"]:::command
    A1["ProcessPath"]:::aggregate
    E1["ProcessPathCreated"]:::event
    C2["RevisePath"]:::command
    A2["ProcessPath"]:::aggregate
    E2["ProcessPathUpdated"]:::event
    C3["DeactivatePath"]:::command
    A3["ProcessPath"]:::aggregate
    E3["ProcessPathDeactivated"]:::event
    P1["Policy: whenever a path event is raised, enqueue it on the events and analytics topics in the same transaction"]:::policy
    X1["fulfillment-execution, wes-work-planning, workforce-management, order-management, network-fulfillment"]:::external
    RM1["Sites whose CPT schedule lists the path - ListSiteIDsReferencingPath"]:::readmodel
    P2["Policy: whenever a deactivation targets a path a CPT schedule lists, refuse it with 409 - ADR 0026"]:::policy
    H2["Hotspot: ICQA path family left undecided - ADR 0008"]:::hotspot

    RM0 -.-> OP
    OP --> C1 --> A1 --> E1 --> P1
    OP --> C2 --> A2 --> E2 --> P1
    OP --> C3 --> A3 --> E3 --> P1
    RM1 -.-> C3
    C3 --> P2
    P1 --> X1
    H2 -.- A1

    classDef actor fill:#fef9c3,stroke:#a16207,color:#000,font-size:11px
    classDef command fill:#4aa3df,stroke:#1f6391,color:#000
    classDef aggregate fill:#f7d84a,stroke:#9a7d0a,color:#000
    classDef event fill:#f6a04d,stroke:#a04000,color:#000
    classDef policy fill:#c39bd3,stroke:#6c3483,color:#000
    classDef readmodel fill:#7dcea0,stroke:#1e8449,color:#000
    classDef external fill:#f1948a,stroke:#922b21,color:#000
    classDef hotspot fill:#e74c3c,stroke:#7b241c,color:#fff
```

Source: `internal/application/usecases/define_path.go`,
`revise_path.go`, `deactivate_path.go`, `queries.go`;
`internal/domain/processpath/process_path.go`;
`internal/adapters/outbound/postgres/outbox_publisher.go`;
`cmd/pathmgmt/main.go` (`buildEventPublisher`). Omits: the no-op branches
(an unchanged revision and a repeated deactivation raise no event) and
validation failures (422, nothing raised). `DefinePath` persists through
the insert-only `ProcessPathRepo.Create`, so two concurrent defines of one
id raise `ProcessPathCreated` once and the loser gets 409. A refused
deactivation (policy P2) raises nothing and leaves the path ACTIVE.

## Process 2 — publish a site's CPT schedule

```mermaid
flowchart LR
    OP["Operator"]:::actor
    RM1["Active process paths - own ProcessPathRepo"]:::readmodel
    C1["DefineCPTSchedule - PUT /sites/siteId/cpt-schedule"]:::command
    A1["CPTSchedule"]:::aggregate
    E1["CPTScheduleChanged"]:::event
    P1["Policy: whenever a schedule changes, publish the full snapshot, never a diff"]:::policy
    X1["order-management, network-fulfillment"]:::external
    H2["Hotspot: siteId is never validated - an unknown site surfaces downstream - ADR 0010"]:::hotspot

    RM1 -.-> C1
    OP --> C1 --> A1 --> E1 --> P1 --> X1
    H2 -.- A1

    classDef actor fill:#fef9c3,stroke:#a16207,color:#000,font-size:11px
    classDef command fill:#4aa3df,stroke:#1f6391,color:#000
    classDef aggregate fill:#f7d84a,stroke:#9a7d0a,color:#000
    classDef event fill:#f6a04d,stroke:#a04000,color:#000
    classDef policy fill:#c39bd3,stroke:#6c3483,color:#000
    classDef readmodel fill:#7dcea0,stroke:#1e8449,color:#000
    classDef external fill:#f1948a,stroke:#922b21,color:#000
    classDef hotspot fill:#e74c3c,stroke:#7b241c,color:#fff
```

Source: `internal/application/usecases/cpt_schedule.go`
(`validateEligiblePathIds`), `internal/domain/cptschedule/cpt_schedule.go`,
`internal/domain/cptschedule/events.go` (`ToSnapshot`),
`docs/docs/adr/0010-fulfillment-capability-contract.md`. Omits: the
identical-schedule no-op and `GetCPTSchedule`.

## Process 3 — project catalogue growth

```mermaid
flowchart LR
    E0["ProcessPathCreated / Updated / Deactivated on the analytics topic"]:::event
    P1["Policy: whenever a path event arrives, dedupe on the CloudEvents id and bump the day bucket"]:::policy
    RM1["catalogue_growth_rollup"]:::readmodel
    U["Console user or MCP host"]:::actor
    X1["warehouse-console context reports"]:::external
    P2["Policy: whenever a message is not a CloudEvent or keeps failing, dead-letter it and commit"]:::policy
    X2["warehouse.process-path-management.analytics.dlq"]:::external
    H1["Hotspot: asyncapi promises traceparent headers the publisher never sets"]:::hotspot

    E0 --> P1 --> RM1
    E0 --> P2 --> X2
    RM1 -.-> X1 -.-> U
    H1 -.- E0

    classDef actor fill:#fef9c3,stroke:#a16207,color:#000,font-size:11px
    classDef command fill:#4aa3df,stroke:#1f6391,color:#000
    classDef aggregate fill:#f7d84a,stroke:#9a7d0a,color:#000
    classDef event fill:#f6a04d,stroke:#a04000,color:#000
    classDef policy fill:#c39bd3,stroke:#6c3483,color:#000
    classDef readmodel fill:#7dcea0,stroke:#1e8449,color:#000
    classDef external fill:#f1948a,stroke:#922b21,color:#000
    classDef hotspot fill:#e74c3c,stroke:#7b241c,color:#fff
```

Source: `internal/adapters/inbound/kafka/analytics_consumer.go`,
`internal/adapters/outbound/analyticsstore/postgres_projection.go`,
`internal/adapters/inbound/http/reports_handler.go`,
`internal/adapters/inbound/mcp/report_tool.go`, `apis/asyncapi.yaml`.
Omits: the freshness endpoint and `CPTScheduleChanged`, which the
projector commits and skips.

## Stickies and their code evidence

| Sticky | Kind | Evidence |
| --- | --- | --- |
| Operator | Actor | `web/src/screens/ProcessPathsScreen.tsx` (the `process_path_mfe` remote) and direct REST callers |
| DefinePath / RevisePath / DeactivatePath | Command | `usecases.DefinePath`, `usecases.RevisePath`, `usecases.DeactivatePath`; routes `POST /process-paths`, `PUT` and `DELETE /process-paths/{pathId}` |
| DefineCPTSchedule | Command | `usecases.DefineCPTSchedule`; route `PUT /sites/{siteId}/cpt-schedule` |
| ProcessPath | Aggregate | `processpath.ProcessPath` (`Define`, `Revise`, `Deactivate`) |
| CPTSchedule | Aggregate | `cptschedule.CPTSchedule` (`Define`, `Revise`) |
| ProcessPathCreated / Updated / Deactivated | Domain event | `shared.ProcessPathCreated`, `shared.ProcessPathUpdated`, `shared.ProcessPathDeactivated` |
| CPTScheduleChanged | Domain event | `cptschedule.CPTScheduleChanged` |
| Enqueue on both topics | Policy | `postgres.OutboxPublisher.Publish` with `IntegrationEncoder` and `AnalyticsEncoder` (`cmd/pathmgmt/main.go`) |
| Full snapshot, never a diff | Policy | `cptschedule.ToSnapshot` |
| Dedupe and bump the day bucket | Policy | `AnalyticsConsumer.handleFetchedMessage` then `ApplyProcessPath*` |
| Dead-letter and commit | Policy | `AnalyticsConsumer.deadLetterAndCommit` (ADR 0012) |
| Process path list | Read model | `usecases.ListPaths` (`GET /process-paths`, MCP `list_process_paths`) |
| Active process paths | Read model | `ProcessPathRepo.FindByID` inside `validateEligiblePathIds` |
| Sites whose CPT schedule lists the path | Read model | `ports.CPTScheduleRepo.ListSiteIDsReferencingPath` inside `DeactivatePath` |
| Refuse deactivation while a schedule lists the path | Policy | `usecases.DeactivatePath` returning `ErrPathReferencedByCPTSchedule` (ADR 0026) |
| catalogue_growth_rollup | Read model | `migrations/analytics/0001_report.up.sql`, served by `pathmgmt-reports` |
| Five consumer contexts | External system | sibling consumer files on the [Context Map](../ecosystem/context-map.md) |
| warehouse-console context reports | External system | warehouse-console `src/features/context-reports/processPathManagement.config.tsx` |

## Hotspots

| Hotspot | Evidence |
| --- | --- |
| `siteId` is never validated | ADR 0010: "A schedule for an unknown site is an operator error that shows up as an unroutable order in order-management, not a coupling here." |
| ICQA path family left undecided | ADR 0008: "ICQA remains a genuinely open question, deliberately not decided here." |
| A schedule written concurrently with the deactivation of a path it lists can slip through | ADR 0026: the reference check and the deactivation share one READ COMMITTED transaction and neither locks the other's rows; the next `PUT` of that schedule is rejected by the Active-path check. |
| Trace context on Kafka | `apis/asyncapi.yaml` says W3C trace context travels in `traceparent`/`tracestate` headers; `kafka.Publisher.Send` sets only the `content-type` header. |
