---
title: Domain Message Flow
sidebar_label: Domain Message Flow
sidebar_position: 6
description: ddd-crew Domain Message Flow Modelling for process-path-management — four business scenarios with every command, event and query numbered.
---

# Domain Message Flow

Following [ddd-crew Domain Message Flow Modelling](https://github.com/ddd-crew/domain-message-flow-modelling).
Part of the [DDD artifact pack](./ddd-artifacts.md). Every arrow is a real
message: a REST route, a full CloudEvents `type` on a Kafka topic, or an
MCP tool. Prefixes: `cmd:` command, `evt:` event, `qry:` query.
Responses are shown as notes, not arrows. Event
arrows from the Kafka topic to a consumer are the consumer reading the
topic; this context never calls a consumer.

## 1. Operator defines a new process path

```mermaid
sequenceDiagram
    autonumber
    actor Op as Operator
    participant UI as warehouse-console with process_path_mfe
    participant PPM as process-path-management
    participant T as Kafka warehouse.process-path-management.events
    participant FE as fulfillment-execution
    participant WWP as wes-work-planning
    participant WFM as workforce-management
    participant OM as order-management
    participant NF as network-fulfillment

    Op->>UI: cmd: define path PICK
    UI->>PPM: cmd: POST /process-paths with Idempotency-Key
    Note over UI,PPM: 201 Created with the processPathResponse body
    PPM->>T: evt: com.warehouse.wes.process-path-management.processpath.ProcessPathCreated
    T-->>FE: evt: ProcessPathCreated into kafkacatalog cache
    T-->>WWP: evt: ProcessPathCreated into kafkacatalog cache
    T-->>WFM: evt: ProcessPathCreated into kafkacatalog cache
    T-->>OM: evt: ProcessPathCreated into kafkacatalog cache
    T-->>NF: evt: ProcessPathCreated into processpathcache
```

Source: `web/src/api.ts`, `internal/adapters/inbound/http/server.go`,
`internal/application/usecases/define_path.go`,
`internal/adapters/outbound/kafka/publisher.go`; sibling consumers listed
on the [Context Map](../ecosystem/context-map.md). Omits: the outbox relay
hop and the analytics copy (see [Sequence Diagrams](./sequence-diagrams.md)),
validation failures. Revise follows the same shape with
`PUT /process-paths/{pathId}` and `ProcessPathUpdated`.

## 2. Operator retires a process path

```mermaid
sequenceDiagram
    autonumber
    actor Op as Operator
    participant UI as warehouse-console with process_path_mfe
    participant PPM as process-path-management
    participant T as Kafka warehouse.process-path-management.events
    participant FE as fulfillment-execution
    participant OM as order-management
    participant NF as network-fulfillment

    Op->>UI: cmd: deactivate PACK
    UI->>PPM: cmd: DELETE /process-paths/PACK
    Note over UI,PPM: 204 No Content
    PPM->>T: evt: com.warehouse.wes.process-path-management.processpath.ProcessPathDeactivated
    T-->>FE: evt: ProcessPathDeactivated into kafkacatalog applyDeactivated
    T-->>OM: evt: ProcessPathDeactivated into kafkacatalog applyDeactivated
    T-->>NF: evt: ProcessPathDeactivated into processpathcache applyDeactivated
    Op->>UI: cmd: deactivate PACK again
    UI->>PPM: cmd: DELETE /process-paths/PACK
    Note over UI,PPM: 204 No Content and no event - idempotent
```

Source: `internal/application/usecases/deactivate_path.go`,
`internal/domain/processpath/process_path.go`; fulfillment-execution and
order-management `kafkacatalog/consumer.go`, network-fulfillment
`processpathcache/consumer.go` (each `applyDeactivated`). Omits:
wes-work-planning and workforce-management (same as fulfillment-execution).

## 3. Operator publishes a site's CPT schedule

```mermaid
sequenceDiagram
    autonumber
    actor Op as Operator
    participant PPM as process-path-management
    participant T as Kafka warehouse.process-path-management.events
    participant OM as order-management
    participant NF as network-fulfillment

    Op->>PPM: cmd: PUT /sites/sp1/cpt-schedule
    Note over PPM: every eligiblePathId must be an Active path in the own store
    alt an eligible path is unknown or deactivated
        Note over Op,PPM: 422 ineligible-path-id and no event
    else schedule valid and changed
        Note over Op,PPM: 200 with the cptScheduleResponse body
        PPM->>T: evt: com.warehouse.wes.process-path-management.cptschedule.CPTScheduleChanged
        T-->>OM: evt: CPTScheduleChanged into kafkacptschedule
        T-->>NF: evt: CPTScheduleChanged into processpathcache
    end
```

Source: `internal/application/usecases/cpt_schedule.go`,
`internal/domain/cptschedule/events.go`; order-management
`kafkacptschedule/consumer.go`. Omits: the identical-schedule branch (200,
no event).

## 4. Console shows catalogue growth

```mermaid
sequenceDiagram
    autonumber
    participant PPM as process-path-management
    participant A as Kafka warehouse.process-path-management.analytics
    participant PROJ as pathmgmt-projector
    participant REP as pathmgmt-reports
    participant UI as warehouse-console
    participant AG as MCP host
    participant MCP as process-path-management cmd/mcp

    PPM->>A: evt: com.warehouse.wes.process-path-management.processpath.ProcessPathCreated analytics copy
    A-->>PROJ: evt: ProcessPathCreated - paths_defined plus 1
    UI->>REP: qry: GET /reports/catalogue-growth with from, to, granularity day
    Note over UI,REP: rows of dayBucket, pathsDefined, pathsRevised, pathsDeactivated
    UI->>REP: qry: GET /reports/catalogue-growth/freshness
    Note over UI,REP: lagSeconds
    AG->>MCP: qry: MCP get_catalogue_growth_report
    MCP->>REP: qry: GET /reports/catalogue-growth
```

Source: `internal/adapters/outbound/kafka/analytics_publisher.go`,
`internal/adapters/inbound/kafka/analytics_consumer.go`,
`internal/adapters/inbound/http/reports_handler.go`,
`internal/adapters/inbound/mcp/report_tool.go`; warehouse-console
`src/features/context-reports/processPathManagement.config.tsx`.
Omits: the DLQ path, and the fact that the MCP report tool is only
registered when `REPORTS_BASE_URL` is set.
