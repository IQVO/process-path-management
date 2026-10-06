---
title: Class Diagram
sidebar_label: Class Diagram
sidebar_position: 9
description: UML class diagrams of process-path-management's domain packages (ProcessPath, CPTSchedule, shared value types and domain events) and of its hexagonal ports and adapters.
---

# Class Diagram

UML class diagrams of `internal/domain/**`, plus the hexagonal ports and
adapters around it. Part of the [DDD artifact pack](./ddd-artifacts.md).
Type, field and method names are the real Go identifiers; Go slices are
written `List~T~` and the unexported fields are shown with `-`.

## 1. Aggregates, entities and value objects

```mermaid
classDiagram
    direction LR

    class ProcessPath {
        <<AggregateRoot>>
        -PathId id
        -string matchPrefix
        -bool direct
        -List~Capability~ requiredCapabilities
        -DestinationLocationRole destinationLocationRole
        -Duration cycleTimeP95
        -Eligibility eligibility
        -Status status
        -Time createdAt
        -Time updatedAt
        -int version
        +Define(id, matchPrefix, direct, requiredCapabilities, destinationLocationRole, cycleTimeP95, eligibility, now) ProcessPath
        +Rehydrate(...) ProcessPath
        +Revise(matchPrefix, requiredCapabilities, cycleTimeP95, eligibility, now) bool
        +Deactivate(now)
        +IsActive() bool
        +Version() int
    }

    class Status {
        <<Enumeration>>
        ACTIVE
        DEACTIVATED
    }

    class CPTSchedule {
        <<AggregateRoot>>
        -SiteId siteId
        -string timezone
        -List~Cutoff~ cutoffs
        -Time createdAt
        -Time updatedAt
        -int version
        +Define(siteId, timezone, cutoffs, now) CPTSchedule
        +Rehydrate(...) CPTSchedule
        +Revise(timezone, cutoffs, now) bool
        +AllEligiblePathIds() List~PathId~
        +Version() int
    }

    class Cutoff {
        <<Entity>>
        -string cptId
        -string localTime
        -List~Weekday~ daysOfWeek
        -string shipMethod
        -List~PathId~ eligiblePathIds
        +NewCutoff(cptId, localTime, daysOfWeek, shipMethod, eligiblePathIds) Cutoff
    }

    class Weekday {
        <<Enumeration>>
        Mon
        Tue
        Wed
        Thu
        Fri
        Sat
        Sun
    }

    class Eligibility {
        <<ValueObject>>
        -int maxUnitsPerLine
        -List~string~ requiredProductAttributes
        -List~string~ excludedProductAttributes
        -bool nonSortable
        +NewEligibility(...) Eligibility
        +Equal(other) bool
    }

    class DestinationLocationRole {
        <<Enumeration>>
        Unset
        Drop
        WorkCenter
        Shipping
        +ParseDestinationLocationRole(s) DestinationLocationRole
    }

    class PathId {
        <<ValueObject>>
        string
    }
    class SiteId {
        <<ValueObject>>
        string
    }
    class Capability {
        <<ValueObject>>
        string
    }

    ProcessPath --> Status
    ProcessPath *-- Eligibility
    ProcessPath --> DestinationLocationRole
    ProcessPath --> PathId : identity
    ProcessPath --> "1..*" Capability : requiredCapabilities
    CPTSchedule --> SiteId : identity
    CPTSchedule *-- "1..*" Cutoff
    Cutoff --> "1..7" Weekday
    Cutoff ..> "1..*" PathId : eligiblePathIds, reference by id
```

Source: `internal/domain/processpath/process_path.go`,
`internal/domain/cptschedule/cpt_schedule.go`,
`internal/domain/shared/shared.go`, `internal/domain/shared/eligibility.go`.
Omits: read accessors (`MatchPrefix()`, `Cutoffs()`, ...), the private
`validate`/equality helpers and the `Err...` sentinels (listed on the
[Aggregate Design Canvas](./aggregate-design-canvas.md)).
`maxUnitsPerLine` is a `*int` (nil means unbounded). `Unset` is
`DestinationLocationRoleUnset = ""`. The `Cutoff` to `PathId` link is a
reference by id only: `CPTSchedule` never holds a `ProcessPath`; the
Active-path rule lives in the `DefineCPTSchedule` use case.

## 2. Domain events

```mermaid
classDiagram
    direction LR

    class DomainEvent {
        <<interface>>
        +EventName() string
        +OccurredAt() Time
    }

    class ProcessPathCreated {
        <<DomainEvent>>
        +PathId PathId
        +MatchPrefix string
        +Direct bool
        +RequiredCapabilities List~Capability~
        +DestinationLocationRole DestinationLocationRole
        +CycleTimeP95 Duration
        +Eligibility Eligibility
        +At Time
    }

    class ProcessPathUpdated {
        <<DomainEvent>>
        +PathId PathId
        +MatchPrefix string
        +Direct bool
        +RequiredCapabilities List~Capability~
        +DestinationLocationRole DestinationLocationRole
        +CycleTimeP95 Duration
        +Eligibility Eligibility
        +At Time
    }

    class ProcessPathDeactivated {
        <<DomainEvent>>
        +PathId PathId
        +At Time
    }

    class CPTScheduleChanged {
        <<DomainEvent>>
        +SiteId SiteId
        +Timezone string
        +Cutoffs List~CutoffSnapshot~
        +At Time
        +ToSnapshot(schedule, now) CPTScheduleChanged
    }

    class CutoffSnapshot {
        <<ValueObject>>
        +CptId string
        +LocalTime string
        +DaysOfWeek List~Weekday~
        +ShipMethod string
        +EligiblePathIds List~PathId~
    }

    DomainEvent <|.. ProcessPathCreated
    DomainEvent <|.. ProcessPathUpdated
    DomainEvent <|.. ProcessPathDeactivated
    DomainEvent <|.. CPTScheduleChanged
    CPTScheduleChanged *-- "1..*" CutoffSnapshot
```

Source: `internal/domain/shared/events.go`,
`internal/domain/cptschedule/events.go`. Omits: the wire structs
(`ProcessPathData`, `CPTScheduleData`; see [Domain Events](./domain-events.md)).
The three path events live in `shared`; `CPTScheduleChanged` and
`CutoffSnapshot` live in `cptschedule`. Events are built in the use cases,
not by the aggregates.

## 3. Ports and adapters

```mermaid
classDiagram
    direction LR

    class ProcessPathRepo {
        <<Repository>>
        +Save(ctx, p) error
        +FindByID(ctx, id) ProcessPath
        +ListActive(ctx) List~ProcessPath~
        +ListAll(ctx) List~ProcessPath~
    }
    class CPTScheduleRepo {
        <<Repository>>
        +Save(ctx, s) error
        +FindBySiteID(ctx, siteId) CPTSchedule
    }
    class EventPublisher {
        <<interface>>
        +Publish(ctx, event) error
    }
    class UnitOfWork {
        <<interface>>
        +Execute(ctx, fn) error
    }
    class Clock {
        <<interface>>
        +Now() Time
    }
    class PathMetrics {
        <<interface>>
        +PathDefinitionAccepted(ctx)
        +PathDefinitionRejected(ctx)
    }

    class DefinePath
    class RevisePath
    class DeactivatePath
    class GetPath
    class ListPaths
    class DefineCPTSchedule
    class GetCPTSchedule

    class HttpServer["http.Server - inbound REST"]
    class McpDeps["mcp.Deps - inbound MCP, read-only"]

    class PgProcessPathRepo["postgres.ProcessPathRepo"]
    class PgCPTScheduleRepo["postgres.CPTScheduleRepo"]
    class MemProcessPathRepo["memory.ProcessPathRepo"]
    class MemCPTScheduleRepo["memory.CPTScheduleRepo"]
    class PgUnitOfWork["postgres.UnitOfWork"]
    class OutboxPublisher["postgres.OutboxPublisher"]
    class FanOutPublisher["kafka.FanOutPublisher"]
    class LogPublisher["events.LogPublisher"]
    class SystemClock["memory.SystemClock"]
    class TelemetryMetrics["telemetry.PathMetrics"]

    HttpServer --> DefinePath
    HttpServer --> RevisePath
    HttpServer --> DeactivatePath
    HttpServer --> GetPath
    HttpServer --> ListPaths
    HttpServer --> DefineCPTSchedule
    HttpServer --> GetCPTSchedule
    McpDeps ..> GetPath : GetPathQuery
    McpDeps ..> ListPaths : ListPathsQuery
    McpDeps ..> GetCPTSchedule : GetCPTScheduleQuery

    DefinePath --> ProcessPathRepo
    DefinePath --> EventPublisher
    DefinePath --> UnitOfWork
    DefinePath --> Clock
    DefinePath --> PathMetrics
    RevisePath --> ProcessPathRepo
    DeactivatePath --> ProcessPathRepo
    DefineCPTSchedule --> CPTScheduleRepo
    DefineCPTSchedule --> ProcessPathRepo

    ProcessPathRepo <|.. PgProcessPathRepo
    ProcessPathRepo <|.. MemProcessPathRepo
    CPTScheduleRepo <|.. PgCPTScheduleRepo
    CPTScheduleRepo <|.. MemCPTScheduleRepo
    UnitOfWork <|.. PgUnitOfWork
    EventPublisher <|.. OutboxPublisher
    EventPublisher <|.. FanOutPublisher
    EventPublisher <|.. LogPublisher
    Clock <|.. SystemClock
    PathMetrics <|.. TelemetryMetrics
```

Source: `internal/application/ports/ports.go`,
`internal/application/usecases/*.go`,
`internal/adapters/inbound/http/server.go`,
`internal/adapters/inbound/mcp/tools.go`,
`internal/adapters/outbound/{postgres,memory,events,kafka,telemetry}`,
`cmd/pathmgmt/main.go`. Omits: `RevisePath`, `DeactivatePath` and
`DefineCPTSchedule` also depend on `EventPublisher`, `UnitOfWork` and
`Clock` (left out to keep the diagram readable); the analytics side
(`internal/analytics/report` ports `ReportStore`/`ProjectionStore`,
`analyticsstore`, the `pathmgmt-projector` Kafka consumer and the
`pathmgmt-reports` HTTP handler); the outbox relay and sweeper, which are
driven by `cmd/pathmgmt`, not by a port. Publisher selection:
`OutboxPublisher` when `EVENT_PUBLISHER=kafka` and `DATABASE_URL` is set;
`FanOutPublisher` from `kafka.NewSharedIdFanOut` (direct to both topics) when `EVENT_PUBLISHER=kafka`
without `DATABASE_URL`; `LogPublisher` otherwise.
