---
title: Sequence Diagrams
sidebar_label: Sequence Diagrams
sidebar_position: 11
description: UML sequence diagrams for every process-path-management command use case — define, revise and deactivate a path, define or revise a CPT schedule — plus the outbox relay and the analytics projection, derived from the use-case code.
---

# Sequence Diagrams

UML sequence diagrams of every command use case, following the code
paths in `internal/application/usecases` and the adapters around them.
Part of the [DDD artifact pack](./ddd-artifacts.md). The diagrams show
the production wiring: `DATABASE_URL` set (Postgres repositories,
`postgres.UnitOfWork`, `postgres.OutboxPublisher`) and
`EVENT_PUBLISHER=kafka`. Without `DATABASE_URL` the in-memory
repositories are used, there is no transaction and no idempotency
middleware, and events go straight to Kafka (or to the log publisher).
Read queries (`GetPath`, `ListPaths`, `GetCPTSchedule`, the MCP tools)
are single repository reads and are not drawn.

## 1. Define a process path

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant MW as RequireIdempotencyKey
    participant H as http.Server handleDefinePath
    participant UC as usecases.DefinePath
    participant AGG as processpath.ProcessPath
    participant REPO as postgres.ProcessPathRepo
    participant UOW as postgres.UnitOfWork
    participant OB as postgres.OutboxPublisher
    participant DB as Postgres

    Client->>MW: POST /process-paths with Idempotency-Key
    alt no Idempotency-Key header
        MW-->>Client: 400 idempotency-key-required
    end
    MW->>DB: BEGIN, INSERT idempotency_keys ON CONFLICT DO NOTHING
    alt key already recorded
        MW->>DB: ROLLBACK, SELECT stored outcome
        alt different request body hash
            MW-->>Client: 422 idempotency-key-reused
        else same body
            MW-->>Client: replay stored status, headers and body
        end
    else new key
        MW->>H: request with the transaction in context
        H->>H: ParseDestinationLocationRole, parseCycleTimeP95
        H->>UC: Execute(id, matchPrefix, direct, caps, role, cycleTimeP95, eligibility)
        UC->>REPO: FindByID(id)
        alt path already exists
            UC-->>H: ErrPathAlreadyExists
            H-->>MW: 409 path-already-exists
        else new id
            UC->>AGG: Define(...)
            alt invariant violated
                AGG-->>UC: Err sentinel
                UC-->>H: error
                H-->>MW: 422 problem+json
            else valid
                AGG-->>UC: ProcessPath ACTIVE, version 1
                UC->>UOW: Execute(fn)
                Note over UOW: joins the middleware transaction
                UOW->>REPO: Save(p)
                REPO->>DB: INSERT process_paths ON CONFLICT DO UPDATE WHERE version matches
                UOW->>OB: Publish(ProcessPathCreated)
                OB->>DB: INSERT outbox_events for the events topic
                OB->>DB: INSERT outbox_events for the analytics topic, same event_id
                UC-->>H: ProcessPath
                H-->>MW: 201 processPathResponse
            end
        end
        MW->>DB: UPDATE idempotency_keys with the outcome, COMMIT
        MW-->>Client: recorded response
    end
```

Source: `internal/adapters/inbound/http/idempotency.go`
(`RequireIdempotencyKey`, `replayCachedResponse`, `runFreshRequest`),
`internal/adapters/inbound/http/server.go` (`handleDefinePath`),
`internal/application/usecases/define_path.go`,
`internal/domain/processpath/process_path.go`,
`internal/adapters/outbound/postgres/{process_path_repo.go,unit_of_work.go,outbox_publisher.go}`.
Omits: the `PathMetrics` accepted/rejected counter calls, the 400 for a
malformed JSON body, and 500 branches. `UnitOfWork.Execute` reuses the
transaction already in the context (`txFrom`) instead of opening its
own, so the path row, both outbox rows and the idempotency row commit
together.

## 2. Revise a process path

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant H as http.Server handleRevisePath
    participant UC as usecases.RevisePath
    participant REPO as postgres.ProcessPathRepo
    participant AGG as processpath.ProcessPath
    participant UOW as postgres.UnitOfWork
    participant OB as postgres.OutboxPublisher
    participant DB as Postgres

    Client->>H: PUT /process-paths/PICK
    H->>UC: Execute(id, matchPrefix, caps, cycleTimeP95, eligibility)
    UC->>REPO: FindByID(id)
    REPO->>DB: SELECT process_paths
    alt not found
        UC-->>H: ErrPathNotFound
        H-->>Client: 404 path-not-found
    else found at version N
        UC->>AGG: Revise(matchPrefix, caps, cycleTimeP95, eligibility, now)
        alt DEACTIVATED or invariant violated
            AGG-->>UC: ErrPathDeactivated or Err sentinel
            H-->>Client: 422 problem+json
        else nothing changed
            AGG-->>UC: changed false
            H-->>Client: 200 unchanged path, no event
        else changed
            AGG-->>UC: changed true
            UC->>UOW: Execute(fn)
            UOW->>DB: BEGIN
            UOW->>REPO: Save(p)
            REPO->>DB: UPSERT WHERE process_paths.version = N
            alt row version moved on
                REPO-->>UOW: ErrConcurrentModification
                UOW->>DB: ROLLBACK
                H-->>Client: 409 concurrent-modification
            else saved, version N+1
                UOW->>OB: Publish(ProcessPathUpdated)
                OB->>DB: INSERT outbox_events, events and analytics topics
                UOW->>DB: COMMIT
                H-->>Client: 200 processPathResponse
            end
        end
    end
```

Source: `internal/adapters/inbound/http/server.go` (`handleRevisePath`),
`internal/application/usecases/revise_path.go`,
`internal/domain/processpath/process_path.go` (`Revise`),
`internal/adapters/outbound/postgres/process_path_repo.go` (`Save`),
`internal/adapters/inbound/http/errors.go` (`statusFor`). Omits: the 422
for an unparsable `cycleTimeP95` (raised by the handler before the use
case runs). `PUT` routes carry no idempotency middleware (ADR 0011).

## 3. Deactivate a process path

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant H as http.Server handleDeactivatePath
    participant UC as usecases.DeactivatePath
    participant REPO as postgres.ProcessPathRepo
    participant AGG as processpath.ProcessPath
    participant UOW as postgres.UnitOfWork
    participant OB as postgres.OutboxPublisher
    participant DB as Postgres

    Client->>H: DELETE /process-paths/PACK
    H->>UC: Execute(id)
    UC->>REPO: FindByID(id)
    alt not found
        UC-->>H: ErrPathNotFound
        H-->>Client: 404 path-not-found
    else already DEACTIVATED
        UC-->>H: nil, nothing saved or published
        H-->>Client: 204 No Content
    else ACTIVE
        UC->>AGG: Deactivate(now)
        UC->>UOW: Execute(fn)
        UOW->>REPO: Save(p)
        REPO->>DB: UPSERT status DEACTIVATED WHERE version matches
        alt version moved on
            H-->>Client: 409 concurrent-modification
        else saved
            UOW->>OB: Publish(ProcessPathDeactivated)
            OB->>DB: INSERT outbox_events, events and analytics topics
            UOW->>DB: COMMIT
            H-->>Client: 204 No Content
        end
    end
```

Source: `internal/application/usecases/deactivate_path.go`,
`internal/domain/processpath/process_path.go` (`Deactivate`),
`internal/adapters/inbound/http/server.go` (`handleDeactivatePath`).
Omits: 500 branches. The row is never deleted; `DELETE` is a soft
deactivation.

## 4. Define or revise a site's CPT schedule

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant H as http.Server handleDefineCPTSchedule
    participant UC as usecases.DefineCPTSchedule
    participant SREPO as postgres.CPTScheduleRepo
    participant PREPO as postgres.ProcessPathRepo
    participant AGG as cptschedule.CPTSchedule
    participant UOW as postgres.UnitOfWork
    participant OB as postgres.OutboxPublisher
    participant DB as Postgres

    Client->>H: PUT /sites/sp1/cpt-schedule
    H->>H: toCutoffs - cptschedule.NewCutoff per cutoff
    alt a cutoff is invalid
        H-->>Client: 422 problem+json
    end
    H->>UC: Execute(siteId, timezone, cutoffs)
    UC->>SREPO: FindBySiteID(siteId)
    loop each distinct eligiblePathId
        UC->>PREPO: FindByID(pathId)
    end
    alt a path is unknown or not ACTIVE
        UC-->>H: ErrIneligiblePathId
        H-->>Client: 422 ineligible-path-id
    else no schedule yet
        UC->>AGG: Define(siteId, timezone, cutoffs, now)
    else schedule exists
        UC->>AGG: Revise(timezone, cutoffs, now)
        alt identical
            AGG-->>UC: changed false
            H-->>Client: 200 stored schedule, no event
        end
    end
    Note over UC,AGG: Define or Revise may still fail with a schedule-level Err sentinel, answered 422
    UC->>UOW: Execute(fn)
    UOW->>SREPO: Save(schedule)
    SREPO->>DB: UPSERT cpt_schedules WHERE version matches
    alt version moved on
        H-->>Client: 409 concurrent-modification
    else saved
        SREPO->>DB: DELETE cpt_schedule_cutoffs, INSERT each cutoff
        UOW->>OB: Publish(CPTScheduleChanged full snapshot)
        OB->>DB: INSERT outbox_events, events and analytics topics
        UOW->>DB: COMMIT
        H-->>Client: 200 cptScheduleResponse
    end
```

Source: `internal/adapters/inbound/http/server.go`
(`handleDefineCPTSchedule`, `toCutoffs`),
`internal/application/usecases/cpt_schedule.go`
(`DefineCPTSchedule.Execute`, `validateEligiblePathIds`),
`internal/domain/cptschedule/cpt_schedule.go`,
`internal/domain/cptschedule/events.go` (`ToSnapshot`),
`internal/adapters/outbound/postgres/cpt_schedule_repo.go` (`Save`).
Omits: 500 branches.

## 5. Outbox relay to Kafka

```mermaid
sequenceDiagram
    autonumber
    participant R as postgres.OutboxRelay
    participant DB as Postgres
    participant K as kafka.Publisher
    participant T as Kafka
    participant C as consumers

    loop every OUTBOX_RELAY_INTERVAL, default 1s
        R->>DB: BEGIN, SELECT unpublished rows ORDER BY id FOR UPDATE SKIP LOCKED
        loop each row in id order
            R->>K: Send(topic, key, CloudEvent value)
            K->>T: WriteMessages with content-type header, Hash balancer, RequireAll acks
            alt send failed
                R->>DB: UPDATE attempts and last_error, COMMIT
                Note over R,DB: pass stops here so later rows never overtake
            else sent
                R->>DB: UPDATE published_at
            end
        end
        R->>DB: COMMIT
    end
    T-->>C: events topic to the five sibling consumers
    T-->>C: analytics topic to pathmgmt-projector
```

Source: `internal/adapters/outbound/postgres/outbox_relay.go`
(`RelayOnce`), `internal/adapters/outbound/kafka/publisher.go`
(`NewPublisher`, `Send`), `internal/adapters/outbound/kafka/writer_config.go`,
`cmd/pathmgmt/main.go`. Omits: the outbox lag gauge and the sweeper
(ADR 0018). A crash between send and commit re-sends the row with the
same CloudEvents `id`; consumers dedupe on it.

## 6. Analytics projection (own topic)

```mermaid
sequenceDiagram
    autonumber
    participant T as Kafka analytics topic
    participant C as AnalyticsConsumer
    participant S as analyticsstore
    participant DLQ as analytics.dlq topic

    T->>C: FetchMessage, group process-path-management-analytics
    alt not a valid CloudEvent
        C->>DLQ: dead-letter with x-dlq-source-topic header
        C->>T: CommitMessages
    else type is not a ProcessPath event
        C->>T: CommitMessages, skipped
    else ProcessPath event
        C->>S: MarkProcessed(ce id) into analytics_consumed_events
        alt already seen
            C->>T: CommitMessages
        else new
            C->>S: ApplyProcessPathCreated, Updated or Deactivated
            S->>S: claim analytics_processed_events, upsert catalogue_growth_rollup
            alt still failing after 3 attempts
                C->>DLQ: dead-letter
            end
            C->>T: CommitMessages
        end
    end
```

Source: `internal/adapters/inbound/kafka/analytics_consumer.go`,
`internal/adapters/outbound/analyticsstore/{consumed_events_repo.go,postgres_projection.go}`,
`cmd/pathmgmt-projector/main.go`. Omits: retry backoff and the DLQ
topic-readiness wait.
