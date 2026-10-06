---
title: Entity Relationship
sidebar_label: Entity Relationship
sidebar_position: 10
description: ER diagrams of process-path-management's final Postgres schemas — the OLTP database (process paths, CPT schedules, outbox, idempotency keys) and the separate analytics database — with the table-to-aggregate mapping.
---

# Entity Relationship

The final schema after applying every migration in order. Part of the
[DDD artifact pack](./ddd-artifacts.md). There are **two databases**:
the OLTP database (`DATABASE_URL`, `migrations/0001`–`0007`) and the
separate analytics database (`ANALYTICS_DATABASE_URL`,
`migrations/analytics/0001`), written only by `pathmgmt-projector` and
read only by `pathmgmt-reports` (ADR 0007). Both are migrated with
golang-migrate, which keeps its own `schema_migrations` table in each.

## OLTP database

```mermaid
erDiagram
    process_paths {
        text id PK
        text match_prefix
        boolean direct
        text_array required_capabilities
        text status "ACTIVE or DEACTIVATED"
        timestamptz created_at
        timestamptz updated_at
        text destination_location_role "nullable, Drop WorkCenter Shipping"
        interval cycle_time_p95
        jsonb eligibility
        integer version
    }

    cpt_schedules {
        text site_id PK
        text timezone
        timestamptz created_at
        timestamptz updated_at
        integer version
    }

    cpt_schedule_cutoffs {
        text schedule_site_id PK,FK
        text cpt_id PK
        text local_time
        text_array days_of_week
        text ship_method
        text_array eligible_path_ids
    }

    outbox_events {
        bigserial id PK
        uuid event_id UK "unique with topic"
        text event_type
        text aggregate_id
        jsonb payload
        timestamptz occurred_at
        timestamptz created_at
        timestamptz published_at "nullable"
        integer attempts
        text last_error "nullable"
        text topic UK "unique with event_id"
    }

    idempotency_keys {
        text key PK
        text method
        text path
        text request_hash
        integer status_code "nullable"
        bytea response_body "nullable"
        jsonb response_headers "nullable"
        timestamptz created_at
        timestamptz completed_at "nullable"
    }

    schema_migrations {
        bigint version PK
        boolean dirty
    }

    cpt_schedules ||--|{ cpt_schedule_cutoffs : "has cutoffs, ON DELETE CASCADE"
```

Source: `migrations/0001_init.up.sql` through
`migrations/0007_version.up.sql`; `schema_migrations` is golang-migrate's
own table (`internal/adapters/outbound/postgres/migrate.go`). `text_array`
stands for Postgres `TEXT[]`. Omits: indexes
(`idx_process_paths_active` partial on `status = 'ACTIVE'`,
`idx_outbox_events_unpublished` partial on `published_at IS NULL`,
`idx_idempotency_keys_created_at`) and the `CHECK` constraints on
`status` and `destination_location_role`.

The only `FOREIGN KEY` in the schema is
`cpt_schedule_cutoffs.schedule_site_id → cpt_schedules.site_id`. Three
logical links deliberately have **no** foreign key:

- `cpt_schedule_cutoffs.eligible_path_ids → process_paths.id` is an
  aggregate boundary. ADR 0010 checks it at write time in the
  `DefineCPTSchedule` use case, "not by a foreign key", and it is stored as
  a `TEXT[]` that is never queried on its own.
- `outbox_events.aggregate_id` holds a `path_id` or `site_id` (the Kafka
  key). The outbox is infrastructure and keeps rows independent of the
  aggregate tables; the sweeper deletes published rows after a retention
  period (ADR 0018).
- `idempotency_keys` stores a recorded HTTP response, not a reference to
  the path it created.

## Analytics database

```mermaid
erDiagram
    analytics_processed_events {
        text event_id PK
        timestamptz occurred_at
        timestamptz applied_at
    }

    analytics_consumed_events {
        text event_id PK
        timestamptz processed_at
    }

    catalogue_growth_rollup {
        timestamptz day_bucket PK
        bigint paths_defined
        bigint paths_revised
        bigint paths_deactivated
    }

    schema_migrations {
        bigint version PK
        boolean dirty
    }
```

Source: `migrations/analytics/0001_report.up.sql`. Omits: the indexes on
`occurred_at` and `day_bucket`. There are no foreign keys: the two dedupe
tables hold CloudEvents `id`s, and the rollup is bucketed by UTC day.

## Table ≠ aggregate

| Table | What it is | Aggregate / owner |
| --- | --- | --- |
| `process_paths` | Domain state | `ProcessPath` aggregate (one row per aggregate) |
| `cpt_schedules` | Domain state | `CPTSchedule` aggregate root |
| `cpt_schedule_cutoffs` | Domain state | `Cutoff` entities inside `CPTSchedule`; replaced wholesale on every `CPTScheduleRepo.Save` (delete then insert) |
| `outbox_events` | Infrastructure: transactional outbox (ADR 0003, topic column from ADR 0007) | One row per event per topic, written in the aggregate's transaction |
| `idempotency_keys` | Infrastructure: `Idempotency-Key` replay cache (ADR 0011), swept by TTL (ADR 0018) | none — HTTP adapter |
| `schema_migrations` | Infrastructure: golang-migrate bookkeeping | none |
| `analytics_processed_events` | Analytics projection dedupe and freshness | `pathmgmt-projector` read model |
| `analytics_consumed_events` | Analytics consumer dedupe gate | `pathmgmt-projector` read model |
| `catalogue_growth_rollup` | Analytics projection, "Process Path Catalogue Growth & Change" | `pathmgmt-projector` read model, served by `pathmgmt-reports` |

There is no inbound `processed_events` table in the OLTP database: this
context consumes no other context's topic.
