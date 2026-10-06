---
title: Domain Events
sidebar_label: Domain Events
sidebar_position: 11
description: Every event process-path-management publishes and consumes — full CloudEvents type, topic, partition key, payload fields, producer use case and known consumers.
---

# Domain Events

Every event this context publishes, with its full CloudEvents 1.0 `type`,
topic, partition key, payload, producing use case and known consumers.
Part of the [DDD artifact pack](./ddd-artifacts.md).

## Envelope (all events)

Every message is a CloudEvents 1.0 event in **structured content mode**
(the Kafka value is the JSON event; ADR 0016), built only by
`internal/adapters/kafka/cloudevents.New`:

| Attribute | Value |
| --- | --- |
| `specversion` | `1.0` |
| `id` | UUID minted once per occurrence (`uuid.NewString`), shared by the integration and analytics copies and stable across outbox redelivery |
| `source` | `/warehouse/process-path-management` |
| `type` | `com.warehouse.wes.process-path-management.<entity>.<EventName>` |
| `subject` | aggregate id — `path_id` or `site_id`; equal to the Kafka key |
| `time` | the domain event's `At`, UTC |
| `datacontenttype` | `application/json` |
| `dataschema` | `urn:warehouse:process-path-management:events:<EventName>:v1` (integration) or `urn:warehouse:process-path-management:analytics:<EventName>:v1` (analytics) |
| Kafka header | `content-type: application/cloudevents+json; charset=UTF-8` (the only header set) |

Topics: `warehouse.process-path-management.events` (integration) and
`warehouse.process-path-management.analytics` (analytics, ADR 0007). Every
event is written to **both** — in one outbox transaction when
`DATABASE_URL` is set, or by `SharedIdFanOut` direct to Kafka otherwise.
Writers use the `kafkago.Hash` balancer (ADR 0013), so all events with the
same key land on the same partition.

## Published events

| Event | Full CloudEvents `type` | Partition key / `subject` | Producer use case | Known consumers |
| --- | --- | --- | --- | --- |
| ProcessPathCreated | `com.warehouse.wes.process-path-management.processpath.ProcessPathCreated` | `path_id` | `usecases.DefinePath` | fulfillment-execution, wes-work-planning, workforce-management, order-management, network-fulfillment; own `pathmgmt-projector` (analytics) |
| ProcessPathUpdated | `com.warehouse.wes.process-path-management.processpath.ProcessPathUpdated` | `path_id` | `usecases.RevisePath` (only when `Revise` returns `changed=true`) | same five; own projector |
| ProcessPathDeactivated | `com.warehouse.wes.process-path-management.processpath.ProcessPathDeactivated` | `path_id` | `usecases.DeactivatePath` (only on an Active path) | same five; own projector |
| CPTScheduleChanged | `com.warehouse.wes.process-path-management.cptschedule.CPTScheduleChanged` | `site_id` | `usecases.DefineCPTSchedule` (first define, or a revise with `changed=true`) | order-management, network-fulfillment; own projector skips it |

### ProcessPathCreated / ProcessPathUpdated — `data`

Go type `kafka.ProcessPathData` (`internal/adapters/outbound/kafka/publisher.go`);
schema `ProcessPathData` in `apis/asyncapi.yaml`.

| Field | Type | Notes |
| --- | --- | --- |
| `path_id` | string | always present |
| `match_prefix` | string | `omitempty` |
| `direct` | boolean | `omitempty` — **absent when `false`**; consumers must default it to `false` |
| `required_capabilities` | string array | `omitempty` |
| `destination_location_role` | string | `Drop` / `WorkCenter` / `Shipping`; absent when unset (ADR 0009) |
| `cycle_time_p95` | string | Go duration, e.g. `2h0m0s` (ADR 0010) |
| `eligibility` | object | always present on Created/Updated, `{}` when fully permissive |
| `eligibility.max_units_per_line` | integer | absent = unbounded |
| `eligibility.required_product_attributes` | string array | `omitempty` |
| `eligibility.excluded_product_attributes` | string array | `omitempty` |
| `eligibility.non_sortable` | boolean | `omitempty` |

`ProcessPathUpdated` carries the full current definition (not a diff);
`destination_location_role` is repeated unchanged because it is never
revisable.

### ProcessPathDeactivated — `data`

Only `path_id`. Every definition field is omitted, not zeroed.

### CPTScheduleChanged — `data`

Go type `kafka.CPTScheduleData`; built from `cptschedule.ToSnapshot`.

| Field | Type | Notes |
| --- | --- | --- |
| `site_id` | string | |
| `timezone` | string | IANA zone |
| `cutoffs[]` | array | full snapshot, never a diff |
| `cutoffs[].cpt_id` | string | unique within the site |
| `cutoffs[].local_time` | string | `HH:MM` |
| `cutoffs[].days_of_week` | string array | `Mon`..`Sun` |
| `cutoffs[].ship_method` | string | |
| `cutoffs[].eligible_path_ids` | string array | Active paths at write time |

## Events NOT raised

- A no-op `PUT /process-paths/{pathId}` (identical `matchPrefix`,
  `requiredCapabilities`, `cycleTimeP95`, `eligibility`) raises nothing.
- `DELETE` on an already-deactivated path raises nothing (204).
- A `PUT /sites/{siteId}/cpt-schedule` identical to the stored schedule
  raises nothing.
- Read operations (REST `GET`, every MCP tool) never raise events.

## Consumed events

This context consumes **no other context's events**. Its only consumer,
`internal/adapters/inbound/kafka.AnalyticsConsumer` (run by
`cmd/pathmgmt-projector`, group `process-path-management-analytics`,
`StartOffset: FirstOffset`), reads its **own** analytics topic:

| Type | Effect |
| --- | --- |
| `...processpath.ProcessPathCreated` | `catalogue_growth_rollup.paths_defined += 1` for the UTC day bucket |
| `...processpath.ProcessPathUpdated` | `paths_revised += 1` |
| `...processpath.ProcessPathDeactivated` | `paths_deactivated += 1` |
| `...cptschedule.CPTScheduleChanged` and any unknown type | committed and skipped |
| non-CloudEvents message | dead-lettered to `warehouse.process-path-management.analytics.dlq`, then committed |

Dedupe is on the CloudEvents `id`, twice: `analytics_consumed_events`
(consumer gate) and `analytics_processed_events` (claimed inside the
projection transaction).

## Delivery guarantees

- **Atomic with state** (Postgres mode): `OutboxPublisher.Publish` inserts
  one `outbox_events` row per topic inside the use case's `UnitOfWork`
  transaction (ADR 0003).
- **At-least-once to Kafka**: `OutboxRelay` sends rows in `id` order with
  `RequiredAcks: RequireAll` and marks them published afterwards; a crash
  between send and mark re-sends with the same `id`.
- **Per-aggregate order**: same key → same partition (Hash balancer); the
  relay stops a pass at the first failed send so later rows never
  overtake it.

Source: `internal/domain/shared/events.go`,
`internal/domain/cptschedule/events.go`,
`internal/adapters/outbound/kafka/publisher.go`,
`internal/adapters/outbound/kafka/analytics_publisher.go`,
`internal/adapters/outbound/kafka/writer_config.go`,
`internal/adapters/kafka/cloudevents/cloudevents.go`,
`internal/adapters/kafka/cloudevents/types.go`,
`internal/adapters/outbound/postgres/outbox_publisher.go`,
`internal/adapters/outbound/postgres/outbox_relay.go`,
`internal/adapters/inbound/kafka/analytics_consumer.go`,
`apis/asyncapi.yaml`.

:::note[Spec vs code]
`apis/asyncapi.yaml` states that W3C trace context travels in
`traceparent`/`tracestate` Kafka headers; the publishers set only the
`content-type` header today. The spec also does not mention that
`direct: false` is omitted from the payload.
:::
