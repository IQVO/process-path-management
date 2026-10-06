---
id: 0016-cloudevents-mandatory-event-envelope
slug: /adr/0016-cloudevents-mandatory-event-envelope
title: "16. CloudEvents 1.0 as the mandatory event envelope"
sidebar_label: "16. CloudEvents 1.0 envelope"
sidebar_position: 16
description: "ADR 0016 — every Kafka message process-path-management produces or consumes (integration warehouse.process-path-management.events AND analytics warehouse.process-path-management.analytics) is a CloudEvents 1.0 event in structured content mode. The flat envelope (event_id/event_type/occurred_at/source/data) and the analytics Envelope v1 (schema_version) are removed with no coexistence. Fleet-wide standard, adopted 2026-09-30."
---

# 16. CloudEvents 1.0 as the mandatory event envelope

## Status

**Accepted** (2026-09-30) — fleet-wide standard, implemented in this
service in the same change that introduces this record.

Supersedes the flat integration envelope this service has published
since ADR 0002/0003 and the analytics "Envelope v1" wrapper of
[ADR 0007](./0007-analytical-data-product.md) §1 (its `schema_version`
field is replaced by `dataschema`). Every other part of ADR 0007 stands.

## Context

Every warehouse-systems publisher wrapped its events in a home-grown,
CloudEvents-*like* envelope (`event_id`, `event_type`, `occurred_at`,
`source`, `data`), and the analytics topics added a `schema_version`
field. "Like" was the problem: `event_type` carried a bare name
(`ProcessPathCreated`) that is only unique within one topic, `source`
was a bare repo name rather than a URI-reference, there was no `subject`,
and each repo had its own decoder struct. Two repos (fulfillment-execution
ADR-0027, wes-work-planning ADR-0021) had started dual-envelope
migrations with an `EVENT_ENVELOPE_MODE` toggle, which would have left
the fleet with three shapes on the wire.

process-path-management is the publisher of the fleet's most widely
consumed contract — `ProcessPathCreated/Updated/Deactivated` are read by
four services (fulfillment-execution, wes-work-planning,
workforce-management, order-management) and `CPTScheduleChanged` by
order-management — so its type strings must be byte-identical on both
sides of every consumer.

## Decision

**Every Kafka message this service writes or reads is a CloudEvents 1.0
event. There is no flat envelope, no dual-write, no dual-read and no
envelope toggle.**

### Encoding

- CloudEvents Kafka protocol binding, **structured content mode**: the
  message value is the JSON event format.
- Every produced message carries the Kafka header
  `content-type: application/cloudevents+json; charset=UTF-8`.
- The Kafka key is unchanged (the aggregate id: `path_id` or `site_id`)
  and the writers keep `kafkago.Hash{}` (ADR 0013).
- W3C trace context, where present, stays in `traceparent`/`tracestate`
  headers; it is never duplicated into extension attributes.
- Events are built, validated and (un)marshalled with the official SDK
  event package `github.com/cloudevents/sdk-go/v2/event` (v2.16.2). The
  sdk-go protocol/client packages are not used; transport stays
  `segmentio/kafka-go`. The only place that touches the SDK is
  `internal/adapters/kafka/cloudevents` (`New`, `Decode`,
  `ContentTypeHeader`, plus this service's `Type*` constants).

### Context attributes (all required)

| attribute | value |
|---|---|
| `specversion` | `1.0` |
| `id` | UUID v4 minted **once** per domain event by `OutboxPublisher` and persisted as `outbox_events.event_id`; the relay republishes the stored bytes, so a redelivery carries the same `id`. The same `id` is shared by the integration and analytics rows of one occurrence. |
| `source` | `/warehouse/process-path-management` |
| `type` | `com.warehouse.wes.process-path-management.<entity>.<EventName>` |
| `subject` | aggregate id — `path_id` for ProcessPath*, `site_id` for CPTScheduleChanged (equal to the Kafka key) |
| `time` | the domain event's occurred-at, UTC, RFC 3339 |
| `datacontenttype` | `application/json` |
| `dataschema` | `urn:warehouse:process-path-management:<events\|analytics>:<EventName>:v1` |

`data` is byte-for-byte the payload this service published before
(`ProcessPathData`, `CPTScheduleData`); this is an envelope migration,
not a payload change. No extension attributes are used.

### Published types (cross-service contract)

| type | entity | consumers |
|---|---|---|
| `com.warehouse.wes.process-path-management.processpath.ProcessPathCreated` | `processpath` | fulfillment-execution, wes-work-planning, workforce-management, order-management |
| `com.warehouse.wes.process-path-management.processpath.ProcessPathUpdated` | `processpath` | same four |
| `com.warehouse.wes.process-path-management.processpath.ProcessPathDeactivated` | `processpath` | same four |
| `com.warehouse.wes.process-path-management.cptschedule.CPTScheduleChanged` | `cptschedule` | order-management |

The same type names the occurrence on both topics; `dataschema` names
the shape. A breaking payload change ships as a new `.v2` type with a new
dataschema version, never by mutating an existing one.

### Consumer rules (this service's analytics projector)

1. Decode with the SDK and `Validate()`. A message that is not a valid
   CloudEvents 1.0 event (legacy flat shape, missing `specversion`, bad
   JSON) is a deterministic poison message: it goes to the existing
   `<topic>.dlq` (ADR 0012) with no retry and the offset is committed.
   The consumer never crashes, never blocks the partition and never
   falls back to parsing a flat envelope.
2. Dispatch on the **full** `type`; unknown types (e.g.
   `CPTScheduleChanged` on the analytics topic) are ignored.
3. Dedupe on the CloudEvents `id` (`analytics_processed_events.event_id`
   keeps its column name, now populated from `id`).
4. `time`/`subject` come from context attributes; payload via `DataAs`.

### Fleet type catalogue (other services' strings)

This service consumes no other service's topic (it has zero inbound
dependencies, ADR 0001). The fleet-wide catalogue of cross-service type
strings lives in the fleet standard
(`warehouse-systems/.hermes/cloudevents/STANDARD.md` §4) and in each
publisher's `apis/asyncapi.yaml`.

## Consequences

**Easier**

- One envelope, one decoder, one validation path across the fleet;
  consumers dispatch on globally unique type strings instead of
  per-topic short names.
- `subject` lets consumers and tooling see which aggregate an event is
  about without parsing `data`.
- Off-the-shelf CloudEvents tooling (SDKs, schema registries, Kafka UI
  plugins) understands the wire format.
- Golden exact-JSON tests pin every attribute of every published type,
  so an accidental contract drift fails CI.

**Harder**

- **Breaking wire change with no coexistence.** This service's PR must
  deploy together with the fleet cutover: drain the outbox (existing
  rows hold flat-encoded bytes), delete and recreate
  `warehouse.process-path-management.events` and `.analytics`, re-seed
  the catalogue, then roll out all services as one set. See
  warehouse-infra `docs/cloudevents-cutover.md`.
- Any old flat message still on a topic after a botched cutover is
  dead-lettered by the projector, not projected; sibling consumers
  likewise skip it.
- `outbox_events.event_type` now stores the full CloudEvents type
  string (operational queries filtering on the short name must be
  updated).
- A new dependency, `github.com/cloudevents/sdk-go/v2` (event package
  only).

### Note (2026-10-04): one id per occurrence on the direct fan-out path

On the dev-only no-Postgres path (`EVENT_PUBLISHER=kafka`, no
`DATABASE_URL`), the composition root fans each event out to the
integration and analytics publishers directly. When that path was first
wired each publisher minted its own `id`, so the two topics carried
*different* ids for the same occurrence — diverging from
`postgres.OutboxPublisher`, which mints one `id` and reuses it for every
topic's row (its `newId` doc comment: "so a redelivery carries the same
id on every topic it was enqueued for"). Fixed by
`kafka.NewSharedIdFanOut`: the fan-out mints ONE id per occurrence and
publishes under it on both topics (`Publisher.PublishWithId` /
`AnalyticsPublisher.PublishWithId`). In-cluster behaviour (Postgres
configured) was never affected — the outbox path already shared the id.
