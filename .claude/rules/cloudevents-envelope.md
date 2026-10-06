---
paths:
  - "internal/adapters/**"
  - "internal/domain/shared/**"
  - "apis/asyncapi.yaml"
  - "features/**"
---

## Events: CloudEvents 1.0 is MANDATORY

Every Kafka message this service produces or consumes (integration
`warehouse.<ctx>.events` AND analytics `warehouse.<ctx>.analytics`) is a
CloudEvents 1.0 event in structured content mode. This is a hard fleet rule,
not a preference:

- No flat envelope (`event_id`/`event_type`/`occurred_at`), no dual-write,
  no dual-read, no envelope toggle env var (`EVENT_ENVELOPE_MODE` is gone).
- Build/validate/(un)marshal with `github.com/cloudevents/sdk-go/v2/event`
  via `internal/adapters/kafka/cloudevents/`; transport stays kafka-go.
- Kafka header `content-type: application/cloudevents+json; charset=UTF-8`.
- Required attributes: `specversion=1.0`, `id` (UUID, stable across outbox
  redelivery), `source=/warehouse/process-path-management`, `type`, `subject` (aggregate id), `time`
  (occurred-at, UTC), `datacontenttype=application/json`,
  `dataschema=urn:warehouse:process-path-management:<events|analytics>:<EventName>:v<N>`.
- `type` = `com.warehouse.<subdomain>.<bounded-context>.<entity>.<EventName>`;
  for this service: `com.warehouse.wes.process-path-management.<entity>.<EventName>`. Breaking payload
  change => new `.v2` type + new dataschema version, never mutate.
- Consumers dispatch on the FULL `type`, ignore unknown types, dedupe on
  `id`, and DLQ/skip (never crash, never parse a legacy shape) anything that
  fails CloudEvents validation.

Full standard and the fleet's cross-service type catalogue: ADR-0016
(`docs/docs/adr/`).

This service's published types (consumed byte-for-byte by
fulfillment-execution, wes-work-planning, workforce-management,
order-management, network-fulfillment): `...processpath.ProcessPathCreated`,
`...processpath.ProcessPathUpdated`, `...processpath.ProcessPathDeactivated`
(subject = `path_id`) and `...cptschedule.CPTScheduleChanged` (subject =
`site_id`), all under `com.warehouse.wes.process-path-management`. Its own
projector DLQs any non-CloudEvents message on the analytics topic.
