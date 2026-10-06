---
id: 0027-w3c-trace-context-on-kafka-headers
slug: /adr/0027-w3c-trace-context-on-kafka-headers
title: 0027. Propagate W3C trace context in Kafka headers
sidebar_label: 0027. W3C trace context on Kafka headers
description: ADR 0027 — every published message carries the traceparent/tracestate Kafka headers of the operation that raised the event, captured at enqueue time and persisted on the outbox row (migration 0008), as apis/asyncapi.yaml already promised.
---

# 0027. Propagate W3C trace context in Kafka headers

## Status

Accepted. Implements the promise in `apis/asyncapi.yaml` and ADR 0016
("W3C trace context stays in the `traceparent`/`tracestate` headers");
supersedes nothing.

## Context

`apis/asyncapi.yaml` and ADR 0016 both say trace context travels in the
`traceparent`/`tracestate` Kafka headers. It did not: `kafka.Publisher.Send`
and the `AnalyticsPublisher` set only `content-type`. A trace that started
at `POST /process-paths` therefore ended at the outbox, and the consumers'
spans (fulfillment-execution, wes-work-planning, workforce-management,
order-management) could never join it.

The constraint that shapes the design: in Postgres mode (the cluster) the
event is not sent by the request. `OutboxPublisher.Publish` enqueues a row
inside the use case's transaction and `OutboxRelay` sends it later from a
background loop whose context has no relation to the request. Injecting
the propagator at send time would therefore stamp every message with the
relay's (empty) context.

## Decision

- The trace context is captured **where the request span is still in
  `ctx`**, with the global OpenTelemetry propagator (the W3C TraceContext
  + Baggage composite `telemetry.Setup` installs): `OutboxPublisher.Publish`
  for the outbox path, `Publisher.Publish`/`PublishWithId` and
  `AnalyticsPublisher.Publish`/`PublishWithId` for the no-Postgres direct
  and fan-out paths.
- Only `traceparent` and `tracestate` are kept. Baggage is deliberately
  not put on the wire (the contract promises two headers, and baggage can
  carry arbitrary request-scoped data).
- The context travels in a new `kafka.Encoded.Trace` field (a
  `TraceContext{Traceparent, Tracestate}` value). `Publisher.Send` — the
  one place a message is written, also the relay's Sink — turns it into
  Kafka headers next to the existing `content-type`. An empty context
  writes **no** trace headers; nothing is invented.
- The CloudEvent body (`value`) is unchanged, so its bytes and the
  consumer idempotency key stay stable across redelivery.
- Persistence: migration `0008_outbox_trace_context` adds two **nullable**
  `TEXT` columns, `outbox_events.traceparent` and `outbox_events.tracestate`
  (additive; rows written before it, or outside a trace, are `NULL` and
  publish without trace headers). The same trace context is stored on
  every row of one event, so both topics carry it. The relay reads them
  back into `Encoded.Trace`.
- A redelivery after a relay crash re-sends the row with the same stored
  trace headers as the first attempt.
- Both topics (`warehouse.process-path-management.events` and
  `.analytics`) get the same headers; `apis/asyncapi.yaml` now says so,
  including that a message raised outside any trace has none.

This is an additive contract change: a new optional Kafka header. Consumers
that ignore headers are unaffected.

## Consequences

**Easier**

- A trace started at a REST/MCP request continues through the outbox into
  the consumers' spans; the AsyncAPI promise is true.
- No change to the CloudEvents envelope, dataschemas or event types.

**Harder**

- Two more columns on a hot, short-lived table (rows are swept after the
  retention window, ADR 0018). Negligible.
- The headers reflect the trace of the operation that **raised** the
  event, not the relay pass that delivered it. That is intended, but a
  consumer seeing a long gap between the parent span and its own span is
  seeing outbox lag, not a tracing bug.
- Events raised by code with no active span (none today in the use cases)
  stay untraced rather than starting an artificial root.
