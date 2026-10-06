---
id: 0019-standard-metrics-convention
slug: /adr/0019-standard-metrics-convention
title: 0019. Adopting the fleet standard-metrics convention
sidebar_label: 0019. Standard metrics convention
description: ADR 0019 — adopting the fleet standard-metrics convention (otelchi http.server.request.duration, runtime metrics, one <context>.<aggregate>.<verb> business counter with an outcome attribute).
---

# 0019. Adopting the fleet standard-metrics convention

## Status

Accepted — codifies instrumentation that already ships, retroactively.
This is the "fleet-standard-metrics ADR" earlier code comments cited
before this record existed (telemetry/metrics.go, server.go).

## Context

Every service in this fleet exports the same baseline: OTel HTTP server
metrics via otelchi (`http.server.request.duration` and friends), Go
runtime metrics, and one business counter per context named
`<context>.<aggregate>.<verb>` with an `outcome` attribute rather than
separate success/failure counters. workforce-management's ADR 0015 is
the fleet reference. This service implemented the convention (otelchi
middleware in `NewRouter`, `process_path_management.paths.defined` in
`internal/adapters/outbound/telemetry/metrics.go`) without its own
record — code comments cited a "fleet-standard-metrics ADR" that did
not exist in this repo.

## Decision

Adopt the convention as this repo's standing rule: otelchi middleware,
runtime metrics, and business counters named
`process_path_management.<aggregate>.<verb>` with an `outcome`
attribute. `process_path_management.outbox.lag_seconds` (ADR 0018)
follows the same prefix rule.

## Consequences

Dashboards and alerts written once for the fleet work here unchanged;
the phantom citation is now a real record. Adding a new business event
means adding a counter following the naming rule, not inventing a new
shape.
