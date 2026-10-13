---
id: 0029-analytics-binaries-telemetry-and-liveness
slug: /adr/0029-analytics-binaries-telemetry-and-liveness
title: 0029. Analytics binaries get telemetry and exit when the consumer loop dies
sidebar_label: 0029. Analytics binaries telemetry and liveness
description: ADR 0029 — cmd/pathmgmt-projector and cmd/pathmgmt-reports call telemetry.Setup like cmd/pathmgmt, the chart always sets LOG_LEVEL, ENVIRONMENT and SERVICE_VERSION on them, and the projector flips readiness and exits non-zero when its analytics consumer loop returns on its own (fleet gap fixes D13 and D21). Amends ADR 0007's "trace-free" clause.
---

# 0029. Analytics binaries get telemetry and exit when the consumer loop dies

## Status

Accepted. Amends [ADR 0007](./0007-analytical-data-product.md)'s statement
that the analytics publisher and consumer are trace-free "because this
service has no observability package for them"; extends the projector
shutdown sequence of [ADR 0012](./0012-kafka-dlq-and-graceful-shutdown.md)
without changing its order.

## Context

The analytics data product (ADR 0007) ships two extra binaries:
`cmd/pathmgmt-projector` (the single writer: consumes
`warehouse.process-path-management.analytics` and projects into the
analytical database) and `cmd/pathmgmt-reports` (the read-only REST reader).
The 2026-10-12 ecosystem-gaps review found two problems in them.

1. **A dead projector looks healthy.** The consumer loop runs in a
   goroutine. When `AnalyticsConsumer.Run` returns on its own (an offset
   commit fails, a dead-letter write fails, the broker goes away), the
   goroutine logged `analytics consumer stopped` and the process carried
   on: `/healthz` and `/readyz` kept answering 200, so Kubernetes never
   restarted the pod while the report silently stopped moving. The only
   signal was one log line.
2. **The analytics binaries have no telemetry.** ADR 0007 justified this
   with "this service has no observability/OTel package for them". That
   is no longer true: `internal/adapters/outbound/telemetry` is used by
   `cmd/pathmgmt` and `cmd/mcp`, and the chart already injects
   `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME`, `SERVICE_VERSION`
   and `ENVIRONMENT` into the projector and reports pods, which the
   binaries ignored. The reports pod got `ENVIRONMENT` only when
   `otel.enabled=true`. Every other projector/reports pair in the fleet
   sets up telemetry.

## Decision

- **Projector liveness (gap fix D13).** The consumer goroutine reports
  `Run`'s result on a channel. `run()` waits for either a shutdown signal
  (SIGINT/SIGTERM: a normal stop, no error) or the consumer exit. A consumer
  exit is always an error (`analytics consumer loop exited`, wrapping the
  consumer's own error; a `nil` return without cancellation is just as
  dead). The projector then runs the same shutdown sequence as a SIGTERM
  (ADR 0012: readiness to not-ready first, drain the admin server, wait for
  the consumer, close the reader/DLQ writer and the pool last) and `run()`
  returns the error, so `main` exits 1 and Kubernetes restarts the pod.
  Committed offsets make the restart resume where the loop stopped.
- **Telemetry parity (gap fix D21).** Both binaries call
  `telemetry.Setup` exactly like `cmd/pathmgmt` (service names
  `pathmgmt-projector` and `pathmgmt-reports` by default, overridable with
  `OTEL_SERVICE_NAME`), defer the bounded flush, log `telemetry configured`
  and log through `telemetry.NewTraceHandler`. Export stays non-blocking:
  an unreachable Collector only drops telemetry. The consumer and the
  reports router still open no spans; this record does not add
  instrumentation, only the providers, the propagator and Go runtime
  metrics.
- **Chart.** The projector and reports Deployments always set `LOG_LEVEL`,
  `ENVIRONMENT` and `SERVICE_VERSION`; `OTEL_EXPORTER_OTLP_ENDPOINT` and
  `OTEL_SERVICE_NAME` stay behind `otel.enabled`, as for the api and mcp.

## Consequences

**Easier**

- A broken broker or analytics database now shows up as pod restarts and,
  if it persists, `CrashLoopBackOff`: the standard Kubernetes alerts catch
  it without a log-based rule.
- The analytics pods appear in the Collector with a service name, version
  and environment, and report Go runtime metrics like the other binaries.
- Tests: `TestAwaitStop_ConsumerLoopExitIsAnError`,
  `TestAwaitStop_ConsumerLoopReturningNilIsStillAnError`,
  `TestAwaitStop_SignalIsAGracefulStop`,
  `TestShutdown_FlipsReadinessAndReleasesEverything` and
  `TestSetupTelemetry_InstallsTheSDKProviders` (projector and reports).

**Harder**

- A projector whose broker is down for a long time now restart-loops
  instead of idling. That is the intended signal; the backoff is
  Kubernetes'.
- Two more processes export to the Collector (runtime metrics every 30 s).
- Spans for the consumer and the reports routes remain a separate,
  future decision.
