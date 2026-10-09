---
id: observability
title: Observability
sidebar_label: Observability
description: Every OpenTelemetry metric and span process-path-management emits, its log lines and fields, how telemetry is exported, the Grafana dashboard that exists for it, and suggested alerts.
---

# Observability

## Which binaries emit what

| Binary | Traces | Metrics | Logs |
| --- | --- | --- | --- |
| `pathmgmt` | yes (otelchi HTTP server spans) | yes | JSON to stdout, `trace_id`/`span_id` on request-scoped lines |
| `mcp` | yes (one span per tool call) | Go runtime only | JSON to stdout, `trace_id`/`span_id` inside tool calls |
| `pathmgmt-projector` | no | no | JSON to stdout |
| `pathmgmt-reports` | no | no | JSON to stdout |

The projector and reports binaries never call `telemetry.Setup`, by design
([ADR 0007](../adr/0007-analytical-data-product.md): the analytics pipeline is
trace-free). Their only signals are logs, the probes and the freshness
endpoint.

## Export

There is no `/metrics` endpoint. `pathmgmt` and `mcp` push traces and metrics
over **OTLP/gRPC, insecure**, to `OTEL_EXPORTER_OTLP_ENDPOINT` (default
`localhost:4317`; the chart sets
`otel-collector.observability.svc.cluster.local:4317`)
(`internal/adapters/outbound/telemetry/telemetry.go`):

- traces: batch span processor;
- metrics: periodic reader, **every 30 s**;
- propagator: W3C `traceparent` + baggage;
- resource: `service.name` (`OTEL_SERVICE_NAME`), `service.version`
  (`SERVICE_VERSION` or ldflags), `deployment.environment.name`
  (`ENVIRONMENT`, default `local`), plus the SDK defaults.

Export never blocks startup. With no Collector you get periodic exporter
errors in the log and nothing else.

## Metrics

| Instrument | Type | Unit | Attributes | Emitted by | Source |
| --- | --- | --- | --- | --- | --- |
| `process_path_management.paths.defined` | Int64Counter | `{path}` | `outcome` = `accepted` \| `rejected` | `pathmgmt`, from the `DefinePath` use case | `internal/adapters/outbound/telemetry/metrics.go` |
| `process_path_management.outbox.lag_seconds` | Float64ObservableGauge | `s` | none | `pathmgmt`, only when `DATABASE_URL` is set | `internal/adapters/outbound/postgres/outbox_metrics.go` |
| `http.server.request.duration` | Float64Histogram | `s` | otelchi semantic-convention attributes (method, route, status code) | `pathmgmt` REST router only | `otelchimetric.NewServerRequestDuration` in `internal/adapters/inbound/http/server.go` |
| Go runtime metrics (`go.goroutine.count`, `go.memory.used`, ...) | various | various | — | `pathmgmt`, `mcp` | `runtime.Start` in `telemetry.go` |

`process_path_management.paths.defined` counts attempts to create a path.
`rejected` covers aggregate invariant failures and a duplicate `pathId`
(`internal/application/usecases/define_path.go`); a rising rejected rate
points at a broken client form, not at the service. Requests rejected by the
HTTP handler before the use case runs (malformed JSON, an unparseable
`cycleTimeP95`, an unknown `destinationLocationRole`, a missing
`Idempotency-Key`) are not counted. Revise, deactivate
and CPT schedule writes have no business counter.

`process_path_management.outbox.lag_seconds` runs one query per collection
(every 30 s): the age of the oldest row in `outbox_events` with
`published_at IS NULL`, or `0` when the outbox is drained. It is registered
whenever Postgres is configured, even with `EVENT_PUBLISHER=log`; in that
mode no rows are written, so it stays `0`.

In Prometheus (through the Collector) these appear as
`process_path_management_paths_defined_total`,
`process_path_management_outbox_lag_seconds` and
`http_server_request_duration_seconds_*`.

The MCP binary has no HTTP metrics middleware, and the reports router has
no OTel at all, so `http.server.request.duration` covers only the REST API.

## Traces

| Span | Kind | Emitted by | Attributes |
| --- | --- | --- | --- |
| one per HTTP request, named after the chi route pattern (e.g. `/process-paths/{pathId}`), not the raw path | server | `pathmgmt` (`otelchi.Middleware` with `WithChiRoutes`) | otelchi HTTP semantic-convention attributes |
| `mcp.tool get_process_path`, `mcp.tool list_process_paths`, `mcp.tool get_cpt_schedule`, `mcp.tool get_catalogue_growth_report` | internal | `mcp` (`internal/adapters/inbound/mcp/tools.go`) | `mcp.tool.name`; status `Error` with the message when the tool fails |

There are no database or Kafka client spans (pgx and kafka-go are not
instrumented). Instead, the trace context of the request that raised an
event is stored on its `outbox_events` row (`traceparent`, `tracestate`)
and sent as Kafka headers when the relay publishes, so a consumer can
continue the trace started by the operator's REST call
([ADR 0027](../adr/0027-w3c-trace-context-on-kafka-headers.md)). The relay
itself starts no span.

## Logs

All four binaries log JSON (`log/slog`) to stdout at `LOG_LEVEL`. In
`pathmgmt` and `mcp` the handler is wrapped by `telemetry.TraceHandler`,
which adds `trace_id` and `span_id` to any record logged with a context that
carries a valid span.

| Message | Level | Fields | Emitted by |
| --- | --- | --- | --- |
| `http request` | INFO | `method`, `route` (pattern; raw path with CR/LF stripped on 404), `status`, `bytes`, `duration_ms`, `request_id` | `pathmgmt`, `pathmgmt-reports` (`internal/adapters/inbound/http/logging.go`) |
| `domain event published` | INFO | `event_name`, `payload` | `pathmgmt` with `EVENT_PUBLISHER=log` |
| `telemetry configured` | INFO | `service_name`, `service_version`, `environment`, `otlp_endpoint` | `pathmgmt` (mcp logs `service_name`, `otlp_endpoint`) |
| `http server listening` / `mcp server listening (Streamable HTTP)` / `reports server listening` / `projector admin server listening` | INFO | `addr` | each binary |
| `kafka event publishing enabled (transactional outbox)` or `(direct, no outbox: DATABASE_URL not set)` | INFO | `brokers`, `topic`, `analytics_topic` | `pathmgmt` |
| `outbox relay running` | INFO | `topic` | `pathmgmt` |
| `outbox relay pass failed` | ERROR | `error` | `pathmgmt` |
| `outbox relay published events` | DEBUG | `count` | `pathmgmt` |
| `outbox relay did not stop before the shutdown deadline` | WARN | — | `pathmgmt` |
| `outbox lag gauge registration failed` | WARN | `error` | `pathmgmt` |
| `housekeeping sweeper running` | INFO | — | `pathmgmt` |
| `housekeeping sweep` | INFO | `idempotency_keys_deleted`, `outbox_events_deleted` (only when something was deleted) | `pathmgmt` |
| `housekeeping sweep failed` | ERROR | `error`, `idempotency_keys_deleted`, `outbox_events_deleted` | `pathmgmt` |
| `retrying` | WARN | `op` (`run migrations`, `ping database`, `run analytics migrations`, `ping analytics database`), `attempt`, `in`, `err` | all, at boot |
| `succeeded after retry` | INFO | `op`, `attempt` | all, at boot |
| `analytics consumer starting` | INFO | `topic`, `group`, `brokers` | projector |
| `analytics: rejecting non-CloudEvents kafka message` | WARN | `topic`, `partition`, `offset`, `error` | projector |
| `analytics: sending message to dead-letter topic` | ERROR | `dlq_topic`, `ce_id`, `ce_type`, `error` | projector |
| `analytics consumer stopped` | ERROR | `error` | projector (the consumer loop has exited; see [Troubleshooting](./troubleshooting.md)) |
| `analytics consumer did not stop before the shutdown deadline` | WARN | — | projector |
| `service exited with error` / `mcp server exited with error` / `pathmgmt-projector exited with error` / `pathmgmt-reports exited with error` | ERROR | `error` | each binary, last line before exit 1 |

## Dashboards

warehouse-infra provisions a Grafana dashboard for this context,
`terraform/dashboards/contexts/process-path-management.json` ("Warehouse —
Process Path Management", checked on warehouse-infra `develop` at
`f9ec4cd`). Its rows:

- Kong north-south view of the `/api/process-path-management` route:
  request rate, 5xx ratio, p95 latency (Kong vs upstream);
- service HTTP RED from `http_server_request_duration_seconds` filtered on
  `service_name=~"process-path-management.*"`: rate by route, 5xx by
  process, p95 by process;
- business: `sum by (outcome) (rate(process_path_management_paths_defined_total[5m]))`;
- Go runtime: goroutines and memory used;
- Loki logs for `app="process-path-management"` (application containers
  only): live stream, volume by level, errors and warnings.

The HTTP RED row title mentions projector, reports and MCP, but only
`pathmgmt` emits that histogram (see above). The outbox lag gauge is not on
the dashboard.

## Suggested alerts

These are not provisioned anywhere today.

| Alert | Expression (PromQL / LogQL) | Why |
| --- | --- | --- |
| Outbox stuck | `max(process_path_management_outbox_lag_seconds) > 60` for 5 m | The relay is failing or not running; consumers are not seeing changes. Normal value is under ~1 s. |
| REST 5xx | 5xx ratio of `http_server_request_duration_seconds_count{service_name="process-path-management"}` > 5 % for 10 m | Database unreachable or statement timeouts (`statement_timeout=5s`). |
| Path definitions rejected | `rate(process_path_management_paths_defined_total{outcome="rejected"}[15m]) > rate(...{outcome="accepted"}[15m])` | A client is sending malformed definitions. |
| Analytics consumer dead | LogQL: `count_over_time({app="process-path-management"} \|= "analytics consumer stopped" [5m]) > 0` | The projector process stays up and ready after its consumer loop exits. |
| DLQ writes | LogQL on `analytics: sending message to dead-letter topic`, or a broker-side message-rate alert on `warehouse.process-path-management.analytics.dlq` | Poison messages in the analytics stream. |
| Boot retries | LogQL on `"msg":"retrying"` | Postgres slow to accept connections at pod start (Istio sidecar warm-up or a real outage). |

Consumer lag of group `process-path-management-analytics` can only be
measured on the broker side (for example a Kafka exporter); the service does
not export it.
