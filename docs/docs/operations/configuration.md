---
id: configuration
title: Configuration
sidebar_label: Configuration
description: Every environment variable each of the four process-path-management binaries reads, with default, whether it is required, what it does and where it is read.
---

# Configuration

Every binary is configured only through environment variables. They are read
in the binary's composition root (`cmd/<binary>/main.go`), with two
exceptions read inside adapters: `CORS_ALLOWED_ORIGINS`
(`internal/adapters/inbound/http/server.go`) and `ENVIRONMENT`
(`internal/adapters/outbound/telemetry/telemetry.go`). There are no config
files and no flags.

An empty value is treated the same as an unset value for every variable
below: each binary uses a `getenv(key, fallback)` helper that returns the
fallback when `os.Getenv` returns `""`.

## `pathmgmt` (REST API, outbox relay, sweeper)

Source: `cmd/pathmgmt/main.go` unless noted.

| Variable | Default | Required | Meaning |
| --- | --- | --- | --- |
| `HTTP_ADDR` | `:8080` | no | Listen address of the REST API (`/process-paths`, `/sites/{siteId}/cpt-schedule`, `/healthz`, `/readyz`). |
| `DATABASE_URL` | **required** | yes | OLTP Postgres DSN for the runtime `pgxpool` (10 connections, `statement_timeout=5s`). Unset stops the boot: `DATABASE_URL is required: this context runs on real Postgres` ([ADR 0037](../adr/0037-postgres-and-kafka-are-the-only-runtime.md)). |
| `MIGRATIONS_DATABASE_URL` | value of `DATABASE_URL` | no | DSN used **only** by the golang-migrate step at boot. Set it to a direct (non-PgBouncer) connection when `DATABASE_URL` goes through PgBouncer in transaction mode, because golang-migrate takes a session-scoped advisory lock ([ADR 0015](../adr/0015-migrations-direct-postgres-connection.md)). |
| `MIGRATIONS_PATH` | `migrations` | no | Directory of the OLTP `*.up.sql` files, opened as `file://<path>` (`internal/adapters/outbound/postgres/migrate.go`). Relative to the working directory. |
| `EVENT_PUBLISHER` | `kafka` | no | Kept only because the chart and warehouse-infra set it: unset or `kafka` (case-insensitive); `log` or any other value stops the boot. There is no log publisher and no direct fan-out ([ADR 0037](../adr/0037-postgres-and-kafka-are-the-only-runtime.md)). |
| `KAFKA_BROKERS` | **required** | yes | Comma-separated broker list for the integration and analytics writers. Unset stops the boot: `KAFKA_BROKERS is required: this context runs on real Kafka`. There is no `localhost:9092` default. |
| `OUTBOX_RELAY_INTERVAL` | `1s` | no | Idle poll interval of the (always-on) outbox relay (Go duration). Malformed, zero or negative values fall back to `1s`. |
| `OUTBOX_MAX_ATTEMPTS` | `20` | no | Failed sends after which the outbox relay stops claiming a row ([ADR 0030](../adr/0030-outbox-relay-attempt-cap.md)). `0` = unlimited (the pre-cap behaviour); an unparseable or negative value falls back to `20` with a warning. The parked row stays in `outbox_events`; re-arm it with `UPDATE outbox_events SET attempts = 0 WHERE id = ...` ([Runbook](./runbook.md#re-arm-an-outbox-row-that-reached-the-attempt-cap)). Only used with the outbox (Postgres + Kafka). |
| `HOUSEKEEPING_INTERVAL` | `1h` | no | How often the housekeeping sweeper runs (Go duration). Only used when `DATABASE_URL` is set. `0` (or any zero/negative duration) disables the sweeper. |
| `IDEMPOTENCY_KEY_TTL` | `24h` | no | Age after which an `idempotency_keys` row is deleted by the sweeper. It is also how long a client retry with the same `Idempotency-Key` is still replayed. `0` keeps the rows forever. |
| `OUTBOX_RETENTION` | `168h` (7 days) | no | Age after which a **published** `outbox_events` row is deleted. Unpublished rows are never deleted. `0` keeps published rows forever. |
| `LOG_LEVEL` | `info` | no | `debug`, `warn` (or `warning`), `error`; anything else means `info`. JSON logs to stdout. |
| `OTEL_SERVICE_NAME` | `process-path-management` | no | OTel `service.name`, also the otelchi span/metric server name (`DefaultServiceName` in `internal/adapters/inbound/http/server.go`). |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | no | OTLP/gRPC endpoint (insecure) for traces and metrics. Export is non-blocking: an unreachable Collector only drops telemetry. |
| `SERVICE_VERSION` | `dev` | no | OTel `service.version`. A build-time `-ldflags "-X main.version=..."` value takes precedence. |
| `ENVIRONMENT` | `local` | no | OTel `deployment.environment.name` resource attribute (`internal/adapters/outbound/telemetry/telemetry.go`). |
| `CORS_ALLOWED_ORIGINS` | `http://localhost:5173,http://localhost:5189` | no | Comma-separated browser origins allowed by the CORS middleware (methods GET/POST/PUT/DELETE, headers `Content-Type`, `Authorization`, `Idempotency-Key`, max-age 300 s). Read in `internal/adapters/inbound/http/server.go`. |

:::note[Zero durations disable the sweeper]
`HOUSEKEEPING_INTERVAL`, `IDEMPOTENCY_KEY_TTL` and `OUTBOX_RETENTION` are
parsed by `housekeepingDurationEnv` (`cmd/pathmgmt/main.go`), which keeps a
zero or negative duration as is: `HOUSEKEEPING_INTERVAL=0` turns the whole
job off, `IDEMPOTENCY_KEY_TTL=0` / `OUTBOX_RETENTION=0` turn off that half
([ADR 0018](../adr/0018-outbox-lag-gauge-and-housekeeping-sweeper.md)). A
malformed value falls back to the default (`1h` / `24h` / `168h`). The
startup line `housekeeping sweeper configured` logs the values in effect.
Before gap fix D15 a zero value silently meant the default.
:::

## `mcp` (MCP server)

Source: `cmd/mcp/main.go` unless noted.

| Variable | Default | Required | Meaning |
| --- | --- | --- | --- |
| `MCP_ADDR` | `:8090` | no | Listen address. MCP Streamable HTTP is mounted at `/`, `/mcp` and `/mcp/`; `GET /healthz` is the probe target. |
| `DATABASE_URL` | **required** | yes | Same OLTP database as `pathmgmt`. Unset stops the boot: `DATABASE_URL is required: this context runs on real Postgres` ([ADR 0037](../adr/0037-postgres-and-kafka-are-the-only-runtime.md)); there is no in-memory store. Both repositories share one pool of 10 connections. |
| `MIGRATIONS_DATABASE_URL` | value of `DATABASE_URL` | no | Same meaning as for `pathmgmt`: `cmd/mcp` also runs `migrations/` at boot. |
| `MIGRATIONS_PATH` | `migrations` | no | Same as `pathmgmt`. |
| `REPORTS_BASE_URL` | unset | no | Base URL of `pathmgmt-reports` (for example `http://<release>-reports.<ns>.svc.cluster.local:80`). When set, the `get_catalogue_growth_report` tool is registered; when unset only the three OLTP read tools exist. |
| `LOG_LEVEL` | `info` | no | As for `pathmgmt`. |
| `OTEL_SERVICE_NAME` | `process-path-management-mcp` | no | OTel `service.name`. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | no | As for `pathmgmt`. |
| `SERVICE_VERSION` | `dev` | no | OTel `service.version` (no ldflags override in this binary). |
| `ENVIRONMENT` | `local` | no | As for `pathmgmt`. |

`cmd/mcp` never reads `EVENT_PUBLISHER`, `KAFKA_BROKERS` or the
housekeeping variables: it wires only the read use cases.

## `a2a` (A2A agent)

Source: `cmd/a2a/main.go` unless noted ([ADR 0039](../adr/0039-a2a-inbound-adapter.md)).

| Variable | Default | Required | Meaning |
| --- | --- | --- | --- |
| `A2A_ADDR` | `:8093` | no | Listen address. The JSON-RPC endpoint is mounted at `/` and `/a2a`, the Agent Card at `/.well-known/agent-card.json`; `GET /healthz` is the probe target. |
| `A2A_AGENT_URL` | **required** | yes | Absolute `http(s)` URL of the JSON-RPC endpoint, advertised on the Agent Card. Unset or not absolute stops the boot: `A2A_AGENT_URL is required: ...`. The chart defaults it to `http://<release>-a2a.<ns>.svc.cluster.local:8093/` (`a2a.agentUrl` overrides). |
| `DATABASE_URL` | **required** | yes | Same OLTP database as `pathmgmt`; unset stops the boot as for `mcp` (ADR 0037). |
| `MIGRATIONS_DATABASE_URL`, `MIGRATIONS_PATH` | `DATABASE_URL`, `migrations` | no | As for `mcp`: `cmd/a2a` also runs `migrations/` at boot. |
| `LOG_LEVEL`, `OTEL_SERVICE_NAME`, `OTEL_EXPORTER_OTLP_ENDPOINT`, `SERVICE_VERSION`, `ENVIRONMENT` | `info`, `process-path-management-a2a`, `localhost:4317`, `dev`, `local` | no | As for `mcp`. `SERVICE_VERSION` is also the Agent Card version. |

`cmd/a2a` is read-only like `cmd/mcp`: it never reads `EVENT_PUBLISHER` or
`KAFKA_BROKERS`, writes no outbox row and calls no other context.

## `pathmgmt-projector` (analytics writer)

Source: `cmd/pathmgmt-projector/main.go`.

| Variable | Default | Required | Meaning |
| --- | --- | --- | --- |
| `ANALYTICS_DATABASE_URL` | none | **yes** | Read-write DSN of the analytical database. The process exits with `ANALYTICS_DATABASE_URL is required` when it is unset. Pool: 5 connections, `statement_timeout=10s` (`internal/adapters/outbound/analyticsstore/pool.go`). |
| `ANALYTICS_MIGRATIONS_PATH` | `migrations/analytics` | no | Directory of the analytical `*.up.sql` files, applied at boot (the projector owns this schema). |
| `KAFKA_BROKERS` | **required** | yes | Comma-separated broker list for the `warehouse.process-path-management.analytics` reader and the `.dlq` writer. Unset stops the boot (`KAFKA_BROKERS is required: this context runs on real Kafka`); no `localhost:9092` default. |
| `ADMIN_ADDR` | `:8091` | no | Listen address of the admin server (`/healthz`, `/readyz`). |
| `LOG_LEVEL` | `info` | no | `debug`, `warn`/`warning`, `error`; anything else means `info`. |
| `OTEL_SERVICE_NAME` | `pathmgmt-projector` | no | OTel `service.name`. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | no | As for `pathmgmt`. |
| `SERVICE_VERSION` | `dev` | no | OTel `service.version`. |
| `ENVIRONMENT` | `local` | no | As for `pathmgmt`. |

Since gap fix D21 ([ADR 0029](../adr/0029-analytics-binaries-telemetry-and-liveness.md))
the projector calls `telemetry.Setup` like `pathmgmt`. It exits 1 when its
analytics consumer loop stops on its own (gap fix D13), so Kubernetes
restarts it instead of leaving a Ready pod that projects nothing.

## `pathmgmt-reports` (analytics reader)

Source: `cmd/pathmgmt-reports/main.go`.

| Variable | Default | Required | Meaning |
| --- | --- | --- | --- |
| `ANALYTICS_DATABASE_URL` | none | **yes** | DSN of the analytical database. Opened read-only (`default_transaction_read_only=on`, 5 connections, `statement_timeout=15s`). Exits with `ANALYTICS_DATABASE_URL is required` when unset. In the chart this is fed from the secret key `ANALYTICS_READER_DATABASE_URL`. |
| `HTTP_ADDR` | `:8092` | no | Listen address of `/reports/catalogue-growth`, `/reports/catalogue-growth/freshness` and `/healthz`. |
| `LOG_LEVEL` | `info` | no | As for the projector. |
| `OTEL_SERVICE_NAME` | `pathmgmt-reports` | no | OTel `service.name`. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | no | As for `pathmgmt`. |
| `SERVICE_VERSION` | `dev` | no | OTel `service.version`. |
| `ENVIRONMENT` | `local` | no | As for `pathmgmt`. |

Like the projector, the reports binary calls `telemetry.Setup` (gap fix
D21). Its routes are specified in `apis/openapi-reports.yaml`.

## Boot-time retry

Every binary dials Postgres eagerly at boot (migrations, then `pool.Ping`)
through `bootretry.Do` (`internal/adapters/outbound/bootretry/retry.go`):
5 attempts with exponential backoff starting at 1 s. The process sleeps only
between attempts (1 + 2 + 4 + 8 s), so a database that never answers fails
the boot after roughly 15 s plus the dial time of each attempt. This is not
configurable.

## How the Helm chart maps values to variables

Chart: `charts/process-path-management`. The api Deployment mounts the chart
ConfigMap with `envFrom: configMapRef` (gap fix D14: every ConfigMap key is
read by `cmd/pathmgmt`) and also sets most variables inline (`env:`), which
win over the ConfigMap for the same key; the ConfigMap's checksum annotates
the api pod so a change rolls it. The mcp, projector and reports
Deployments set their variables inline only and do not mount the ConfigMap.
`charts/process-path-management/tests/render_test.sh` (CI job
`chart-tests`) asserts this wiring.

| Helm value | Variable | Binary |
| --- | --- | --- |
| `config.httpAddr` (`:8080`) | `HTTP_ADDR` | pathmgmt |
| `config.migrationsPath` (`migrations`) | `MIGRATIONS_PATH` | pathmgmt, mcp |
| `config.logLevel` (`info`) | `LOG_LEVEL` | all four |
| `config.corsAllowedOrigins` | `CORS_ALLOWED_ORIGINS` | pathmgmt |
| `config.eventPublisher` (`kafka`, the only accepted value) | `EVENT_PUBLISHER` | pathmgmt |
| `config.outboxMaxAttempts` (`"20"`) | `OUTBOX_MAX_ATTEMPTS` | pathmgmt |
| `config.housekeeping.interval` / `.idempotencyKeyTtl` / `.outboxRetention` | `HOUSEKEEPING_INTERVAL` / `IDEMPOTENCY_KEY_TTL` / `OUTBOX_RETENTION` | pathmgmt |
| `environment` (`local`) | `ENVIRONMENT` | all four (always set, independent of `otel.enabled`) |
| `image.tag` or chart `appVersion` | `SERVICE_VERSION` | all four (always set) |
| `otel.endpoint`, `otel.serviceName` (when `otel.enabled`) | `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME` | all four; mcp, projector and reports get the fixed names `process-path-management-mcp`, `pathmgmt-projector`, `pathmgmt-reports` |
| secret `database.existingSecretKey` (`DATABASE_URL`) | `DATABASE_URL` | pathmgmt, mcp (always; the render fails without `database.url` or `database.existingSecret`) |
| secret `database.migrationsExistingSecretKey` (`MIGRATIONS_DATABASE_URL`, `optional: true`) | `MIGRATIONS_DATABASE_URL` | pathmgmt, mcp |
| `kafka.brokers` (required, no default) | `KAFKA_BROKERS` | pathmgmt, projector (always; the render fails when empty) |
| `mcp.httpAddr` (`:8090`) | `MCP_ADDR` | mcp |
| `mcp.reportsBaseUrl`, else the in-cluster reports Service when `analytics.enabled` | `REPORTS_BASE_URL` | mcp |
| `analytics.projector.adminAddr` (`:8091`) | `ADMIN_ADDR` | projector |
| `analytics.migrationsPath` (`migrations/analytics`) | `ANALYTICS_MIGRATIONS_PATH` | projector |
| analytics secret key `ANALYTICS_DATABASE_URL` | `ANALYTICS_DATABASE_URL` | projector |
| analytics secret key `ANALYTICS_READER_DATABASE_URL` | `ANALYTICS_DATABASE_URL` | reports |
| `analytics.reports.httpAddr` (`:8092`) | `HTTP_ADDR` | reports |

`extraEnv` (api) and `mcp.extraEnv` append arbitrary variables, which is how
you set `OUTBOX_RELAY_INTERVAL` in a cluster: the chart has no dedicated
value for it.

The chart refuses to render, naming the value, without `database.url` or
`database.existingSecret`, without `kafka.brokers`, with `kafka.enabled=false`
(the key is still accepted as `true` because warehouse-infra sets it, but it is
no longer a switch), with `config.eventPublisher` other than `kafka`, with an
`EVENT_PUBLISHER` or `*_MODE` entry in `extraEnv`, and with a
`terminationGracePeriodSeconds` under 20
([ADR 0037](../adr/0037-postgres-and-kafka-are-the-only-runtime.md)).
