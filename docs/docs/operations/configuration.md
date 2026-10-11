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
| `DATABASE_URL` | unset | no | OLTP Postgres DSN for the runtime `pgxpool` (10 connections, `statement_timeout=5s`). Unset selects the in-memory repositories, no idempotency middleware, no outbox, no sweeper. |
| `MIGRATIONS_DATABASE_URL` | value of `DATABASE_URL` | no | DSN used **only** by the golang-migrate step at boot. Set it to a direct (non-PgBouncer) connection when `DATABASE_URL` goes through PgBouncer in transaction mode, because golang-migrate takes a session-scoped advisory lock ([ADR 0015](../adr/0015-migrations-direct-postgres-connection.md)). |
| `MIGRATIONS_PATH` | `migrations` | no | Directory of the OLTP `*.up.sql` files, opened as `file://<path>` (`internal/adapters/outbound/postgres/migrate.go`). Relative to the working directory. |
| `EVENT_PUBLISHER` | `log` | no | `kafka` (case-insensitive) enables Kafka publishing; any other value keeps the log publisher. See the delivery-mode table on [Architecture](../overview/architecture.md#delivery-modes). |
| `KAFKA_BROKERS` | `localhost:9092` | only with `EVENT_PUBLISHER=kafka` | Comma-separated broker list for the integration and analytics writers. Ignored when `EVENT_PUBLISHER` is not `kafka`. |
| `OUTBOX_RELAY_INTERVAL` | `1s` | no | Idle poll interval of the outbox relay (Go duration). Only used when both `DATABASE_URL` and `EVENT_PUBLISHER=kafka` are set. Malformed, zero or negative values fall back to `1s`. |
| `HOUSEKEEPING_INTERVAL` | `1h` | no | How often the housekeeping sweeper runs (Go duration). Only used when `DATABASE_URL` is set. |
| `IDEMPOTENCY_KEY_TTL` | `24h` | no | Age after which an `idempotency_keys` row is deleted by the sweeper. It is also how long a client retry with the same `Idempotency-Key` is still replayed. |
| `OUTBOX_RETENTION` | `168h` (7 days) | no | Age after which a **published** `outbox_events` row is deleted. Unpublished rows are never deleted. |
| `LOG_LEVEL` | `info` | no | `debug`, `warn`, `error`; anything else means `info`. JSON logs to stdout. |
| `OTEL_SERVICE_NAME` | `process-path-management` | no | OTel `service.name`, also the otelchi span/metric server name (`DefaultServiceName` in `internal/adapters/inbound/http/server.go`). |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | no | OTLP/gRPC endpoint (insecure) for traces and metrics. Export is non-blocking: an unreachable Collector only drops telemetry. |
| `SERVICE_VERSION` | `dev` | no | OTel `service.version`. A build-time `-ldflags "-X main.version=..."` value takes precedence. |
| `ENVIRONMENT` | `local` | no | OTel `deployment.environment.name` resource attribute (`internal/adapters/outbound/telemetry/telemetry.go`). |
| `CORS_ALLOWED_ORIGINS` | `http://localhost:5173,http://localhost:5189` | no | Comma-separated browser origins allowed by the CORS middleware (methods GET/POST/PUT/DELETE, headers `Content-Type`, `Authorization`, `Idempotency-Key`, max-age 300 s). Read in `internal/adapters/inbound/http/server.go`. |

:::warning[Zero durations do not disable the sweeper]
`HOUSEKEEPING_INTERVAL`, `IDEMPOTENCY_KEY_TTL` and `OUTBOX_RETENTION` are
parsed by `durationEnv`, which returns the **default** for a malformed,
zero or negative value. The `Sweeper` itself supports "`<= 0` disables"
(`internal/adapters/outbound/postgres/sweeper.go`), but that path cannot be
reached through these variables: setting `"0s"` gives you the default
(`1h` / `24h` / `168h`), not a disabled sweep. The chart comments in
`values.yaml` and `templates/deployment.yaml` say the opposite; the code
wins.
:::

## `mcp` (MCP server)

Source: `cmd/mcp/main.go` unless noted.

| Variable | Default | Required | Meaning |
| --- | --- | --- | --- |
| `MCP_ADDR` | `:8090` | no | Listen address. MCP Streamable HTTP is mounted at `/`, `/mcp` and `/mcp/`; `GET /healthz` is the probe target. |
| `DATABASE_URL` | unset | no | Same OLTP database as `pathmgmt`. Unset selects this process's **own** in-memory repositories (it will not see what `pathmgmt` wrote). Both repositories share one pool of 10 connections. |
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

## `pathmgmt-projector` (analytics writer)

Source: `cmd/pathmgmt-projector/main.go`.

| Variable | Default | Required | Meaning |
| --- | --- | --- | --- |
| `ANALYTICS_DATABASE_URL` | none | **yes** | Read-write DSN of the analytical database. The process exits with `ANALYTICS_DATABASE_URL is required` when it is unset. Pool: 5 connections, `statement_timeout=10s` (`internal/adapters/outbound/analyticsstore/pool.go`). |
| `ANALYTICS_MIGRATIONS_PATH` | `migrations/analytics` | no | Directory of the analytical `*.up.sql` files, applied at boot (the projector owns this schema). |
| `KAFKA_BROKERS` | `localhost:9092` | no | Comma-separated broker list for the `warehouse.process-path-management.analytics` reader and the `.dlq` writer. |
| `ADMIN_ADDR` | `:8091` | no | Listen address of the admin server (`/healthz`, `/readyz`). |
| `LOG_LEVEL` | `info` | no | `debug`, `warn`/`warning`, `error`; anything else means `info`. |

The projector does not set up OpenTelemetry. The chart still injects
`OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME`, `SERVICE_VERSION` and
`ENVIRONMENT` into its pod when `otel.enabled=true`; the binary ignores them.

## `pathmgmt-reports` (analytics reader)

Source: `cmd/pathmgmt-reports/main.go`.

| Variable | Default | Required | Meaning |
| --- | --- | --- | --- |
| `ANALYTICS_DATABASE_URL` | none | **yes** | DSN of the analytical database. Opened read-only (`default_transaction_read_only=on`, 5 connections, `statement_timeout=15s`). Exits with `ANALYTICS_DATABASE_URL is required` when unset. In the chart this is fed from the secret key `ANALYTICS_READER_DATABASE_URL`. |
| `HTTP_ADDR` | `:8092` | no | Listen address of `/reports/catalogue-growth`, `/reports/catalogue-growth/freshness` and `/healthz`. |
| `LOG_LEVEL` | `info` | no | As for the projector. |

Like the projector, the reports binary has no OTel wiring, so the `OTEL_*`,
`SERVICE_VERSION` and `ENVIRONMENT` values the chart injects are unused.

## Boot-time retry

Every binary dials Postgres eagerly at boot (migrations, then `pool.Ping`)
through `bootretry.Do` (`internal/adapters/outbound/bootretry/retry.go`):
5 attempts with exponential backoff starting at 1 s. The process sleeps only
between attempts (1 + 2 + 4 + 8 s), so a database that never answers fails
the boot after roughly 15 s plus the dial time of each attempt. This is not
configurable.

## How the Helm chart maps values to variables

Chart: `charts/process-path-management`. The Deployments set the variables
inline (`env:`), not through the ConfigMap; the ConfigMap is rendered and
its checksum annotates the api pod so a change rolls it.

| Helm value | Variable | Binary |
| --- | --- | --- |
| `config.httpAddr` (`:8080`) | `HTTP_ADDR` | pathmgmt |
| `config.migrationsPath` (`migrations`) | `MIGRATIONS_PATH` | pathmgmt, mcp |
| `config.logLevel` (`info`) | `LOG_LEVEL` | pathmgmt, mcp |
| `config.corsAllowedOrigins` | `CORS_ALLOWED_ORIGINS` | pathmgmt |
| `config.eventPublisher` (`log`) | `EVENT_PUBLISHER` | pathmgmt |
| `config.housekeeping.interval` / `.idempotencyKeyTtl` / `.outboxRetention` | `HOUSEKEEPING_INTERVAL` / `IDEMPOTENCY_KEY_TTL` / `OUTBOX_RETENTION` | pathmgmt |
| `environment` (`local`) | `ENVIRONMENT` | all four (only pathmgmt and mcp use it) |
| `image.tag` or chart `appVersion` | `SERVICE_VERSION` | all four (only pathmgmt and mcp use it) |
| `otel.endpoint`, `otel.serviceName` (when `otel.enabled`) | `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME` | pathmgmt; mcp gets the fixed name `process-path-management-mcp` |
| secret `database.existingSecretKey` (`DATABASE_URL`) | `DATABASE_URL` | pathmgmt, mcp (only when `database.url` or `database.existingSecret` is set) |
| secret `database.migrationsExistingSecretKey` (`MIGRATIONS_DATABASE_URL`, `optional: true`) | `MIGRATIONS_DATABASE_URL` | pathmgmt, mcp |
| `kafka.brokers` (`kafka:9092`) | `KAFKA_BROKERS` | pathmgmt (only when `kafka.enabled`), projector (always) |
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

Note that `kafka.enabled=true` alone does not make the api publish to Kafka:
`config.eventPublisher` must also be `kafka`.
