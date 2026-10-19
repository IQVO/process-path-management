---
id: 0037-postgres-and-kafka-are-the-only-runtime
slug: /adr/0037-postgres-and-kafka-are-the-only-runtime
title: "0037. Postgres and Kafka are the only runtime"
sidebar_label: "0037. Postgres and Kafka only"
description: "ADR 0037 — every cmd binary of process-path-management runs on real Postgres and real Kafka. No in-memory repositories, no log publisher, no direct fan-out, no optional outbox relay, no localhost broker default. Mandatory configuration is a boot error naming the variable; /readyz checks Postgres and a Kafka broker; sensors, the Schemathesis contract job (with the NUL fix) and the chart keep it true. Public event shapes are unchanged."
---

# 0037. Postgres and Kafka are the only runtime

## Status

**Accepted** (2026-10-19). Supersedes the in-memory fallback, the log
publisher, the direct (non-outbox) Kafka publisher and the `localhost:9092`
default that [ADR 0001](./0001-process-path-management-bounded-context.md),
[ADR 0003](./0003-transactional-outbox.md),
[ADR 0006](./0006-mcp-server-second-inbound-adapter.md),
[ADR 0007](./0007-analytical-data-product.md),
[ADR 0011](./0011-idempotency-key-middleware.md) and
[ADR 0025](./0025-schemathesis-contract-job.md) allowed. Fleet decision of
2026-10-17, "everything must use real database and broker", following
product-master, workforce-management, facility-layout and labor-performance
(their ADR 0034). No data migration. The CloudEvents envelope
([ADR 0016](./0016-cloudevents-mandatory-event-envelope.md)), the topics and
every public event shape are unchanged. One REST contract detail follows:
`Idempotency-Key` is now `required: true` and `/readyz` is in
`apis/openapi.yaml`.

## Context

The binaries could start in states that cannot serve the cluster, and did so
silently:

- `cmd/pathmgmt` and `cmd/mcp` without `DATABASE_URL` served from their own
  empty in-memory repositories: data vanished on restart, the MCP server saw
  none of the API's data, there was no outbox, no sweeper and no
  Idempotency-Key check.
- `EVENT_PUBLISHER` defaulted to the log publisher: nothing reached
  `warehouse.process-path-management.events` (consumed by
  fulfillment-execution, wes-work-planning, workforce-management,
  order-management and network-fulfillment) or `.analytics`, while `/readyz`
  said ready. With Kafka but without a database the service published
  directly, outside any transaction.
- `KAFKA_BROKERS` defaulted to `localhost:9092` in `cmd/pathmgmt` and
  `cmd/pathmgmt-projector`, and the chart's `kafka.enabled: false` default left
  it unset.
- `/readyz` was only a drain gate: ready while Postgres or Kafka was down.
- The Schemathesis `contract` job booted the service in memory, so it tested a
  mode that is not deployed.

A fallback that works makes a mis-set environment look like a healthy
deployment.

## Decision

1. **One runtime.** `cmd/pathmgmt` and `cmd/mcp` run on real Postgres;
   `cmd/pathmgmt` and `cmd/pathmgmt-projector` run on real Kafka; the projector
   and `cmd/pathmgmt-reports` run on the analytical Postgres. There is no
   in-memory mode, no log sink and no direct publisher: events are written
   through the transactional outbox in the use case's transaction and the
   relay and the housekeeping sweeper always run.
2. **Missing configuration is a boot error that names the variable**, raised
   before telemetry, any connection and the listener, every problem reported at
   once: `DATABASE_URL is required: this context runs on real Postgres`,
   `KAFKA_BROKERS is required: this context runs on real Kafka`. `cmd/mcp`
   requires `DATABASE_URL`; `cmd/pathmgmt-projector` requires
   `ANALYTICS_DATABASE_URL` and `KAFKA_BROKERS`; `cmd/pathmgmt-reports` already
   required `ANALYTICS_DATABASE_URL`. `EVENT_PUBLISHER` stays only because the
   chart and warehouse-infra set it: unset and `kafka` are accepted, `log` and
   anything else stop the boot.
3. **The composition roots build real adapters only.** The `memory` adapters
   stay in `internal/adapters/outbound/memory` as test doubles and are never
   imported by a binary. The wall clock the binaries need moved to
   `internal/adapters/outbound/system`.
4. **`/readyz` tells the truth.** `cmd/pathmgmt` answers `200` only while the
   drain gate is open AND Postgres answers a ping AND a Kafka broker answers a
   metadata request. A failed dependency is `503 dependency-unavailable`
   (problem+json) naming it. The verdict is cached 3 s and bounded to 2 s;
   `/healthz` stays a pure liveness check.
5. **Readiness checks the broker, not the topics.** The metadata request names
   no topic, on a fresh connection each time; the broker auto-creates topics on
   the first write and this service never creates topics through an admin API.
   A not-ready pod receives no traffic and could never make the first write:
   requiring the topics would deadlock a cold cluster.
6. **Sensors keep it true.** `TestRuntimeBinaryHasNoInMemoryAdapters`
   (`internal/architecture`, with `TestRuntimeBinarySensorBites`) fails when a
   non-test file under `cmd/**` imports `internal/adapters/outbound/memory`.
7. **The deployment mirrors it.** The chart fails the render, naming the value,
   without `database.url`/`database.existingSecret` or `kafka.brokers`, with
   `config.eventPublisher` other than `kafka`, with `kafka.enabled=false`, with
   an `EVENT_PUBLISHER` or `*_MODE` entry in `extraEnv`, and with a grace period
   under 20 s. `KAFKA_BROKERS` and `DATABASE_URL` are always rendered.
   `kafka.enabled` is still accepted (warehouse-infra sets it to `'true'`) but
   is no longer a switch. `charts/process-path-management/tests/warehouse-infra-values.yaml`
   mirrors what warehouse-infra sets and `render_test.sh` renders it, so infra
   needs no change.
8. **The contract job runs the real service.** `docker-compose.yml` now carries
   `confluentinc/confluent-local:7.6.1` next to `postgres:16`; the CI `contract`
   job uses the same two images as service containers and
   `scripts/contract-test.sh` requires `DATABASE_URL` and `KAFKA_BROKERS` and
   waits for `/readyz`. The contract job needs no catalogue data: it starts on
   an empty database exactly as the old in-memory boot did, and Schemathesis
   creates what it needs through the API. A Schemathesis hook
   (`scripts/contract_hooks.py`) gives every request that carries the header a
   fresh `Idempotency-Key`, since a reused key with a different body is by
   design a `422`.
9. **A NUL is a client error.** A fuzzed U+0000 in a string reaches a Postgres
   text parameter, which aborts the transaction (SQLSTATE 22021) and surfaces
   as `500` on the real runtime; the in-memory adapters never noticed (found by
   facility-layout's contract run). `rejectNUL`
   (`internal/adapters/inbound/http/nul.go`) answers a NUL in a body with
   `400 malformed-request-body` before the Idempotency-Key middleware records
   anything, and a NUL in the path with `404`. The contract script passes
   `--generation-allow-x00 false`, because OpenAPI 3.0 cannot express "no
   U+0000"; the behaviour is pinned by `TestRejectNUL` and
   `TestNUL_IsAClientErrorAndDoesNotConsumeTheKey` (real Postgres).

## Consequences

- A mis-set environment fails at boot with a message that names the missing
  variable.
- Local `go run ./cmd/pathmgmt` needs Postgres and Kafka:
  `docker compose up -d --wait postgres kafka` provides both. Fast feedback
  stays in unit and BDD tests over the memory doubles.
- A broker outage takes the api pods out of the Service where the readiness
  probe is `/readyz`; liveness is unaffected, so nothing restarts. Accepted
  writes stay in the outbox and are relayed when the broker returns
  ([ADR 0003](./0003-transactional-outbox.md),
  [ADR 0030](./0030-outbox-relay-attempt-cap.md)).
- `make seed-catalogue` is unchanged: it applies `catalogue/paths.json` to a
  RUNNING service over REST, so it now runs against a service on real
  Postgres + Kafka (the data survives restarts, which the in-memory service
  never gave it).
- Tests: `TestRunOnRealPostgresAndKafka` (`cmd/pathmgmt/e2e_integration_test.go`,
  testcontainers) boots the real `run()` with nothing but the deployment's
  environment, drives REST only and asserts the Postgres rows, the outbox
  drain, the CloudEvents envelope on both topics, `Idempotency-Key` replay, a
  NUL body, an in-process restart and `/readyz` following the dependencies.
  Every boot error is a table test (`cmd/pathmgmt/bootconfig_test.go`,
  `cmd/pathmgmt-projector/config_test.go`, `cmd/mcp/wiring_test.go`).

## Out of scope

process-path-management makes no REST or MCP call to a sibling context
([ADR 0007](./0007-analytical-data-product.md)), so no `*_MODE` switch exists
here. Left as follow-ups: the projector's admin listener answers `/readyz`
without checking its dependencies; `cmd/pathmgmt-reports` is unchanged.
