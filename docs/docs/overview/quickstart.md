---
id: quickstart
title: Quickstart
sidebar_label: Quickstart
description: Build, test and run process-path-management locally on real Postgres and Kafka (docker compose), plus the MCP server and the analytics pair - and make the first calls.
---

# Quickstart

This page takes you from a clean checkout to a running service and your first
API calls. The binaries run only on real Postgres and real Kafka
([ADR 0037](../adr/0037-postgres-and-kafka-are-the-only-runtime.md)): there
is no in-memory mode, and a missing `DATABASE_URL` or `KAFKA_BROKERS` stops
the boot with an error that names it.

## Prerequisites

| Tool | Version / note | Needed for |
| --- | --- | --- |
| Go | `go 1.27.2` (from `go.mod`) | building and testing |
| GNU make | any | the quality-gate targets |
| Docker | a running daemon | `docker compose` Postgres, and `make integration` (testcontainers) |
| golangci-lint | `v2.14.0` (pinned in `.github/workflows/ci.yml`) | `make lint`, `make check` |
| gremlins | `v0.6.0` | `make mutation-fast` |
| Spectral CLI | from `apis/tooling` or `npm install -g @stoplight/spectral-cli` | `make api-lint` |
| Schemathesis | `schemathesis==4.28.0` (`st` on `PATH`) | `make contract` |
| lefthook | optional | git hooks: `lefthook install` once per clone |
| Node.js | 20 for `docs/`, 22 for `web/` (the CI versions) | the docs site and the operator remote |

There is no `make run` target. Run a binary with `go run ./cmd/<binary>` (or
build all four with `go build ./cmd/...`).

## Build and test

```bash
make build        # go build ./...
make test         # go test ./... -race
make check        # fmt-check vet build lint test   (the pre-push hook)
make check-all    # check + coverage (90% gate) + arch-test + bdd
```

The full list of targets and what CI runs is on [Testing](../development/testing.md).

## 1. Start Postgres and Kafka, then run the service

`docker-compose.yml` provides Postgres 16 (user, password and database
`pathmgmt`, host port `5436`) and a single-node Kafka (host port `9092`; stop
the kind cluster's broker first if it publishes the same port).

```bash
docker compose up -d --wait postgres kafka
export DATABASE_URL='postgres://pathmgmt:pathmgmt@localhost:5436/pathmgmt?sslmode=disable'
export KAFKA_BROKERS=localhost:9092
go run ./cmd/pathmgmt          # runs migrations/ at startup, then serves
# {"level":"INFO","msg":"kafka event publishing enabled (transactional outbox)",...}
# {"level":"INFO","msg":"http server listening","addr":":8080"}
```

Every domain event is committed to the outbox in the same transaction as the
change and relayed to Kafka by an always-on relay. Without an OTel Collector
on `localhost:4317` you also get a periodic `traces export: ... connection
refused` INFO line; it is harmless (export is non-blocking).

## 2. First calls

`GET /readyz` is `200` only while Postgres and a Kafka broker answer.
`POST /process-paths` requires an `Idempotency-Key` header (missing → 400
`idempotency-key-required`; same key and body replays the stored response;
same key with a different body → 409 `idempotency-key-reused` (ADR 0040), see
[ADR 0011](../adr/0011-idempotency-key-middleware.md)).

```bash
curl -s localhost:8080/healthz
# {"status":"ok"}
curl -s localhost:8080/readyz
# {"status":"ready"}

curl -s -X POST localhost:8080/process-paths \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{"pathId":"PICK","matchPrefix":"pick","direct":true,"requiredCapabilities":["pick"],"cycleTimeP95":"2h"}'
# 201 {"pathId":"PICK","matchPrefix":"pick","direct":true,"requiredCapabilities":["pick"],
#      "cycleTimeP95":"2h0m0s","eligibility":{},"status":"ACTIVE","createdAt":"...","updatedAt":"..."}

curl -s localhost:8080/process-paths              # active paths only: {"items":[...],"nextCursor":"..."} (nextCursor absent on the last page)
curl -s 'localhost:8080/process-paths?all=true'   # include deactivated
curl -s 'localhost:8080/process-paths?limit=50&cursor=<nextCursor>'   # next page (limit 1-500, default 100)

curl -s -X PUT localhost:8080/sites/sp1/cpt-schedule \
  -H 'Content-Type: application/json' \
  -d '{"timezone":"America/Sao_Paulo","cutoffs":[{"cptId":"cpt-1800","localTime":"18:00","daysOfWeek":["Mon","Tue"],"shipMethod":"ground","eligiblePathIds":["PICK"]}]}'
# 200 {"siteId":"sp1","timezone":"America/Sao_Paulo","cutoffs":[...],...}

curl -s -X DELETE localhost:8080/process-paths/PICK
# 409 application/problem+json, type .../path-referenced-by-cpt-schedule:
# "path \"PICK\" is listed by the schedule of site(s) [sp1]; revise those schedules first"
```

Errors are RFC 7807 `application/problem+json`. A few you will meet early:

| Request | Status | `type` slug |
| --- | --- | --- |
| Re-POST an existing `pathId` | 409 | `path-already-exists` |
| `"matchPrefix":"PICK"` | 422 | `match-prefix-not-lowercase` |
| `GET /process-paths?all=yes` | 400 | `invalid-query-parameter` |
| Any unknown route | 404 | `route-not-found` |

The [Troubleshooting](../operations/troubleshooting.md) page lists all of them.

## 3. Where the events go

Every event is written to both `warehouse.process-path-management.events`
and `warehouse.process-path-management.analytics` as CloudEvents 1.0. The
compose broker is yours alone; on the shared fleet broker the five consumers
of the integration topic apply what you publish.

## 4. Run the MCP server

```bash
go run ./cmd/mcp                 # :8090, needs the same DATABASE_URL as pathmgmt
curl -s localhost:8090/healthz   # {"status":"ok"}
```

`cmd/mcp` requires `DATABASE_URL` (no Kafka) and reads the same store as
`cmd/pathmgmt`, so it sees paths you created through the API. A raw Streamable HTTP handshake:

```bash
curl -si localhost:8090/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"curl","version":"0"}}}'
# 200, Content-Type: text/event-stream, header Mcp-Session-Id: <id>
# data: {"jsonrpc":"2.0","id":1,"result":{..."serverInfo":{"name":"process-path-management-mcp","version":"1.0.0"}}}
```

Send the returned `Mcp-Session-Id` header on later `tools/list` and
`tools/call` requests. Tools and their inputs: [MCP tools](../mcp/tools.md).

## 5. Run the analytics pair

`pathmgmt-projector` and `pathmgmt-reports` both refuse to start without
`ANALYTICS_DATABASE_URL`. Use a **separate database**: the projector runs
`migrations/analytics` with golang-migrate, which shares the default
`schema_migrations` table with the OLTP migrations if both point at the same
database.

```bash
docker compose exec postgres createdb -U pathmgmt pathmgmt_analytics
export ANALYTICS_DATABASE_URL='postgres://pathmgmt:pathmgmt@localhost:5436/pathmgmt_analytics?sslmode=disable'
KAFKA_BROKERS=localhost:9092 go run ./cmd/pathmgmt-projector   # admin :8091
go run ./cmd/pathmgmt-reports                                  # :8092

curl -s 'localhost:8092/reports/catalogue-growth?from=2026-10-01T00:00:00Z&to=2026-11-01T00:00:00Z'
curl -s localhost:8092/reports/catalogue-growth/freshness
```

`go run ./cmd/pathmgmt-reports` uses `HTTP_ADDR` (default `:8092`). Do not
export `HTTP_ADDR` for `pathmgmt` in the same shell or both will try to bind
the same port.

## 6. Seed the catalogue

There is no seed command in this repository. Seed through the REST API, one
`POST /process-paths` per path, as in step 2 (the `Idempotency-Key` header is always required), then one `PUT /sites/{siteId}/cpt-schedule`
per site. `make seed-catalogue` does this from `catalogue/paths.json` against the running service.

The fleet's starter catalogue is the retired YAML file
`warehouse-infra/config/process-paths/sortable-fc.yaml`, and warehouse-infra
ships `scripts/seed-process-paths.py` to replay it through this API. Read
that script before relying on it: on warehouse-infra `develop` (checked
2026-10-09) its create body carries only `pathId`, `matchPrefix`, `direct`
and `requiredCapabilities`, and it sends no `Idempotency-Key`. Against the
current API a create without `cycleTimeP95` is rejected with 422
`invalid-cycle-time-p95`, and a create without the
header is rejected with 400 `idempotency-key-required`.

## 7. The operator remote

```bash
cd web && npm install && npm run dev     # http://localhost:5189
```

In development the remote calls `http://localhost:8087`
(`DEV_API_BASE` in `web/src/config.ts`), not the service's default `:8080`.
Start the API with `HTTP_ADDR=:8087` (or port-forward to 8087) when you work
on the remote. `CORS_ALLOWED_ORIGINS` already allows `http://localhost:5189`.

## Next

- Every variable each binary reads: [Configuration](../operations/configuration.md)
- Deploying to the kind cluster: [Runbook](../operations/runbook.md)
