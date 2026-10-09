# Process Path Management

> **⚠️ Study project.** This repository is an educational exercise in
> Domain-Driven Design applied to warehouse management/execution systems. It
> follows real industry-standard patterns and terminology (WMS/WES/WCS,
> CloudEvents 1.0, RFC 7807, hexagonal architecture) but is
> **not a production system** and is **not affiliated with, endorsed by, or
> representative of any real-world
> company**.

The operator-configurable process-path catalogue for the `warehouse-systems`
fleet — a Generic Subdomain bounded context (same classification as
`facility-layout`), replacing a static YAML file
(`warehouse-infra/config/process-paths/sortable-fc.yaml`) previously
boot-loaded by `fulfillment-execution`, `wes-work-planning`, and
`workforce-management`. Since ADR 0010 it also publishes each path's
**fulfillment capability contract** (`cycleTimeP95`, `eligibility`) and
each site's **CPT schedule**, which `order-management` consumes to derive
its promise.

📚 **Full documentation site:** https://iqvo.github.io/process-path-management/

## Documentation

The site above is built from `docs/` (Docusaurus). The pages, by task:

| Task | Page |
| --- | --- |
| What this context owns, its class and tier | [Introduction](docs/docs/overview/introduction.md) |
| Hexagonal layout, the four binaries and their ports, data stores | [Architecture](docs/docs/overview/architecture.md) |
| Build, test, run locally, first calls | [Quickstart](docs/docs/overview/quickstart.md) |
| Every environment variable per binary, Helm mapping | [Configuration](docs/docs/operations/configuration.md) |
| Deployments, probes, migrations, topics, outbox, DLQ, sweeper, scaling, `/reports` API, procedures | [Runbook](docs/docs/operations/runbook.md) |
| Metrics, spans, log lines, dashboard, alerts | [Observability](docs/docs/operations/observability.md) |
| Symptom → cause → fix, every problem `type` | [Troubleshooting](docs/docs/operations/troubleshooting.md) |
| Test pyramid, `make` targets, CI jobs | [Testing](docs/docs/development/testing.md) |
| Upstreams, downstreams, event contract, failure behaviour | [Integration](docs/docs/ecosystem/integration.md), [Context Map](docs/docs/ecosystem/context-map.md) |
| Use cases: trigger, inputs, invariants, events | [Use cases](docs/docs/ddd/use-cases.md) |
| Why Generic, neighbour classes | [Subdomain classification](docs/docs/ddd/subdomain-classification.md) |
| MCP tools and their inputs | [MCP tools](docs/docs/mcp/tools.md), [Governance charter](docs/docs/mcp/governance-charter.md) |
| Vocabulary, aggregates, the ddd-crew pack | [Ubiquitous language](docs/docs/ddd/ubiquitous-language.md), [Aggregates and invariants](docs/docs/ddd/aggregates-and-invariants.md), [DDD artifacts](docs/docs/ddd/ddd-artifacts.md) |
| Decisions | [ADR index](docs/docs/adr/about.md) |
| REST contract | [`apis/openapi.yaml`](apis/openapi.yaml) (rendered under API Reference on the site) |

## Why this context exists

Before this service existed, a path's canonical identity, its `matchPrefix`
match rule, and its required capabilities lived in a static YAML file
loaded once at boot by three separate services — a published language with
no single owner, no audit trail, and no way to revise it without a
coordinated redeploy of all three consumers. This service replaces that
file with a real bounded context: an aggregate, a REST API, and a
Kafka-published integration event. See
[ADR 0001](docs/docs/adr/0001-process-path-management-bounded-context.md)
for the full reasoning, including why propagation is Kafka-event-driven
rather than synchronous HTTP.

## Bounded-context boundary (read this first)

This service is the **SOURCE** of the process-path published language — it
consumes **no other context's topic** and makes **no synchronous REST or
MCP call** to any other service (enforced by
`internal/architecture/fitness_test.go`'s `TestNoSiblingContextOutboundCalls`).
Its only Kafka consumer is its own analytics projector, which reads this
service's own analytics topic (ADR 0007). It publishes `ProcessPathCreated`
/ `ProcessPathUpdated` / `ProcessPathDeactivated` / `CPTScheduleChanged`
onto `warehouse.process-path-management.events` when
`EVENT_PUBLISHER=kafka`. `fulfillment-execution`, `wes-work-planning` and
`workforce-management` each consume that topic into a local catalogue
cache (cutover executed 2026-09-06, see ADR 0002); `order-management`
and `network-fulfillment` consume the same topic for path capability and
CPT schedules (ADR 0010).
With a database
configured, events go through a **transactional outbox** — committed in
the same transaction as the aggregate and relayed to Kafka by an
in-process relay (ADR 0003) — so the store and the topic can never
diverge. See
[docs/docs/ecosystem/context-map.md](docs/docs/ecosystem/context-map.md)
for the full picture.

## Architecture

Hexagonal (ports & adapters), with a strict inward-only dependency rule —
**domain depends on nothing; application depends on domain; adapters
depend on application/domain** — identical in shape to every other service
in the fleet.

```
cmd/
  pathmgmt/                       REST API + in-process outbox relay (:8080)
  mcp/                            MCP server, Streamable HTTP (:8090, ADR 0006)
  pathmgmt-projector/             analytics projector: consumes the analytics topic (admin :8091, ADR 0007)
  pathmgmt-reports/               read-only catalogue-growth report API (:8092, ADR 0007)
internal/
  domain/
    processpath/                  ProcessPath aggregate
    cptschedule/                  CPTSchedule aggregate (per-site CPT cutoffs, ADR 0010)
    shared/                       PathId, Capability, SiteId, DestinationLocationRole, Eligibility, domain events
  application/
    ports/                        OUT: ProcessPathRepo, CPTScheduleRepo, EventPublisher, UnitOfWork, Clock, PathMetrics
    usecases/                     DefinePath, RevisePath, DeactivatePath, GetPath, ListPaths, DefineCPTSchedule, GetCPTSchedule
  analytics/report/               catalogue-growth read model + ports
  adapters/
    inbound/http/                 chi handlers, DTOs, RFC 7807 error mapping; reports router
    inbound/mcp/                  MCP tools over the read use cases
    inbound/kafka/                analytics consumer (this service's OWN analytics topic only)
    outbound/postgres/            pgxpool repos, unit of work, outbox publisher + relay, golang-migrate runner
    outbound/memory/              in-memory repos for tests/local
    outbound/events/              log publisher (default)
    outbound/kafka/               integration + analytics publishers (EVENT_PUBLISHER=kafka)
    outbound/analyticsstore/      analytics projection/report store (Postgres + in-memory)
    outbound/telemetry/           OTel traces/metrics/logs
  architecture/                   arch-go + fitness tests
migrations/                       golang-migrate SQL files (0001–0008); migrations/analytics/ for the report DB
apis/openapi.yaml                 This service's OWN REST API (8 operations; /readyz is served but not in the spec)
apis/asyncapi.yaml                What this service PUBLISHES (integration + analytics topics)
features/                         godog/Gherkin BDD acceptance tests
web/                              process_path_mfe Module Federation remote (operator SPA)
charts/process-path-management/   Helm chart (API, MCP, projector, reports, frontend)
docker-compose.yml                Local Postgres 16
docs/docs/adr/                    Architecture Decision Records
```

The domain layer is pure Go: no `chi`, no `pgx`, no `kafka-go`. No JSON
struct tags in the domain packages.

## Business rules worth knowing before you read the code

- **A path's identity is permanent once created.** Re-defining an id that
  already exists (active or deactivated) is rejected with 409, never a
  silent overwrite.
- **`matchPrefix` must be non-empty and lower-case, validated (not
  coerced).** A request with `"PICK"` as its matchPrefix is rejected, not
  silently lower-cased.
- **`requiredCapabilities` must be non-empty.** A path with zero required
  capabilities is not a meaningful business fact.
- **`cycleTimeP95` is required and must be a positive Go duration**
  (e.g. `"2h"`, `"90m"`) on both define and revise (ADR 0010). It is
  returned normalised (`"2h0m0s"`). `eligibility` is optional; its empty
  value is fully permissive.
- **`destinationLocationRole` is optional and immutable** — one of
  `Drop`, `WorkCenter`, `Shipping` when set (ADR 0009).
- **A CPT schedule's `eligiblePathIds` must reference Active paths.**
  `PUT /sites/{siteId}/cpt-schedule` replaces the site's schedule
  wholesale and returns 422 otherwise.
- **Deactivation is terminal and idempotent.** A deactivated path can never
  be revised again (422), and deactivating an already-deactivated path is a
  no-op 204, never a spurious error or a double-published event.
- **A no-op revision does not republish `ProcessPathUpdated`** (nor a
  no-op schedule revision `CPTScheduleChanged`). Consumers never have to
  diff two identical payloads to notice nothing changed.

## Running locally

### 1. Without a database or a broker (fastest, verified working)

With no `DATABASE_URL`, the service starts on the in-memory adapter and is
fully functional over REST:

```bash
go run ./cmd/pathmgmt
# {"level":"INFO","msg":"DATABASE_URL not set, using in-memory ProcessPathRepo and CPTScheduleRepo"}
# {"level":"INFO","msg":"http server listening","addr":":8080"}
```

### 2. With Postgres

```bash
docker compose up -d postgres          # Postgres 16 on localhost:5436

export DATABASE_URL='postgres://pathmgmt:pathmgmt@localhost:5436/pathmgmt?sslmode=disable'
go run ./cmd/pathmgmt                  # migrations run automatically at startup
```

### 3. With Kafka publishing enabled

By default this service logs its domain events instead of publishing them.
**Kafka publishing requires `EVENT_PUBLISHER=kafka`:**

```bash
export EVENT_PUBLISHER=kafka
export KAFKA_BROKERS=localhost:9092
go run ./cmd/pathmgmt
# {"level":"INFO","msg":"kafka event publishing enabled (direct, no outbox: DATABASE_URL not set)","brokers":["localhost:9092"],"topic":"warehouse.process-path-management.events","analytics_topic":"warehouse.process-path-management.analytics"}
```

Every event is published to BOTH the integration topic and the analytics
topic (`warehouse.process-path-management.analytics`, ADR 0007). With
`DATABASE_URL` also set, the use cases write one `outbox_events` row per
topic inside the same transaction as the aggregate change, and a relay
goroutine drains that table onto Kafka
(`"kafka event publishing enabled (transactional outbox)"` /
`"outbox relay running"` at startup). This is the mode the cluster runs.

### 4. With Docker

```bash
docker build -t process-path-management:local .
docker run --rm -p 8080:8080 \
  -e DATABASE_URL='postgres://pathmgmt:pathmgmt@host.docker.internal:5436/pathmgmt?sslmode=disable' \
  process-path-management:local
curl -s localhost:8080/healthz
```

### 5. With Helm (kind / any Kubernetes cluster)

```bash
helm install process-path-management charts/process-path-management \
  --set database.url='postgres://pathmgmt:pathmgmt@postgres:5432/pathmgmt?sslmode=disable'
kubectl port-forward svc/process-path-management 8080:80
curl -s localhost:8080/healthz
```

See `charts/process-path-management/values.yaml` for the full configuration
surface (`kafka.enabled`, `config.eventPublisher`, `otel.enabled`,
`ingress`, the additive `gatewayApi` HTTPRoute block, autoscaling, an
`existingSecret` pattern for `DATABASE_URL`, and the optional `mcp`,
`analytics` and `frontend` workloads — all disabled by default).

### Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `HTTP_ADDR` | `:8080` | Listen address. |
| `DATABASE_URL` | *(unset)* | Postgres DSN. Unset ⇒ in-memory adapter. |
| `MIGRATIONS_DATABASE_URL` | `DATABASE_URL` | Direct (non-PgBouncer) DSN used only for the startup migration step (ADR 0015). |
| `MIGRATIONS_PATH` | `migrations` | golang-migrate source directory. |
| `EVENT_PUBLISHER` | `log` | `log` (default) or `kafka`. Kafka publishing requires this to be set to `kafka`. |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated broker addresses (only read when `EVENT_PUBLISHER=kafka`). |
| `OUTBOX_RELAY_INTERVAL` | `1s` | How long the outbox relay sleeps between empty passes (only used when both `DATABASE_URL` and `EVENT_PUBLISHER=kafka` are set). |
| `HOUSEKEEPING_INTERVAL` | `1h` | Housekeeping sweeper interval (ADR 0018; only with `DATABASE_URL`). |
| `IDEMPOTENCY_KEY_TTL` | `24h` | Age after which the sweeper deletes `idempotency_keys` rows. |
| `OUTBOX_RETENTION` | `168h` | Age after which the sweeper deletes **published** `outbox_events` rows. |
| `CORS_ALLOWED_ORIGINS` | `http://localhost:5173,http://localhost:5189` | Comma-separated allowed origins. |
| `OTEL_SERVICE_NAME` | `process-path-management` | OTel `service.name` resource attribute. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | OTLP/gRPC Collector endpoint. |
| `SERVICE_VERSION` | `dev` | OTel `service.version` (a `-ldflags -X main.version=` build value wins). |
| `ENVIRONMENT` | `local` | OTel `deployment.environment.name` resource attribute. |
| `LOG_LEVEL` | `info` | `debug` \| `info` \| `warn` \| `error`. |

The other binaries read:

| Binary | Variable | Default | Purpose |
| --- | --- | --- | --- |
| `cmd/mcp` | `MCP_ADDR` | `:8090` | MCP Streamable HTTP listen address (`/`, `/mcp`; `/healthz`). |
| `cmd/mcp` | `DATABASE_URL`, `MIGRATIONS_DATABASE_URL`, `MIGRATIONS_PATH` | *(unset)*, `DATABASE_URL`, `migrations` | Same store as the API; unset ⇒ in-memory. |
| `cmd/mcp` | `OTEL_SERVICE_NAME` | `process-path-management-mcp` | OTel service name for MCP spans. |
| `cmd/mcp` | `REPORTS_BASE_URL` | *(unset)* | When set, registers `get_catalogue_growth_report` (calls `pathmgmt-reports`). |
| `cmd/pathmgmt-projector` | `ANALYTICS_DATABASE_URL` | *(required)* | Analytics Postgres DSN. |
| `cmd/pathmgmt-projector` | `KAFKA_BROKERS` | `localhost:9092` | Broker for the analytics topic (consumer group `process-path-management-analytics`). |
| `cmd/pathmgmt-projector` | `ADMIN_ADDR` / `ANALYTICS_MIGRATIONS_PATH` | `:8091` / `migrations/analytics` | Admin server (`/healthz`, `/readyz`); analytics migrations. |
| `cmd/pathmgmt-reports` | `HTTP_ADDR` / `ANALYTICS_DATABASE_URL` | `:8092` / *(required)* | Read-only reports API. |

## API

Nine routes on `cmd/pathmgmt`. Eight are operations in
[`apis/openapi.yaml`](apis/openapi.yaml) (the full contract, including the
RFC 7807 error schema); `GET /readyz` is served by the router but is not in
the spec.

| Method | Path | Use case |
| --- | --- | --- |
| `POST` | `/process-paths` | DefinePath |
| `GET` | `/process-paths` | ListPaths (`?all=true` for the audit view) |
| `GET` | `/process-paths/{pathId}` | GetPath |
| `PUT` | `/process-paths/{pathId}` | RevisePath |
| `DELETE` | `/process-paths/{pathId}` | DeactivatePath |
| `PUT` | `/sites/{siteId}/cpt-schedule` | DefineCPTSchedule (define or wholesale revise) |
| `GET` | `/sites/{siteId}/cpt-schedule` | GetCPTSchedule |
| `GET` | `/healthz` | Liveness probe |
| `GET` | `/readyz` | Readiness probe — `503 {"status":"not_ready"}` once graceful shutdown starts (ADR 0012) |

The separate `pathmgmt-reports` binary serves the analytics report (not in
`apis/openapi.yaml`): `GET /reports/catalogue-growth?from=&to=[&granularity=day]`,
`GET /reports/catalogue-growth/freshness`, and `GET /healthz`.

The MCP server (`cmd/mcp`) exposes read-only tools: `get_process_path`,
`list_process_paths`, `get_cpt_schedule`, and — only when
`REPORTS_BASE_URL` is set — `get_catalogue_growth_report`.

Every error response is `application/problem+json` (RFC 7807), the same
shape every other service in this fleet emits.

When `DATABASE_URL` is set, `POST /process-paths` requires an
`Idempotency-Key` request header (see ADR 0011): a byte-identical retry
(same key + same body) replays the original response verbatim instead of
hitting a natural-key `409`; the same key with a different body gets
`422`; a missing header gets `400`. The middleware needs the Postgres pool,
so on the in-memory adapter (no `DATABASE_URL`) it is not applied and the
header is ignored.

Every REST route and MCP tool is unauthenticated — there is no auth layer
in front of either (see ADR 0005, which supersedes ADR 0004's earlier
bearer-key adoption).

### Curl walkthrough

**Health:**

```bash
curl -s localhost:8080/healthz
# {"status":"ok"}
```

**Define a path:**

```bash
curl -s -X POST localhost:8080/process-paths \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: 3f9e2b1a-1e7a-4c9e-9a3f-1c2d3e4f5a6b' \
  -d '{"pathId":"PICK","matchPrefix":"pick","direct":true,"requiredCapabilities":["pick"],"cycleTimeP95":"2h"}'
# 201 Created (omitting cycleTimeP95 is a 422; with DATABASE_URL set, omitting Idempotency-Key is a 400)
```

**List (active only by default):**

```bash
curl -s localhost:8080/process-paths
curl -s 'localhost:8080/process-paths?all=true'   # includes deactivated
```

**Revise:**

```bash
curl -s -X PUT localhost:8080/process-paths/PICK \
  -H 'Content-Type: application/json' \
  -d '{"matchPrefix":"pick-zone-a","requiredCapabilities":["pick","hazmat"],"cycleTimeP95":"90m"}'
# 200 OK
```

**Define a site's CPT schedule:**

```bash
curl -s -X PUT localhost:8080/sites/sp1/cpt-schedule \
  -H 'Content-Type: application/json' \
  -d '{"timezone":"America/Sao_Paulo","cutoffs":[{"cptId":"cpt-1800","localTime":"18:00","daysOfWeek":["Mon","Tue"],"shipMethod":"ground","eligiblePathIds":["PICK"]}]}'
# 200 OK
curl -s localhost:8080/sites/sp1/cpt-schedule
```

**Deactivate:**

```bash
curl -s -X DELETE localhost:8080/process-paths/PICK
# 204 No Content
```

## Quality gate

Every `make` target mirrors a step in `.github/workflows/ci.yml`, so the
same feedback CI gives you post-push is available locally, pre-commit:

```bash
make check       # fmt-check + vet + build + lint + test -race
make check-all   # check + coverage (90% gate) + arch-test + bdd
```

| Target | What it runs |
| --- | --- |
| `build` | `go build ./...` |
| `vet` | `go vet ./...` |
| `fmt` / `fmt-check` | `gofmt -w .` / fail if `gofmt -l .` is non-empty |
| `lint` | `golangci-lint run ./...` (CI pins `v2.14.0`) |
| `test` | `go test ./... -race` |
| `coverage` | coverage profile + the 90% gate (domain + application + analytics; CI's `test` job measures domain + application) |
| `bdd` | `go test ./... -run TestFeatures -v` (godog/Gherkin) |
| `arch-test` | `go test ./internal/architecture/... -v` (arch-go fitness) |
| `integration` | `go build`/`go vet`/`go test -tags=integration ./... -race -count=1` (needs Docker: every Postgres and Kafka integration test boots its own container via testcontainers) |
| `mutation-fast` / `mutation` | `gremlins unleash ./internal/domain` (see `.gremlins.yaml`) |
| `api-lint` | Spectral on both specs |
| `vuln` | `govulncheck ./...` |
| `contract` | `scripts/contract-test.sh` — Schemathesis against `apis/openapi.yaml` (ADR 0025; needs `st` on PATH, not part of `check`) |
| `check-fast` | `fmt-check` + `vet` + `arch-test` + tests of the Go packages changed vs `HEAD` |
| `guide-lint` / `harness-test` | Agent-guide lint and the agent-hook unit tests (`scripts/harness/`) |

Additional verification surfaces, each with its own CI job:

```bash
go test ./... -run TestFeatures -v                  # BDD (godog/Gherkin) — 32 scenarios in 6 feature files
go test ./internal/architecture/... -v               # arch-fitness (arch-go)
go test -tags=integration ./... -race -count=1       # Postgres + Kafka integration (testcontainers)
gremlins unleash ./internal/domain --workers 1 --timeout-coefficient 30   # mutation testing
helm lint charts/process-path-management
spectral lint apis/openapi.yaml --ruleset .spectral.yaml --fail-severity=warn
spectral lint apis/asyncapi.yaml --ruleset .spectral.asyncapi.yaml --fail-severity=warn
```

Git hooks are wired through [lefthook](https://github.com/evilmartians/lefthook)
— `pre-commit` runs fmt-check/vet/lint, `pre-push` runs `make check`. Hooks
are not tracked by git, so activate them once per clone:

```bash
brew install lefthook   # or: go install github.com/evilmartians/lefthook@latest
lefthook install
```

CI (`.github/workflows/ci.yml`) runs the full fleet-standard matrix:
**`lint`**, **`guide-lint`** (agent guides), **`complexity`**
(gocyclo/gocognit/cyclop/funlen/nestif), **`test`** (with the 90%
domain + application coverage gate), **`bdd`**, **`contract`**
(Schemathesis, ADR 0025), **`evals-tests`** (MCP E1–E3 evals, ADR 0023),
**`integration`** (testcontainers — no service container),
**`mutation-fast`** (blocking, `./internal/domain`),
**`api-lint`** (Spectral against both `apis/openapi.yaml` and
`apis/asyncapi.yaml`), **`vuln`** (govulncheck), **`arch-test`** (arch-go
and fitness tests), **`docs-api-drift`** (regenerates
`docs/docs/api-reference/rest` from `apis/openapi.yaml` and fails on any
diff), **`web`** (lint, typecheck, test and build of the `web/` remote),
**`drift`** (advisory, weekly/manual only: deadcode, `go mod tidy -diff`,
knip, coverage quality), **`helm-lint`**/**`trivy-scan`** (gated to
pull-request-targeting-`main` only, per this fleet's convention —
this repo has no `main`-targeting PR yet, so these two jobs are expected
to skip, not fail), **`docker-publish`** (main-only, cosign keyless signing
+ SPDX SBOM attestation), and **`release`** (main-only, auto-tagged
GitHub release + published Helm chart). Plus `.github/workflows/codeql.yml`
(security-extended CodeQL analysis) and `.github/workflows/scorecard.yml`
(OpenSSF Scorecard), `.github/workflows/ai-review.yml` (advisory AI
architecture review on PRs into `develop`, never blocking), and
`.github/workflows/docs.yml`, which builds this documentation site and
deploys it to GitHub Pages on pushes to `develop` that touch `docs/**`.

**Mutation testing baseline (measured, not fabricated):** the first run on
2026-09-05 found 11 mutants, all killed. Re-measured 2026-09-25 against
`./internal/domain` (now `processpath`, `cptschedule` and `shared`) —
**63 mutants, all killed, zero survivors: 100.00% efficacy, 100.00%
mutator coverage.** `.gremlins.yaml` sets the threshold to 99 (strictly
below the measured 100%), the same "lock in today's quality" philosophy
every other repo in this fleet's `.gremlins.yaml` uses.

## Helm chart

`charts/process-path-management/` ships with BOTH a default-enabled-capable
`ingress.yaml` (disabled by default, `ingress.enabled=false`) AND an
additive `gatewayApi`/`httproute.yaml` template (also disabled by default),
matching the dual ingress/Gateway API pattern this fleet's other charts
have all migrated to. `helm lint` and two real `helm template` renders
(default `Ingress`, and `--set gatewayApi.enabled=true` with a populated
`parentRefs`/`hosts` value producing a correctly-shaped `HTTPRoute` with
`sectionName: http`) were verified locally before this chart shipped.

## Known gaps

- **`destinationLocationRole` is carried but not yet acted on.**
  `fulfillment-execution`, `wes-work-planning` and `workforce-management`
  decode `destination_location_role` into their local catalogue caches;
  no routing decision in those repos reads it yet. See
  [docs/docs/ecosystem/context-map.md](docs/docs/ecosystem/context-map.md).
- **The ops agent's MCP client is wired but unused.** `warehouse-ops-agent`
  constructs a client for this server's tools but no use case calls it
  yet.

## Architecture Decision Records

1. [0001 — Process Path Management as a new Generic Subdomain bounded context](docs/docs/adr/0001-process-path-management-bounded-context.md)
2. [0002 — Cutting the fleet over from the static YAML catalogue to this service's events](docs/docs/adr/0002-yaml-to-kafka-cutover.md)
3. [0003 — Transactional outbox for the process-path Published Language](docs/docs/adr/0003-transactional-outbox.md)
4. [0004 — Adopting the fleet REST identity standard (static bearer keys, read/read-write scopes)](docs/docs/adr/0004-rest-auth-adoption.md) (superseded by 0005)
5. [0005 — Removing the REST auth layer](docs/docs/adr/0005-remove-rest-auth.md)
6. [0006 — MCP server as a second inbound adapter](docs/docs/adr/0006-mcp-server-second-inbound-adapter.md)
7. [0007 — Analytical data product (report) via a separate analytics topic](docs/docs/adr/0007-analytical-data-product.md)
8. [0008 — Seven new process-path families aligned to real FC labor-tracking vocabulary](docs/docs/adr/0008-fclm-aligned-process-path-families.md)
9. [0009 — Optional destination LocationRole on a ProcessPath](docs/docs/adr/0009-destination-location-role-on-process-path.md)
10. [0010 — Process paths publish a fulfillment capability contract (cycle time, eligibility, CPT schedule)](docs/docs/adr/0010-fulfillment-capability-contract.md)
11. [0011 — Transactional Idempotency-Key middleware for POST /process-paths](docs/docs/adr/0011-idempotency-key-middleware.md)
12. [0012 — Analytics-consumer dead-letter queue and graceful shutdown hardening](docs/docs/adr/0012-kafka-dlq-and-graceful-shutdown.md)
13. [0013 — Hash balancer for the outbound Kafka writers](docs/docs/adr/0013-kafka-writer-hash-balancer.md)
14. [0014 — Per-workload HorizontalPodAutoscaler and pgxpool tuning](docs/docs/adr/0014-horizontal-autoscaling-and-pgxpool-tuning.md)
15. [0015 — Run golang-migrate against a direct Postgres connection, not PgBouncer](docs/docs/adr/0015-migrations-direct-postgres-connection.md)
16. [0016 — CloudEvents 1.0 as the mandatory event envelope](docs/docs/adr/0016-cloudevents-mandatory-event-envelope.md)
17. [0017 — Optimistic concurrency (version column) on ProcessPath and CPTSchedule](docs/docs/adr/0017-optimistic-concurrency-version-column.md)
18. [0018 — Outbox lag gauge and housekeeping sweeper](docs/docs/adr/0018-outbox-lag-gauge-and-housekeeping-sweeper.md)
19. [0019 — Adopting the fleet standard-metrics convention](docs/docs/adr/0019-standard-metrics-convention.md)
20. [0020 — Adopting RFC 7807 Problem Details for all HTTP error responses](docs/docs/adr/0020-rfc-7807-problem-details.md)
21. [0021 — The architecture fitness test suite as a merge gate](docs/docs/adr/0021-architecture-fitness-suite.md)
22. [0022 — The operator console MFE remote and its chart component](docs/docs/adr/0022-mfe-console-remote.md)
23. [0023 — The MCP eval and governance harness](docs/docs/adr/0023-mcp-eval-and-governance-harness.md)
24. [0024 — Boot-time dial retry for the Istio native-sidecar warm-up race](docs/docs/adr/0024-bootretry-for-istio-native-sidecar-warmup.md)
25. [0025 — Schemathesis property-based contract testing against the live API](docs/docs/adr/0025-schemathesis-contract-job.md)
26. [0026 — Refuse to deactivate a process path that a CPT schedule still lists](docs/docs/adr/0026-reject-deactivation-of-paths-in-cpt-schedules.md)
27. [0027 — Propagate W3C trace context in Kafka headers](docs/docs/adr/0027-w3c-trace-context-on-kafka-headers.md)
28. [0028 — Close the path / CPT-schedule race with row locks](docs/docs/adr/0028-close-path-cpt-schedule-race-with-row-locks.md)

## License

MIT — see [LICENSE](LICENSE).

## Operator micro-frontend (`web/`)

`web/` is `process_path_mfe`, this context's Module Federation remote. It talks only to
this service's own REST API and is never part of `make check`.

**Standalone development** is unchanged:

```bash
cd web && npm install && npm run dev     # http://localhost:5189
```

**Deployed to the kind cluster**, it is built into a static bundle and served
by its own `nginx-unprivileged` pod:

```bash
cd web
docker build --build-context uikit=../../warehouse-ui-kit \
  -t warehouse/process-path-management-frontend:local .
```

The cluster's localhost topology separates the two kinds of traffic onto two
independent entrypoints, and neither proxies to the other:

| URL | Served by | Carries |
|---|---|---|
| `http://localhost/mfes/process-path-management/` | Nginx web gateway → this remote's nginx pod | HTML, JS, CSS, fonts, `remoteEntry.js` |
| `http://localhost:8000/api/process-path-management/` | Kong | this service's REST API |

Kong never serves frontend assets, and the Nginx gateway never proxies an API.
Enable the workload with `frontend.enabled=true` in the Helm chart; the Service
is deliberately `ClusterIP` with no Ingress/HTTPRoute, because frontend path
routing belongs to the Nginx web gateway in `warehouse-infra`.

Because one image must work in more than one environment, the remote reads its
API origin at runtime from `window.__WAREHOUSE_CONFIG__.apiOrigin` (published
by the console shell) rather than baking a hostname in at build time. A
production build with no runtime config **fails loudly** instead of silently
falling back to a developer port; standalone `npm run dev` still uses
`http://localhost:8087`. See `web/src/config.ts`.

Chart invariants are asserted by:

```bash
python3 charts/process-path-management/tests/test_service_selectors.py
```

which proves every Service selects exactly one Deployment — the OLTP Service
must never select the frontend, analytics or MCP pods.
