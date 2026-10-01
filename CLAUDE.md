# Project: Process Path Management (Generic Subdomain)

> This is the **ninth** bounded-context Go service in the warehouse-systems
> fleet (after order-management, inventory-storage, wes-work-planning,
> workforce-management, fulfillment-execution, facility-layout,
> warehouse-ops-agent, labor-performance). See ADR 0001 for the full
> decision record.

The fleet's **operator-configurable process-path catalogue**: a path's
canonical identity (`PathId`, e.g. `PICK`, `PACK`, `REBIN`, `SLAM`), the
`matchPrefix` rule downstream consumers use to resolve a caller-supplied id
to a path family, whether it is `Direct`, and the capabilities a
station/associate must hold to work it. Before this service existed, that
definition lived in a static YAML file
(`warehouse-infra/config/process-paths/sortable-fc.yaml`) loaded once at
boot by `fulfillment-execution`, `wes-work-planning`, and
`workforce-management` — a published language with no single owner, no
audit trail, and no way to revise without a coordinated redeploy of all
three consumers. This service replaces that file as the single source of
truth.

## Strategic classification (read this before writing any code)

**Generic Subdomain**, the same bucket as `facility-layout` — well
understood, not a competitive differentiator, but needed identically by
multiple existing contexts, so it is extracted rather than duplicated or
left as an unowned static file (ADR 0001).

This service is the **Open Host Service / Published Language SOURCE** for
`warehouse.process-path-management.events`, with `fulfillment-execution`
(Core), `wes-work-planning` (Core), and `workforce-management`
(Supporting) as its **Conformist** consumers. It has **zero inbound
dependency** — no inbound Kafka consumer, no synchronous REST dependency
in either direction. It never calls into any other service, synchronously
or otherwise; every change propagates exclusively via Kafka.

**Consumers (live):** the three above replaced their static YAML catalogue
with this topic on 2026-09-06 (ADR 0002); `order-management` also
consumes it for `cycle_time_p95`/`eligibility` and `CPTScheduleChanged`
(ADR 0010). `warehouse-ops-agent` reads this service over MCP;
`warehouse-console` mounts its `web/` remote. See
`docs/docs/ecosystem/context-map.md`, and verify against the sibling
repos' `origin/develop` before relying on it.

## Architecture (NON-NEGOTIABLE)

Hexagonal / Ports & Adapters. Strict inward-only dependency rule —
**domain depends on nothing; application depends on domain; adapters
depend on application/domain** — enforced by `internal/architecture/architecture_test.go`
(arch-go fitness tests, `make arch-test`), not just convention:

- domain (`internal/domain/**`) may only depend on other domain packages.
- application (`internal/application/**`) may only depend on domain +
  application.
- inbound adapters never depend on outbound adapters, and vice versa.
- nothing under `internal/**` may import `cmd/**` — `cmd` is a leaf
  composition root, never a dependency of the layers it wires.

No framework, HTTP, Kafka, or SQL types in the domain layer. No JSON
struct tags in the domain packages.

```
cmd/
  pathmgmt/                       REST API + in-process outbox relay (:8080)
  mcp/                            MCP server, Streamable HTTP (:8090, ADR 0006)
  pathmgmt-projector/             analytics projector (admin :8091, ADR 0007)
  pathmgmt-reports/               read-only catalogue-growth report API (:8092, ADR 0007)
internal/
  domain/
    processpath/                  ProcessPath aggregate (process_path.go)
    cptschedule/                  CPTSchedule aggregate + CPTScheduleChanged (ADR 0010)
    shared/                       PathId, Capability, SiteId, DestinationLocationRole, Eligibility; domain events
  application/
    ports/                        OUT: ProcessPathRepo, CPTScheduleRepo, EventPublisher, UnitOfWork, Clock, PathMetrics
    usecases/                     DefinePath, RevisePath, DeactivatePath, GetPath, ListPaths, DefineCPTSchedule, GetCPTSchedule
  analytics/report/               catalogue-growth read model + ports
  adapters/
    inbound/http/                 chi handlers, DTOs, RFC 7807 error mapping (server.go); reports router
    inbound/mcp/                  read-only MCP tools over the read use cases
    inbound/kafka/                analytics consumer — this service's OWN analytics topic only
    outbound/postgres/            pgxpool repos, unit of work, outbox publisher + relay, golang-migrate runner
    outbound/memory/              in-memory repos for tests/local (also the zero-DATABASE_URL runtime path)
    outbound/events/              log publisher (default when EVENT_PUBLISHER != kafka)
    kafka/cloudevents/            the ONLY CloudEvents 1.0 envelope code: New/Decode/ContentTypeHeader + Type* consts (ADR 0016)
    outbound/kafka/               integration + analytics publishers (EVENT_PUBLISHER=kafka), topic constants
    outbound/analyticsstore/      analytics projection/report store
    outbound/telemetry/           OTel traces/metrics/logs
  architecture/                   arch-go + fitness tests (architecture_test.go, fitness_test.go)
migrations/                       golang-migrate SQL files (0001–0005); migrations/analytics/ for the report DB
apis/openapi.yaml                 This service's OWN REST API (8 endpoints)
apis/asyncapi.yaml                What this service PUBLISHES (integration + analytics topics, CloudEvents 1.0)
features/                         godog/Gherkin BDD acceptance tests
web/                              process_path_mfe: Vite + React Module Federation remote (operator SPA)
charts/process-path-management/   Helm chart (API, MCP, projector, reports, frontend)
docker-compose.yml                Local Postgres 16
docs/docs/adr/                    Architecture Decision Records (Nygard format)
```

This service **never subscribes to another context's topic**. Its only
Kafka consumer (`internal/adapters/inbound/kafka`, run by
`cmd/pathmgmt-projector`) reads its OWN analytics topic
`warehouse.process-path-management.analytics` (ADR 0007). Every event is
enqueued onto both the integration and the analytics topic in the same
outbox transaction. `TestNoSiblingContextOutboundCalls` fails the build if
`internal/adapters/outbound/**` imports `net/http` — no REST or MCP client
to a sibling may ever be added.

Delivery modes (`DATABASE_URL` × `EVENT_PUBLISHER`) and the transactional
outbox (ADR 0003 — this repo is the fleet's reference implementation):
`.claude/rules/runtime-and-outbox.md`.

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
order-management): `...processpath.ProcessPathCreated`,
`...processpath.ProcessPathUpdated`, `...processpath.ProcessPathDeactivated`
(subject = `path_id`) and `...cptschedule.CPTScheduleChanged` (subject =
`site_id`), all under `com.warehouse.wes.process-path-management`. Its own
projector DLQs any non-CloudEvents message on the analytics topic.

## Key Commands

Every `make` target mirrors a step in `.github/workflows/ci.yml`:

```bash
make check         # FAST bundle: fmt-check vet build lint test — run after every change
make check-all      # check + coverage (90% gate on domain + application) + arch-test + bdd — run before pushing
make build           # go build ./...
make vet             # go vet ./...
make fmt             # gofmt -w . (in place)
make fmt-check       # fail if gofmt -l . is non-empty
make lint            # golangci-lint run ./... (CI pins v2.13.1)
make test            # go test ./... -race
make coverage        # coverage profile + the 90% threshold gate
make bdd             # go test ./... -run TestFeatures -v (godog/Gherkin)
make arch-test       # go test ./internal/architecture/... -v (hexagonal dependency rule)
make integration     # go test -tags=integration ./... -race -count=1 (needs DATABASE_URL; testcontainers-backed)
make mutation-fast   # gremlins unleash ./internal/domain — CI's blocking mutation job (see .gremlins.yaml, threshold 99%)
make mutation        # alias for mutation-fast
make api-lint        # Spectral lint on apis/openapi.yaml and apis/asyncapi.yaml
make vuln            # govulncheck ./...
```

Helm: `helm lint charts/process-path-management` (CI runs it only on PRs
targeting `main`).

Local run (in-memory, Postgres, Kafka): `.claude/rules/runtime-and-outbox.md`.

Docs site (Docusaurus, generated OpenAPI reference pages):

```bash
cd docs && npm ci && npm run gen-api-docs pathmgmt   # regenerate docs/docs/api-reference/rest/* from apis/openapi.yaml
npm run build                                          # full site build, verifies no broken links
```

Git hooks via [lefthook](https://github.com/evilmartians/lefthook) — not
tracked by git, activate once per clone: `lefthook install`. `pre-commit`
runs fmt-check/vet/lint; `pre-push` runs `make check`.

## Further reading (`.claude/rules/`)

Full ubiquitous language, aggregate invariants, and domain events:
`.claude/rules/domain-model.md`.

Full REST API endpoint table, testing discipline, and CI job matrix:
`.claude/rules/testing-and-api.md`.

## Docs site and GitFlow (repo-specific — do not "fix" without checking first)

- GitFlow: `develop` is the working branch; `main` is release-only,
  synced by explicit fast-forward.
- `.github/workflows/docs.yml` triggers on **push to `develop`** with
  paths `docs/**` (not `main`, and not on every push) — this is
  deliberate for this repo (its GitHub Pages `github-pages` deployment
  environment only allows the `develop` branch), matching every other
  service's docs pipeline in this fleet. Do not "fix" this to trigger off
  `main`.
- `docs/package.json`'s `gen-api-docs` script (`docusaurus gen-api-docs
  pathmgmt`) regenerates `docs/docs/api-reference/rest/*.api.mdx` from
  `apis/openapi.yaml`. Whenever `apis/openapi.yaml` changes (new
  operation, changed request/response shape, changed
  summary/description), regenerate and commit the `.mdx`/`.json`
  companions — they are committed generated output, not hand-written.
  CI's `docs-api-drift` job runs `npm run clean-api-docs pathmgmt && npm
  run gen-api-docs pathmgmt` and fails on any diff.

## What this service deliberately does not own

- Does not decide dispatch, routing, or task assignment — defines WHAT a
  path is and WHICH capabilities it requires; never claims, assigns, or
  completes work. That is `fulfillment-execution`'s job.
- Does not call any other context — REST, MCP or otherwise — ever.
- Consumes no other context's topic (its only consumer reads its own
  analytics topic).
