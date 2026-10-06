---
paths:
  - "cmd/**"
  - "internal/**"
  - "migrations/**"
  - "charts/**"
  - "features/**"
---

# Architecture details and package layout

Moved out of `CLAUDE.md`; still authoritative. The headline rules (inward-only
dependencies, no sibling calls) stay in `CLAUDE.md`.

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

Layout (annotations are the non-obvious part):

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
migrations/                       golang-migrate SQL files (0001–0007); migrations/analytics/ for the report DB
apis/openapi.yaml                 This service's OWN REST API (8 operations; /readyz is served but not in the spec)
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
