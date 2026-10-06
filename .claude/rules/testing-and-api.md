---
paths:
  - "internal/adapters/inbound/http/**"
  - "apis/openapi*.yaml"
  - "apis/openapi/**"
  - "**/*_test.go"
  - "features/**"
---

# Testing discipline, CI matrix, and REST API reference

## REST API (inbound adapter — `internal/adapters/inbound/http`)

Nine routes, all unauthenticated (ADR 0005 removed the REST auth layer
ADR 0004 had added — do not re-add auth without checking that ADR first).
Eight are operations in `apis/openapi.yaml`; `GET /readyz` is served by the
router (`readiness.go`) but is not in the spec:

| Method | Path | Use case |
| --- | --- | --- |
| `POST` | `/process-paths` | DefinePath — 201, 409 if id exists, 422 on invariant violation |
| `GET` | `/process-paths` | ListPaths — active-only default, `?all=true` for audit view |
| `GET` | `/process-paths/{pathId}` | GetPath — regardless of status |
| `PUT` | `/process-paths/{pathId}` | RevisePath — 422 if deactivated |
| `DELETE` | `/process-paths/{pathId}` | DeactivatePath — 204, idempotent |
| `PUT` | `/sites/{siteId}/cpt-schedule` | DefineCPTSchedule — 200, wholesale define/revise; 422 if an eligiblePathId is not an Active path |
| `GET` | `/sites/{siteId}/cpt-schedule` | GetCPTSchedule — 404 if none |
| `GET` | `/healthz` | Liveness probe |
| `GET` | `/readyz` | Readiness probe — 503 once graceful shutdown starts (ADR 0012) |

`POST /process-paths` is wrapped by the Idempotency-Key middleware (ADR
0011) only when a Postgres pool is wired (`DATABASE_URL` set).

`POST`/`PUT /process-paths*` require `cycleTimeP95` (Go duration string,
> 0; 422 otherwise). The separate `cmd/pathmgmt-reports` binary serves
`GET /reports/catalogue-growth` and `/reports/catalogue-growth/freshness`
(ADR 0007); it is not in `apis/openapi.yaml`. The MCP server (`cmd/mcp`)
exposes read-only `get_process_path`, `list_process_paths`,
`get_cpt_schedule`, and `get_catalogue_growth_report` (only when
`REPORTS_BASE_URL` is set).

Every error is `application/problem+json` (RFC 7807), the fleet-standard
shape. `apis/openapi.yaml` is the full contract, Spectral-linted in CI
(`api-lint` job) — treat it as authoritative over any hand description
here, including this file.

## Testing Discipline

- `make check` after every change (fmt-check, vet, build, lint, test
  -race); `make check-all` (adds the 90% coverage gate, arch-test and
  bdd) before pushing. `make coverage` measures
  `./internal/domain/...`, `./internal/application/...` and
  `./internal/analytics/...`; CI's `test` job measures domain +
  application only.
- **Unit tests are colocated** (`process_path_test.go`, `events_test.go`,
  `usecases_test.go`, `unit_of_work_test.go`, `fakes_test.go`,
  `server_test.go`).
- **BDD**: `features/*.feature` (godog/Gherkin) — `define_and_read.feature`,
  `define_validation.feature`, `revise.feature`, `revise_and_events.feature`,
  `deactivate.feature`, `cpt_schedule.feature` (30 scenarios), driven
  through `features_test.go`. Run via `make bdd`.
- **Architecture fitness**: `internal/architecture/architecture_test.go`
  (arch-go) enforces the hexagonal dependency rule described above —
  every PR that reshapes package imports must keep this green, not just
  `go build`.
- **Integration** (`-tags=integration`, needs Docker):
  `internal/adapters/outbound/postgres/*_integration_test.go`,
  `internal/adapters/outbound/analyticsstore/postgres_integration_test.go`
  and `internal/adapters/inbound/kafka/*_integration_test.go` each boot
  their own Postgres/Kafka container via testcontainers — no
  `DATABASE_URL`, no CI service container
  (`TestPostgresIntegrationTestsUseTestcontainers` and
  `TestKafkaIntegrationTestsUseTestcontainers` enforce it). Files behind
  `//go:build integration` are invisible to default `go build`/`go test
  ./...` — always also run `go build -tags=integration ./...` and `go vet
  -tags=integration ./...` before pushing any change to a constructor or
  interface signature integration tests call.
- **Mutation testing**: `.gremlins.yaml` targets `./internal/domain` only,
  threshold 99% (first baseline 11/11 mutants killed on 2026-09-05;
  re-measured 63/63 killed, 100% efficacy and coverage, on 2026-09-25
  across `processpath`, `cptschedule` and `shared`) — "lock in today's
  quality," not an aspirational bar. Run via `make mutation`.
- **API contract linting**: `spectral lint` against both
  `apis/openapi.yaml` (`.spectral.yaml`) and `apis/asyncapi.yaml`
  (`.spectral.asyncapi.yaml`), CI job `api-lint`.
- CI (`.github/workflows/ci.yml`) full matrix: `lint`, `guide-lint`,
  `complexity`, `test`, `bdd`, `contract` (Schemathesis), `evals-tests`
  (MCP E1–E3), `integration` (testcontainers), `mutation-fast` (blocking),
  `api-lint`, `vuln` (govulncheck), `arch-test`, `docs-api-drift`
  (regenerates `docs/docs/api-reference/rest` and fails on any diff —
  run `npm run clean-api-docs pathmgmt && npm run gen-api-docs pathmgmt`
  in `docs/` after any `apis/openapi.yaml` change), `web` (lint, `tsc -b`,
  test, build of `web/`), `drift` (advisory, schedule/manual only),
  `helm-lint`/`trivy-scan` (gated to PRs targeting `main` only),
  `docker-publish` and `release` (main-only). Plus
  `.github/workflows/codeql.yml`, `.github/workflows/scorecard.yml`,
  `.github/workflows/ai-review.yml` (advisory), and
  `.github/workflows/docs.yml` (builds the docs site and deploys GitHub
  Pages on pushes to `develop` touching `docs/**`).
