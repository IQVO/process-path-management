---
id: testing
title: Testing
sidebar_label: Testing
description: The process-path-management test pyramid as it exists in this repository - unit, BDD, contract, integration, mutation, architecture fitness and MCP evals - with the exact make targets and the CI jobs that run them.
---

# Testing

## The pyramid

| Layer | Where | How it runs | Run it |
| --- | --- | --- | --- |
| Unit | `*_test.go` next to the code (no build tag): domain aggregates, use cases with fakes (`internal/application/usecases/fakes_test.go`), HTTP handlers via `httptest`, encoders, relay/consumer logic with fake writers | In-process, in-memory adapters, `-race` | `make test` |
| BDD (godog) | 6 files in `features/` with **32 scenarios**, driven by `features_test.go` (`TestFeatures`) | Black-box over HTTP against the real chi router wired to the in-memory repositories, a fixed clock and a recording publisher, so scenarios assert responses **and** the domain events raised | `make bdd` |
| Contract | `scripts/contract-test.sh` | Builds `cmd/pathmgmt`, boots it in-memory on `127.0.0.1:18085`, runs Schemathesis `4.28.0` against `apis/openapi.yaml` (100 examples per operation, 4 workers) | `make contract` |
| Integration | 19 files tagged `//go:build integration` under `internal/adapters/...` | Each test starts its own Postgres 16 and/or Kafka with testcontainers-go; no `DATABASE_URL`, no compose | `make integration` (needs Docker) |
| Architecture fitness | `internal/architecture/` | Static checks over the source tree and import graph | `make arch-test` |
| MCP evals | `internal/adapters/inbound/mcp/eval_*_test.go`, `evalsuite_test.go`, `testdata/features/mcp_tools.feature` (14 scenarios) | In-process MCP client/server sessions | `go test ./internal/adapters/inbound/mcp/... -race -run '^TestEval\|^TestMCPEvalSuite' -v` |
| Mutation | `.gremlins.yaml` | gremlins `v0.6.0` over `./internal/domain` | `make mutation-fast` (alias `make mutation`) |
| Frontend | `web/` | ESLint, `tsc -b`, the remote's own tests, Vite build | `cd web && npm run lint && npx tsc -b && npm test && npm run build` |

### BDD features

| File | Scenarios | Covers |
| --- | --- | --- |
| `features/define_and_read.feature` | 5 | Define a path, read it back, 404 for an unknown id, duplicate id rejected, list returns only active paths by default |
| `features/define_validation.feature` | 6 | Empty and upper-case `matchPrefix`, no capabilities, unknown `destinationLocationRole`, a declared role and eligibility carried on the path |
| `features/revise.feature` | 3 | Revise an Active path, 404 for an unknown id, a deactivated path rejects revision |
| `features/revise_and_events.feature` | 6 | `ProcessPathCreated`/`Updated`/`Deactivated` published (deactivation exactly once), no-op revision publishes nothing, malformed `cycleTimeP95`, re-defining a deactivated id rejected |
| `features/deactivate.feature` | 5 | Deactivate, idempotent re-deactivate, 404, 409 while a CPT schedule lists the path, success once the schedule no longer lists it |
| `features/cpt_schedule.feature` | 7 | Define and get a site schedule, 404 before definition, unknown or deactivated path in a cutoff, duplicate `cptId`, unrecognised timezone |

### Contract test exclusions

`scripts/contract-test.sh` disables two Schemathesis checks, each documented
in the script:

- `positive_data_acceptance`: the API correctly returns 422 for
  schema-valid data that OpenAPI 3.0.3 cannot express (lower-case
  `matchPrefix`, positive `cycleTimeP95`, IANA timezone, Active-only revise
  and `eligiblePathIds`). Each rule is covered by a BDD scenario and a unit
  test.
- `use_after_free`: `DELETE /process-paths/{pathId}` is a soft, idempotent
  retirement; `GET` after `DELETE` returns 200 with `status: DEACTIVATED`
  by design.

Only the REST API in `apis/openapi.yaml` is contract-tested. The reports
routes and MCP have no Schemathesis coverage (MCP has the evals instead).

### Integration tests

| Area | Files (all `internal/adapters/...`) | What they prove against real infrastructure |
| --- | --- | --- |
| Postgres repositories | `outbound/postgres/process_path_repo_integration_test.go`, `process_path_create_integration_test.go`, `cpt_schedule_repo_integration_test.go`, `integration_test.go` | Mapping, insert-only create, schedule persistence |
| Concurrency | `define_concurrent_integration_test.go`, `version_integration_test.go`, `process_path_locks_integration_test.go`, `cpt_path_race_integration_test.go`, `deactivate_cpt_integration_test.go` | Duplicate-create race, optimistic `version` column, `FOR UPDATE`/`FOR SHARE` serialisation of deactivate vs CPT schedule ([ADR 0028](../adr/0028-close-path-cpt-schedule-race-with-row-locks.md)) |
| Outbox | `outbox_integration_test.go`, `analytics_outbox_integration_test.go`, `outbox_trace_integration_test.go` | Two rows per event, relay ordering and retry, trace context stored and sent |
| Housekeeping and pools | `sweeper_integration_test.go`, `pool_limits_integration_test.go` | Batch deletes, `MaxConns` and `statement_timeout` actually applied |
| Idempotency | `inbound/http/idempotency_integration_test.go` | Replay, key reuse 422, concurrent same-key requests |
| Kafka | `outbound/kafka/publisher_balancer_integration_test.go`, `inbound/kafka/analytics_consumer_integration_test.go`, `analytics_consumer_dlq_integration_test.go` | Same key → same partition, projector end to end, DLQ routing |
| Analytics store | `outbound/analyticsstore/postgres_integration_test.go` | Projection idempotency, report query, freshness on an empty store |

The fitness tests `TestPostgresIntegrationTestsUseTestcontainers` and
`TestKafkaIntegrationTestsUseTestcontainers` fail the build if an
integration test gates on `DATABASE_URL`/`KAFKA_BROKERS` or hardcodes
`localhost:9092`, because a skip-gated test silently proves nothing in CI.

### Architecture fitness tests

| Test | Enforces |
| --- | --- |
| `TestHexagonalArchitecture` (`architecture_test.go`, arch-go) | domain → nothing; application → domain; adapters inward only |
| `TestMCPAdapterDependencyRule` | `internal/adapters/inbound/mcp` depends only on application and domain, and nothing imports it except `cmd/mcp` |
| `TestNoAuthMiddlewareReintroduced` | No bearer/JWT middleware in `internal/adapters/inbound` ([ADR 0005](../adr/0005-remove-rest-auth.md)) |
| `TestNoSiblingContextOutboundCalls` (+ `_DetectsViolation`) | No REST/MCP client to another fleet context |
| `TestKafkaConsumerGroupNeverHardcodedInline` | Consumer `GroupID` comes from a named symbol, never an inline literal |
| `TestKafkaIntegrationTestsUseTestcontainers`, `TestPostgresIntegrationTestsUseTestcontainers` (+ `_DetectsViolation`) | See above |
| `TestEventCatalogueMatchesContract`, `TestEventCatalogueDetector` (`catalogue_fitness_test.go`) | The CloudEvents types in the code match the event catalogue declared in `apis/asyncapi.yaml` |
| `TestCloudEventsOnly`, `TestReplayConsumersSetCommitInterval` (`events_fitness_test.go`) | CloudEvents 1.0 is the only envelope (no envelope-mode switch); a Kafka reader whose group id is generated per instance must set a commit interval |

### MCP evals

`TestEval_*` (governance: input schemas resolve and constrain, every
parameter described, tool registry matches a golden list, fleet-unique tool
names, conditional tools pinned), `TestEvalConformance_*` (wire: initialize
handshake, unknown tool / wrong-typed / extra arguments rejected, no
resources or prompts, session close) and `TestMCPEvalSuite` (behavioural
scenarios from `mcp_tools.feature`). See
[ADR 0023](../adr/0023-mcp-eval-and-governance-harness.md) and the
[Governance charter](../mcp/governance-charter.md).

### Mutation testing

`.gremlins.yaml`: `workers: 1`, `timeout-coefficient: 30`, thresholds
`efficacy: 99` and `mutant-coverage: 99` (gremlins fails when the measured
value is at or below the threshold, so any surviving mutant fails the job).
When the thresholds were set (2026-09-05) the domain produced 11 mutants,
all in `internal/domain/processpath`, all killed.

## Make targets

| Target | Runs |
| --- | --- |
| `make build` | `go build ./...` |
| `make vet` | `go vet ./...` |
| `make fmt` / `make fmt-check` | `gofmt -w .` / fail if `gofmt -l .` lists files |
| `make lint` | `golangci-lint run ./...` (CI pins `v2.14.0`) |
| `make test` | `go test ./... -race` |
| `make coverage` | `go test ./... -race -coverprofile=coverage.out -coverpkg=./internal/domain/...,./internal/application/...,./internal/analytics/...` and a 90 % gate |
| `make bdd` | `go test ./... -run TestFeatures -v` |
| `make arch-test` | `go test ./internal/architecture/... -v` |
| `make integration` | `go build`, `go vet` and `go test -tags=integration ./... -race -count=1` |
| `make mutation-fast` / `make mutation` | `gremlins unleash ./internal/domain --workers 1 --timeout-coefficient 30` |
| `make api-lint` | Spectral on `apis/openapi.yaml` (`.spectral.yaml`) and `apis/asyncapi.yaml` (`.spectral.asyncapi.yaml`), `--fail-severity=warn` |
| `make vuln` | `govulncheck ./...` |
| `make contract` | `./scripts/contract-test.sh` |
| `make check` | `fmt-check vet build lint test` (the lefthook pre-push gate) |
| `make check-all` | `check` + `coverage` + `arch-test` + `bdd` |
| `make check-fast`, `make guide-lint`, `make harness-test` | Agent-harness gates (`scripts/harness/`) |

The coverage package list differs between the two gates: `make coverage`
includes `./internal/analytics/...`, the CI `test` job measures only
`./internal/domain/...,./internal/application/...`.

## CI (`.github/workflows/ci.yml`)

Triggers: push and pull request to `main`, `develop`; weekly schedule
(Monday 06:00 UTC); manual dispatch.

| Job | Runs | When |
| --- | --- | --- |
| `lint` | golangci-lint `v2.14.0` | every run |
| `guide-lint` | `scripts/harness/guide_lint.py`, `repo_lint.py`, `test_hook.py`, `test_repo_lint.py` | every run |
| `complexity` | golangci-lint `--enable-only gocyclo,gocognit,cyclop,funlen,nestif`, plus an informational gocyclo report (> 10) | every run |
| `test` | build, vet, gofmt, unit tests with coverage, 90 % gate | every run |
| `bdd` | `go test ./... -run TestFeatures -v` | every run |
| `contract` | Schemathesis via `scripts/contract-test.sh` | every run |
| `evals-tests` | `go test ./internal/adapters/inbound/mcp/... -race -run '^TestEval\|^TestMCPEvalSuite' -v` | every run |
| `integration` | `go test -tags=integration ./... -race -count=1` (testcontainers on the runner's Docker) | every run |
| `mutation-fast` | gremlins over `./internal/domain` | every run |
| `api-lint` | Spectral on both specs | every run |
| `vuln` | govulncheck `v1.8.0` | every run |
| `arch-test` | `go test ./internal/architecture/... -v` | every run |
| `docs-api-drift` | in `docs/`: `npm ci`, `npm run clean-api-docs pathmgmt && npm run gen-api-docs pathmgmt`, then `git diff --exit-code -- docs/api-reference/rest` | every run |
| `web` | checks out warehouse-ui-kit `develop`, builds it, then lint, `tsc -b`, tests and build of `web/` | every run |
| `drift` | deadcode, `go mod tidy -diff`, knip, coverage-quality; opens/closes a `harness:red` issue | schedule and dispatch only (advisory) |
| `helm-lint` | `ct lint` on the chart | PRs into `main` only |
| `trivy-scan` | builds the image, Trivy SARIF + blocking CRITICAL/HIGH gate | PRs into `main` only |
| `docker-publish` | build, push to GHCR, cosign sign, SBOM attest | push to `main` only |
| `release` | tag, versioned image, Helm chart to `oci://ghcr.io/iqvo` | push to `main` after `docker-publish` |

Other workflows: `docs.yml` builds this Docusaurus site and deploys GitHub
Pages on push to `develop` touching `docs/**` (there is no PR-time docs
build, so run `npm run build` in `docs/` before pushing doc changes);
`codeql.yml`; `scorecard.yml` (OpenSSF); `ai-review.yml` (advisory).

## Before you push

```bash
make check-all          # what the pre-push hook runs, plus coverage/arch/bdd
make integration        # if you touched an adapter (needs Docker)
make contract           # if you touched the REST API or apis/openapi.yaml
cd docs && npm ci && npm run build   # if you touched docs/
```
