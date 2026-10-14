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
| Contract | `scripts/contract-test.sh`; `internal/adapters/inbound/http/reports_contract_test.go` | Builds `cmd/pathmgmt`, boots it in-memory on `127.0.0.1:18085`, runs Schemathesis `4.28.0` against `apis/openapi.yaml` (100 examples per operation, 4 workers). The reports router is checked against `apis/openapi-reports.yaml` by Go tests ([ADR 0031](../adr/0031-reports-api-openapi-contract.md)) | `make contract`; the Go tests run in `make test` |
| Integration | 26 files tagged `//go:build integration` (25 under `internal/adapters/...`, plus `internal/application/usecases/usecases_wiring_integration_test.go`) | Each test starts its own Postgres 16 and/or Kafka with testcontainers-go; no `DATABASE_URL`, no compose | `make integration` (needs Docker) |
| Architecture fitness | `internal/architecture/` | Static checks over the source tree and import graph | `make arch-test` |
| MCP evals | `internal/adapters/inbound/mcp/eval_*_test.go`, `evalsuite_test.go`, `testdata/features/mcp_tools.feature` (14 scenarios) | In-process MCP client/server sessions | `go test ./internal/adapters/inbound/mcp/... -race -run '^TestEval\|^TestMCPEvalSuite' -v` |
| Mutation | `.gremlins.yaml` | gremlins `v0.6.0` over `./internal/domain` | `make mutation-fast` (alias `make mutation`) |
| Chart | `charts/process-path-management/tests/render_test.sh`, `test_service_selectors.py` | `helm template` assertions: the api Deployment mounts the ConfigMap via `envFrom` (D14), `OUTBOX_MAX_ATTEMPTS` (ADR 0030), a zero housekeeping interval, the analytics pods' `LOG_LEVEL`/`ENVIRONMENT`/`SERVICE_VERSION` (ADR 0029); each Service selects exactly one Deployment | `make chart-test` (helm + PyYAML) |
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
| `features/relocate_path.feature` | 4 | The declared RELOCATE entry: defined Active, published with `intake` `SLOT_RELOCATION` and no fake eligibility, re-seed refused 409, sits beside PICK |
| `features/process_path_intake.feature` | 12 | Intake ([ADR 0034](../adr/0034-process-path-intake.md)): define/read/publish, `[]` on the wire when absent, 422 `invalid-intake` for an unknown or repeated source, intake-only revise publishes `ProcessPathUpdated`, an omitted intake is kept, `[]` clears it, and the declared intake of PICK/PACK/REBIN/SLAM/RELOCATE (outline) |

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

Schemathesis runs only against the REST API in `apis/openapi.yaml`. The
reports routes are contract-tested in Go instead:
`TestReportsContract_RoutesMatchSpec` walks the real chi router and fails
when a served route is missing from `apis/openapi-reports.yaml` (or the
reverse); `TestReportsContract_ResponsesMatchSpec` drives every documented
status (200/400/500) and fails on an undeclared status or media type, a
missing required property or an undocumented one. MCP has no Schemathesis
coverage (it has the evals instead).

### Integration tests

| Area | Files (all `internal/adapters/...`) | What they prove against real infrastructure |
| --- | --- | --- |
| Postgres repositories | `outbound/postgres/process_path_repo_integration_test.go`, `process_path_create_integration_test.go`, `cpt_schedule_repo_integration_test.go`, `intake_integration_test.go`, `integration_test.go` | Mapping, insert-only create, schedule persistence; the `intake` column (migration 0009) round-trips, a pre-0009 row reads as `[]`, an unknown stored value fails the read (`TestPostgres_IntakeRoundTrip`, `TestPostgres_RowWithoutIntake_ReadsAsTheEmptySet`, `TestPostgres_UnknownStoredIntake_FailsTheRead`, [ADR 0034](../adr/0034-process-path-intake.md)) |
| Concurrency | `define_concurrent_integration_test.go`, `version_integration_test.go`, `process_path_locks_integration_test.go`, `cpt_path_race_integration_test.go`, `deactivate_cpt_integration_test.go` | Duplicate-create race, optimistic `version` column, `FOR UPDATE`/`FOR SHARE` serialisation of deactivate vs CPT schedule ([ADR 0028](../adr/0028-close-path-cpt-schedule-race-with-row-locks.md)) |
| Outbox | `outbox_integration_test.go`, `analytics_outbox_integration_test.go`, `outbox_trace_integration_test.go`, `outbox_trace_kafka_integration_test.go` | Two rows per event, relay ordering and retry, the attempt cap (`TestOutboxRelay_MaxAttempts_PoisonRowIsExcludedAndTheNextRowPublishes`, `TestOutboxRelay_WithoutMaxAttempts_RetriesWithoutBound`, [ADR 0030](../adr/0030-outbox-relay-attempt-cap.md)), trace context stored and sent, and end to end through a real broker (`TestOutboxTrace_TraceparentCrossesTheOutboxToKafka`, `TestOutboxTrace_RowWithoutTraceHeadersRelaysUnchanged`, gap fix D27) |
| Housekeeping and pools | `sweeper_integration_test.go`, `pool_limits_integration_test.go` | Batch deletes, `MaxConns` and `statement_timeout` actually applied |
| Idempotency | `inbound/http/idempotency_integration_test.go` | Replay, key reuse 422, concurrent same-key requests |
| Kafka | `outbound/kafka/publisher_balancer_integration_test.go`, `inbound/kafka/analytics_consumer_integration_test.go`, `analytics_consumer_dlq_integration_test.go`, `analytics_shutdown_integration_test.go`, `analytics_atomic_claim_integration_test.go` | Same key → same partition, projector end to end, DLQ routing, clean shutdown without dead-lettering, and claim + apply in one transaction (`TestAnalyticsConsumer_CrashBetweenClaimAndApply_RedeliveryAppliesExactlyOnce`: the projector stops after the claim and before the apply commits; no claim is left and the redelivery applies the event exactly once, [ADR 0032](../adr/0032-analytics-claim-and-apply-in-one-transaction.md)) |
| Analytics store | `outbound/analyticsstore/postgres_integration_test.go`, `unit_of_work_integration_test.go` | Projection idempotency, report query, freshness on an empty store; the analytics UnitOfWork rolls back the consumer claim, the projection claim and the rollup row together (`TestUnitOfWork_ClaimAndProjectionRollBackTogether`, `TestUnitOfWork_CommitsClaimAndProjectionAndJoinsNestedScope`) |

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
| `TestOutboxRelaysPassAttemptCap` (+ `TestOutboxRelayCapSensorFailsOnBadFixtures`, `outbox_relay_cap_fitness_test.go`) | Every non-test `cmd/` call to `NewOutboxRelay` passes `WithMaxAttempts` ([ADR 0030](../adr/0030-outbox-relay-attempt-cap.md)) |
| `TestKafkaWritersSetRequiredAcks` (+ `TestKafkaWriterAcksSensorBites`, `kafka_writer_acks_fitness_test.go`) | Every kafka-go `Writer` literal in a non-test file under `internal/` or `cmd/` sets `RequiredAcks` (publishers use `syncWriterRequiredAcks`, the analytics DLQ writer `kafkago.RequireAll`): kafka-go defaults to `RequireNone` (acks=0), which silently drops a write on a broker blip while `WriteMessages` returns nil (fleet rollout 2026-10-13) |
| `TestMCPOptionalToolInputsCarryOmitempty` (+ `TestMCPOptionalToolInputsSensorFailsOnBadFixtures`, `mcp_optional_inputs_test.go`) | An MCP tool input field whose `jsonschema` description says "optional" carries `omitempty`, so the SDK does not mark it required (gap fix D16) |

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
all in `internal/domain/processpath`, all killed. The README records a
re-measurement on 2026-09-25, after `cptschedule` and `shared` joined the
domain layer: 63 mutants, all killed.

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
| `make api-lint` | Spectral on `apis/openapi.yaml` and `apis/openapi-reports.yaml` (`.spectral.yaml`) and `apis/asyncapi.yaml` (`.spectral.asyncapi.yaml`), `--fail-severity=warn` |
| `make chart-test` | `bash charts/process-path-management/tests/render_test.sh` and `python3 charts/process-path-management/tests/test_service_selectors.py` |
| `make vuln` | `govulncheck ./...` |
| `make contract` | `./scripts/contract-test.sh` |
| `make check` | `fmt-check vet build lint test` (the lefthook pre-push gate) |
| `make check-all` | `check` + `coverage` + `arch-test` + `bdd` |
| `make check-fast`, `make guide-lint`, `make harness-test` | Agent-harness gates (`scripts/harness/`) |
| `make catalogue-test` | `python3 scripts/test_seed_catalogue.py`: the declared-catalogue seed against an in-process fake API (ADR 0033) |
| `make seed-catalogue BASE_URL=... [SEED_ARGS=--dry-run]` | applies `catalogue/paths.json` to a running service through the REST API (not a test; see the runbook) |

The declared catalogue (`catalogue/paths.json`, ADR 0033) has its own tests:
`internal/domain/processpath/declared_catalogue_test.go` defines every entry
through the aggregate, requires an `intake` array on every entry, pins the
per-path intake table and exactly one `CUSTOMER_ORDER` entry point (PICK,
[ADR 0034](../adr/0034-process-path-intake.md)), and pins `RELOCATE` field by
field;
`internal/adapters/outbound/kafka/relocate_golden_test.go` pins RELOCATE's
`ProcessPathCreated` CloudEvent byte for byte on both streams;
`features/relocate_path.feature` (step `the client defines the declared
catalogue entry "<id>"` in `features_catalogue_test.go`) posts the file's
bytes through the real router; and `scripts/test_seed_catalogue.py` covers
every row of the seed's convergence table (it also runs in the CI
`guide-lint` job).

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
| `api-lint` | Spectral on the three specs | every run |
| `vuln` | govulncheck `v1.8.0` | every run |
| `arch-test` | `go test ./internal/architecture/... -v` | every run |
| `docs-api-drift` | in `docs/`: `npm ci`, `npm run clean-api-docs:all && npm run gen-api-docs:all`, then fails on any `git status --porcelain` change under `docs/api-reference/rest` or `rest-reports` | every run |
| `chart-tests` | helm `v3.16.4` + PyYAML `6.0.3`: `render_test.sh` and `test_service_selectors.py` | every run |
| `web` | checks out warehouse-ui-kit `develop`, builds it, then lint, `tsc -b`, tests and build of `web/` | every run |
| `drift` | deadcode, `go mod tidy -diff`, knip, coverage-quality; opens/closes a `harness:red` issue | schedule and dispatch only (advisory) |
| `helm-lint` | `ct lint` on the chart | PRs into `main` only |
| `trivy-scan` | builds the image, Trivy SARIF + blocking CRITICAL/HIGH gate | PRs into `main` only |
| `docker-publish` | build, push to GHCR, cosign sign, SBOM attest | push to `main` only |
| `release` | tag, versioned image, Helm chart to `oci://ghcr.io/iqvo` | push to `main` after `docker-publish` |

Other workflows: `docs.yml` builds this Docusaurus site on pull requests
touching `docs/**` or `apis/**` (build only, gap fix D25) and builds and
deploys GitHub Pages on push to `develop`;
`codeql.yml`; `scorecard.yml` (OpenSSF); `ai-review.yml` (advisory).

## Before you push

```bash
make check-all          # what the pre-push hook runs, plus coverage/arch/bdd
make integration        # if you touched an adapter (needs Docker)
make contract           # if you touched the REST API or apis/openapi.yaml
make chart-test         # if you touched charts/ (needs helm + PyYAML)
cd docs && npm ci && npm run build   # if you touched docs/
```
