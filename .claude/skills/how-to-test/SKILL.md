---
name: how-to-test
description: Write or review tests and diagnose a failing coverage, mutation, bdd or integration CI job: the four test layers, the 90% gate, gremlins threshold semantics, the testcontainers rule. Use when adding tests, killing a surviving mutant, or fixing a red check.
---

# How to test

Use when writing or reviewing tests in this repo, or diagnosing a
failing `coverage`/`mutation-fast`/`bdd`/`integration` CI job. This
fleet's quality bar is layered — passing `go test` is necessary but is
the WEAKEST signal of the four; mutation testing exists specifically
because green tests can assert nothing.

## The four layers, in order of what they actually prove

1. **Unit tests** (`go test ./...`) — prove the code runs without
   panicking and returns SOMETHING. Table-driven, in-memory
   fakes/adapters only (see `internal/application/usecases/fakes_test.go`),
   never a real network/DB call.
2. **Coverage** (`make coverage`, 90% gate on
   `./internal/domain/...,./internal/application/...`) — proves lines
   executed. Proves nothing about whether the test asserted the right
   thing.
3. **Mutation testing** (`make mutation`/`mutation-fast`, gremlins) —
   proves the tests actually ASSERT, not merely execute. A mutant is a
   deliberately broken version of the code (`<` -> `<=`, `+` -> `-`,
   etc.); if the test suite still passes against the mutant, it
   "survived" (LIVED) — meaning no test would catch that exact bug in
   production. This is the sensor most worth understanding deeply.
4. **BDD / behaviour** (`make bdd`, godog) — proves the use case works
   end-to-end through the real HTTP surface, not through a mocked port.
   See `features/define_and_read.feature`, `features/revise.feature`,
   `features/deactivate.feature` for this repo's exact Given/When/Then
   shape.

## Mutation testing: this repo's gate is unusually strict — read `.gremlins.yaml`'s own comment first

`.gremlins.yaml` sets `efficacy: 99` / `mutant-coverage: 99` — gremlins
fails when the MEASURED value is `<=` the threshold, so `99` means "must
be 100%, i.e. zero survivors." The file's own comment documents exactly
why this number is set where it is: measured 2026-09-05, this service's
first-ever mutation run found 11 mutants total (all in
`internal/domain/processpath`, the only aggregate in the domain layer at
the time) with ALL killed — zero survivors, zero not-covered, 100%
efficacy and mutator coverage. `99` locks that in and fails the build the
moment a single mutant survives. The domain tree has since grown to
`processpath`, `cptschedule` and `shared`; re-measured 2026-09-25 it is
**63 mutants, all killed** (100% efficacy and mutator coverage), so the
threshold still holds. If a new domain package introduces a survivor, you
may need to re-baseline the threshold in the SAME PR with a dated comment
explaining why — never silently.

CI enforces this as `mutation-fast`, scoped to `./internal/domain` (the
entire domain layer, since there's no second aggregate to split a "fast
subset" away from the way labor-performance splits standard/performance)
— see `.github/workflows/ci.yml`'s `mutation-fast` job, which also runs
weekly via `schedule: cron` independent of pushes. Locally:

```bash
go run github.com/go-gremlins/gremlins/cmd/gremlins@v0.6.0 unleash \
  ./internal/domain --workers 1 --timeout-coefficient 30
```

`workers: 1` and `timeout-coefficient: 30` in `.gremlins.yaml` are a
deliberate workaround for a spurious-timeout failure mode under parallel
workers contending over the build cache — this repo, labor-performance,
and inventory-storage all carry the same workaround in their own
`.gremlins.yaml`; don't "simplify" it back to the default worker count.

## Three real pitfalls that have each cost a real CI failure in this fleet

### 1. Zero/origin-value fixtures hide arithmetic mutants

A test built around zero-valued operands (e.g. a duration of `0` for
`CycleTimeP95`, or an empty `Eligibility{}`) makes `a - b` and `a + b`
produce the same result, so a mutant flipping `-` to `+` survives even
though coverage looks complete. Any new value object with real
arithmetic (this repo's `shared.Eligibility`'s `MaxUnitsPerLine` handling
is a real candidate) needs fixture values where EVERY operand and every
per-field delta is distinct and non-zero, and the test must assert the
exact expected value, not just "no error".

### 2. Boundary guards need the boundary value itself

A test for a guard like `matchPrefix` non-empty/lower-case validation in
`processpath.Define` that only tries an obviously-invalid value and an
obviously-valid one never exercises the exact boundary — so a
`CONDITIONALS_BOUNDARY` mutant survives silently. Every boundary guard
needs an explicit test for the boundary value itself. See
`internal/domain/processpath/process_path_test.go` and
`internal/domain/shared/eligibility_test.go` for this repo's existing
boundary-value test style before adding a new guard.

### 3. Tie-break / near-equivalent mutants: know when NOT to chase them

If a future domain rule ever compares two values for a tie-break
decision, know that a `<` -> `<=` mutant on a tie-break condition is
undetectable by ANY test whose values are all distinct — the mutation
only diverges on an exact tie. Do NOT force an artificial tied-value
fixture just to kill this; that pins an arbitrary, currently-unspecified
tie-break order as if it were a real invariant, which is worse than an
accepted near-equivalent survivor. This repo does not have a
`MUTATION.md` triage file yet (check before assuming one exists) — if
you hit a genuinely near-equivalent, undetectable mutant, create one
documenting it rather than chasing it or silently lowering the 99%
threshold.

## Diagnosing a `mutation-fast` CI failure: diff against develop, don't chase every LIVED line

```bash
gremlins unleash ./internal/domain          # on your branch
git stash && git checkout origin/develop -- . && gremlins unleash ./internal/domain   # baseline
```

Given this repo's baseline is zero survivors today, ANY new survivor on
your branch is a real regression, not noise to filter — there is no
existing accepted-survivor set to diff against yet the way some other
fleet services have.

## Kafka/Postgres integration tests: testcontainers, never a skip-gate — and it's enforced by a fitness test here

A `-tags=integration` test touching Kafka or Postgres MUST start its own
container via `testcontainers-go`. Never gate on
`os.Getenv("KAFKA_BROKERS")` + `t.Skip(...)`, and never hardcode
`localhost:9092`. This is not just a fleet convention here — this repo's
`internal/architecture/fitness_test.go` has
`TestKafkaIntegrationTestsUseTestcontainers`, which scans every
`*_integration_test.go` file that touches Kafka (imports
`segmentio/kafka-go`, references `kafka.Reader`/`kafka.Writer`, or
mentions `KAFKA_BROKERS`) and statically fails the build if it finds a
skip-gate, a hardcoded broker address, or a missing
`testcontainers-go/modules/kafka` import. This repo's own integration
tests (`internal/adapters/outbound/postgres/outbox_integration_test.go`,
`analytics_outbox_integration_test.go`, `process_path_repo_integration_test.go`)
are Postgres-testcontainers-based — CI's `integration` job provisions a
Postgres service container only (`.github/workflows/ci.yml`'s
`integration` job), never Kafka. If you add a REAL Kafka round-trip test
here for the first time, follow inventory-storage's
`consumer_integration_test.go` recipe: unique topic per test, one shared
container per package, explicit `CreateTopics` + poll `ReadPartitions`
for the leader before the first read/write.

Also run `go build -tags=integration ./...` and
`go vet -tags=integration ./...` before pushing any change to a
constructor/interface signature that an integration test might call —
integration files are invisible to the default build, so a signature
change can pass `make check-all` locally and only break CI's separate
`integration` job.

## Verify before opening the PR

```bash
make check-all   # check + coverage + arch-test + bdd (the full local gate)
```

`make check-all` does NOT include `mutation-fast`/`vuln`/`api-lint`
locally per this repo's own Makefile comment structure — run those
explicitly too:

```bash
make mutation     # alias for mutation-fast
make vuln         # govulncheck ./...
make api-lint     # spectral lint on both apis/openapi.yaml and apis/asyncapi.yaml
make arch-test    # go test ./internal/architecture/... -v — also catches a reintroduced
                   # auth middleware, a literal Kafka GroupID, or an outbound HTTP client
```

CI runs all of these even when your local habit doesn't, so a PR can
pass a narrower local check and still go red in CI otherwise.
