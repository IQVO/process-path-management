# Project: Process Path Management (Generic Subdomain)

The fleet's **operator-configurable process-path catalogue** (`PathId`, `matchPrefix`,
`Direct`, required capabilities) plus the per-site `CPTSchedule`; it replaced the static
`sortable-fc.yaml` and is the single source of truth (ADR 0001). It owns the path catalogue
and a **transactional outbox** (ADR 0003, the fleet's reference implementation).
It is the Open Host Service / Published Language SOURCE for
`warehouse.process-path-management.events`; `fulfillment-execution`, `wes-work-planning`,
`workforce-management` and `order-management` are Conformist consumers.

## Hard rules

### NO calls to sibling bounded contexts — NON-NEGOTIABLE

- This service **never calls any other context**: no REST, no MCP, no gRPC, no live lookup —
  ever, in either direction. Cross-context data must be local/declarative, never a live lookup.
- It **never subscribes to another context's topic**. Its only Kafka consumer
  (`internal/adapters/inbound/kafka`, run by `cmd/pathmgmt-projector`) reads its OWN analytics
  topic `warehouse.process-path-management.analytics` (ADR 0007).
- `TestNoSiblingContextOutboundCalls` fails the build if `internal/adapters/outbound/**` imports
  `net/http`. Every change propagates exclusively via Kafka.
- It does not decide dispatch, routing or assignment: it defines WHAT a path is and WHICH
  capabilities it requires, never claims/assigns/completes work (`fulfillment-execution`'s job).

### Architecture (NON-NEGOTIABLE)

Hexagonal / Ports & Adapters, inward-only: **domain depends on nothing; application depends on
domain; adapters depend on application/domain**. Enforced by `internal/architecture/architecture_test.go`
(arch-go, `make arch-test`), not just convention:

- domain (`internal/domain/**`) may only depend on other domain packages; application
  (`internal/application/**`) only on domain + application.
- inbound adapters never depend on outbound adapters, and vice versa.
- nothing under `internal/**` may import `cmd/**` (`cmd` is a leaf composition root).
- No framework, HTTP, Kafka or SQL types in the domain layer. No JSON struct tags in domain packages.

### Events: CloudEvents 1.0 is MANDATORY (ADR-0016)

Every Kafka message produced or consumed (integration AND analytics topic) is a CloudEvents 1.0
event in structured content mode. A hard fleet rule, not a preference:

- No flat envelope, no dual-write, no dual-read, no envelope toggle env var.
- Build/validate/(un)marshal ONLY via `internal/adapters/kafka/cloudevents/`
  (`github.com/cloudevents/sdk-go/v2/event`); header `content-type: application/cloudevents+json; charset=UTF-8`.
- A breaking payload change => new `.v2` type + new dataschema version, never mutate.
- Consumers dispatch on the FULL `type`, ignore unknown types, dedupe on `id`, and DLQ/skip
  (never crash, never parse a legacy shape) anything that fails CloudEvents validation.
- Every event is enqueued onto the integration AND analytics topic in the SAME outbox
  transaction; the CloudEvents `id` is stable across outbox redelivery.
- Required attributes, `type`/`dataschema` formats and this service's published types:
  `.claude/rules/cloudevents-envelope.md`.

### Docs and GitFlow (repo-specific; do not "fix" without checking first)

- `develop` is the working branch; `main` is release-only, synced by explicit fast-forward.
- `.github/workflows/docs.yml` deliberately triggers on **push to `develop`** (the `github-pages`
  environment only allows `develop`). Do not "fix" it to trigger off `main`.
- Whenever `apis/openapi.yaml` changes you MUST regenerate and commit the generated
  `docs/docs/api-reference/rest/*` output (CI `docs-api-drift` fails on any diff). Never hand-edit it.
  Commands: `.claude/rules/docs-and-gitflow.md`.

## Key Commands

Every `make` target mirrors a step in `.github/workflows/ci.yml`:

```bash
make check-fast    # run after every change; before saying "done"
make check         # fmt-check vet build lint test
make check-all     # check + coverage (90% gate on domain + application) + arch-test + bdd — run before pushing
make arch-test     # hexagonal dependency rule
make bdd           # godog/Gherkin acceptance tests
make integration   # -tags=integration, needs DATABASE_URL (testcontainers)
make mutation-fast # gremlins on ./internal/domain, CI-blocking (threshold 99%)
make api-lint      # Spectral on apis/openapi.yaml + apis/asyncapi.yaml
```

Other targets: `make build|vet|fmt|fmt-check|lint|test|coverage|vuln`. Helm: `helm lint charts/process-path-management`
(CI runs it only on PRs targeting `main`). Hooks (lefthook, not tracked by git): activate once per clone
with `lefthook install`; `pre-commit` runs fmt-check/vet/lint, `pre-push` runs `make check`.

## Further reading (`.claude/rules/`)

OpenCode and Codex do not auto-load these: read the rule BEFORE editing matching files.

- `strategic-context.md`: Generic Subdomain classification, live consumers, what this service does not own.
- `architecture-and-layout.md`: package layout and the full layering rules.
- `domain-model.md`: ubiquitous language, aggregate invariants, domain events.
- `runtime-and-outbox.md`: delivery modes (`DATABASE_URL` x `EVENT_PUBLISHER`), outbox, local run.
- `testing-and-api.md`: REST endpoint table, testing discipline, CI job matrix.
- `cloudevents-envelope.md`, `docs-and-gitflow.md`: see above.

Fleet-wide rules: `.claude/rules/fleet/*.md` (canonical in IQVO/warehouse-docs `agents/fleet/`; never hand-edit).

<!-- harness:scoped-rules:start (generated by tools/migrate_v3.py in warehouse-harness-template; do not hand-edit) -->
## Scoped rules and harness

Claude Code loads each rule below automatically when you touch the matching paths. OpenCode and Codex do NOT: read the rule BEFORE editing matching files.

| When touching | Read |
|---|---|
| `internal/adapters/**/kafka/**`, `internal/adapters/outbound/events/**`, `apis/asyncapi*` ... | `.claude/rules/runtime-and-outbox.md` |
| `internal/adapters/inbound/http/**`, `apis/openapi*.yaml`, `apis/openapi/**` ... | `.claude/rules/testing-and-api.md` |

Hooks (`scripts/harness/hook.py`, wired for Claude Code, Codex and OpenCode) block pushes to develop/main, `--no-verify`, bare `rm -rf`, and edits to generated files, and feed gofmt/vet findings back after each edit. Before saying "done" run `make check-fast`; the full gate is `make check-all`. `HARNESS_OFF=1` disables the hooks when debugging the harness itself.
<!-- harness:scoped-rules:end -->
