---
name: how-to-add-a-rest-endpoint
description: Add or change a REST endpoint in this service in the fleet's hexagonal order (domain invariant, use case, port, HTTP adapter, apis/openapi.yaml, generated docs, godog scenario). Use when touching internal/adapters/inbound/http, apis/openapi.yaml, or exposing a use case over HTTP.
---

# How to add a REST endpoint

Use when asked to add a new REST use case/endpoint to this service. Follow
this order — domain first, adapter last — never the reverse; writing the
HTTP handler before the domain invariant it enforces produces handlers
that validate nothing and use cases that get bypassed.

This walks the exact path `PUT /process-paths/{pathId}` (revise) took
(`internal/application/usecases/revise_path.go` +
`internal/adapters/inbound/http/server.go`'s `handleRevisePath`) as the
concrete worked example — read those two files alongside this guide. The
sibling `PUT /sites/{siteId}/cpt-schedule` endpoint
(`handleDefineCPTSchedule`) is a second real example of the same shape
applied to a second aggregate (`cptschedule.CPTSchedule`), useful if your
new endpoint belongs to a new aggregate rather than `ProcessPath`.

## 1. Domain first: does an invariant already exist, or do you need one?

Check `internal/domain/processpath/` (or `internal/domain/cptschedule/`)
for the rule this endpoint enforces. A REST endpoint should almost never
contain business logic itself — it decodes a request, calls a use case,
encodes the result. If the operation needs a new domain rule (e.g. this
repo's own `matchPrefix` must be non-empty and lower-case, enforced in
`processpath.Define`/`Revise`), add it to the aggregate/value-object in
`internal/domain/`, with its own table-driven unit test
(`process_path_test.go`), BEFORE touching the application or adapter
layers.

## 2. Application: define the use case

Add a new file in `internal/application/usecases/` (one file per use
case, this repo's convention — see `define_path.go`, `revise_path.go`,
`deactivate_path.go`, `cpt_schedule.go` each as their own file, never one
giant `usecases.go`). Shape, mirroring `RevisePath`:

```go
package usecases

type RevisePath struct {
    Repo      ports.ProcessPathRepo   // driven ports only — never a concrete adapter
    Publisher ports.EventPublisher    // this repo raises a domain event on every write
    Clock     ports.Clock             // never call time.Now() directly
    // UnitOfWork brackets Save + Publish atomically (ADR 0003). Optional:
    // nil means "no transactional backing" (in-memory / log-publisher dev).
    UnitOfWork ports.UnitOfWork
}

func (uc *RevisePath) Execute(ctx context.Context, id shared.PathId, /* domain-typed args */) (*processpath.ProcessPath, error) {
    p, err := uc.Repo.FindByID(ctx, id)
    // ... load, apply the aggregate's own method (never inline the
    // invariant here — that belongs in internal/domain/), then:
    err = atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
        if err := uc.Repo.Save(ctx, p); err != nil {
            return err
        }
        return uc.Publisher.Publish(ctx, shared.ProcessPathUpdated{ /* ... */ })
    })
    return p, err
}
```

Add the port to `internal/application/ports/ports.go` if it doesn't exist
yet — ports are interfaces ONLY (arch-go's `TestMCPAdapterDependencyRule`-
style fitness tests in `internal/architecture/` enforce the layer
boundary; a struct in the wrong place fails `make arch-test`). Every
publishing use case in this repo wraps its `Repo.Save` + `Publisher.Publish`
in `atomically(ctx, uc.UnitOfWork, func(ctx) error {...})` — copy that
shape rather than inventing a new one, even for a use case that has no
`UnitOfWork` wired yet (a nil `UnitOfWork` is a legitimate, tested
configuration — see `atomically`'s doc comment in `define_path.go`).

Write the use case's unit test against a fake repo/publisher (see
`internal/application/usecases/destination_location_role_test.go` and
`fakes_test.go` — never a real Postgres/HTTP call in a unit test). Cover
the success path AND the domain-rule failure path (e.g.
`TestDefinePath_RejectsInvalidDestinationLocationRole` asserts both the
returned error and that nothing was published).

## 3. Adapter: wire the HTTP handler

In `internal/adapters/inbound/http/`:

1. `dto.go` — add the request/response DTO structs (JSON tags, this
   repo's naming convention: `reviseProcessPathRequest`/
   `processPathResponse`). DTOs live ONLY in the adapter layer — domain
   types never carry JSON tags.
2. `server.go` — add the route (`r.Put("/process-paths/{pathId}",
   s.handleRevisePath)` inside `NewRouter`) and the handler function:
   - decode + validate the request (`decodeJSON`), converting to domain
     value objects immediately (`shared.PathId(chi.URLParam(r,
     "pathId"))`, `parseCycleTimeP95`, `shared.ParseDestinationLocationRole`)
     — a bad value fails here as an RFC 7807 validation error, never
     reaches the use case
   - call the use case's `Execute`
   - map use-case errors to HTTP status via `writeError` (check
     `errors.go` for the existing error→status mapping before adding a
     new error type)
   - encode the domain result back to the response DTO (`toProcessPathResponse`)
     and `writeJSON`
3. Add the new use case field to the `Server` struct (see `Server`'s
   fields in `server.go` — `DefineCPTSchedule`/`GetCPTSchedule` are
   documented as optionally-nil per ADR 0010, everything else is always
   populated) and wire it in the composition root (`cmd/pathmgmt/main.go`).

Write at least one httptest per endpoint: one success path, one error
path (validation failure AND/OR the domain-rule failure) — see
`server_test.go`/`destination_location_role_test.go`/`cpt_schedule_test.go`
for the existing shape.

## 4. Contract: update OpenAPI, then regenerate docs

Add the path to `apis/openapi.yaml` (request/response schemas, the RFC
7807 `ProblemDetails` response for each error case — see the existing
`/process-paths` `POST`/`PUT` entries, including the 409/422 cases, for
the shape).

Regenerate the Docusaurus REST reference — this repo's `docs-api-drift`
CI job fails the PR if you skip this:

```bash
cd docs
npm ci
npm run clean-api-docs pathmgmt
npm run gen-api-docs pathmgmt
```

`docs-api-drift` re-runs exactly this and fails if `git diff` on
`docs/api-reference/rest` is non-empty — commit the regenerated
`.mdx`/`.json` files, they are generated output, not hand-written.

## 5. Behaviour: add a godog scenario

If this endpoint is user-facing behaviour (not purely internal
plumbing), add a `.feature` file under `features/` exercising it
end-to-end against the real HTTP server — see `features/revise.feature`
and `features/define_and_read.feature` for the exact Given/When/Then
shape this repo's `bdd` CI job expects (real HTTP, not mocked).

## 6. Verify before opening the PR

```bash
make check       # fmt-check vet build lint test
make check-all    # + coverage (90% gate) + arch-test + bdd
```

`make coverage` gates `./internal/domain/...,./internal/application/...`
at 90% — a new use case with no test on its failure path is the most
common way to miss this gate. Also run `make arch-test` explicitly if
you touched anything under `internal/adapters/`: this repo's fitness
tests are stricter than most of the fleet's (see the integration-event
guide's zero-inbound-dependency callout) and a new outbound HTTP client
import anywhere under `internal/adapters/outbound/` fails
`TestNoSiblingContextOutboundCalls` even if `make check` alone passes.
