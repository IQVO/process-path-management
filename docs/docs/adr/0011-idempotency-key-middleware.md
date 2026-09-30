---
id: 0011-idempotency-key-middleware
slug: /adr/0011-idempotency-key-middleware
title: 11. Transactional Idempotency-Key middleware for POST /process-paths
sidebar_label: 11. Idempotency-Key middleware
description: "ADR 0011 — POST /process-paths requires a caller-supplied Idempotency-Key header. A route-scoped middleware begins the outer Postgres transaction, joins it with DefinePath's own UnitOfWork via the existing tx-in-context mechanism (internal/pgtx), and lets the database's own unique-index lock do request de-duplication with no polling, no timeout, and no in-progress state."
---

# 11. Transactional Idempotency-Key middleware for POST /process-paths

## Status

Accepted — implemented in the same change that introduced this record.
Ports order-management's ADR 0023 (the fleet's reference implementation)
to this service's one resource-creation endpoint.

## Context

`POST /process-paths` (`DefinePath`) is this service's one mutating route
that creates a genuinely new resource. A client that never receives the
response to a successful call — a dropped connection, a load-balancer
timeout, a client-side retry policy — has no safe way to tell "my request
never arrived" apart from "my request arrived and succeeded but I never
saw the response."

### The caller-supplied-id question, resolved

Unlike order-management's `POST /orders` (server-generated id),
`process-paths`' `PathId` is **caller-supplied** — the request body names
its own id (e.g. `"pathId":"PICK"`). This was flagged as an open question
for this rollout: does a caller-supplied id already make `DefinePath`
"idempotent enough" on its own, since `DefinePath` already rejects a
duplicate id with `ErrPathAlreadyExists` (409)?

The answer implemented here: **wire the middleware anyway.** A
caller-supplied natural key changes what a *duplicate* looks like, not
whether the middleware is worth having:

- Without this middleware, a client that retries an already-succeeded
  `POST /process-paths {"pathId":"PICK",...}` gets a `409 Conflict`
  (`ErrPathAlreadyExists`) on the retry — technically safe (no double
  create), but indistinguishable from a genuine "someone else already
  claimed this id" conflict. A retrying client has no way to tell "my
  own earlier call already succeeded" from "a different caller raced me
  for this id," and cannot recover the original `201` response (e.g. the
  server-assigned `createdAt` timestamp) without a second `GET`.
- With the middleware, a byte-identical retry (same `Idempotency-Key` +
  same body) gets the ORIGINAL `201` response back, verbatim — a clean
  idempotent replay instead of a confusing `409`. A retry with the SAME
  key but a DIFFERENT body (e.g. the caller reused a key across two
  distinct path definitions by mistake) gets a `422`
  (`idempotency-key-reused`), which is a more actionable diagnostic than
  a natural-key `409` would have been for that case too.

So the middleware strictly improves the retry experience here even
though the endpoint was already safe against silent double-creation via
its natural-key check — it turns an ambiguous natural-key conflict into
an unambiguous, response-preserving idempotent replay.

The other mutating routes (`PUT /process-paths/{pathId}`,
`DELETE /process-paths/{pathId}`, `PUT /sites/{siteId}/cpt-schedule`)
already act on a caller-supplied, existing resource id and are
idempotent by ordinary HTTP `PUT`/`DELETE` semantics; they are
deliberately left unprotected by this middleware, matching
order-management ADR 0023's own scoping rule.

ADR 0003 (transactional outbox) left this service with exactly the
transactional infrastructure this problem needs already in place:
`ports.UnitOfWork` + `postgres.UnitOfWork.Execute`, and a context-based
transaction-join mechanism that lets a repo's own SQL detect and join an
already-open outer transaction. This ADR's core design choice, unchanged
from order-management's, is to make idempotency bookkeeping join that
SAME transaction rather than build a second, parallel transactional
mechanism — so the entire HTTP-request-to-response cycle (idempotency
bookkeeping, the `ProcessPath` aggregate write, the outbox insert)
commits or rolls back as one atomic unit.

## Decision

### 1. `Idempotency-Key` header, required on `POST /process-paths`

A request to `POST /process-paths` without an `Idempotency-Key` header
gets `400 application/problem+json` (`idempotency-key-required`). A
deliberate v1 choice: require the header on the one true
resource-creation endpoint rather than making it optional — an optional
header is trivial for a client to forget to set on exactly the retry
path where it matters most.

### 2. `idempotency_keys` table (migration `0006_idempotency_keys`)

```sql
CREATE TABLE idempotency_keys (
    key              TEXT PRIMARY KEY,
    method           TEXT NOT NULL,
    path             TEXT NOT NULL,
    request_hash     TEXT NOT NULL,
    status_code      INTEGER,
    response_body    BYTEA,
    response_headers JSONB,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at     TIMESTAMPTZ
);
CREATE INDEX idx_idempotency_keys_created_at ON idempotency_keys (created_at);
```

`request_hash` is `hex(sha256(request body))`. `status_code`,
`response_body`, and `response_headers` start `NULL` and are populated by
exactly one `UPDATE`, in the same transaction that inserted the row,
immediately before that transaction commits (§4).

### 3. `RequireIdempotencyKey` middleware (`internal/adapters/inbound/http/idempotency.go`)

Route-scoped only:
`r.With(RequireIdempotencyKey(pool)).Post("/process-paths", ...)` in
`server.go`, never `r.Use(...)` on the whole router. The middleware is
skipped entirely when the service has no transactional Postgres backing
wired (`Server.IdempotencyPool == nil` — the in-memory dev/test
configuration), mirroring this repo's existing convention for every
other optional Postgres-backed capability.

The request body is read fully into memory once (needed both for
hashing and for replaying to the real handler), hashed, and then
restored via `io.NopCloser(bytes.NewReader(...))` so downstream JSON
decoding in `DefinePath`'s handler is completely unaffected.

The middleware then begins a `pgxpool` transaction directly (it does not
need to import `internal/adapters/outbound/postgres` to do this) and
binds it into the request's `context.Context` via the same mechanism
`postgres.UnitOfWork` uses (§5).

`INSERT INTO idempotency_keys (key, method, path, request_hash) VALUES
($1,$2,$3,$4) ON CONFLICT (key) DO NOTHING` runs inside that
transaction:

- **1 row inserted** (genuinely new key): call the real handler with the
  tx-carrying context, using an `httptest.ResponseRecorder` to CAPTURE
  its status/headers/body rather than writing to the real
  `http.ResponseWriter` yet (§4).
- **0 rows** (a row for this key already exists): roll back this new,
  empty transaction (it never wrote anything) and fall through to a
  plain, non-transactional `SELECT` (§5 explains why this is safe
  without any additional locking or waiting):
  - `request_hash` mismatch → `422` (`idempotency-key-reused`):
    "Idempotency-Key was already used with a different request."
  - `request_hash` match → this is a genuine retry: write
    `response_headers`, then `status_code`, then `response_body`
    VERBATIM to the real `http.ResponseWriter`, and return WITHOUT
    calling the real handler at all — including for the natural-key-409
    scenario described above, so a duplicate `DefinePath` call with the
    same key+body never even reaches `Repo.FindByID`.

### 4. The no-null-status-code-ever-committed invariant

On the fresh-key path, after the wrapped handler runs against the
recorder:

- **Normal completion (no panic):** within the SAME transaction that
  inserted the bare row, `UPDATE idempotency_keys SET status_code=$1,
  response_body=$2, response_headers=$3, completed_at=now() WHERE
  key=$4`, then `COMMIT`. Only AFTER a successful commit does the
  middleware copy the recorder's headers/status/body onto the real
  `http.ResponseWriter`.
- **Panic:** the middleware recovers it, `ROLLBACK`s the transaction (so
  neither the idempotency row nor any domain write the handler made ever
  becomes visible), and re-panics so the outer chi `Recoverer` still
  produces the service's normal `500`. A panic's outcome is never
  cached — a retry after a panic must re-attempt the real work.

The invariant this ordering buys: a transaction that reads a COMMITTED
`idempotency_keys` row can never observe a NULL `status_code`. A row is
either (a) never committed at all — the inserting transaction rolled
back, so no other transaction can ever see it — or (b) committed, in
which case `status_code`/`response_body`/`response_headers` were already
populated by the `UPDATE` that ran, in the same transaction, strictly
before the `COMMIT` that made the row visible at all. There is no third
state: no "in-progress" marker, no client-facing retry-after/409, no
polling loop or timeout anywhere in this design.

### 5. Concurrency: Postgres' own unique-index lock does the serialization, not application logic

Two concurrent requests carrying the SAME key race on the `INSERT ... ON
CONFLICT (key) DO NOTHING` above. Postgres serializes them at the
primary-key unique index: the SECOND (and every later) inserter's
statement BLOCKS until the FIRST inserter's transaction resolves —
commits or rolls back. So by the time ANY transaction observes
`rowsAffected() == 0` on this insert, the ORIGINAL inserting transaction
has unconditionally finished. Combined with §4's invariant: whenever the
middleware takes the "0 rows" branch, the pre-existing row — if that
original transaction committed — already has its outcome fully
populated; if it rolled back, the row does not exist at all, and THIS
caller's own (previously blocked) insert instead succeeds with
`rowsAffected() == 1`, taking the fresh-key branch. Either way, no
polling loop, no lock-retry budget, and no timeout are needed anywhere
in this code — the database's own MVCC/locking semantics ARE the
synchronization primitive. This claim is proven with a real test, not
asserted from theory:
`TestIdempotency_Concurrent_SameKeySameBody_ExactlyOnePathCreated` fires
five real goroutines at the real router with the same key and body and
asserts, via a direct DB count, that exactly one `process_paths` row
exists afterward.

### 6. Reusing, not duplicating, the transaction-join mechanism (`internal/pgtx`)

The transaction-join mechanism pre-existed as unexported symbols inside
`internal/adapters/outbound/postgres/unit_of_work.go` (`withTx`/`txFrom`,
an unexported `txKey{}` context-key type), introduced by ADR 0003. The
idempotency middleware, however, lives in
`internal/adapters/inbound/http` — and this repo's architecture fitness
tests (`internal/architecture/fitness_test.go`) forbid the inbound HTTP
adapter from importing the outbound Postgres adapter, and vice versa.
Reusing the mechanism unchanged was therefore impossible without either
violating that boundary or inventing a second, parallel mechanism.

The fix, at the lowest correct layer: extract the bare
key-type-plus-`WithTx`/`TxFrom` pair into a new, tiny, dependency-free
package, `internal/pgtx`, that both adapter packages import.
`postgres.withTx`/`postgres.txFrom` (and therefore `querierFrom` and
`UnitOfWork.Execute`) now delegate to `pgtx.WithTx`/`pgtx.TxFrom` — an
internal refactor with no change in observable behaviour for any
existing caller. The idempotency middleware calls the exact same
`pgtx.WithTx`/`pgtx.TxFrom` functions.

This means `DefinePath`'s own use case and its `atomically()`/
`UnitOfWork.Execute` call needed **zero changes** to pick up the
middleware's transaction — `Execute`'s existing "already in a
transaction? just run `fn(ctx)`" branch already does exactly the right
thing once the context it receives carries a `pgtx`-bound transaction
from any source, not just its own. This join behaviour is asserted with
a real test, not assumed: every integration test scenario in
`idempotency_integration_test.go` runs the full `POST /process-paths`
request through the real chi router and a real Postgres, and
`TestIdempotency_FreshKey_CreatesPathAndRecordsOutcome` /
`TestIdempotency_Replay_...` directly assert both the `idempotency_keys`
row AND the `process_paths` row exist/don't-exist exactly as the
atomic-commit argument predicts.

### 7. Response caching scope: every normal response, including business errors

The middleware caches every NORMAL (non-panic) response the wrapped
handler produces — including a `422` business-logic error (e.g. an
empty `requiredCapabilities` list, rejected by `processpath.Define`).
This is a deliberate v1 simplification: a client retrying the exact same
key + body deterministically gets the exact same answer, including a
validation error, rather than re-running (and potentially re-deciding)
the same validation. A client wanting a genuinely different outcome must
use a new `Idempotency-Key`. Proven by
`TestIdempotency_BusinessErrorResponse_IsCachedAndReplayed`.

## Consequences

- `POST /process-paths` now requires an `Idempotency-Key` header; every
  existing client/BDD/contract-test caller of that route needs one on
  every call.
- The whole request cycle — idempotency bookkeeping, `ProcessPath`
  aggregate write, outbox insert — is one Postgres transaction; a
  failure anywhere in that cycle after the idempotency row's `INSERT`
  rolls back the ENTIRE cycle, including the idempotency row itself. A
  retried request after such a failure re-attempts the real work from
  scratch.
- A retry of an already-succeeded `DefinePath` call with the same
  `Idempotency-Key` + body now gets the original `201` back instead of a
  `409` from the natural-key check — see the "caller-supplied-id
  question" in Context for why this is strictly better even though the
  endpoint was already safe against silent double-creation.
- `internal/pgtx` is a new, tiny shared package; any future
  cross-cutting-transaction feature in this service should extend it
  rather than re-invent a parallel tx-in-context mechanism.
- **Known follow-up, explicitly deferred:** no TTL/cleanup job exists yet
  for old `idempotency_keys` rows. The table grows unboundedly today;
  `idx_idempotency_keys_created_at` exists specifically so a future
  scheduled job (e.g. `DELETE ... WHERE created_at < now() - interval
  '30 days'`) can find old rows without a full table scan. Building that
  job is out of scope for this change.
- `PUT /process-paths/{pathId}`, `DELETE /process-paths/{pathId}`, and
  `PUT /sites/{siteId}/cpt-schedule` remain unprotected by this
  middleware — they are idempotent by ordinary HTTP semantics already.

## Alternatives considered

- **Rely on the natural-key `409` alone and skip the middleware, on the
  premise that a caller-supplied id is "already idempotent enough":**
  rejected — see the "caller-supplied-id question" above. A natural-key
  conflict is ambiguous (my own retry vs. a genuine collision) and does
  not return the original response; the middleware removes that
  ambiguity for a small, well-understood cost.
- **Application-level in-memory de-duplication (e.g. a local cache of
  recently-seen keys):** rejected — does not survive a pod restart or
  work across replicas.
- **A three-state design (`pending`/`completed`/`failed`) with a timeout
  and a `409`-retry-later response for requests that arrive while
  another is still `pending`:** rejected in favor of the transactional
  design in §4/§5. Postgres' own lock on the unique index already
  serializes concurrent identical-key requests without any extra state,
  and the no-null-status-code invariant means there is never an
  observable "pending" row to design a timeout for in the first place.
