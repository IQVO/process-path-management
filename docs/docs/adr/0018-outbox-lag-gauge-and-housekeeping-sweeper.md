---
id: 0018-outbox-lag-gauge-and-housekeeping-sweeper
slug: /adr/0018-outbox-lag-gauge-and-housekeeping-sweeper
title: 0018. Outbox lag gauge and housekeeping sweeper
sidebar_label: 0018. Outbox lag gauge and housekeeping sweeper
description: ADR 0018 — process_path_management.outbox.lag_seconds behind the relay (closing ADR 0003's deferred follow-up) and a periodic sweeper bounding idempotency_keys (24h) and published outbox_events (7d), mirroring the fleet's inventory-storage pattern.
---

# 0018. Outbox lag gauge and housekeeping sweeper

## Status

Accepted — implemented in the same change that introduced this record.
Mirrors inventory-storage's merged sweeper and workforce-management's
outbox lag gauge (both on origin/develop), adapted to this service's
schema.

## Context

Two deferred follow-ups had accumulated:

1. **ADR 0003 (transactional outbox) left "an outbox-lag metric is a
   follow-up" unwritten.** The relay logs a failed pass, but nothing
   reports HOW BEHIND the relay is — a relay silently stuck (broker
   unreachable, sink erroring) is invisible until consumers notice
   stale catalogues.
2. **Two append-only tables grow without limit.** `idempotency_keys`
   (ADR 0011) gains one row per protected POST, forever; `outbox_events`
   (ADR 0003) keeps every PUBLISHED row forever for forensics. Neither
   has any natural bound; migration 0006's index comment explicitly
   anticipated "a future cleanup/TTL job".

## Decision

1. **`process_path_management.outbox.lag_seconds`** — an asynchronous
   OTel gauge sampled once per collection
   (`postgres.RegisterOutboxLagGauge`), reporting
   `EXTRACT(EPOCH FROM (now() - created_at))` of the OLDEST
   unpublished `outbox_events` row; 0 when fully drained. It answers
   "how long has SOMETHING been waiting", not "why" (relay idle vs
   stuck retrying both raise it — that is the point).

2. **A `Sweeper`** (`postgres.Sweeper`) runs in the same process as the
   HTTP server (cmd/pathmgmt), once immediately at boot and then every
   `HOUSEKEEPING_INTERVAL` (default 1h), deleting in batches of 1000:
   - `idempotency_keys` rows older than `IDEMPOTENCY_KEY_TTL`
     (default 24h — the ADR 0011 replay window),
   - `outbox_events` rows `published_at`-older than
     `OUTBOX_RETENTION` (default 7d). **Unpublished rows are never
     swept, however old** — they are pending events, not forensics.

   A value of 0 (or negative) disables that half, restoring the
   pre-ADR behaviour. Safe across replicas: each DELETE targets an
   explicit id/key set chosen by a subquery, so concurrent sweepers
   can only delete rows that are anyway eligible.

   Env vars are read in the composition root (cmd/pathmgmt), never in
   the adapter; the chart wires them under `config.housekeeping`.

## Consequences

**Positive**

- Relay health is observable with one alert-able number; a stuck relay
  shows up as a monotonically climbing lag gauge.
- Both tables are bounded; the indexes each query needs already exist
  (`idx_idempotency_keys_created_at`, `idx_outbox_events_unpublished`).
- Batched DELETEs keep each pass in short transactions — no long table
  locks while clearing a large backlog.

**Negative / accepted**

- Published outbox rows beyond the retention window are gone for
  forensics; 7d matches the fleet's convention.
- Deleting a still-in-flight idempotency key after the TTL would let a
  very late retry execute twice — 24h bounds that window and matches
  the fleet's convention; retries that late are already pathological.
