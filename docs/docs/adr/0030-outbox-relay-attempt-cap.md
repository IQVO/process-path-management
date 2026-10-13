---
id: 0030-outbox-relay-attempt-cap
slug: /adr/0030-outbox-relay-attempt-cap
title: 0030. Outbox relay attempt cap
sidebar_label: 0030. Outbox relay attempt cap
description: ADR 0030 — the outbox relay stops claiming a row after OUTBOX_MAX_ATTEMPTS failed sends (default 20, 0 = unlimited), leaves it in outbox_events for inspection, logs it once at ERROR and lets later rows publish; an operator re-arms it with SQL. Amends ADR 0003's per-path ordering guarantee (fleet gap fix D12).
---

# 0030. Outbox relay attempt cap

## Status

Accepted. Amends [ADR 0003](./0003-transactional-outbox.md): a row that has
reached the cap no longer holds back later rows of the same path.

## Context

The relay (ADR 0003, `internal/adapters/outbound/postgres/outbox_relay.go`)
drains `outbox_events` in id order and stops a pass at the first failed
send, recording `attempts` and `last_error` on the row, so that a later
event of the same path can never overtake an earlier one. The price was not
written down: a row the broker will **never** accept (an oversized message,
a topic that cannot be created, a payload a broker-side policy rejects) is
claimed first on every pass, fails every time, and stops every event behind
it forever. One poison row silences the whole Published Language that
fulfillment-execution, wes-work-planning and workforce-management build
their path caches from, and the analytics topic with it. The only signals
were `outbox relay pass failed` lines and a rising
`process_path_management.outbox.lag_seconds`.

The fleet's 2026-10-12 gap review (D12) decided that every relay gets an
attempt cap with one shared convention, the one inventory-storage already
ships.

## Decision

- `postgres.WithMaxAttempts(n)` is a new relay option. With `n > 0` the
  claim query only selects unpublished rows with `attempts < n`. The zero
  value (no option, or `n = 0`) keeps the previous unlimited behaviour.
- When a failure brings a row to the cap, the relay logs it once at ERROR,
  `outbox relay: row reached the attempt cap and is no longer claimed`,
  with `outbox_id`, `topic`, `event_type`, `key` (the `pathId` or `siteId`),
  `attempts`, `max_attempts`, `error` and the re-arm SQL. The row is not
  deleted, not moved and not marked published: it stays in `outbox_events`
  with `attempts` and `last_error` for inspection, and the housekeeping
  sweeper (ADR 0018) never deletes it because it only deletes published
  rows.
- An operator re-arms a row with
  `UPDATE outbox_events SET attempts = 0 WHERE id = ...`; the relay claims it
  on its next pass.
- `cmd/pathmgmt` passes the cap from `OUTBOX_MAX_ATTEMPTS` (default `20`,
  `0` = unlimited; an unparseable or negative value falls back to 20 with a
  warning) and logs it on the `kafka event publishing enabled
  (transactional outbox)` line. The chart exposes it as
  `config.outboxMaxAttempts`.
- No migration: `attempts` and `last_error` exist since the outbox was
  introduced.
- Sensor: the architecture fitness test `TestOutboxRelaysPassAttemptCap`
  fails when a non-test file under `cmd/` calls `NewOutboxRelay` without a
  `WithMaxAttempts(...)` argument, so the cap is always a deliberate setting
  at the composition root.

### Per-key ordering

ADR 0003 promised that a later event of a path never overtakes an earlier
one. With the cap that holds only until the earlier row is parked: from then
on later rows with the same key publish ahead of it, and a re-armed row is
delivered late, after newer events of that path. This is accepted on
purpose. Twenty consecutive failures of one row (at the default 1 s idle
interval that is at least twenty passes) are not a transient broker problem,
and the alternative is that every consumer stops receiving every change.
Consumers already treat the topic as replayable and key their caches by
`pathId`; an operator who re-arms a parked row should prefer re-publishing
the path's current state (revise it through the API) when a late, older
event could overwrite newer cached state.

## Consequences

**Easier**

- One poison row can no longer stop the relay: healthy events keep flowing
  after at most `OUTBOX_MAX_ATTEMPTS` failed passes.
- The parked row is visible (ERROR log with its identity, the row itself)
  and recoverable with one statement.
- Tests: `TestOutboxRelay_MaxAttempts_PoisonRowIsExcludedAndTheNextRowPublishes`
  and `TestOutboxRelay_WithoutMaxAttempts_RetriesWithoutBound`
  (testcontainers Postgres), `TestOutboxMaxAttemptsFromEnv`,
  `TestOutboxRelaysPassAttemptCap` and its fixture test.

**Harder**

- Per-path ordering is no longer absolute (see above).
- A broker outage longer than twenty passes parks every row that failed
  twenty times; after the outage those rows need re-arming. The ERROR log
  and the query in the runbook find them. Raise `OUTBOX_MAX_ATTEMPTS` (or
  set `0`) in environments where long broker outages are expected.
- `outbox.lag_seconds` keeps measuring the oldest unpublished row, so a
  parked row keeps the gauge high until it is re-armed or resolved. That is
  intended: the gauge must not hide a lost event.
