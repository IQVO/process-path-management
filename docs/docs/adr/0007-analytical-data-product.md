---
id: 0007-analytical-data-product
slug: /adr/0007-analytical-data-product
title: 0007. Analytical data product (report) via a separate analytics topic
sidebar_label: 0007. Analytical data product
description: ADR 0007 — an additive analytical read model, the "Process Path Catalogue Growth & Change" report, built from process-path-management's own domain events on a dedicated warehouse.process-path-management.analytics topic, projected into a separate analytical database and served by a read-only reports binary over REST and MCP. Closes the last remaining gap in the fleet's analytics data-mesh rollout.
---

# 0007. Analytical data product (the "report")

## Status

**Accepted.** §1's analytics "Envelope v1" wire format (`event_id`/
`event_type`/`occurred_at`/`source`/`schema_version`/`data`) is
**Superseded by ADR-0016** (CloudEvents 1.0 as the mandatory event
envelope); the rest of this decision stands.

## Context

As of this record, process-path-management was the **only** backend
bounded context in the fleet with no analytics data product: seven of the
other eight (order-management, inventory-storage, wes-work-planning,
fulfillment-execution, workforce-management, facility-layout,
labor-performance) each ship a projector/reports pair building a
per-service analytical read model from their own domain events, following
the data-mesh pattern facility-layout established in its own ADR-0010 and
labor-performance repeated in its ADR-0007. This service's own three
domain events — `ProcessPathCreated`, `ProcessPathUpdated`,
`ProcessPathDeactivated` — are exactly the kind of past-tense, replayable
event stream that pattern is built on; the gap was not a missing
substrate, just an unbuilt read side.

The forces are identical to the fleet precedent (facility-layout's
ADR-0010), restated for this service:

- **The integration contract must not become coupled to reporting.** This
  service's ONE existing topic, `warehouse.process-path-management.events`
  (ADR 0002), is the Published Language three sibling contexts
  (fulfillment-execution, wes-work-planning, workforce-management) are
  meant to consume. Adding analytics-only shape to it would entangle two
  contracts that should evolve independently.
- **Analytics must never contend with OLTP.** A report query or a
  projection rebuild must not touch the transactional database the
  catalogue's REST API and MCP tools read from.
- **The service still owns its data as a product.** The read side lives in
  this repo, with its own contract, owner, and freshness SLA — not shipped
  to a central team.
- **No new central platform.** Reuse what the estate already runs: Kafka,
  Postgres, chi, the MCP SDK — exactly the toolchain this service's OLTP
  side and its transactional outbox (ADR 0003) already depend on.

Unlike facility-layout's report, this service's catalogue has **no
spatial dimension** — a process path is a single flat identity (`PathId`),
not a site/zone hierarchy — so the report needs no scope grouping, only a
time bucket.

## Decision

**process-path-management owns an analytical data product built solely
from its own domain events, delivered on a dedicated analytics topic,
projected into a separate analytical database, and served read-only over
REST and MCP. Three processes; one writer. Purely additive — the OLTP
domain and application layers are unmodified.**

### 1. Separate analytics topic

A **second** Kafka encoder (`internal/adapters/outbound/kafka/analytics_publisher.go`)
maps the same three domain events the existing integration `Encode`
function already serializes to **`warehouse.process-path-management.analytics`**,
using the Envelope v1 wrapper (`event_id`, `event_type`, `occurred_at`,
`source`, `schema_version`, `data`). The ADR-0002/0003 integration
publisher and `warehouse.process-path-management.events` are **left
untouched**.

Because this service's writes already flow through the transactional
outbox (ADR 0003), the analytics encoder is wired in as a **second
`Encoder`** alongside the existing `IntegrationEncoder` in the SAME
`postgres.NewOutboxPublisher(...)` call: one domain event now enqueues
**two** outbox rows — one per topic — in the exact transaction that
persisted the aggregate change, sharing the same `event_id`. The
`outbox_events` table's uniqueness constraint moves from `event_id` alone
to `(event_id, topic)` (migration `0003_outbox_topic`) to allow this. The
no-Postgres, `EVENT_PUBLISHER=kafka` dev path fans out the same way via a
small `FanOutPublisher`, publishing directly to both topics with no
transaction to bind them to.

Consistent with the ADR-0002 integration publisher, the analytics
publisher and consumer are **trace-free**: this service has no
observability/OTel package for them.

### 2. Separate analytical database

Projections land in a **separate analytical database** with its own
credentials (`ANALYTICS_DATABASE_URL`), its own golang-migrate migration
set (`migrations/analytics/`), and a **read-only role** for the reader.
The OLTP `DATABASE_URL` database is never opened by the analytical side.
The reader additionally pins every connection to
`default_transaction_read_only=on` — defence in depth on top of the
read-only role.

### 3. Three processes, one writer

- **`cmd/pathmgmt`** — the OLTP binary. Unchanged except its composition
  root additionally enqueues the analytics encoder into the same outbox
  publisher.
- **`cmd/pathmgmt-projector`** — the analytics **writer**. Consumes
  `warehouse.process-path-management.analytics` (consumer group
  `process-path-management-analytics`, reading from the earliest offset),
  applies idempotent projections, and is the **only** writer of the
  analytical database. Runs the analytical migrations on start.
- **`cmd/pathmgmt-reports`** — the **read-only reader**. Opens the
  analytical database read-only and serves `GET /reports/catalogue-growth`
  and `GET /reports/catalogue-growth/freshness`. Never writes, never
  migrates.

### 4. Served over REST and MCP

The reports binary serves the REST report resource. A curated, read-only
MCP tool (`get_catalogue_growth_report`) is added as a **third** tool
alongside the existing `get_process_path` and `list_process_paths` (ADR
0006) — it calls the reports REST service (via `REPORTS_BASE_URL`) rather
than opening the analytical database itself, so no process touches a
datastore it does not own.

### 5. The report

A **Process Path Catalogue Growth & Change** read model, bucketed by
**DAY** alone — unlike facility-layout's site/zone-scoped rollup, this
service's catalogue has no spatial dimension to group by. Per bucket it
counts paths defined (`ProcessPathCreated`), revised
(`ProcessPathUpdated`), and deactivated (`ProcessPathDeactivated`).

It is a **projection** from events, eventually consistent to a freshness
SLA, not real-time. The analytical read model lives in a new
`internal/analytics/report` region that **depends on nothing**; the
consumer and store adapters depend on it. The OLTP **domain and
application layers are not modified**, and `arch-test` (extended in this
change, mirroring labor-performance's own arch-fitness suite) enforces
that neither they nor the read-model region import the analytics store.

## Consequences

### Easier

- **The integration contract is untouched** — evolving the report never
  risks fulfillment-execution, wes-work-planning, or workforce-management,
  the three intended consumers of the integration topic.
- **Analytics cannot contend with OLTP** — separate database, separate
  connection, read-only reader role.
- **The report is rebuilt purely from events** — no dual-write from OLTP;
  it can be rebuilt from scratch by replaying the analytics topic from the
  earliest offset.
- **No central platform.** Everything reuses the estate's existing Kafka,
  Postgres, chi and MCP SDK.
- **The transactional outbox already existed** (ADR 0003), so adding a
  second topic was a matter of a second `Encoder` in the same publisher,
  not a new delivery mechanism.

### Harder

- **One more topic, two more binaries, and a second database** to operate.
- **Eventual consistency.** The report lags OLTP truth by the freshness
  SLA; the freshness endpoint communicates this explicitly.
- **The `outbox_events` schema changed** (a `topic` column, and the
  uniqueness constraint widened to `(event_id, topic)`) to support one
  event enqueuing rows on more than one topic. This is a one-time,
  additive migration; existing rows are backfilled onto the integration
  topic they were always meant for.
- **First deploy has an empty report** until events flow; historical
  backfill requires replaying the analytics topic from earliest into a
  fresh projector.

## References

- [ADR 0002 — YAML to Kafka cutover](./0002-yaml-to-kafka-cutover.md)
- [ADR 0003 — Transactional outbox](./0003-transactional-outbox.md)
- [ADR 0006 — MCP server as a second inbound adapter](./0006-mcp-server-second-inbound-adapter.md)
- Fleet precedent: facility-layout's ADR-0010 and labor-performance's
  ADR-0007, both titled "Per-service analytical data product (report) via
  a separate analytics topic" — the pattern this record follows.
