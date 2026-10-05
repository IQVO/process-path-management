---
id: 0001-process-path-management-bounded-context
slug: /adr/0001-process-path-management-bounded-context
title: 0001. Process Path Management as a new Generic Subdomain bounded context
sidebar_label: 0001. Why a new bounded context
description: ADR 0001 — why the process-path catalogue is its own Generic Subdomain, replacing a static YAML file, propagated via Kafka rather than synchronous HTTP.
---

# 0001. Process Path Management as a new Generic Subdomain bounded context

## Status

Accepted. Established with the initial implementation of this bounded
context.

## Context

Before this service existed, the process-path definition — a path's
canonical identity (`PathId`), the `matchPrefix` rule downstream consumers
use to resolve a caller-supplied id to a path family, whether it is
`Direct`, and the capabilities a station/associate must hold to work it —
lived in a static YAML file, `warehouse-infra/config/process-paths/sortable-fc.yaml`,
loaded once at boot by three separate services: `fulfillment-execution`,
`wes-work-planning`, and `workforce-management`. This is functionally a
**published language with no single owner**: three consumers each parse
the same file, each trust it to be internally consistent, and none of them
can revise it without a coordinated redeploy of all three, and with zero
audit trail of who changed what and when.

This is the same shape of problem `facility-layout` was built to solve for
physical location structure: a well-understood, industry-common concern
that several existing contexts need identically, where no single existing
context is a more natural owner than the others. `facility-layout`'s own
ADR made the case that physical location "sits in the same bucket the
platform's DDD reference puts Cartonization and WCS in" — Generic
Subdomains, extracted once rather than duplicated. The process-path
catalogue is the same shape of decision: not a competitive differentiator
(unlike `fulfillment-execution`'s task lifecycle or `inventory-storage`'s
chaotic-storage inventory truth), but genuinely needed, identically, by
three different services.

Two integration shapes were available once the catalogue became its own
service:

1. **Synchronous HTTP read-through** — each of the three consumers calling
   this service's REST API at dispatch/claim time to resolve a path
   definition live.
2. **Asynchronous Kafka event propagation** — this service publishes
   `ProcessPathCreated`/`ProcessPathUpdated`/`ProcessPathDeactivated`, and
   each consumer maintains its own local cache/read model, updated from the
   event stream.

Forces favoring option 2:

- **Every other cross-context integration in this fleet that resembles
  "context A needs a fact that context B owns" is Kafka-driven.**
  `StockReserved`, `ShiftPlanCommitted`, and `TaskCompleted` are all Kafka
  events, consumed asynchronously — never a synchronous hot-path RPC. A
  process-path lookup happening on every `claimNext`/dispatch decision in
  `fulfillment-execution` is exactly the kind of latency-sensitive,
  high-frequency call that a synchronous dependency on a separate service
  should never sit on.
- **Path definitions change rarely relative to how often they are read.**
  A local, event-maintained cache in each consumer is the natural fit for
  data that is read on every dispatch decision but written by an operator
  perhaps a few times a day.
- **A synchronous dependency would put a Generic-subdomain service's
  availability on the hot path of three Core-subdomain contexts.** If this
  service (or its database) were briefly unavailable, a synchronous design
  would mean `fulfillment-execution`, `wes-work-planning`, and
  `workforce-management` could not dispatch work at all. An
  event-propagated cache means each consumer keeps operating on its last-
  known-good local copy even if this service is down.

## Decision

**A new Generic-subdomain bounded context, `process-path-management`, owns
the `ProcessPath` aggregate and replaces the static YAML catalogue as the
single source of truth for process-path definitions.**

It is explicitly **Generic, not Core or Supporting**, matching
`facility-layout`'s classification: it is well-understood and not a
competitive differentiator, but it is needed identically by multiple
contexts, so it is extracted rather than duplicated or left as an
unowned static file.

**Propagation is exclusively via Kafka.** This service publishes
`ProcessPathCreated`, `ProcessPathUpdated`, and `ProcessPathDeactivated` on
`warehouse.process-path-management.events`. It has **zero REST dependency**
on `fulfillment-execution`, `wes-work-planning`, or `workforce-management`,
and none of them has (as of this document) a synchronous dependency on this
service either — see the
[Context Map](/docs/ecosystem/context-map) for the current, honest state
of that integration (topic and publisher real and tested; as of this
record's writing no consumer was wired yet in any of the three downstream
repos — all three have since consumed the topic, via ADR 0002's cutover
and ADR 0016's CloudEvents envelope).

This service is the SOURCE of the process-path published language, never a
consumer of anyone else's — it has no inbound Kafka consumer and no
synchronous dependency in any direction.

## Consequences

### Easier

- **A single, auditable source of truth for process-path definitions**,
  with a real REST API to define/revise/deactivate paths and a domain
  model that enforces the same invariants (non-empty lower-case
  `matchPrefix`, non-empty `requiredCapabilities`) the retired YAML file
  could only enforce by convention, never by construction.
- **Revising a path definition no longer requires a coordinated redeploy**
  of `fulfillment-execution`, `wes-work-planning`, and
  `workforce-management` — an operator calls this service's REST API, and
  (once consumers are wired) each downstream context picks up the change
  from the event stream on its own cadence.
- **This service can be deployed, scaled, and even go down independently**
  without blocking any consumer's ability to dispatch work, since none of
  them depends on it synchronously.

### Harder

- **A ninth service in the fleet is a ninth thing to run, monitor, and
  deploy.** Accepted here for the same reason `facility-layout` was: the
  concern is real and shared enough to earn its own home rather than
  staying an unowned static file.
- **Until a consumer is actually wired in each of the three downstream
  repos, this service's publisher has no real effect on fleet behavior** —
  it is additive and tested, but genuinely unconsumed today. This is
  documented honestly (see the Context Map) rather than implied to already
  be wired, and each of those three wirings is a separate, tracked
  follow-up PR.
- **Each consumer must build and maintain its own local read model /
  cache** derived from this service's event stream, rather than always
  reading a live value — the standard cost of choreography over
  orchestration, accepted here for the same latency/availability reasons
  every other cross-context integration in this fleet already accepts it.
