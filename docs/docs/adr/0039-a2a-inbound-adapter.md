---
id: 0039-a2a-inbound-adapter
slug: /adr/0039-a2a-inbound-adapter
title: 0039. A2A inbound adapter
sidebar_label: 0039. A2A inbound adapter
description: "ADR 0039: process-path-management is an Agent2Agent (A2A) agent through a third driving adapter over the same read use cases as REST and MCP. Two curated, goal-shaped, READ-ONLY skills, get_process_path and list_process_paths, in a message-only agent served by cmd/a2a on port 8093. The MCP charter is entirely read-only, so the agent never writes; A2A carries delegation only, and facts still leave as CloudEvents through the transactional outbox."
---

# 0039. A2A inbound adapter

## Status

**Accepted** (2026-10-29). Follows [ADR 0006](./0006-mcp-server-second-inbound-adapter.md)
(the MCP adapter this one is modelled on),
[ADR 0005](./0005-remove-rest-auth.md) (no auth on any surface),
[ADR 0016](./0016-cloudevents-mandatory-event-envelope.md) (CloudEvents on every
Kafka message) and [ADR 0037](./0037-postgres-and-kafka-are-the-only-runtime.md)
(Postgres and Kafka are the only runtime). Mirrors the fleet's A2A rollout
(workforce-management ADR 0039, wes-work-planning ADR 0046).

## Context

The fleet is adopting the Agent2Agent protocol so that a bounded context can
**delegate** a goal to another context's agent. This context owns the
process-path catalogue that every other context references, and already
exposes it to agents over MCP (read-only tools) and to services over REST.

Three transports now carry three different things and must not overlap:

| Transport | Carries |
|---|---|
| Kafka + CloudEvents | **facts** between contexts (ADR 0016) |
| MCP | an agent's **tools** |
| A2A | **delegation** of a goal to another context's agent |

Two repository rules constrain the design:

- **AGENTS.md: no call to any sibling context, in either direction** (no REST,
  no MCP, no gRPC, no live lookup). That concerns outbound clients. An inbound
  A2A server is allowed; an outbound A2A client is not.
- **The MCP surface is entirely read-only** ([ADR 0006](./0006-mcp-server-second-inbound-adapter.md),
  the [governance charter](../mcp/governance-charter.md) §4): paths and CPT
  schedules change only through the REST API.

## Decision

Add `internal/adapters/inbound/a2a`, a third driving adapter over the **same**
use cases (`GetPath`, `ListPaths`) the HTTP and MCP adapters use, built on the
official `a2aproject/a2a-go/v2` SDK (pinned `v2.7.0`) and served by a new
`cmd/a2a` binary (JSON-RPC 2.0 over HTTP, port **8093**, the Agent Card at
`/.well-known/agent-card.json`, the endpoint mounted at `/` and `/a2a`).

### The agent is read-only, like the MCP charter

An A2A skill may change state only if the context's MCP surface already offers
that write. It offers none, so this agent offers none: it never widens what the
context exposes. Defining, revising or deactivating a path, and defining a CPT
schedule, stay REST-only. Asking this agent for one of them is an `unknown-skill`
problem and writes nothing (`TestWireOffersNoWriteSkill`). A fitness test
(`TestA2AAdapterIsReadOnlyAndHasNoOutboundClient`) fails the build if the
adapter references a write use case, the unit of work or the event publisher.

### Two curated skills, not one per endpoint

| Skill | Kind | What it does | Derived from |
|---|---|---|---|
| `get_process_path` | read | the full definition of one path by `pathId`: `matchPrefix`, `direct`, `requiredCapabilities`, `cycleTimeP95`, `eligibility`, `intake`, status and timestamps; deactivated paths too | MCP `get_process_path` |
| `list_process_paths` | read | the catalogue: active paths by default, or every path with `activeOnly=false` | MCP `list_process_paths` |

The skills are derived from the MCP tools and carry their names, so one name
identifies one question across both agent-facing surfaces. `get_cpt_schedule`
and `get_catalogue_growth_report` (MCP tools) are deliberately **not** skills:
the catalogue is what other contexts delegate lookups to, and two skills is the
fleet's ceiling. The report tool also calls the reports REST service, which an
A2A agent has no reason to proxy.

### Message-only, answered in-band

Both skills are transactional request/response reads, so the agent is
message-only (no Task lifecycle). Domain rejections and malformed requests are
**answered** as RFC 7807-shaped `problem`s inside the message envelope, never
raised as JSON-RPC transport errors, using the REST adapter's type namespace
([ADR 0020](./0020-rfc-7807-problem-details.md)) and status classification
(`statusFor`) for the outcomes a read can have:

| Status | Problem type | When |
|---|---|---|
| `404` | `path-not-found` | no path has that `pathId` (same type as REST `GET /process-paths/{pathId}`) |
| `400` | `invalid-argument` | missing or blank `pathId`, an unknown or mistyped argument |
| `400` | `malformed-skill-request`, `unknown-skill` | the message is not a skill request, or names a skill the card does not list |
| `500` | `internal-error` | anything unrecognised; the answer carries no underlying error text |

REST's `409` (conflict, concurrent modification) and `422` (domain validation)
classes belong to writes and to REST path/body parsing that this read-only
agent does not have, so they are not mapped. The `404` and `500` answers carry
adapter-owned detail text: the shared use case error's internal `usecases:`
prefix and any infrastructure error text never reach an agent client (tests
assert both). This context has no deprecated alias or key for an A2A skill to
leak.

### Strict input

Unknown argument names are **rejected**, not ignored (a misspelled
`activeOnlyy` must not silently fall back to the active-only default), and data
after the JSON document is rejected. Go's `encoding/json` matches names
case-insensitively, so `activeonly` still reaches `activeOnly`. A text part
holding the same JSON is accepted.

### Facts still travel on Kafka; nothing calls out

A2A never becomes a data-sync channel, and this agent has nothing to publish:
it writes no outbox row, so `cmd/a2a` has no `EventPublisher`, no `UnitOfWork`
and no Kafka dependency (like `cmd/mcp`), and never runs the relay (only
`cmd/pathmgmt` drains `outbox_events`, ADR 0003). Every change to the catalogue
still leaves as a CloudEvent through the REST write path. The adapter contains
no outbound A2A client, no `net/http` client and no call to another context;
`TestNoSiblingContextOutboundCalls` and the A2A fitness test enforce it.

### Unauthenticated, like REST and MCP

The card declares no security schemes: this surface is unauthenticated by the
same fleet-wide decision as REST and MCP (ADR 0005). Schemes are added to the
card, not a redesign, if that decision is ever reversed.

## Consequences

- A fifth binary (`/app/a2a`) in the same image and an `a2a.*` chart block
  (`<release>-a2a`, default **off**), exposed through Kong at
  `/api/a2a/process-path-management` rewritten to `/`. `A2A_AGENT_URL`
  (absolute `http(s)` URL advertised on the card) is mandatory at boot; the
  chart defaults it to the in-cluster Service DNS and `a2a.agentUrl` overrides
  it.
- The binary reads the same `DATABASE_URL` as `cmd/pathmgmt` and, like
  `cmd/mcp`, applies the idempotent migrations at boot. It needs no Kafka
  variable.
- Delegating *to* this agent does not change who decides: this context defines
  what a path is and which capabilities it requires, and never decides
  dispatch, routing or assignment.
- Port 8093 is the fleet's A2A port (8090 is MCP, 8091 the projector admin and
  8092 the reports API in this chart).
- Not in this decision: CPT schedule or growth-report skills (a follow-up if a
  delegating agent needs them), any write skill, and an outbound A2A client.
