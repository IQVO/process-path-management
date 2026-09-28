---
slug: /adr
title: Architecture Decision Records
sidebar_label: About ADRs
description: Why this service keeps ADRs, the template it uses, and how to propose a new one.
---

# Architecture Decision Records

## What these are

An **Architecture Decision Record (ADR)** captures a single architecturally
significant decision, its context, and its consequences — at the moment it was
made, in the words that were true then.

The value is not the decision; it is the **context**. Six months later, the
code shows *what* was chosen and can never show *why*, or what the alternatives
were, or what was known at the time. A reader who does not know the why has two
bad options: assume the decision was arbitrary and change it, or assume it was
sacred and work around it. An ADR gives them a third.

## The format

These records use **Michael Nygard's template** — the de facto standard:

```markdown
# NNNN. Title (a short noun phrase)

## Status
Accepted | Proposed | Deprecated | Superseded by ADR-XXXX

## Context
The forces at play: technical, business, operational. What is true that makes
this a decision rather than an obvious default? Written in full sentences and
in the present tense, describing the situation as it is.

## Decision
The response to those forces, stated actively: "We will…"

## Consequences
What becomes easier and what becomes harder — both. A record with only
positive consequences has not been thought about.
```

One decision per file, numbered `0001-`, `0002-`, … Numbers are never reused
and files are never deleted.

## Immutability

**An accepted ADR is never edited to change its decision.** If a decision is
reversed, write a new ADR that supersedes it, and update the old one's
*Status* line to point at the successor. The historical record of what was
believed at the time is the entire asset.

Typos, broken links and formatting are of course fair game.

## The records

| # | Title | Status |
| --- | --- | --- |
| [0001](./0001-process-path-management-bounded-context.md) | Process Path Management as a new Generic Subdomain bounded context | Accepted |
| [0002](./0002-yaml-to-kafka-cutover.md) | Cutting the fleet over from the static YAML catalogue to this service's events | Accepted |
| [0003](./0003-transactional-outbox.md) | Transactional outbox for the process-path Published Language | Accepted |
| [0004](./0004-rest-auth-adoption.md) | Adopting the fleet REST identity standard | Superseded by 0005 |
| [0005](./0005-remove-rest-auth.md) | Removing the REST auth layer | Accepted |
| [0006](./0006-mcp-server-second-inbound-adapter.md) | MCP server as a second inbound adapter | Accepted |
| [0007](./0007-analytical-data-product.md) | Analytical data product (report) via a separate analytics topic | Accepted |
| [0008](./0008-fclm-aligned-process-path-families.md) | Seven new process-path families aligned to real FC labor-tracking vocabulary | Accepted |
| [0009](./0009-destination-location-role-on-process-path.md) | Optional destination LocationRole on a ProcessPath | Accepted |
| [0010](./0010-fulfillment-capability-contract.md) | Process paths publish a fulfillment capability contract (cycle time, eligibility, CPT schedule) | Accepted |
| [0011](./0011-idempotency-key-middleware.md) | Transactional Idempotency-Key middleware for POST /process-paths | Accepted |
| [0012](./0012-kafka-dlq-and-graceful-shutdown.md) | Analytics-consumer dead-letter queue and graceful shutdown hardening | Accepted |
| [0013](./0013-kafka-writer-hash-balancer.md) | Hash balancer for the outbound Kafka writers, closing a latent partition-affinity gap | Accepted |

Each of these reconstructs a decision that is actually visible in this
repository's history or code — none is a generic placeholder.

## Proposing a new one

1. Copy the template above into `docs/docs/adr/NNNN-short-kebab-title.md`,
   taking the next free number.
2. Open it with **Status: Proposed**.
3. Write the *Context* before the *Decision*. If the context does not make the
   decision feel inevitable, the context is incomplete — or the decision is
   wrong.
4. Fill in *Consequences* honestly, including the ones you dislike.
5. Add it to the table above and to `sidebars.ts`.
6. Raise it as a pull request; the discussion belongs on the PR, not inside the
   record.
7. On merge, flip the status to **Accepted**.

## What deserves an ADR

Something is architecturally significant if reversing it later would be
expensive — it constrains the structure of the code, pins a contract other
teams depend on, or is costly to undo.

**Yes:** the bounded-context boundary decision; the integration transport and
envelope; a quality gate that blocks merges.

**No:** which linter rules are enabled; a library upgrade; a refactor with no
external consequence. Padding the log with trivia makes the significant records
harder to find.
