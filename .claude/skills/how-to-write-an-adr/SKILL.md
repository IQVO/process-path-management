---
name: how-to-write-an-adr
description: Write an Architecture Decision Record in this repo's numbering and format, including companion ADRs for cross-repo changes. Use when a design decision should be recorded or a change contradicts an existing ADR.
---

# How to write an ADR

Use when a change is architecturally significant — a new bounded-context
integration, a reversal of a prior decision, a cross-repo contract
change, or anything a future reader would otherwise have to
reverse-engineer from the diff. Not every change needs one: a bug fix or
a routine feature addition inside an already-decided architecture
doesn't. This repo already has ten (docs/docs/adr/0001 through 0010),
covering a real range of the categories above — read a couple of the
ones named below before writing a new one.

## Numbering and location

`docs/docs/adr/NNNN-kebab-case-title.md`, four-digit zero-padded,
sequential — check the highest existing number
(`git ls-tree --name-only origin/develop -- docs/docs/adr/` — as of this
writing the highest is `0010-fulfillment-capability-contract.md`, so the
next ADR is `0011-...`) and pick the next integer, never reuse or guess.
`docs/docs/adr/about.md` explains the format to readers; you don't need
to touch it when adding a new ADR.

## Frontmatter (Docusaurus needs all five fields)

```yaml
---
id: 0011-kebab-case-title
slug: /adr/0011-kebab-case-title
title: "11. Title (a short noun phrase, matching the heading)"
sidebar_label: "11. Short label for the nav sidebar"
sidebar_position: 11
description: "One or two sentences — this shows up in search and link
  previews, so make it stand alone without the rest of the doc."
---
```

`id`/`slug` are the full kebab-case filename (minus `.md`); `title`/
`sidebar_label` repeat the number as plain text (`"11. ..."`, not
`#11`); `sidebar_position` is the bare integer. Getting these
inconsistent is the most common cause of a broken sidebar entry or 404
after merge — verify by running the docs build (see below) before
opening the PR. See `docs/docs/adr/0003-transactional-outbox.md`'s
frontmatter for an exact, real, currently-published example of this
shape.

## Format: Michael Nygard's template

```markdown
# NNNN. Title (a short noun phrase)

## Status
Accepted | Proposed | Deprecated | Superseded by ADR-XXXX

## Context
The forces at play — technical, business, constraints — that make this
decision necessary. Write in the past tense, as if explaining to someone
who wasn't there. State the alternatives seriously considered, not just
the one chosen; a reader six months from now needs to know a simpler
option was weighed and rejected, not assume nobody thought of it.

## Decision
What was actually decided, stated as an active, present-tense
declaration ("we will...", not "we might..."). Be specific about the
mechanism, not just the intent — this section should let a reader
implement the same decision from scratch without asking follow-up
questions.

## Consequences
What becomes easier, what becomes harder, and what future work this
creates or forecloses. Be honest about the downsides — an ADR that only
lists benefits reads as marketing, not a decision record.
```

The `## Decision` section is the part worth the most editing effort: see
this repo's own **ADR-0003**
(`docs/docs/adr/0003-transactional-outbox.md`) for a model example — it
states the exact mechanism (the `outbox_events` table schema, the
`ports.UnitOfWork` interface, the `postgres.OutboxRelay`'s `SELECT ...
FOR UPDATE SKIP LOCKED` claim strategy, and the full
`DATABASE_URL`/`EVENT_PUBLISHER` mode matrix), names the delivery
semantics precisely (atomicity, at-least-once, per-path ordering,
latency bound), and is specific enough that four sibling fleet services
(wes-work-planning, fulfillment-execution, workforce-management,
labor-performance) were handed this exact ADR as their own
implementation template — the "how to add an integration event" guide's
publishing section leans on the same `Encode`/`Send` split ADR-0003 (and
its ADR-0007 fan-out extension) established.

## Superseding an earlier ADR

Don't edit the old ADR's Decision section. Add a `## Status` line noting
`Superseded by ADR-XXXX` on the OLD one (a one-line patch), and open the
new ADR referencing it. This repo has a real, live example of this
already: **ADR-0005** (`docs/docs/adr/0005-remove-rest-auth.md`)
supersedes **ADR-0004** (`0004-rest-auth-adoption.md`) — read that pair
for the exact wording pattern (ADR-0004 adopted bearer-token REST auth;
ADR-0005 reverses it fleet-wide and is the ADR
`internal/architecture/fitness_test.go`'s `TestNoAuthMiddlewareReintroduced`
fitness test exists to statically enforce, so that a future "helpful" PR
re-adding a JWT middleware fails CI instead of shipping silently).

## Cross-repo decisions: use a companion ADR, not one repo's private opinion

When a decision genuinely spans two bounded-context repos, write ONE ADR
per repo, each referencing the other explicitly as "the companion ADR"
with a one-line description of the split of responsibility. This repo's
own **ADR-0003** is the reference example the fleet points delegated
agents at for the transactional-outbox pattern (see the
`warehouse-systems-fleet-ops` skill's
`references/transactional-outbox-rollout.md`, handed verbatim to the
agents rolling the same pattern out in the four sibling services) — when
those sibling repos write their own outbox ADRs, they should reference
this repo's ADR-0003 as the template they followed, the same way
facility-layout's ADR-0016/0017 pair reference each other for a genuinely
two-repo decision. Don't write the decision once in one repo and expect
another repo's readers to find it; each bounded context's docs site is
read independently.

## After writing: regenerate and verify the docs build

```bash
cd docs
npm ci
npm run build   # onBrokenLinks / onBrokenAnchors are both 'throw' — this
                 # WILL fail if the frontmatter/slug is wrong or a
                 # cross-reference link is broken
```

A broken ADR link or malformed frontmatter fails the build with a clear
Docusaurus error, not a silent 404 — always run this locally before
opening the PR. This repo's `.github/workflows/docs.yml` triggers on
push to `develop` only (paths `docs/**`) — see this repo's own
`AGENTS.md`, which calls out that this is deliberate (this repo's GitHub
Pages `github-pages` deployment environment only allows the `develop`
branch) and asks that it not be "fixed" to trigger off `main` without
checking first.
