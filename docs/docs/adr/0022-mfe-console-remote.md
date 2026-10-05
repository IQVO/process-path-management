---
id: 0022-mfe-console-remote
slug: /adr/0022-mfe-console-remote
title: 0022. The operator console MFE remote and its chart component
sidebar_label: 0022. MFE console remote
description: ADR 0022 — web/ builds the process_path_mfe Module Federation remote served by its own nginx pod behind the fleet console's web gateway; ADR 0010's capability fields (cycleTimeP95/eligibility) are part of its contract.
---

# 0022. The operator console MFE remote and its chart component

## Status

Accepted — codifies what already ships, retroactively.

## Context

Operators manage process paths (and, since ADR 0010, their capability
contract: cycleTimeP95, eligibility, CPT schedules) through the fleet
console — a Module Federation host that loads one remote per bounded
context. This repo's `web/` builds the `process_path_mfe` remote
(vite.config.ts exposes `./App` as `remoteEntry.js` under the
`/mfes/process-path-management/` prefix), served by its own
nginx-unprivileged pod (chart `frontend.*` templates) reached only
through the warehouse-infra Nginx web gateway — never Kong (APIs only)
and no second host-facing entrypoint. POSTs send a per-submit
`crypto.randomUUID()` Idempotency-Key (ADR 0011). ADR 0010 initially
shipped without the capability fields on the MFE forms, which made the
console unable to define or revise paths at all (422 on every submit) —
fixed in this repo's ADR-conformance pass.

## Decision

Keep the remote shape: one vite Module Federation remote per repo,
unique asset prefix, served by the chart's frontend component, gateway
routing owned by warehouse-infra. MFE forms must expose every field the
REST contract requires for POST/PUT (the ADR 0010 lesson: a server-side
field without a form input bricks the console's write path).

## Consequences

Each context's console UI lives with its service and deploys
independently. The cost is the discipline that forms track the API
contract — now enforced by the ADR 0010 lesson above and the web CI
job (lint, tsc, tests, build against the shared ui-kit).
