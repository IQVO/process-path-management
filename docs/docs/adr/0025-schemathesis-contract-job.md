---
id: 0025-schemathesis-contract-job
slug: /adr/0025-schemathesis-contract-job
title: 0025. Schemathesis property-based contract testing against the live API
sidebar_label: 0025. Schemathesis contract job
description: ADR 0025 — CI's contract job boots the real router on in-memory adapters and throws Schemathesis-generated valid/invalid requests at every operation in apis/openapi.yaml, catching spec/implementation drift example-based suites cannot see.
---

# 0025. Schemathesis property-based contract testing against the live API

## Status

Accepted — codifies the job that already ships, retroactively.

## Context

Example-based suites (unit, httptest, BDD) only assert the cases their
author imagined. The fleet's drift-catcher is Schemathesis: CI's
`contract` job (`.github/workflows/ci.yml`, same invocation as
`make contract` via `scripts/contract-test.sh`) boots the service on
its in-memory adapters and generates valid AND invalid requests for
every operation in `apis/openapi.yaml`, asserting response conformance
— status codes, content types, response schemas, negative data
rejected. It has already caught real drift here (e.g. `?all=F`
answering 200, see server_test.go's lenient-boolean test).

## Decision

Keep the contract job gating every PR. The OpenAPI file is the single
source of truth; any handler/spec divergence the generator can reach
fails CI. In-memory mode means no Postgres is needed — which is also
why contract-visible headers are declared `required: false` with no
`minLength` (the Idempotency-Key header, ADR 0011): in-memory mode
does not enforce the middleware's 400/422 paths.

## Consequences

Spec drift is caught before merge, not by a client in production. The
cost is that the spec must stay honest about what in-memory mode can
enforce — documented per-declaration where it differs from
Postgres-backed behaviour (the ADR 0011 header note).
