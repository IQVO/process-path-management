---
id: 0021-architecture-fitness-suite
slug: /adr/0021-architecture-fitness-suite
title: 0021. The architecture fitness test suite as a merge gate
sidebar_label: 0021. Architecture fitness suite
description: ADR 0021 — internal/architecture encodes this repo's structural rules (hexagonal imports, CloudEvents envelope, no sibling-context calls, testcontainers-only Kafka tests) as Go tests that gate every PR.
---

# 0021. The architecture fitness test suite as a merge gate

## Status

Accepted — codifies the suite that already ships, retroactively.

## Context

Rules that live only in AGENTS.md decay. This repo's structural
invariants — hexagonal import direction, CloudEvents 1.0 on every Kafka
message, zero synchronous dependency on any sibling bounded context,
Kafka integration tests must use testcontainers — are each the kind a
well-meaning PR can silently violate. arch-go is the fleet's tool for
the import-direction subset; this repo encodes the rest as plain Go
tests in `internal/architecture` (fitness_test.go,
architecture_test.go, events_fitness_test.go, violation_test.go with
deliberate-violation fixtures), run by CI's `arch-test` job.

## Decision

Keep the fitness suite as a blocking gate: every structural rule gets a
test (with a violation fixture proving the test can fail), new rules
land as new tests in the same package, and the suite stays fast enough
to run on every PR (~35s).

## Consequences

A PR that imports infra from the domain, mints a flat event envelope,
adds an HTTP client aimed at a sibling context, or writes a
Kafka-touching test gated on `KAFKA_BROKERS` + `t.Skip` fails CI with
a message naming the rule. The cost is maintaining the tests alongside
the rules — deliberately cheap here because each test reuses the same
file-walking helpers.
