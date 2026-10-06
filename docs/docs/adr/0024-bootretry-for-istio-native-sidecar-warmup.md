---
id: 0024-bootretry-for-istio-native-sidecar-warmup
slug: /adr/0024-bootretry-for-istio-native-sidecar-warmup
title: 0024. Boot-time dial retry for the Istio native-sidecar warm-up race
sidebar_label: 0024. Boot retry (sidecar warm-up)
description: ADR 0024 — internal/adapters/outbound/bootretry wraps every composition root's first Postgres dial in a bounded retry so Istio 1.30 native-sidecar warm-up does not turn into CrashLoopBackOff; fail-closed once the budget is spent.
---

# 0024. Boot-time dial retry for the Istio native-sidecar warm-up race

## Status

Accepted — codifies what already ships, retroactively.

## Context

Fleet-wide known condition: every injected pod's FIRST outbound TCP
dial (Postgres here) fails with "read: connection reset by peer" ~10s
after start, because Istio 1.30's native sidecars finish warming their
outbound listener after the app's first connection attempt
(`holdApplicationUntilProxyStarts` is a no-op for native sidecars). A
single attempt turns that one-time transient into `os.Exit(1)` and a
kubelet CrashLoopBackOff; the second boot is always clean.
`internal/adapters/outbound/bootretry` wraps the migration run, pool
ping, and analytics dials in all four composition roots
(cmd/pathmgmt, -projector, -reports, -mcp) in a bounded retry
(~31s budget), mirroring network-fulfillment's shipped reference.

## Decision

Keep the bounded startup retry. It is explicitly NOT a weakening of
the fail-closed convention: once the budget is exhausted the process
still refuses to boot and reports the real last error. Retry scope is
boot-time dials only — never request-path calls.

## Consequences

Pods survive the sidecar warm-up race instead of crash-looping; a
genuinely unreachable database still fails the boot with the real
error after ~31s. The startupProbe window (periodSeconds 2 x
failureThreshold 30) already covers the retry budget.
