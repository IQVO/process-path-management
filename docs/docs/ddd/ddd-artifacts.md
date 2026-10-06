---
title: DDD Artifacts
sidebar_label: DDD Artifacts (index)
sidebar_position: 1
description: Index of the ddd-crew DDD artifact pack for process-path-management — core domain chart, canvases, context map, message flows, EventStorming, glossary, UML class, ER and sequence diagrams, all derived from the code.
---

# DDD Artifacts

The DDD artifact pack for **process-path-management**, built with the
[ddd-crew](https://github.com/ddd-crew) tools plus UML/ER diagrams. Every
diagram is Mermaid, derived from the code on `develop`, and carries a
"Source:" line naming the files it came from and a note on what it omits.

| Artifact | What it shows | ddd-crew tool / notation |
| --- | --- | --- |
| [Core Domain Chart](./core-domain-chart.md) | Where this context sits on business differentiation vs model complexity (Generic) and its evolution | [Core Domain Charts](https://github.com/ddd-crew/core-domain-charts) |
| [Bounded Context Canvas](./bounded-context-canvas.md) | Purpose, classification, roles, every inbound and outbound message | [Bounded Context Canvas v5](https://github.com/ddd-crew/bounded-context-canvas) |
| [Context Map](../ecosystem/context-map.md) | This context's slice of the fleet map: OHS + PL to five Conformists over Kafka, MCP and console edges, deliberately absent edges | [Context Mapping](https://github.com/ddd-crew/context-mapping) |
| [Aggregate Design Canvas](./aggregate-design-canvas.md) | `ProcessPath` and `CPTSchedule`: states, invariants, commands, events, throughput, size | [Aggregate Design Canvas v1.1](https://github.com/ddd-crew/aggregate-design-canvas) |
| [Domain Message Flow](./domain-message-flow.md) | Four business scenarios as numbered `cmd:` / `evt:` / `qry:` flows | [Domain Message Flow Modelling](https://github.com/ddd-crew/domain-message-flow-modelling) |
| [EventStorming](./eventstorming.md) | Design-level boards for the path lifecycle, the CPT schedule and the analytics projection, with real hotspots | [EventStorming glossary and cheat sheet](https://github.com/ddd-crew/eventstorming-glossary-cheat-sheet) |
| [Ubiquitous Language](./ubiquitous-language.md) | Glossary, every term mapped to its code identifier and wire name | ddd-crew glossary practice |
| [Class Diagram](./class-diagram.md) | UML class diagrams of `internal/domain/**` and the hexagonal ports and adapters | UML (Mermaid `classDiagram`) |
| [Entity Relationship](./entity-relationship.md) | Final Postgres schema of the OLTP and analytics databases after every migration | ER (Mermaid `erDiagram`) |
| [Sequence Diagrams](./sequence-diagrams.md) | Every command use case through adapter, use case, aggregate, repository and outbox | UML (Mermaid `sequenceDiagram`) |
| [Domain Events](./domain-events.md) | Every published and consumed event: full CloudEvents `type`, topic, key, payload, producer, consumers | Event catalogue |

Narrative background that predates the pack and is still current:
[Aggregates and Invariants](./aggregates-and-invariants.md) (the "why"
behind each rule) and the [ADRs](../adr/about.md).

## Sources of truth

Code wins over every page here. When a page and the code disagree, the
code is right and the page is a bug.

- Domain model: `internal/domain/processpath`, `internal/domain/cptschedule`,
  `internal/domain/shared`.
- Use cases and ports: `internal/application/usecases`,
  `internal/application/ports`.
- REST contract: `apis/openapi.yaml` (rendered in the
  [API Reference](/docs/api-reference/rest/process-path-management-api));
  routes in `internal/adapters/inbound/http/server.go`.
- Event contract: `apis/asyncapi.yaml`; the exact `type` strings in
  `internal/adapters/kafka/cloudevents/types.go`; payload structs in
  `internal/adapters/outbound/kafka/publisher.go`.
- Schema: `migrations/*.up.sql` and `migrations/analytics/*.up.sql`.
- Sibling relationships: the consumer files named on the
  [Context Map](../ecosystem/context-map.md), read from each sibling's
  `origin/develop`.
