---
id: 0034-process-path-intake
slug: /adr/0034-process-path-intake
title: "0034. A process path declares its intake: which demand sources may route work onto it"
sidebar_label: "0034. Process-path intake"
description: "ADR 0034 — ProcessPath gains intake, the set of demand sources (CUSTOMER_ORDER, STOCK_TRANSFER, SLOT_RELOCATION, INVENTORY_CONTROL) allowed to route work onto the path. Empty means the path is reached only by an explicit path id. Revisable through the existing update command, additive on REST, MCP and ProcessPathCreated/Updated (snake_case intake), stored by migration 0009. The declared catalogue gives PICK CUSTOMER_ORDER, PACK/REBIN/SLAM nothing and RELOCATE SLOT_RELOCATION, replacing RELOCATE's fake slot-relocation eligibility. Decision E5 of the 2026-10-13 decision log; order-management's path selection consumes it."
---

# 0034. A process path declares its intake: which demand sources may route work onto it

## Status

**Accepted** (2026-10-14). Decision E5 of the 2026-10-13 decision log. Amends
[ADR 0033](./0033-relocate-process-path.md) (RELOCATE keeps customer order
lines off through its intake, not through an eligibility attribute) and adds
a field to the [ADR 0010](./0010-fulfillment-capability-contract.md) capability
contract. Companion to order-management's path-selection-intake ADR, which
consumes the field.

## Context

order-management's `PathSelectionPolicy` routes each customer order line to the
eligible active path with the shortest known `cycleTimeP95`, ties broken on the
lower path id. Its catalogue cache (`ListActive`) returns **every** active path,
so every path is a candidate entry point for an order line.

The live catalogue holds PACK, PICK, REBIN and SLAM, all at the migration
default of 2h with permissive eligibility. Every one ties, and the lowest id
wins, so a customer order line resolves to **PACK**, a downstream stage that
only PICK's output should reach. No order lines have flowed yet, so nothing
has been misrouted. Adding paths makes it worse: ADR 0008's families
(`AMNESTY` sorts before `PICK`) and RELOCATE (ADR 0033) are candidates too.
The first draft of ADR 0033 kept order lines off RELOCATE with a fake
eligibility attribute (`requiredProductAttributes: ["slot-relocation"]`) that
no product may ever carry. That encodes a routing rule as a product
attribute, and every new family would have to repeat the trick.

The root cause is a missing concept. Which demand may enter a path is routing
policy, and the catalogue never declared it.

## Decision

`ProcessPath` gains **`intake`**: the set of demand sources allowed to route
work onto the path.

| Demand source | Who routes work with it |
| --- | --- |
| `CUSTOMER_ORDER` | order-management, for customer order lines (its `PathSelectionPolicy`) |
| `STOCK_TRANSFER` | network-inventory-planning transfer legs, planned through wes-work-planning |
| `SLOT_RELOCATION` | approved slot-plan moves from slotting-optimization, planned through wes-work-planning |
| `INVENTORY_CONTROL` | internal inventory work: amnesty, damage, problem solve |
| `REPLENISHMENT` | pick-face top-ups from pick-face-replenishment, planned through wes-work-planning (added by [ADR 0036](./0036-replenish-process-path.md)) |

- **Shape.** A set: values are distinct and kept in canonical (sorted) order.
  An unknown, blank, lower-case or repeated value is rejected (`422
  invalid-intake` on REST). The vocabulary is closed and owned by this
  service.
- **Empty set.** Valid, and the default. Such a path is routed only by an
  explicit path id from an upstream stage (PACK, REBIN, SLAM are fed by
  PICK's output); no demand source selects it.
- **Mutability.** Intake is policy, not identity, so `PUT
  /process-paths/{pathId}` revises it like `eligibility`. A change of intake
  alone is a real revision and raises `ProcessPathUpdated`. On `PUT` the field
  is optional: absent or `null` keeps the current intake, so a client that
  predates the field (the console's revise form, an older seed) never clears
  it; `[]` clears it.
- **Contracts, all additive.**
  - REST: optional `intake` on `POST /process-paths` (absent means `[]`) and
    on `PUT`; always present on every `ProcessPath` response.
  - MCP: `get_process_path` and `list_process_paths` carry `intake`.
  - Events: `ProcessPathCreated`/`ProcessPathUpdated` carry snake_case
    `intake`, always present (`[]` for the empty set, never omitted), and
    absent on `ProcessPathDeactivated` like every other definition field. A
    consumer that sees no `intake` is reading a producer older than this ADR
    and must treat it as unknown, not as `[]`.
  - Storage: migration `0009_intake` adds `process_paths.intake TEXT[] NOT NULL
    DEFAULT '{}'`. Existing rows read as the empty set. The default stays so a
    pod of the previous release can still insert during a rolling deploy. The
    repository re-validates the stored value on read and fails loudly on an
    unknown one rather than routing on a guess.
- **Declared catalogue.** `catalogue/paths.json` (ADR 0033) declares an
  `intake` array for every path, `[]` included:

  | Path | Intake |
  | --- | --- |
  | `PICK` | `["CUSTOMER_ORDER"]` |
  | `PACK`, `REBIN`, `SLAM` | `[]` |
  | `RELOCATE` | `["SLOT_RELOCATION"]` |

  RELOCATE drops the fake `slot-relocation` eligibility and returns to the
  permissive `{}`. `scripts/seed_catalogue.py` treats intake as a revisable
  field compared as a set, so a path stored before this ADR (intake `[]`) is
  converged with `PUT`, which publishes `ProcessPathUpdated`. A declaration
  without an `intake` array is rejected at load.
- **Consumer rule** (order-management, its own ADR): consider only active paths
  whose intake contains `CUSTOMER_ORDER`. If no path declares it (catalogue not
  yet seeded, or an older producer), fall back to `shared.DefaultPathId`
  (PICK) exactly like the empty-catalogue rule today. It must fail open to
  PICK, never to PACK.

## Consequences

**Easier**

- Each path declares where its work comes from in one explicit field. A new
  family states it in `paths.json`, and the declared-catalogue tests require
  it.
- order-management's selection stops treating downstream stages and internal
  paths as order entry points. Once seeded, PICK is the only CUSTOMER_ORDER
  path, so the PACK-wins tie disappears.
- ADR 0008's families (AMNESTY, DAMAGE, ...) default to `[]` and are no longer
  candidates for customer orders, which closes ADR 0033's follow-up without
  touching them.

**Harder**

- Ordering across contexts: until the seed has run in an environment, no path
  declares `CUSTOMER_ORDER`, and order-management falls back to PICK. That is
  the intended fail-open behaviour, not an outage. Seeding is a data step
  (`make seed-catalogue`) and is held for the operator.
- wes-work-planning and network-inventory-planning do not yet filter on
  `STOCK_TRANSFER` / `SLOT_RELOCATION`. They route by explicit path id today,
  so this is not a misroute risk; the values are declared now so those
  consumers can adopt them without another contract change.
- The vocabulary is closed. A fifth demand source needs a code change and an
  ADR here, which is intended for a routing-policy enumeration. (ADR 0036 added
  the fifth, `REPLENISHMENT`, by exactly that route.)
