---
id: process-path-catalogue
title: The process-path catalogue
sidebar_label: Process-path catalogue
description: Which process-path families the fleet uses, where each one is declared, which demand sources may route work onto each (intake, ADR 0034), how a path gets into the store and onto warehouse.process-path-management.events, and the declared catalogue (catalogue/paths.json) with its idempotent seed (ADR 0033, ADR 0035, ADR 0036).
---

# The process-path catalogue

The catalogue is the set of Active `ProcessPath` aggregates in this service's
store. No path is compiled into the binary. A path exists because someone
defined it through `POST /process-paths`. The `DefinePath` use case wrote the
row and its `ProcessPathCreated` outbox row in one transaction, and the relay
then published the event on `warehouse.process-path-management.events`.
Five consumers read that topic: wes-work-planning, fulfillment-execution,
workforce-management, order-management and network-fulfillment. The
catalogue consumers replay it from the earliest offset into an in-memory
cache and apply new events live. A new path reaches running pods without a
restart.

## Families and where each is declared

| Family (`pathId` / `matchPrefix`) | Required capability | Declared in | Applied by |
| --- | --- | --- | --- |
| `PICK` / `pick`, `PACK` / `pack`, `REBIN` / `rebin`, `SLAM` / `slam` | same as the prefix | first by warehouse-infra's frozen sortable-fc catalogue file (the rollback copy of the retired file catalogue, [ADR 0002](../adr/0002-yaml-to-kafka-cutover.md)); their intake is declared in **`catalogue/paths.json`** ([ADR 0034](../adr/0034-process-path-intake.md)) | warehouse-infra's seed created them; `scripts/seed_catalogue.py` converges their intake |
| `PREP`, `PROBLEM_SOLVE`, `WATER_SPIDER`, `AMNESTY`, `RETURNS`, `TRANSFER`, `DAMAGE` (prefix = kebab-case name) | same as the prefix | [ADR 0008](../adr/0008-fclm-aligned-process-path-families.md) | one-off `POST /process-paths` calls recorded in that ADR |
| `DISPATCH` / `dispatch` | `dispatch` | **`catalogue/paths.json`** in this repository ([ADR 0035](../adr/0035-dispatch-process-path.md)) | `scripts/seed_catalogue.py` (`make seed-catalogue`) |
| `RELOCATE` / `relocate` | `relocate` | **`catalogue/paths.json`** in this repository ([ADR 0033](../adr/0033-relocate-process-path.md)) | `scripts/seed_catalogue.py` (`make seed-catalogue`) |
| `REPLENISH` / `replenish` | `replenish` | **`catalogue/paths.json`** in this repository ([ADR 0036](../adr/0036-replenish-process-path.md)) | `scripts/seed_catalogue.py` (`make seed-catalogue`) |

New families are declared in `catalogue/paths.json`, never as an undocumented
`curl`.

## Intake: who may route work onto a path

Every path declares its `intake`, the demand sources allowed to route work
onto it ([ADR 0034](../adr/0034-process-path-intake.md)). Eligibility says
what a unit of work must look like; intake says who may send it.

| Path | `intake` | Why |
| --- | --- | --- |
| `PICK` | `["CUSTOMER_ORDER"]` | The one entry point for customer order lines. |
| `PACK`, `REBIN`, `SLAM` | `[]` | Downstream stages, fed by PICK's output through an explicit path id. |
| `RELOCATE` | `["SLOT_RELOCATION"]` | Approved slot-plan moves only. |
| `DISPATCH` | `["STOCK_TRANSFER"]` | The transfer dispatch leg only ([ADR 0035](../adr/0035-dispatch-process-path.md)). |
| `REPLENISH` | `["REPLENISHMENT"]` | Pick-face top-ups only ([ADR 0036](../adr/0036-replenish-process-path.md)). `REPLENISHMENT` is accepted on no other path. |
| ADR 0008's families | `[]` (not declared here yet) | Routed by explicit path id today. |

order-management's path selection considers only active paths whose intake
contains `CUSTOMER_ORDER`. When none does (the catalogue has not been seeded
yet), it falls back to `PICK`, never to the lowest path id. Before ADR 0034
it considered every active path, and with four 2h paths a customer line tied
and resolved to `PACK`.

## The declared catalogue

`catalogue/paths.json` holds `{"paths": [...]}`, and every entry is a
`POST /process-paths` request body exactly as the API takes it. Every entry
must carry an `intake` array (`[]` included); the seed refuses a file where
one is missing. The file is
not a second source of truth: nothing reads it at boot, and the store remains
the catalogue. The file is the reviewed record of what an environment must
define. Three tests keep it honest:

- `internal/domain/processpath/declared_catalogue_test.go` defines every
  entry through the aggregate, so a declaration the service would reject
  cannot be merged.
- `features/relocate_path.feature`, `features/replenish_path.feature` and `features/process_path_intake.feature`
  post the file's bytes through the real router and assert the published
  CloudEvent, including each path's `intake`.
- `scripts/test_seed_catalogue.py` runs the seed against a fake API.

### RELOCATE

Approved slot-plan moves from slotting-optimization run as floor work
(slotting-optimization ADR 0010). wes-work-planning plans one RELOCATE work
unit per move and releases it on this path. fulfillment-execution executes it
as a `RELOCATE` task for an operator holding `relocate`.
inventory-storage then records the move from the task's `TaskCompleted`.

```json
{"pathId": "RELOCATE", "matchPrefix": "relocate", "direct": true,
 "requiredCapabilities": ["relocate"], "cycleTimeP95": "2h",
 "intake": ["SLOT_RELOCATION"]}
```

The intake matters. order-management routes a customer order line only to
paths whose intake contains `CUSTOMER_ORDER`, so RELOCATE is never chosen for
an order, whatever its cycle time. wes-work-planning plans relocation units
directly on the path id. The first draft of ADR 0033 used a fake
`requiredProductAttributes: ["slot-relocation"]` eligibility for this; ADR
0034 replaced it, and the seed clears it with a `PUT` wherever it was
applied.

### DISPATCH

network-inventory-planning releases the dispatch leg of an inter-warehouse
stock transfer as a `WorkDemandReleased` that names a path id. wes-work-planning
forwards it, and fulfillment-execution builds the task from this catalogue. It
sets the task type from the path's id, so the id has to be the task type
fulfillment-execution's ADR-0036 names for the leg: `DISPATCH`, not
`TRANSFER_DISPATCH`. Without this entry a dispatch demand matches no path and
is dead-lettered.

```json
{"pathId": "DISPATCH", "matchPrefix": "dispatch", "direct": true,
 "requiredCapabilities": ["dispatch"], "cycleTimeP95": "2h",
 "intake": ["STOCK_TRANSFER"]}
```

Who must set what ([ADR 0035](../adr/0035-dispatch-process-path.md)):

- network-inventory-planning's configured path id must be `dispatch-transfer`
  (it starts with `dispatch-`, so it resolves to this entry). That is a
  warehouse-infra change, not a change in this repository.
- Stations that execute the leg need the capability `dispatch`.
- The earlier instruction to `POST` a `TRANSFER_DISPATCH` /
  `transfer-dispatch` path by hand is superseded: do not run it.

The intake is `STOCK_TRANSFER` only, so order-management never routes a
customer order line onto DISPATCH. No arrival family is declared yet.

### REPLENISH

Pick-face top-ups run through the relocation chain: pick-face-replenishment
requests one, wes-work-planning plans a `REPLENISH` work unit and releases it
on this path, fulfillment-execution executes it as a `REPLENISH` task for an
operator holding `replenish` (fulfillment-execution ADR 0050), and
inventory-storage records the move. Without this entry a released replenishment
matches no path and cannot be accepted.

```json
{"pathId": "REPLENISH", "matchPrefix": "replenish", "direct": true,
 "requiredCapabilities": ["replenish"], "cycleTimeP95": "2h",
 "intake": ["REPLENISHMENT"]}
```

`REPLENISHMENT` is a new value in the closed intake vocabulary
([ADR 0036](../adr/0036-replenish-process-path.md)). It is the only source
allowed to route work onto REPLENISH, and REPLENISH carries no other source.
Eligibility is the permissive `{}` and `cycleTimeP95` is RELOCATE's flat 2h:
neither is tuned here. Stations that execute the task need the capability
`replenish`.

### Applying it

```bash
kubectl port-forward -n warehouse-systems svc/process-path-management 8080:80
make seed-catalogue BASE_URL=http://localhost:8080 SEED_ARGS=--dry-run   # preview
make seed-catalogue BASE_URL=http://localhost:8080                       # converge
```

| Store state vs declaration | Seed action | Exit |
| --- | --- | --- |
| absent | `POST /process-paths` with an `Idempotency-Key`, which publishes `ProcessPathCreated` | 0 |
| present, identical | nothing | 0 |
| present, `matchPrefix`, `requiredCapabilities`, `cycleTimeP95`, `eligibility` or `intake` differs (intake compared as a set; a path stored before ADR 0034 has `[]`) | `PUT /process-paths/{pathId}`, which publishes `ProcessPathUpdated` | 0 |
| present, `direct` or `destinationLocationRole` differs | reported; both are immutable, so declare a new `pathId` | 1 |
| present, `DEACTIVATED` | reported; a deactivated id is never resurrected | 1 |

Run it after deploying a release that adds an entry. For RELOCATE, run it
before slotting-optimization approves the first plan in that environment.
Until then, wes-work-planning rejects relocation work as an unknown path id.
For DISPATCH, run it before network-inventory-planning releases the first
dispatch leg; until then the demand is dead-lettered. For REPLENISH, run it
before wes-work-planning releases the first replenishment; re-running it
afterwards changes nothing.
Check what the consumers will replay with
`curl -s localhost:8080/process-paths/RELOCATE` (or `.../DISPATCH`, `.../REPLENISH`). Every entry also appears on
the topic as a `ProcessPathCreated` keyed by its `pathId`.
