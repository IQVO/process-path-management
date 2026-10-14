---
id: process-path-catalogue
title: The process-path catalogue
sidebar_label: Process-path catalogue
description: Which process-path families the fleet uses, where each one is declared, how a path gets into the store and onto warehouse.process-path-management.events, and the declared catalogue (catalogue/paths.json) with its idempotent seed - starting with RELOCATE (ADR 0033).
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
| `PICK` / `pick`, `PACK` / `pack`, `REBIN` / `rebin`, `SLAM` / `slam` | same as the prefix | warehouse-infra's frozen `config/process-paths/sortable-fc.yaml` (the rollback copy of the retired file catalogue, [ADR 0002](../adr/0002-yaml-to-kafka-cutover.md)) | warehouse-infra's `seed-process-paths.py`, through this API |
| `PREP`, `PROBLEM_SOLVE`, `WATER_SPIDER`, `AMNESTY`, `RETURNS`, `TRANSFER`, `DAMAGE` (prefix = kebab-case name) | same as the prefix | [ADR 0008](../adr/0008-fclm-aligned-process-path-families.md) | one-off `POST /process-paths` calls recorded in that ADR |
| `TRANSFER_DISPATCH` / `transfer-dispatch` | the dispatch crew's capability | warehouse-infra's README (network-inventory-planning section) | a manual `POST` before the transfer dispatch leg is enabled |
| `RELOCATE` / `relocate` | `relocate` | **`catalogue/paths.json`** in this repository ([ADR 0033](../adr/0033-relocate-process-path.md)) | `scripts/seed_catalogue.py` (`make seed-catalogue`) |

New families are declared in `catalogue/paths.json`, never as an undocumented
`curl`.

## The declared catalogue

`catalogue/paths.json` holds `{"paths": [...]}`, and every entry is a
`POST /process-paths` request body exactly as the API takes it. The file is
not a second source of truth: nothing reads it at boot, and the store remains
the catalogue. The file is the reviewed record of what an environment must
define. Three tests keep it honest:

- `internal/domain/processpath/declared_catalogue_test.go` defines every
  entry through the aggregate, so a declaration the service would reject
  cannot be merged.
- `features/relocate_path.feature` posts the file's bytes through the real
  router and asserts the published CloudEvent.
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
 "eligibility": {"requiredProductAttributes": ["slot-relocation"]}}
```

The eligibility matters. order-management routes every customer order line
to the eligible active path with the shortest known cycle time, and it
considers every active path. No order line ever carries the
`slot-relocation` attribute, so RELOCATE is never eligible for a customer
order. order-management is the only consumer that evaluates `eligibility`.
wes-work-planning does not, so relocation work is unaffected.

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
| present, `matchPrefix`, `requiredCapabilities`, `cycleTimeP95` or `eligibility` differs | `PUT /process-paths/{pathId}`, which publishes `ProcessPathUpdated` | 0 |
| present, `direct` or `destinationLocationRole` differs | reported; both are immutable, so declare a new `pathId` | 1 |
| present, `DEACTIVATED` | reported; a deactivated id is never resurrected | 1 |

Run it after deploying a release that adds an entry. For RELOCATE, run it
before slotting-optimization approves the first plan in that environment.
Until then, wes-work-planning rejects relocation work as an unknown path id.
Check what the consumers will replay with
`curl -s localhost:8080/process-paths/RELOCATE`. Every entry also appears on
the topic as a `ProcessPathCreated` keyed by its `pathId`.
