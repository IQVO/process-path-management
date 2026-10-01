# Domain model — ubiquitous language, aggregates, invariants, domain events

## Ubiquitous Language (use these exact names)

- **ProcessPath** — the aggregate root. `PathId` (identity, immutable once
  constructed), `MatchPrefix` and `RequiredCapabilities` (revisable while
  Active via `Revise`), `Direct` (immutable — a structural fact about
  routing shape, reserved for a future multi-hop topology, never an
  operator-tunable parameter), `DestinationLocationRole` (optional,
  immutable, ADR 0009), `CycleTimeP95` and `Eligibility` (revisable, ADR
  0010), `Status` (Active/Deactivated). The original fields were a
  like-for-like migration of the retired YAML file's schema; the later
  ones were added by ADRs 0009 and 0010.
- **PathId** — the canonical identity of a process path (e.g. `"PICK"`,
  `"PACK"`, `"REBIN"`, `"SLAM"`). The SAME identity `fulfillment-execution`'s
  `task.Type`, `wes-work-planning`'s `WorkPool.PathId`, and
  `workforce-management`'s `PathPlan.PathId` all reference — this service
  is the one place that identity is DEFINED, not just consumed. Kept as a
  plain string (not an enum) because the whole point of this service is
  that the valid set is operator-configurable, not compiled in.
- **Capability** — a named qualification a station/associate must hold to
  work a process path (e.g. `"pick"`, `"pack"`, `"hazmat"`) — the exact
  same vocabulary `workforce-management`'s `Certification` and
  `fulfillment-execution`'s `Station.Capability` already use. This service
  does not invent a new capability vocabulary; it is the authoritative
  SOURCE for which capabilities a given path requires.
- **MatchPrefix** — the lower-case prefix downstream consumers match a
  caller-supplied id against: `id == matchPrefix` OR `id` starts with
  `matchPrefix + "-"` — **never** a bare substring match without the
  separator (a hypothetical `"picking-station"` must not match `"pick"`).
  Validated lower-case at construction time (`ErrMatchPrefixNotLowercase`)
  — never silently lower-cased for the caller, so persisted data is
  exactly what was validated. This is the fleet's documented pitfall on
  exact-match-vs-prefix-match design: an exact-match default passes
  synthetic fixtures (`"PICK"` in, `"PICK"` out) while rejecting every
  real caller-supplied value (`"pick-zone-a"`) in production.
- **Direct** — a structural fact about the path's routing shape (reserved
  for a future multi-hop topology). Immutable once set at `Define` time —
  never revisable via `Revise`.
- **DestinationLocationRole** — optional declared facility-layout
  `LocationRole` for a path's completed work: `Drop`, `WorkCenter`,
  `Shipping`, or unset. Declarative intent only — never validated live
  against facility-layout (`shared.ErrInvalidDestinationLocationRole`
  for an unknown value).
- **CycleTimeP95** — operator-declared p95 release-to-manifest cycle time
  (ADR 0010), a declared standard, not a measurement. Required, > 0
  (`ErrInvalidCycleTime`); a Go duration string on the wire.
- **Eligibility** — value object: `maxUnitsPerLine` (nil = unbounded),
  `requiredProductAttributes`, `excludedProductAttributes`, `nonSortable`.
  Zero value is fully permissive; no invariants.
- **CPTSchedule** — the second aggregate root, one per `SiteId`: an IANA
  `timezone` plus recurring **Cutoffs** (`cptId`, `localTime` HH:MM,
  `daysOfWeek` Mon..Sun, `shipMethod`, `eligiblePathIds`). Revised
  wholesale (ADR 0010).
- **Status (ACTIVE / DEACTIVATED)** — the activation lifecycle. There is
  no "draft" state — a path is live the instant it is defined; this
  service has no review workflow. Deactivation is one-way and idempotent
  (`Deactivate` on an already-deactivated path is a no-op success, not an
  error) — matches the fleet's "duplicate/redelivered command is a no-op"
  convention (see `fulfillment-execution`'s `WorkPool.Complete`).
  `ErrPathDeactivated` blocks any mutation on a deactivated path: it is a
  closed historical record, the same posture `labor-performance`'s
  `LaborStandard.Close` takes.

## Aggregates & invariants (enforce in domain, unit-tested)

- **ProcessPath.Define**: `matchPrefix` must be non-empty
  (`ErrEmptyMatchPrefix`) and lower-case (`ErrMatchPrefixNotLowercase`);
  `requiredCapabilities` must be non-empty (`ErrNoRequiredCapabilities`). A
  malformed path definition must never be constructible — no "fix it
  later" state.
- **ProcessPath.Revise**: only permitted while Active
  (`ErrPathDeactivated` otherwise). Returns `changed bool` — a no-op
  revision (identical `matchPrefix` and `requiredCapabilities`) returns
  `false` and raises nothing, so consumers never have to diff two
  identical payloads to notice nothing changed. `pathId` and `direct` are
  never revisable.
- **ProcessPath.Deactivate**: idempotent — deactivating an
  already-deactivated path is a no-op success and does NOT republish
  `ProcessPathDeactivated`.
- **ProcessPath** (all writes): `cycleTimeP95` > 0
  (`ErrInvalidCycleTime`); `destinationLocationRole` empty or one of
  Drop/WorkCenter/Shipping. A revision is a no-op only if `matchPrefix`,
  `requiredCapabilities`, `cycleTimeP95` and `eligibility` are all
  unchanged.
- **CPTSchedule.Define/Revise**: valid IANA timezone, ≥1 cutoff, unique
  `cptId`s; each cutoff validated by `NewCutoff` (non-empty id, HH:MM
  time, non-empty valid days, non-empty ship method, non-empty
  eligiblePathIds). `Revise` returns `changed bool`. The cross-aggregate
  rule — every eligiblePathId references an Active path
  (`ErrIneligiblePathId`) — is enforced in the `DefineCPTSchedule` use
  case, not the domain.
- **A path's identity is permanent once created.** `DefinePath` rejects
  (409) re-defining an id that already exists, active or deactivated — a
  caller wanting to re-use an id after deactivation must be told
  explicitly, never silently overwritten.

## Domain events (past tense, on `warehouse.process-path-management.events`)

`ProcessPathCreated`, `ProcessPathUpdated`, `ProcessPathDeactivated` —
defined in `internal/domain/shared/events.go` — and `CPTScheduleChanged`
(`internal/domain/cptschedule/events.go`, a full schedule snapshot). One
shared topic (not one per event type), matching the fleet's convention;
path events are keyed by `path_id` so a consumer replaying the topic sees
one path's events in publish order; `CPTScheduleChanged` is keyed by
`site_id`. `ProcessPathDeactivated`'s payload
carries only `path_id` — every definition field is omitted (not
empty-arrayed) since a deactivation carries no definition data.

Every event is also enqueued onto `warehouse.process-path-management.analytics`
(ADR 0007) in the same outbox transaction; that topic is consumed only by
this service's own `pathmgmt-projector`.

On the wire every event is a CloudEvents 1.0 event (ADR 0016) with
`type` `com.warehouse.wes.process-path-management.<processpath|cptschedule>.<EventName>`
and `subject` = `path_id` / `site_id` (the Kafka key). These four full type
strings are consumed byte-for-byte by four sibling services — see
`internal/adapters/kafka/cloudevents/types.go`.
