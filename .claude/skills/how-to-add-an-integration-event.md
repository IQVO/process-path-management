# How to add an integration event (publish and consume)

Use when asked to publish a new cross-context integration event from this
service, or (see the callout below) to add an inbound Kafka consumer —
which this repo does NOT have today and should think hard before adding.
This fleet's Kafka is ONE broker platform-wide — every design decision
below exists because that shared-broker reality has already caused a
real incident once (wes-work-planning#67, a hardcoded consumer group
colliding with a live in-cluster Deployment).

## 0. This service has zero inbound dependency — read AGENTS.md before adding a consumer

Unlike most of the fleet, process-path-management is a pure Open Host
Service / Published Language SOURCE for
`warehouse.process-path-management.events`, with `fulfillment-execution`,
`wes-work-planning` (Core) and `workforce-management` (Supporting) as its
Conformist consumers. Per this repo's own `AGENTS.md`: "It has **zero
inbound dependency** — no inbound Kafka consumer, no synchronous REST
dependency in either direction. It never calls into any other service,
synchronously or otherwise; every change propagates exclusively via
Kafka." (The one Kafka consumer this repo does have,
`internal/adapters/inbound/kafka/analytics_consumer.go`, reads only this
service's OWN analytics topic for `cmd/pathmgmt-projector`, ADR 0007 — it
is not a sibling-context subscription.) This is enforced in code, not just prose:
`internal/architecture/fitness_test.go`'s `TestNoSiblingContextOutboundCalls`
fails the build if `internal/adapters/outbound/**` ever imports
`"net/http"` as an HTTP client — there is no legitimate outbound
synchronous call this service should ever make to another bounded
context, and no legitimate reason to subscribe to a sibling's topic
either. If you are asked to make this service consume
something from another context, stop and confirm that's really the intent — it would reverse
this service's whole strategic position as the fleet's Published
Language source (ADR 0001).

## Publishing a new integration event

### 1. Is it actually cross-service?

This service is simpler than most of the fleet here: everything it
raises IS meant to reach the wire — there's no separate "local-only,
Postgres-outbox-for-audit-not-broadcast" concern to filter out (compare
inventory-storage, which forwards only a subset of its domain events).
All four of this service's event types
(`ProcessPathCreated`/`Updated`/`Deactivated`, `CPTScheduleChanged`) go on
`warehouse.process-path-management.events` — see
`internal/adapters/outbound/kafka/publisher.go`'s doc comment: "This is
the ONLY way fulfillment-execution, wes-work-planning, and
workforce-management learn about a process-path change." Still confirm a
sibling context genuinely needs your new event before wiring it — check
`docs/docs/ecosystem/context-map.md` for who's actually downstream
(fulfillment-execution, wes-work-planning and workforce-management
consume `ProcessPath*`; order-management also consumes `cycle_time_p95`,
`eligibility` and `CPTScheduleChanged`) — and verify by grepping the
consumer's decoder in the sibling repo on `origin/develop`, since a field
on the wire is not the same as a field someone reads.

### 2. Envelope: CloudEvents 1.0, mandatory (ADR 0016)

Every message on BOTH topics is a CloudEvents 1.0 event in structured
content mode, built ONLY via `internal/adapters/kafka/cloudevents`
(`New`/`Decode`/`ContentTypeHeader`, wrapping
`github.com/cloudevents/sdk-go/v2/event`). There is no flat envelope, no
dual mode, no envelope toggle — never hand-roll an envelope struct.

```json
{
  "specversion": "1.0",
  "id": "uuid-v4 (minted once, persisted with the outbox row)",
  "source": "/warehouse/process-path-management",
  "type": "com.warehouse.wes.process-path-management.processpath.ProcessPathCreated",
  "subject": "PICK",
  "time": "2026-09-06T00:00:00Z",
  "datacontenttype": "application/json",
  "dataschema": "urn:warehouse:process-path-management:events:ProcessPathCreated:v1",
  "data": { }
}
```

Kafka header: `content-type: application/cloudevents+json; charset=UTF-8`.
`type` = `com.warehouse.wes.process-path-management.<entity>.<EventName>`;
entities today are `processpath` and `cptschedule`. Add a new event's
full type string as a `Type*` constant in
`internal/adapters/kafka/cloudevents/types.go` (and its assertion in
`cloudevents_test.go`'s `TestPublishedTypeCatalogue`). A breaking payload
change is a NEW `.v2` type plus `dataschema` `:v2`, never a mutation.

### 3. Implementation: `mapEvent`, not `Publish`, is where the wire shape lives

Add the event struct to `internal/domain/processpath/` or
`internal/domain/cptschedule/` (it should already exist as a domain
event the aggregate raises — publishing wires an EXISTING domain event
onto Kafka, it doesn't invent a new payload shape at the adapter layer;
see `shared.ProcessPathCreated`/`ProcessPathUpdated`/`ProcessPathDeactivated`
and `cptschedule.CPTScheduleChanged`).

Both the integration `Encode` and the `AnalyticsEncoder` go through the
ONE `encodeFor` → `mapEvent` switch in
`internal/adapters/outbound/kafka/publisher.go`, so the two streams can
never disagree on type/subject/time/payload (only topic and `dataschema`
differ). Add your case there:

```go
case shared.YourNewEvent:
    key := string(e.PathId) // Kafka key AND CloudEvents subject
    return mapped{cloudevents.EntityProcessPath, "YourNewEvent", key, YourNewEventData{ /* ... */ }}, nil
```

Never add logic to `Publish`/`Send` themselves — they only write what
`Encode` already produced (and attach the content-type header). If the
analytics projector should react to it, add the full `type` to
`isProjectingEventType`/`applyProjection` in
`internal/adapters/inbound/kafka/analytics_consumer.go` — it dispatches on
the FULL type and ignores unknown types.

### 4. Contract + docs

- Add the message to `apis/asyncapi.yaml` under BOTH channels (with its
  exact `type` const and `dataschema` via a `<Event>CloudEvent` /
  `<Event>AnalyticsCloudEvent` schema `allOf` the shared
  `CloudEventEnvelope`),
  matching the entity-grouping convention already there (group by
  aggregate — `ProcessPathCreated`/`Updated`/`Deactivated` keyed by
  `path_id`, `CPTScheduleChanged` keyed by `site_id` — not chronologically).
- Run `spectral lint apis/asyncapi.yaml --ruleset .spectral.asyncapi.yaml
  --fail-severity=warn` (this repo's `api-lint` CI job runs this on every
  PR).
- This repo's asyncapi doc is publisher-side only (no `docs-api-drift`-
  style generated-file regen step is currently wired for asyncapi the way
  it is for the OpenAPI REST reference) — double check
  `docs/package.json` for a `gen-async-docs` script before assuming one
  exists; if this repo gains one later, re-run it here too.

### 5. Test

Add the event to `golden_test.go`'s table — it asserts the EXACT
CloudEvents JSON (every attribute, the full `type`, `dataschema`) on both
the integration and analytics streams plus the `content-type` header.
Other marshal-shape tests go against `kafka.Encode` directly (see
`publisher_test.go`/`analytics_publisher_test.go`/
`cpt_schedule_publisher_test.go` — never a real broker in a unit test).
If you also add a `-tags=integration` test asserting real delivery, this
repo's `internal/architecture/fitness_test.go` has a
`TestKafkaIntegrationTestsUseTestcontainers` fitness test that
STATICALLY fails the build on a skip-gated `KAFKA_BROKERS` test or a
hardcoded `localhost:9092` — it greps every `*_integration_test.go` file
for those patterns and for the `testcontainers-go/modules/kafka` import.
This repo's own outbox integration tests
(`internal/adapters/outbound/postgres/outbox_integration_test.go`) are
Postgres-testcontainers-based rather than Kafka-testcontainers-based
(the relay's `Sink` is faked, not a real broker) — if you need a REAL
Kafka round-trip test, follow inventory-storage's
`consumer_integration_test.go` recipe (`tckafka.Run`, unique topic per
test, poll `ReadPartitions` before the first read/write) rather than
copying this repo's Postgres-only integration pattern.

## Consuming an integration event from a sibling context (not currently done in this repo — read section 0 first)

This repo has no example of this today. If a future ADR reverses the
zero-inbound-dependency rule, the two consumer-group patterns and the
readiness-gate discipline documented in the fleet's `how-to-test.md`
guide and the `warehouse-systems-fleet-ops` skill's "Event-sourced
local-cache consumers" note both apply verbatim — in particular: a
consumer that replays a topic's full history from `FirstOffset` on every
process start MUST use a per-process-unique consumer group
(hostname+PID+timestamp), never a fixed shared string, or a locally-run
harness process can silently starve the live in-cluster Deployment of
its partition (the exact wes-work-planning#67 incident). Any such consumer
must also follow ADR 0016: `cloudevents.Decode`, dispatch on the full
`type` string from the fleet catalogue, dedupe on `id`, DLQ/skip anything
that fails validation. This repo's own
`internal/architecture/fitness_test.go` already carries
`TestKafkaConsumerGroupNeverHardcodedInline`, which statically bans a
`kafkago.ReaderConfig`'s `GroupID:` field from being assigned a bare
string literal anywhere in this codebase — that test would already catch
the mistake if a consumer package were ever added here, but the far
cheaper fix is to design the group-id story correctly the first time
using this repo's own `AnalyticsConsumerGroup`-style named-constant
pattern (single long-lived instance) or a
`uniqueConsumerGroup()`-style generated id (per-process-unique,
event-sourced full-replay cache) from the start, per which case applies.

## Verify before opening the PR

```bash
make check-all    # includes arch-test — will catch a reintroduced outbound HTTP client or a literal GroupID
spectral lint apis/asyncapi.yaml --ruleset .spectral.asyncapi.yaml --fail-severity=warn
```
