# The analytics path: this service's own AnalyticsConsumer projects the events
# it published into the "Process Path Catalogue Growth & Change" report, served
# over REST by pathmgmt-reports. The Kafka messages are not REST-reachable, so
# each scenario drives REST changes, encodes the recorded events as their
# analytics-topic messages and hands them to the real consumer's HandleMessage
# (the entry point the Kafka loop uses); the read model is asserted only through
# the reports REST router. Derived from the project documentation:
#   - docs/docs/operations/runbook.md, "Reports API (pathmgmt-reports)": counters
#     per UTC `dayBucket` of ProcessPathCreated/Updated/Deactivated events by
#     CloudEvents `time`; `from` inclusive, `to` exclusive; "Days with no events
#     have no row"; "`rows` is [], never null, when nothing matches"; 400
#     invalid-report-query for a missing/non-RFC 3339 from/to or a granularity
#     other than day; freshness "0 when the read model is empty".
#   - docs/docs/adr/0007-analytical-data-product.md: the report counts defined,
#     revised and deactivated paths per DAY.
#   - AGENTS.md / .claude/rules/cloudevents-envelope.md: consumers dispatch on
#     the FULL type, ignore unknown types, dedupe on `id`, and reject (never
#     parse) anything that fails CloudEvents validation.
Feature: Catalogue growth report from the analytics topic
  As an operator reading the catalogue growth report
  I want each catalogue change counted exactly once per day
  So that the report reflects what was actually defined, revised and retired

  Background:
    Given the Process Path Management service is running
    And the analytics projector is running

  @bdd
  Scenario: The report counts defined, revised and deactivated paths for a day and ignores CPT schedule changes
    Given a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    And a process path "PACK" is already defined with matchPrefix "pack" and capabilities "pack"
    And the CPT schedule for site "sp1" is already defined with timezone "America/Sao_Paulo" and cutoff "sp1-1500" eligible for paths "PICK"
    And the process path "PICK" is revised with matchPrefix "pick-zone-a" and capabilities "pick"
    And the process path "PACK" is already deactivated
    When the published events are delivered to the analytics projector
    And the catalogue growth report is requested from "2026-09-06T00:00:00Z" to "2026-09-07T00:00:00Z"
    Then the request is accepted with status 200
    And the report has 1 row
    And the report row for day "2026-09-06T00:00:00Z" counts 2 defined, 1 revised, and 1 deactivated

  @bdd
  Scenario: Events land in the UTC day they occurred and the window is from-inclusive and to-exclusive
    Given a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    And the clock reads "2026-09-08T23:59:00Z"
    And a process path "PACK" is already defined with matchPrefix "pack" and capabilities "pack"
    When the published events are delivered to the analytics projector
    And the catalogue growth report is requested from "2026-09-06T00:00:00Z" to "2026-09-09T00:00:00Z"
    Then the report has 2 rows
    And the report row for day "2026-09-06T00:00:00Z" counts 1 defined, 0 revised, and 0 deactivated
    And the report row for day "2026-09-08T00:00:00Z" counts 1 defined, 0 revised, and 0 deactivated
    When the catalogue growth report is requested from "2026-09-07T00:00:00Z" to "2026-09-08T00:00:00Z"
    Then the report has 0 rows
    When the catalogue growth report is requested from "2026-09-08T00:00:00Z" to "2026-09-09T00:00:00Z"
    Then the report has 1 row
    And the report row for day "2026-09-08T00:00:00Z" counts 1 defined, 0 revised, and 0 deactivated

  @bdd
  Scenario: A redelivered analytics message is counted once
    Given a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    When the published events are delivered to the analytics projector
    And the analytics topic redelivers every message
    And the catalogue growth report is requested from "2026-09-06T00:00:00Z" to "2026-09-07T00:00:00Z"
    Then the report row for day "2026-09-06T00:00:00Z" counts 1 defined, 0 revised, and 0 deactivated

  @bdd
  Scenario: A message in the retired flat envelope is rejected and changes nothing
    Given a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    And the published events are delivered to the analytics projector
    When the analytics topic delivers a message in the retired flat envelope
    Then the projector rejected the message as not a CloudEvent
    When the catalogue growth report is requested from "2026-09-06T00:00:00Z" to "2026-09-07T00:00:00Z"
    Then the report has 1 row
    And the report row for day "2026-09-06T00:00:00Z" counts 1 defined, 0 revised, and 0 deactivated

  @bdd
  Scenario: An empty read model reports no rows and no freshness lag
    When the catalogue growth report is requested from "2026-09-01T00:00:00Z" to "2026-10-01T00:00:00Z"
    Then the request is accepted with status 200
    And the report has 0 rows
    When the catalogue growth freshness is requested
    Then the request is accepted with status 200
    And the freshness lag is 0 seconds

  @bdd
  Scenario: Freshness measures the time since the newest projected change
    Given a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    And the analytics store clock reads "2026-09-06T00:05:00Z"
    When the published events are delivered to the analytics projector
    And the catalogue growth freshness is requested
    Then the request is accepted with status 200
    And the freshness lag is 300 seconds
