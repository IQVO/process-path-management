# Runtime, failure-mode and analytics rules taken from the context documentation
# that no earlier scenario pins (warehouse-docs docs/contexts/process-path-management):
#   - troubleshooting.md, "Boot and readiness": "/readyz returns 503
#     {"status":"not_ready"} when the process received SIGTERM and is draining
#     (this is the only thing that flips it)"; "/readyz answers 503
#     dependency-unavailable when Postgres or every Kafka broker is unreachable
#     (verdict cached 3 s); the problem detail names the dependency". Liveness
#     (/healthz) is never flipped.
#   - troubleshooting.md, "Writes and the API": 409 concurrent-modification ("the
#     row's version changed between load and save"), 409 path-already-exists
#     ("also a concurrent create that lost the insert race"), 500 internal-error
#     ("any unmapped error: database down"), 500 report-store-error, 400
#     invalid-report-query.
#   - aggregate-design-canvas.md, corrective policies: "409
#     concurrent-modification: the client reloads and retries"; "no lost update
#     between load and save ... used by revise and deactivate, not by create".
#   - domain-events.md, "Consumed events": the projector dispatches on the FULL
#     type, "CPTScheduleChanged and any unknown type committed and skipped",
#     "Dedupe is on the CloudEvents id", the day bucket is the UTC day.
#   - eventstorming.md, Process 3: "whenever a path event arrives, dedupe on the
#     CloudEvents id and bump the day bucket".
Feature: Runtime failure modes and the analytics projection
  As an operator running the service and a reader of the catalogue growth report
  I want the documented readiness, conflict and outage behaviours and projection rules to hold
  So that an outage or a redelivery never changes what the catalogue or its report say

  Background:
    Given the Process Path Management service is running

  # --- readiness (SIGTERM drain, dependency checks) ---------------------------------

  @bdd
  Scenario: A service whose dependencies are reachable reports ready
    Given a service whose Postgres and Kafka dependencies are reachable
    When the readiness probe is queried
    Then the request is accepted with status 200
    And the response field "status" is "ready"

  @bdd
  Scenario: SIGTERM flips readiness to not_ready while liveness stays 200
    Given a service whose Postgres and Kafka dependencies are reachable
    When the service receives SIGTERM
    And the readiness probe is queried
    Then the request is rejected with status 503
    And the response field "status" is "not_ready"
    When the liveness probe is queried
    Then the request is accepted with status 200
    And the response field "status" is "ok"

  @bdd
  Scenario Outline: An unreachable dependency is named in a dependency-unavailable problem
    Given a service whose Postgres and Kafka dependencies are reachable
    And the dependencies "<down>" are unreachable
    When the readiness probe is queried
    Then the request is rejected with status 503
    And the response problem reports type "dependency-unavailable"
    And the response problem detail is "<detail>"
    When the liveness probe is queried
    Then the request is accepted with status 200

    Examples:
      | down           | detail                  |
      | postgres       | not ready: postgres     |
      | kafka          | not ready: kafka        |
      | postgres,kafka | not ready: postgres, kafka |

  @bdd
  Scenario: A dependency verdict is reused for a few seconds, then the outage is re-checked
    Given a service whose Postgres and Kafka dependencies are reachable
    And the dependencies "kafka" are unreachable
    When the readiness probe is queried
    Then the request is rejected with status 503
    When the "kafka" dependency is reachable again
    And the readiness probe is queried
    Then the request is rejected with status 503
    When the readiness verdict has expired
    And the readiness probe is queried
    Then the request is accepted with status 200
    And the response field "status" is "ready"

  # --- a database outage and the two kinds of 409 ----------------------------------------

  @bdd
  Scenario Outline: A database outage is a 500 internal-error problem on every read and write route
    Given a service whose stores can be made to fail
    And the database becomes unreachable
    When the client sends <method> "<path>"
    Then the request is rejected with status 500
    And the response problem reports type "internal-error"
    And the response content type is "application/problem+json"
    And no events were published

    Examples:
      | method | path                     |
      | GET    | /process-paths/PICK      |
      | GET    | /process-paths           |
      | DELETE | /process-paths/PICK      |
      | GET    | /sites/sp1/cpt-schedule  |

  @bdd
  Scenario: A define during a database outage publishes and stores nothing, and the same define succeeds once it is back
    Given a service whose stores can be made to fail
    And the database becomes unreachable
    When a process path "PICK" is defined with matchPrefix "pick" and capabilities "pick"
    Then the request is rejected with status 500
    And the response problem reports type "internal-error"
    And no events were published
    When the database is reachable again
    And a process path "PICK" is defined with matchPrefix "pick" and capabilities "pick"
    Then the request is accepted with status 201
    And "ProcessPathCreated" was published 1 time for "PICK"

  @bdd
  Scenario: A revision that loses the version-guarded save is a 409 concurrent-modification, and a retry takes effect
    Given a service whose stores can be made to fail
    And a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    And a concurrent writer commits first, so the next save of a process path is refused
    When the process path "PICK" is revised with matchPrefix "pick-zone-a" and capabilities "pick"
    Then the request is rejected with status 409
    And the response problem reports type "concurrent-modification"
    And "ProcessPathUpdated" was published 0 times for "PICK"
    And the process path "PICK" still has matchPrefix "pick" and cycleTimeP95 "2h"
    When the process path "PICK" is revised with matchPrefix "pick-zone-a" and capabilities "pick"
    Then the request is accepted with status 200
    And "ProcessPathUpdated" was published 1 time for "PICK"
    And the process path "PICK" still has matchPrefix "pick-zone-a" and cycleTimeP95 "2h"

  @bdd
  Scenario: A deactivation that loses the version-guarded save leaves the path active and publishes nothing until the retry
    Given a service whose stores can be made to fail
    And a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    And a concurrent writer commits first, so the next save of a process path is refused
    When the process path "PICK" is deactivated
    Then the deactivation is rejected with status 409
    And the response problem reports type "concurrent-modification"
    And no "ProcessPathDeactivated" event was published for path "PICK"
    And the process path "PICK" now has status "ACTIVE"
    When the process path "PICK" is deactivated
    Then the deactivation is accepted with status 204
    And exactly one "ProcessPathDeactivated" event was published for path "PICK"
    And the process path "PICK" now has status "DEACTIVATED"

  @bdd
  Scenario: A schedule that loses the version-guarded save is stored nowhere and published nowhere until the retry
    Given a service whose stores can be made to fail
    And a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    And a concurrent writer commits first, so the next save of a CPT schedule is refused
    When the CPT schedule for site "sp1" is defined with timezone "America/Sao_Paulo" and cutoff "sp1-1500" eligible for paths "PICK"
    Then the request is rejected with status 409
    And the response problem reports type "concurrent-modification"
    And "CPTScheduleChanged" was published 0 times for "sp1"
    When the CPT schedule for site "sp1" is requested
    Then the request is rejected with status 404
    When the CPT schedule for site "sp1" is defined with timezone "America/Sao_Paulo" and cutoff "sp1-1500" eligible for paths "PICK"
    Then the request is accepted with status 200
    And "CPTScheduleChanged" was published 1 time for "sp1"

  @bdd
  Scenario: A define that loses the insert race is a 409 path-already-exists, keeps the winner's row and publishes nothing
    Given a service whose stores can be made to fail
    And another operator's define of "PICK" with matchPrefix "winner" commits between the duplicate check and the insert
    When a process path "PICK" is defined with matchPrefix "loser" and capabilities "pick"
    Then the request is rejected with status 409
    And the response problem reports type "path-already-exists"
    And "ProcessPathCreated" was published 0 times for "PICK"
    And the process path "PICK" still has matchPrefix "winner" and cycleTimeP95 "2h"

  # --- the catalogue growth report: its query and its store ------------------------------------

  @bdd
  Scenario Outline: A missing or malformed report query is a 400 invalid-report-query
    Given the analytics projector is running
    When the catalogue growth report is requested with the query "<query>"
    Then the request is rejected with status 400
    And the response problem reports type "invalid-report-query"

    Examples:
      | query                                                           |
      | to=2026-09-07T00:00:00Z                                         |
      | from=2026-09-06T00:00:00Z                                       |
      | from=2026-09-06&to=2026-09-07T00:00:00Z                         |
      | from=2026-09-06T00:00:00Z&to=yesterday                          |
      | from=2026-09-06T00:00:00Z&to=2026-09-07T00:00:00Z&granularity=week |
      | from=2026-09-06T00:00:00Z&to=2026-09-07T00:00:00Z&granularity=DAY  |

  @bdd
  Scenario: An explicit granularity of day is the same report as the default
    Given a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    And the analytics projector is running
    When the published events are delivered to the analytics projector
    And the catalogue growth report is requested with the query "from=2026-09-06T00:00:00Z&to=2026-09-07T00:00:00Z&granularity=day"
    Then the request is accepted with status 200
    And the report has 1 row
    And the report row for day "2026-09-06T00:00:00Z" counts 1 defined, 0 revised, and 0 deactivated

  @bdd
  Scenario: An unreachable analytical database is a 500 report-store-error, not an empty report
    Given the analytics projector is running
    And the report store is unavailable
    When the catalogue growth report is requested from "2026-09-06T00:00:00Z" to "2026-09-07T00:00:00Z"
    Then the request is rejected with status 500
    And the response problem reports type "report-store-error"
    And the response content type is "application/problem+json"
    When the catalogue growth freshness is requested
    Then the request is rejected with status 500
    And the response problem reports type "report-store-error"

  # --- the projector: full-type dispatch, id dedupe, UTC days ------------------------------------

  @bdd
  Scenario Outline: A message whose full type is not one of the three projecting types is skipped and counts nothing
    Given the analytics projector is running
    When the analytics topic delivers a CloudEvent of type "<type>" with id "evt-1" for "PICK" at "2026-09-06T10:00:00Z"
    Then the projector accepted the message without error
    When the catalogue growth report is requested from "2026-09-06T00:00:00Z" to "2026-09-07T00:00:00Z"
    Then the report has 0 rows
    When the catalogue growth freshness is requested
    Then the freshness lag is 0 seconds

    Examples:
      | type                                                                       |
      | com.warehouse.wes.process-path-management.processpath.ProcessPathArchived  |
      | ProcessPathCreated                                                         |
      | com.warehouse.wes.order-management.order.ProcessPathCreated                |
      | com.warehouse.wes.process-path-management.cptschedule.CPTScheduleChanged   |

  @bdd
  Scenario: Dedupe is on the CloudEvents id, so two different ids with identical content count twice and a repeated id once
    Given the analytics projector is running
    When the analytics topic delivers a CloudEvent of type "com.warehouse.wes.process-path-management.processpath.ProcessPathUpdated" with id "evt-a" for "PICK" at "2026-09-06T10:00:00Z"
    And the analytics topic delivers a CloudEvent of type "com.warehouse.wes.process-path-management.processpath.ProcessPathUpdated" with id "evt-b" for "PICK" at "2026-09-06T10:00:00Z"
    And the analytics topic redelivers the CloudEvent with id "evt-a"
    And the catalogue growth report is requested from "2026-09-06T00:00:00Z" to "2026-09-07T00:00:00Z"
    Then the report has 1 row
    And the report row for day "2026-09-06T00:00:00Z" counts 0 defined, 2 revised, and 0 deactivated

  @bdd
  Scenario Outline: The day bucket is the UTC day of the event time, however its offset is written
    Given the analytics projector is running
    When the analytics topic delivers a CloudEvent of type "com.warehouse.wes.process-path-management.processpath.ProcessPathCreated" with id "evt-1" for "PICK" at "<time>"
    And the catalogue growth report is requested from "2026-09-05T00:00:00Z" to "2026-09-10T00:00:00Z"
    Then the report has 1 row
    And the report row for day "<day>" counts 1 defined, 0 revised, and 0 deactivated

    Examples:
      | time                      | day                  |
      | 2026-09-06T23:30:00-03:00 | 2026-09-07T00:00:00Z |
      | 2026-09-07T00:30:00+02:00 | 2026-09-06T00:00:00Z |
      | 2026-09-06T23:59:59Z      | 2026-09-06T00:00:00Z |

  @bdd
  Scenario: A CPT schedule change moves neither the counters nor the freshness of the report
    Given a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    And the clock reads "2026-09-06T01:00:00Z"
    And the CPT schedule for site "sp1" is already defined with timezone "America/Sao_Paulo" and cutoff "sp1-1500" eligible for paths "PICK"
    And the analytics projector is running
    And the analytics store clock reads "2026-09-06T02:00:00Z"
    When the published events are delivered to the analytics projector
    And the catalogue growth freshness is requested
    Then the freshness lag is 7200 seconds
    When the catalogue growth report is requested from "2026-09-06T00:00:00Z" to "2026-09-07T00:00:00Z"
    Then the report row for day "2026-09-06T00:00:00Z" counts 1 defined, 0 revised, and 0 deactivated
