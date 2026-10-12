# Revision boundaries of PUT /process-paths/{pathId} that no earlier scenario
# pins, each scenario derived from the project documentation:
#   - apis/openapi.yaml, revisePath: "A no-op revision (identical matchPrefix,
#     requiredCapabilities, cycleTimeP95, and eligibility) is a successful 200
#     that does NOT republish ProcessPathUpdated" -- so a change to cycleTimeP95
#     or eligibility alone IS a change; 422 when "the path is deactivated, or
#     matchPrefix/requiredCapabilities/cycleTimeP95 is invalid"; the ProcessPath
#     schema's createdAt/updatedAt.
#   - .claude/rules/domain-model.md, "Aggregates & invariants": "ProcessPath
#     (all writes): cycleTimeP95 > 0 ...  A revision is a no-op only if
#     matchPrefix, requiredCapabilities, cycleTimeP95 and eligibility are all
#     unchanged"; ErrPathDeactivated "blocks any mutation on a deactivated
#     path".
#   - apis/asyncapi.yaml, ProcessPathUpdated: "Raised when RevisePath actually
#     changes something" with the full revised definition.
Feature: Revision boundaries and what a revision publishes
  As a downstream consumer of the process path management topic
  I want a rejected revision to change nothing and every accepted one to say what changed
  So that my read model never diverges from the catalogue

  Background:
    Given the Process Path Management service is running

  @bdd
  Scenario Outline: A rejected revision leaves the path unchanged and publishes nothing
    Given a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    When the process path "PICK" is revised with matchPrefix "<matchPrefix>", capabilities "pick", and cycleTimeP95 "<cycleTimeP95>"
    Then the request is rejected with status 422
    And the response problem reports type "<problem>"
    And the process path "PICK" still has matchPrefix "pick" and cycleTimeP95 "2h"
    And no "ProcessPathUpdated" event was published for path "PICK"

    Examples:
      | matchPrefix | cycleTimeP95 | problem                    |
      | Pick-Zone   | 3h           | match-prefix-not-lowercase |
      | pick-zone   | 0s           | invalid-cycle-time-p95     |

  @bdd
  Scenario: Revising only the cycleTimeP95 is a real change that publishes the new value
    Given a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    And the clock reads "2026-09-07T12:00:00Z"
    When the process path "PICK" is revised with matchPrefix "pick", capabilities "pick", and cycleTimeP95 "3h"
    Then the request is accepted with status 200
    And the response field "cycleTimeP95" equals the duration "3h"
    And the response field "createdAt" is the timestamp "2026-09-06T00:00:00Z"
    And the response field "updatedAt" is the timestamp "2026-09-07T12:00:00Z"
    And the "ProcessPathUpdated" event for "PICK" carries matchPrefix "pick", capabilities "pick", and cycleTimeP95 "3h"
    And the "ProcessPathUpdated" event for "PICK" occurred at "2026-09-07T12:00:00Z"

  @bdd
  Scenario: Revising only the eligibility is a real change that publishes the new rules
    Given a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    When the client sends PUT "/process-paths/PICK" with body:
      """
      {"matchPrefix":"pick","requiredCapabilities":["pick"],"cycleTimeP95":"2h","eligibility":{"maxUnitsPerLine":1}}
      """
    Then the request is accepted with status 200
    And the response field "eligibility.maxUnitsPerLine" is "1"
    And the "ProcessPathUpdated" event for "PICK" carries eligibility maxUnitsPerLine 1

  @bdd
  Scenario: Revising a deactivated path reports path-deactivated and publishes nothing
    Given a process path "PACK" is already defined with matchPrefix "pack" and capabilities "pack"
    And the process path "PACK" is already deactivated
    When the process path "PACK" is revised with matchPrefix "pack-zone-b", capabilities "pack", and cycleTimeP95 "2h"
    Then the request is rejected with status 422
    And the response problem reports type "path-deactivated"
    And the process path "PACK" now has status "DEACTIVATED"
    And no "ProcessPathUpdated" event was published for path "PACK"
