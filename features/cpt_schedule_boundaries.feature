# CPT schedule behaviours that no earlier scenario pins, each scenario derived
# from the project documentation:
#   - apis/openapi.yaml, defineCPTSchedule: "every PUT replaces the schedule's
#     timezone and cutoffs in full (ADR 0010)"; "A no-op revision (identical
#     timezone and cutoffs) is a successful 200 that does NOT republish
#     CPTScheduleChanged"; 422 for "timezone is empty ... cutoffs is empty ... a
#     localTime is empty/malformed, daysOfWeek is empty/invalid, shipMethod is
#     empty, eligiblePathIds is empty, or an eligiblePathIds entry does not
#     reference an Active process path"; Cutoff.localTime pattern
#     ^([01][0-9]|2[0-3]):[0-5][0-9]$ and the 404 for an empty siteId segment.
#   - docs/docs/adr/0010-fulfillment-capability-contract.md, "Revising a
#     schedule raises a new event, CPTScheduleChanged, carrying the full
#     schedule".
#   - .claude/rules/domain-model.md: "A path named by a CPT schedule cannot be
#     deactivated: refused with 409 path-referenced-by-cpt-schedule (ADR 0026)
#     until the schedule is revised"; deactivatePath description "naming the
#     sites".
Feature: CPT schedule revision, validation and path protection
  As an operator of the process path management service
  I want a site's schedule to be replaced atomically and to keep its paths alive
  So that a promise is never derived from a half-applied or dangling schedule

  Background:
    Given the Process Path Management service is running

  @bdd
  Scenario Outline: A malformed cutoff is rejected with its own problem type and nothing is stored
    Given a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    When the CPT schedule for site "sp1" is defined with a cutoff at "<localTime>" on days "<days>" via "<shipMethod>" eligible for paths "PICK"
    Then the request is rejected with status 422
    And the response problem reports type "<problem>"
    And "CPTScheduleChanged" was published 0 times for "sp1"
    When the CPT schedule for site "sp1" is requested
    Then the request is rejected with status 404

    Examples:
      | localTime | days   | shipMethod | problem             |
      | 24:00     | Mon    | ground     | invalid-local-time  |
      | 15:00     |        | ground     | no-days-of-week     |
      | 15:00     | Funday | ground     | invalid-day-of-week |

  @bdd
  Scenario: A schedule needs a timezone and at least one cutoff
    Given a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    When the client sends PUT "/sites/sp1/cpt-schedule" with body:
      """
      {"timezone":"","cutoffs":[{"cptId":"sp1-1500","localTime":"15:00","daysOfWeek":["Mon"],"shipMethod":"ground","eligiblePathIds":["PICK"]}]}
      """
    Then the request is rejected with status 422
    And the response problem reports type "empty-timezone"
    When the client sends PUT "/sites/sp1/cpt-schedule" with body:
      """
      {"timezone":"America/Sao_Paulo","cutoffs":[]}
      """
    Then the request is rejected with status 422
    And the response problem reports type "no-cutoffs"
    And "CPTScheduleChanged" was published 0 times for "sp1"

  @bdd
  Scenario: An empty siteId path segment is no route and stores no schedule
    Given a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    When the client sends PUT "/sites//cpt-schedule" with body:
      """
      {"timezone":"America/Sao_Paulo","cutoffs":[{"cptId":"x-1500","localTime":"15:00","daysOfWeek":["Mon"],"shipMethod":"ground","eligiblePathIds":["PICK"]}]}
      """
    Then the request is rejected with status 404
    And the response problem reports type "route-not-found"
    And "CPTScheduleChanged" was published 0 times for ""

  @bdd
  Scenario: A second PUT replaces the whole schedule and publishes the full new snapshot
    Given a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    And a process path "PACK" is already defined with matchPrefix "pack" and capabilities "pack"
    And the CPT schedule for site "sp1" is already defined with timezone "America/Sao_Paulo" and cutoff "sp1-1500" eligible for paths "PICK"
    And the clock reads "2026-09-07T09:00:00Z"
    When the CPT schedule for site "sp1" is defined with timezone "America/New_York" and cutoff "sp1-1700" eligible for paths "PACK"
    Then the request is accepted with status 200
    And the response field "timezone" is "America/New_York"
    And the CPT schedule response lists cutoff ids "sp1-1700"
    And the response field "createdAt" is the timestamp "2026-09-06T00:00:00Z"
    And the response field "updatedAt" is the timestamp "2026-09-07T09:00:00Z"
    And "CPTScheduleChanged" was published 2 times for "sp1"
    And the "CPTScheduleChanged" event for "sp1" carries timezone "America/New_York" and cutoff ids "sp1-1700"
    When the process path "PICK" is deactivated
    Then the deactivation is accepted with status 204

  @bdd
  Scenario: Re-submitting an identical schedule succeeds without publishing a second event
    Given a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    And the CPT schedule for site "sp1" is already defined with timezone "America/Sao_Paulo" and cutoff "sp1-1500" eligible for paths "PICK"
    When the CPT schedule for site "sp1" is defined with timezone "America/Sao_Paulo" and cutoff "sp1-1500" eligible for paths "PICK"
    Then the request is accepted with status 200
    And "CPTScheduleChanged" was published 1 time for "sp1"

  @bdd
  Scenario: A rejected revision leaves the existing schedule and its event stream untouched
    Given a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    And the CPT schedule for site "sp1" is already defined with timezone "America/Sao_Paulo" and cutoff "sp1-1500" eligible for paths "PICK"
    When the CPT schedule for site "sp1" is defined with timezone "America/New_York" and cutoff "sp1-1700" eligible for paths "PICK,GHOST"
    Then the request is rejected with status 422
    And the response problem reports type "ineligible-path-id"
    When the CPT schedule for site "sp1" is requested
    Then the request is accepted with status 200
    And the response field "timezone" is "America/Sao_Paulo"
    And the CPT schedule response lists cutoff ids "sp1-1500"
    And "CPTScheduleChanged" was published 1 time for "sp1"

  @bdd
  Scenario: A path stays protected until every site's schedule has stopped listing it
    Given a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    And a process path "PACK" is already defined with matchPrefix "pack" and capabilities "pack"
    And the CPT schedule for site "sp1" is already defined with timezone "America/Sao_Paulo" and cutoff "sp1-1500" eligible for paths "PICK"
    And the CPT schedule for site "sp2" is already defined with timezone "America/Sao_Paulo" and cutoff "sp2-1500" eligible for paths "PICK"
    When the process path "PICK" is deactivated
    Then the deactivation is rejected with status 409
    And the response problem reports type "path-referenced-by-cpt-schedule"
    And the response problem detail mentions "sp1"
    And the response problem detail mentions "sp2"
    Given the CPT schedule for site "sp1" is already defined with timezone "America/Sao_Paulo" and cutoff "sp1-1500" eligible for paths "PACK"
    When the process path "PICK" is deactivated
    Then the deactivation is rejected with status 409
    And the response problem detail mentions "sp2"
    And the response problem detail does not mention "sp1"
    Given the CPT schedule for site "sp2" is already defined with timezone "America/Sao_Paulo" and cutoff "sp2-1500" eligible for paths "PACK"
    When the process path "PICK" is deactivated
    Then the deactivation is accepted with status 204
    And the process path "PICK" now has status "DEACTIVATED"
