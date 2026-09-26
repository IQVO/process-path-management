# Site-scoped CPT schedules (the ADR 0010 fulfillment capability
# contract), each scenario derived from the project documentation:
#   - docs/docs/adr/0010-fulfillment-capability-contract.md, "2. A new
#     CPTSchedule aggregate, site-scoped": invariants are "at least one
#     cutoff; cptIds unique within a site; every eligiblePathIds entry
#     names an Active ProcessPath in this service's own store (checked
#     at write time)", timezone is an IANA zone, and "Revising a
#     schedule raises a new event, CPTScheduleChanged, carrying the full
#     schedule". Section "3. REST and MCP surfaces" adds
#     PUT/GET /sites/{siteId}/cpt-schedule "RFC 7807 errors as today".
#   - apis/openapi.yaml, cpt-schedule tag: getCPTSchedule "Returns 404
#     if no schedule has been defined for this siteId yet";
#     defineCPTSchedule 422 when "timezone is empty/not a recognized
#     IANA zone, ... a cptId is empty/duplicate, ... or an
#     eligiblePathIds entry does not reference an Active process path in
#     this service's own store".
Feature: Site-scoped CPT schedules
  As an operator of the process path management service
  I want to define each site's Critical Pull Time schedule against active paths
  So that a delivery promise can be derived from real departure times

  Background:
    Given the Process Path Management service is running

  @bdd
  Scenario: Defining a site's CPT schedule succeeds
    Given a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    When the CPT schedule for site "sp1" is defined with timezone "America/Sao_Paulo" and cutoff "sp1-1500" eligible for paths "PICK"
    Then the request is accepted with status 200
    And the CPT schedule response reports siteId "sp1", timezone "America/Sao_Paulo", and 1 cutoff
    And a "CPTScheduleChanged" event was published for site "sp1"

  @bdd
  Scenario: Getting a defined CPT schedule returns it
    Given a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    And the CPT schedule for site "sp1" is already defined with timezone "America/Sao_Paulo" and cutoff "sp1-1500" eligible for paths "PICK"
    When the CPT schedule for site "sp1" is requested
    Then the request is accepted with status 200
    And the CPT schedule response reports siteId "sp1", timezone "America/Sao_Paulo", and 1 cutoff

  @bdd
  Scenario: Getting a CPT schedule that was never defined returns 404
    When the CPT schedule for site "sp1" is requested
    Then the request is rejected with status 404
    And the response problem reports type "cpt-schedule-not-found"

  @bdd
  Scenario: A cutoff referencing an unknown process path is rejected
    When the CPT schedule for site "sp1" is defined with timezone "America/Sao_Paulo" and cutoff "sp1-1500" eligible for paths "GHOST"
    Then the request is rejected with status 422
    And the response problem reports type "ineligible-path-id"

  @bdd
  Scenario: A cutoff referencing a deactivated process path is rejected
    Given a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    And the process path "PICK" is already deactivated
    When the CPT schedule for site "sp1" is defined with timezone "America/Sao_Paulo" and cutoff "sp1-1500" eligible for paths "PICK"
    Then the request is rejected with status 422
    And the response problem reports type "ineligible-path-id"

  @bdd
  Scenario: Duplicate cutoff ids within a schedule are rejected
    Given a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    When the CPT schedule for site "sp1" is defined with duplicate cutoff ids "sp1-1500" eligible for paths "PICK"
    Then the request is rejected with status 422
    And the response problem reports type "duplicate-cpt-id"

  @bdd
  Scenario: An unrecognized timezone is rejected
    Given a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    When the CPT schedule for site "sp1" is defined with timezone "Mars/Olympus_Mons" and cutoff "sp1-1500" eligible for paths "PICK"
    Then the request is rejected with status 422
    And the response problem reports type "invalid-timezone"
