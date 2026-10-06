Feature: Deactivating process paths
  As an operator of the process path management service
  I want to deactivate (retire) a process path
  So that downstream consumers stop accepting new work against it

  Background:
    Given the Process Path Management service is running

  @bdd
  Scenario: Deactivating an active path succeeds
    Given a process path "SLAM" is already defined with matchPrefix "slam" and capabilities "slam"
    When the process path "SLAM" is deactivated
    Then the deactivation is accepted with status 204
    And the process path "SLAM" now has status "DEACTIVATED"

  @bdd
  Scenario: Deactivating an already-deactivated path is idempotent
    Given a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    And the process path "PICK" is already deactivated
    When the process path "PICK" is deactivated
    Then the deactivation is accepted with status 204

  @bdd
  Scenario: Deactivating a process path that was never defined returns 404
    When the process path "MISSING" is deactivated
    Then the deactivation is rejected with status 404

  # ADR 0026: ADR 0010 requires every cutoff's eligiblePathIds to name an
  # Active path; deactivation is refused while a schedule still lists it.
  @bdd
  Scenario: Deactivating a path a CPT schedule still lists is rejected with 409
    Given a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    And the CPT schedule for site "sp1" is already defined with timezone "America/Sao_Paulo" and cutoff "sp1-1500" eligible for paths "PICK"
    When the process path "PICK" is deactivated
    Then the deactivation is rejected with status 409
    And the response problem reports type "path-referenced-by-cpt-schedule"
    And the process path "PICK" now has status "ACTIVE"
    And no "ProcessPathDeactivated" event was published for path "PICK"

  @bdd
  Scenario: A path can be deactivated once the schedule no longer lists it
    Given a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    And a process path "PACK" is already defined with matchPrefix "pack" and capabilities "pack"
    And the CPT schedule for site "sp1" is already defined with timezone "America/Sao_Paulo" and cutoff "sp1-1500" eligible for paths "PICK"
    And the CPT schedule for site "sp1" is already defined with timezone "America/Sao_Paulo" and cutoff "sp1-1500" eligible for paths "PACK"
    When the process path "PICK" is deactivated
    Then the deactivation is accepted with status 204
    And the process path "PICK" now has status "DEACTIVATED"
    And exactly one "ProcessPathDeactivated" event was published for path "PICK"
