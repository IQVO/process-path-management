# The startup catalogue republish (ADR 0038): consumers rebuild their path
# catalogue by replaying the integration topic from the earliest offset, and
# the broker's retention expires old events. At every start the service
# re-enqueues the CURRENT state of every ACTIVE path (as ProcessPathUpdated)
# and of every CPT schedule (as CPTScheduleChanged), never a deactivated path.
Feature: Republishing the current catalogue at startup
  As a consumer that rebuilds its cache from warehouse.process-path-management.events
  I want the topic to carry the current state of every live path and schedule
  So that I never depend on the broker's retention

  Background:
    Given the Process Path Management service is running

  @bdd
  Scenario: A restart republishes every active path and every schedule
    Given a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    And a process path "PACK" is already defined with matchPrefix "pack" and capabilities "pack"
    And the CPT schedule for site "sp1" is already defined with timezone "America/Sao_Paulo" and cutoff "sp1-1500" eligible for paths "PICK"
    When the service is restarted
    Then a "ProcessPathUpdated" event was republished for path "PICK"
    And a "ProcessPathUpdated" event was republished for path "PACK"
    And a "CPTScheduleChanged" event was republished for site "sp1"
    And the restart republished 2 paths and 1 schedule

  @bdd
  Scenario: A deactivated path is not republished
    Given a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    And a process path "SLAM" is already defined with matchPrefix "slam" and capabilities "slam"
    And the process path "SLAM" is deactivated
    When the service is restarted
    Then a "ProcessPathUpdated" event was republished for path "PICK"
    And no "ProcessPathUpdated" event was republished for path "SLAM"
    And the restart republished 1 path and 0 schedules

  @bdd
  Scenario: An empty catalogue republishes nothing
    When the service is restarted
    Then the restart republished 0 paths and 0 schedules
