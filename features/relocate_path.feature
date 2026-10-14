# The RELOCATE process path (ADR 0033; slotting-optimization ADR 0010,
# Option A of the 2026-10-13 decision log). Approved slot-plan moves are floor
# work: wes-work-planning releases them on this path, fulfillment-execution
# executes them as RELOCATE tasks for operators holding the `relocate`
# capability, and order-management must never route a customer order line
# onto it. The entry is declared in catalogue/paths.json and applied through
# POST /process-paths by scripts/seed_catalogue.py; these scenarios post the
# declared bytes through the real router and assert what the five catalogue
# consumers will replay from warehouse.process-path-management.events.
Feature: The RELOCATE process path
  As wes-work-planning releasing approved slot-plan moves
  I want a RELOCATE path in the process-path catalogue
  So that relocation work can be released, staffed and executed like any floor work

  Background:
    Given the Process Path Management service is running

  @bdd
  Scenario: The declared RELOCATE entry defines an Active path
    When the client defines the declared catalogue entry "RELOCATE"
    Then the request is accepted with status 201
    And the response field "pathId" is "RELOCATE"
    And the response field "matchPrefix" is "relocate"
    And the response field "direct" is "true"
    And the response field "requiredCapabilities" is "relocate"
    And the response field "cycleTimeP95" is "2h0m0s"
    And the response field "eligibility.requiredProductAttributes" is "slot-relocation"
    And the response field "destinationLocationRole" is absent
    And the response field "status" is "ACTIVE"
    And exactly one "ProcessPathCreated" event was published for path "RELOCATE"

  @bdd
  Scenario: The RELOCATE path is published as the catalogue consumers replay it
    When the client defines the declared catalogue entry "RELOCATE"
    Then the request is accepted with status 201
    When the "ProcessPathCreated" event for "RELOCATE" is published to the integration topic
    Then the Kafka key is "RELOCATE"
    And the CloudEvent field "type" is "com.warehouse.wes.process-path-management.processpath.ProcessPathCreated"
    And the CloudEvent field "subject" is "RELOCATE"
    And the CloudEvent field "data.path_id" is "RELOCATE"
    And the CloudEvent field "data.match_prefix" is "relocate"
    And the CloudEvent field "data.direct" is "true"
    And the CloudEvent field "data.required_capabilities" is "relocate"
    And the CloudEvent field "data.eligibility.required_product_attributes" is "slot-relocation"
    And the CloudEvent field "data.destination_location_role" is absent

  @bdd
  Scenario: Seeding the declared entry twice is refused, never duplicated
    Given the client defines the declared catalogue entry "RELOCATE"
    When the client defines the declared catalogue entry "RELOCATE"
    Then the request is rejected with status 409
    And exactly one "ProcessPathCreated" event was published for path "RELOCATE"

  @bdd
  Scenario: RELOCATE sits beside the order paths without replacing them
    Given a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    When the client defines the declared catalogue entry "RELOCATE"
    Then the request is accepted with status 201
    When the process paths are listed
    Then the process path list reports 2 paths
