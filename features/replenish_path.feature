# The REPLENISH process path and the REPLENISHMENT demand source (ADR 0036;
# pick-face-replenishment -> wes-work-planning -> fulfillment-execution ADR
# 0050 -> inventory-storage). A pick-face top-up is floor work that runs
# through the relocation chain: wes-work-planning releases it on this path,
# fulfillment-execution executes it as a REPLENISH task for operators holding
# the `replenish` capability, and order-management must never route a customer
# order line onto it: its intake is REPLENISHMENT only (ADR 0034). The entry is
# declared in catalogue/paths.json and applied through POST /process-paths by
# scripts/seed_catalogue.py; these scenarios post the declared bytes through
# the real router and assert what the catalogue consumers will replay from
# warehouse.process-path-management.events.
Feature: The REPLENISH process path
  As wes-work-planning releasing pick-face top-ups
  I want a REPLENISH path in the process-path catalogue
  So that replenishment work can be released, staffed and executed like any floor work

  Background:
    Given the Process Path Management service is running

  @bdd
  Scenario: The declared REPLENISH entry defines an Active path
    When the client defines the declared catalogue entry "REPLENISH"
    Then the request is accepted with status 201
    And the response field "pathId" is "REPLENISH"
    And the response field "matchPrefix" is "replenish"
    And the response field "direct" is "true"
    And the response field "requiredCapabilities" is "replenish"
    And the response field "cycleTimeP95" is "2h0m0s"
    And the response field "eligibility.requiredProductAttributes" is absent
    And the response field "intake" is "REPLENISHMENT"
    And the response field "destinationLocationRole" is absent
    And the response field "status" is "ACTIVE"
    And exactly one "ProcessPathCreated" event was published for path "REPLENISH"

  @bdd
  Scenario: The REPLENISH path is published as the catalogue consumers replay it
    When the client defines the declared catalogue entry "REPLENISH"
    Then the request is accepted with status 201
    When the "ProcessPathCreated" event for "REPLENISH" is published to the integration topic
    Then the Kafka key is "REPLENISH"
    And the CloudEvent field "type" is "com.warehouse.wes.process-path-management.processpath.ProcessPathCreated"
    And the CloudEvent field "subject" is "REPLENISH"
    And the CloudEvent field "data.path_id" is "REPLENISH"
    And the CloudEvent field "data.match_prefix" is "replenish"
    And the CloudEvent field "data.direct" is "true"
    And the CloudEvent field "data.required_capabilities" is "replenish"
    And the CloudEvent field "data.eligibility.required_product_attributes" is absent
    And the CloudEvent field "data.intake" is "REPLENISHMENT"
    And the CloudEvent field "data.destination_location_role" is absent

  @bdd
  Scenario: Seeding the declared entry twice is refused, never duplicated
    Given the client defines the declared catalogue entry "REPLENISH"
    When the client defines the declared catalogue entry "REPLENISH"
    Then the request is rejected with status 409
    And exactly one "ProcessPathCreated" event was published for path "REPLENISH"

  @bdd
  Scenario: REPLENISH sits beside the order paths without replacing them
    Given a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    When the client defines the declared catalogue entry "REPLENISH"
    Then the request is accepted with status 201
    When the process paths are listed
    Then the process path list reports 2 paths

  @bdd
  Scenario: REPLENISHMENT is a recognised demand source on any path definition
    When the client sends POST "/process-paths" with body:
      """
      {"pathId":"REPLENISH","matchPrefix":"replenish","direct":true,"requiredCapabilities":["replenish"],"cycleTimeP95":"2h","intake":["REPLENISHMENT"]}
      """
    Then the request is accepted with status 201
    And the response field "intake" is "REPLENISHMENT"

  @bdd
  Scenario: A near miss of the REPLENISHMENT spelling is rejected
    When the client sends POST "/process-paths" with body:
      """
      {"pathId":"REPLENISH","matchPrefix":"replenish","direct":true,"requiredCapabilities":["replenish"],"cycleTimeP95":"2h","intake":["REPLENISH"]}
      """
    Then the request is rejected with status 422
    And the response problem reports type "invalid-intake"
    And no events were published
