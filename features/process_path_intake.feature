# Process-path intake (ADR 0034; decision E5 of the 2026-10-13 decision log).
# A path declares which demand sources may route work onto it. order-management
# routes customer order lines only onto paths whose intake contains
# CUSTOMER_ORDER; an empty intake means the path is reached only by an explicit
# path id from an upstream stage (PACK, REBIN, SLAM). Intake is routing policy,
# so it is revisable through the existing update command and published on
# ProcessPathCreated/ProcessPathUpdated as snake_case `intake`.
Feature: Process-path intake
  As order-management and wes-work-planning choosing a path for new work
  I want every path to declare which demand sources may route work onto it
  So that a customer order line never lands on a downstream stage or a relocation path

  Background:
    Given the Process Path Management service is running

  @bdd
  Scenario: A path defined with an intake reads it back and publishes it
    When the client sends POST "/process-paths" with body:
      """
      {"pathId":"PICK","matchPrefix":"pick","direct":true,"requiredCapabilities":["pick"],"cycleTimeP95":"2h","intake":["CUSTOMER_ORDER"]}
      """
    Then the request is accepted with status 201
    And the response field "intake" is "CUSTOMER_ORDER"
    When the "ProcessPathCreated" event for "PICK" is published to the integration topic
    Then the CloudEvent field "data.intake" is "CUSTOMER_ORDER"

  @bdd
  Scenario: A path defined without an intake has the empty intake, published as an empty array
    When the client sends POST "/process-paths" with body:
      """
      {"pathId":"PACK","matchPrefix":"pack","direct":true,"requiredCapabilities":["pack"],"cycleTimeP95":"2h"}
      """
    Then the request is accepted with status 201
    And the response field "intake" is ""
    When the "ProcessPathCreated" event for "PACK" is published to the integration topic
    Then the CloudEvent field "data.intake" is present
    And the CloudEvent field "data.intake" is ""

  @bdd
  Scenario: An unknown demand source is rejected
    When the client sends POST "/process-paths" with body:
      """
      {"pathId":"PICK","matchPrefix":"pick","direct":true,"requiredCapabilities":["pick"],"cycleTimeP95":"2h","intake":["ORDERS"]}
      """
    Then the request is rejected with status 422
    And the response problem reports type "invalid-intake"
    And no events were published

  @bdd
  Scenario: A repeated demand source is rejected
    When the client sends POST "/process-paths" with body:
      """
      {"pathId":"PICK","matchPrefix":"pick","direct":true,"requiredCapabilities":["pick"],"cycleTimeP95":"2h","intake":["CUSTOMER_ORDER","CUSTOMER_ORDER"]}
      """
    Then the request is rejected with status 422
    And no events were published

  @bdd
  Scenario: Revising only the intake publishes ProcessPathUpdated with it
    Given a process path "PICK" is defined with matchPrefix "pick", capabilities "pick", and cycleTimeP95 "2h"
    When the client sends PUT "/process-paths/PICK" with body:
      """
      {"matchPrefix":"pick","requiredCapabilities":["pick"],"cycleTimeP95":"2h","intake":["CUSTOMER_ORDER"]}
      """
    Then the request is accepted with status 200
    And the response field "intake" is "CUSTOMER_ORDER"
    And "ProcessPathUpdated" was published 1 time for "PICK"
    When the "ProcessPathUpdated" event for "PICK" is published to the integration topic
    Then the CloudEvent field "data.intake" is "CUSTOMER_ORDER"

  @bdd
  Scenario: A revision that omits the intake keeps it and publishes nothing new
    Given the client sends POST "/process-paths" with body:
      """
      {"pathId":"PICK","matchPrefix":"pick","direct":true,"requiredCapabilities":["pick"],"cycleTimeP95":"2h","intake":["CUSTOMER_ORDER"]}
      """
    When the client sends PUT "/process-paths/PICK" with body:
      """
      {"matchPrefix":"pick","requiredCapabilities":["pick"],"cycleTimeP95":"2h"}
      """
    Then the request is accepted with status 200
    And the response field "intake" is "CUSTOMER_ORDER"
    And "ProcessPathUpdated" was published 0 times for "PICK"

  @bdd
  Scenario: Clearing the intake makes the path reachable only by its id
    Given the client sends POST "/process-paths" with body:
      """
      {"pathId":"PACK","matchPrefix":"pack","direct":true,"requiredCapabilities":["pack"],"cycleTimeP95":"2h","intake":["CUSTOMER_ORDER"]}
      """
    When the client sends PUT "/process-paths/PACK" with body:
      """
      {"matchPrefix":"pack","requiredCapabilities":["pack"],"cycleTimeP95":"2h","intake":[]}
      """
    Then the request is accepted with status 200
    And the response field "intake" is ""
    And "ProcessPathUpdated" was published 1 time for "PACK"

  @bdd
  Scenario Outline: The declared catalogue gives every path its intake
    When the client defines the declared catalogue entry "<pathId>"
    Then the request is accepted with status 201
    And the response field "intake" is "<intake>"
    When the "ProcessPathCreated" event for "<pathId>" is published to the integration topic
    Then the CloudEvent field "data.intake" is "<intake>"

    Examples:
      | pathId   | intake          |
      | PICK     | CUSTOMER_ORDER  |
      | PACK     |                 |
      | REBIN    |                 |
      | SLAM     |                 |
      | RELOCATE | SLOT_RELOCATION |
