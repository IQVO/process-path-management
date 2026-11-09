# The full-case and cross-dock flows (ADR 0038, contract P11-4). Both are catalogue data under the existing
# ProcessPath model, applied by scripts/seed_catalogue.py through POST /process-paths: FULL_CASE (whole-case
# picking, no each handling), CROSS_DOCK (receive -> ship with no put-away) and BREAK_CASE (the loose remainder of a
# line that is not a whole number of cases). FULL_CASE and CROSS_DOCK accept CUSTOMER_ORDER only for lines carrying
# a declared product attribute, so no line that exists today can be routed onto them. These scenarios post the
# declared bytes through the real router and assert what the catalogue consumers replay from
# warehouse.process-path-management.events. No pack fact is read or stored here: this service never calls or
# subscribes to a sibling context.
Feature: The full-case and cross-dock process paths
  As the operator of a warehouse that receives and ships whole cases
  I want FULL_CASE, CROSS_DOCK and BREAK_CASE declared in the process-path catalogue
  So that whole-case and cross-dock work can be routed, staffed and executed without each handling or put-away

  Background:
    Given the Process Path Management service is running

  @bdd
  Scenario: The declared FULL_CASE entry defines an Active path gated on the fullCase attribute
    When the client defines the declared catalogue entry "FULL_CASE"
    Then the request is accepted with status 201
    And the response field "pathId" is "FULL_CASE"
    And the response field "matchPrefix" is "full-case"
    And the response field "direct" is "true"
    And the response field "requiredCapabilities" is "pick"
    And the response field "cycleTimeP95" is "1h30m0s"
    And the response field "eligibility.requiredProductAttributes" is "fullCase"
    And the response field "intake" is "CUSTOMER_ORDER"
    And the response field "destinationLocationRole" is absent
    And the response field "status" is "ACTIVE"
    And exactly one "ProcessPathCreated" event was published for path "FULL_CASE"

  @bdd
  Scenario: The declared CROSS_DOCK entry defines an Active path that ends at Shipping
    When the client defines the declared catalogue entry "CROSS_DOCK"
    Then the request is accepted with status 201
    And the response field "pathId" is "CROSS_DOCK"
    And the response field "matchPrefix" is "cross-dock"
    And the response field "requiredCapabilities" is "dispatch"
    And the response field "cycleTimeP95" is "1h0m0s"
    And the response field "eligibility.requiredProductAttributes" is "crossDock"
    And the response field "intake" is "CUSTOMER_ORDER,STOCK_TRANSFER"
    And the response field "destinationLocationRole" is "Shipping"
    And the response field "status" is "ACTIVE"
    And exactly one "ProcessPathCreated" event was published for path "CROSS_DOCK"

  @bdd
  Scenario: The declared BREAK_CASE entry is reached only by an explicit path id
    When the client defines the declared catalogue entry "BREAK_CASE"
    Then the request is accepted with status 201
    And the response field "matchPrefix" is "break"
    And the response field "requiredCapabilities" is "break"
    And the response field "cycleTimeP95" is "2h0m0s"
    And the response field "eligibility.requiredProductAttributes" is absent
    And the response field "intake" is ""
    And exactly one "ProcessPathCreated" event was published for path "BREAK_CASE"

  @bdd
  Scenario: FULL_CASE is published as the catalogue consumers replay it
    When the client defines the declared catalogue entry "FULL_CASE"
    Then the request is accepted with status 201
    When the "ProcessPathCreated" event for "FULL_CASE" is published to the integration topic
    Then the Kafka key is "FULL_CASE"
    And the CloudEvent field "type" is "com.warehouse.wes.process-path-management.processpath.ProcessPathCreated"
    And the CloudEvent field "subject" is "FULL_CASE"
    And the CloudEvent field "data.path_id" is "FULL_CASE"
    And the CloudEvent field "data.match_prefix" is "full-case"
    And the CloudEvent field "data.required_capabilities" is "pick"
    And the CloudEvent field "data.eligibility.required_product_attributes" is "fullCase"
    And the CloudEvent field "data.intake" is "CUSTOMER_ORDER"
    And the CloudEvent field "data.destination_location_role" is absent

  @bdd
  Scenario: CROSS_DOCK is published with its Shipping destination
    When the client defines the declared catalogue entry "CROSS_DOCK"
    Then the request is accepted with status 201
    When the "ProcessPathCreated" event for "CROSS_DOCK" is published to the integration topic
    Then the Kafka key is "CROSS_DOCK"
    And the CloudEvent field "data.match_prefix" is "cross-dock"
    And the CloudEvent field "data.required_capabilities" is "dispatch"
    And the CloudEvent field "data.eligibility.required_product_attributes" is "crossDock"
    And the CloudEvent field "data.intake" is "CUSTOMER_ORDER,STOCK_TRANSFER"
    And the CloudEvent field "data.destination_location_role" is "Shipping"

  @bdd
  Scenario Outline: Seeding a declared entry twice is refused, never duplicated
    Given the client defines the declared catalogue entry "<path>"
    When the client defines the declared catalogue entry "<path>"
    Then the request is rejected with status 409
    And exactly one "ProcessPathCreated" event was published for path "<path>"

    Examples:
      | path       |
      | FULL_CASE  |
      | CROSS_DOCK |
      | BREAK_CASE |

  @bdd
  Scenario: The new flows sit beside the order paths without replacing them
    Given a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    When the client defines the declared catalogue entry "FULL_CASE"
    And the client defines the declared catalogue entry "CROSS_DOCK"
    And the client defines the declared catalogue entry "BREAK_CASE"
    And the process paths are listed
    Then the process path list reports 4 paths

  @bdd
  Scenario: A cross-dock path cannot be declared with a destination role outside the closed set
    When the client sends POST "/process-paths" with body:
      """
      {"pathId":"CROSS_DOCK","matchPrefix":"cross-dock","direct":true,"requiredCapabilities":["dispatch"],"destinationLocationRole":"Dock","cycleTimeP95":"1h","intake":["CUSTOMER_ORDER"]}
      """
    Then the request is rejected with status 422
    And no events were published

  @bdd
  Scenario: A zero cycle time is refused on a flow path
    When the client sends POST "/process-paths" with body:
      """
      {"pathId":"FULL_CASE","matchPrefix":"full-case","direct":true,"requiredCapabilities":["pick"],"cycleTimeP95":"0s","intake":["CUSTOMER_ORDER"]}
      """
    Then the request is rejected with status 422
    And the response problem reports type "invalid-cycle-time-p95"
    And no events were published

  @bdd
  Scenario: An upper-case match prefix is refused on a flow path
    When the client sends POST "/process-paths" with body:
      """
      {"pathId":"FULL_CASE","matchPrefix":"Full-Case","direct":true,"requiredCapabilities":["pick"],"cycleTimeP95":"1h30m","intake":["CUSTOMER_ORDER"]}
      """
    Then the request is rejected with status 422
    And the response problem reports type "match-prefix-not-lowercase"
    And no events were published
