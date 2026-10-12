# Define-time boundaries of POST /process-paths that no earlier scenario pins,
# each scenario derived from the project documentation:
#   - apis/openapi.yaml, POST /process-paths 422: "pathId is empty, ...
#     cycleTimeP95 is not a positive Go duration string"; schema
#     DestinationLocationRole ("Omitted entirely -- or the equivalent JSON null /
#     empty string -- means no destination role was declared") and CycleTimeP95
#     ("Required and must be positive").
#   - .claude/rules/domain-model.md, "Aggregates & invariants": ProcessPath
#     (all writes) "cycleTimeP95 > 0 (ErrInvalidCycleTime); destinationLocationRole
#     empty or one of Drop/WorkCenter/Shipping".
Feature: Define-time boundaries of process path definitions
  As an operator of the process path management service
  I want every Define-time boundary of a path definition enforced
  So that no malformed or ambiguous definition is ever stored or published

  Background:
    Given the Process Path Management service is running

  @bdd
  Scenario: Defining a path with an empty pathId is rejected
    When a process path "" is defined with matchPrefix "pick" and capabilities "pick"
    Then the request is rejected with status 422
    And the response problem reports type "empty-path-id"
    And no events were published

  @bdd
  Scenario Outline: Defining a path with a non-positive cycleTimeP95 is rejected and stores nothing
    When a process path "PICK" is defined with matchPrefix "pick", capabilities "pick", and cycleTimeP95 "<cycleTimeP95>"
    Then the request is rejected with status 422
    And the response problem reports type "invalid-cycle-time-p95"
    And no events were published
    When the process path "PICK" is requested
    Then the request is rejected with status 404

    Examples:
      | cycleTimeP95 |
      | 0s           |
      | -90m         |

  @bdd
  Scenario: An empty or null destinationLocationRole means no destination was declared
    When the client sends POST "/process-paths" with body:
      """
      {"pathId":"PICK","matchPrefix":"pick","requiredCapabilities":["pick"],"cycleTimeP95":"2h","destinationLocationRole":""}
      """
    Then the request is accepted with status 201
    And the response field "destinationLocationRole" is absent
    When the client sends POST "/process-paths" with body:
      """
      {"pathId":"PACK","matchPrefix":"pack","requiredCapabilities":["pack"],"cycleTimeP95":"2h","destinationLocationRole":null}
      """
    Then the request is accepted with status 201
    And the response field "destinationLocationRole" is absent
