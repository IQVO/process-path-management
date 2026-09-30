# Revision semantics and the domain events each change publishes, each
# scenario derived from the project documentation:
#   - .claude/rules/domain-model.md, "Aggregates & invariants":
#     "A path's identity is permanent once created. DefinePath rejects
#     (409) re-defining an id that already exists, active or
#     deactivated"; Revise "Returns changed bool -- a no-op revision
#     (identical matchPrefix and requiredCapabilities) returns false and
#     raises nothing"; "Deactivate: idempotent ... does NOT republish
#     ProcessPathDeactivated".
#   - .claude/rules/domain-model.md, "Domain events":
#     ProcessPathCreated, ProcessPathUpdated, ProcessPathDeactivated on
#     the shared warehouse.process-path-management.events topic.
#   - apis/openapi.yaml: definePath ("Publishes ProcessPathCreated on
#     success"; "Rejects with 409 if the given pathId is already defined
#     -- active or deactivated"), revisePath ("A no-op revision ... is a
#     successful 200 that does NOT republish ProcessPathUpdated"; 422
#     when "matchPrefix/requiredCapabilities/cycleTimeP95 is invalid"),
#     deactivatePath ("Publishes ProcessPathDeactivated the first time a
#     path transitions to Deactivated"), and the CycleTimeP95 schema
#     ("Required and must be positive", a Go duration string).
Feature: Revising process paths and the events changes publish
  As a downstream consumer of the process path management topic
  I want every real change to a path published as a domain event
  So that my read model stays in sync without refetching

  Background:
    Given the Process Path Management service is running

  @bdd
  Scenario: Re-defining a deactivated path's id is rejected
    Given a process path "SLAM" is already defined with matchPrefix "slam" and capabilities "slam"
    And the process path "SLAM" is already deactivated
    When a process path "SLAM" is defined with matchPrefix "slam" and capabilities "slam"
    Then the request is rejected with status 409
    And the response problem reports type "path-already-exists"

  @bdd
  Scenario: A no-op revision succeeds without publishing ProcessPathUpdated
    Given a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    When the process path "PICK" is revised with matchPrefix "pick" and capabilities "pick"
    Then the request is accepted with status 200
    And no "ProcessPathUpdated" event was published for path "PICK"

  @bdd
  Scenario: Revising with a malformed cycleTimeP95 is rejected
    Given a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    When the process path "PICK" is revised with matchPrefix "pick", capabilities "pick", and cycleTimeP95 "forever"
    Then the request is rejected with status 422
    And the response problem reports type "invalid-cycle-time-p95"

  @bdd
  Scenario: Defining a path publishes ProcessPathCreated
    When a process path "PICK" is defined with matchPrefix "pick" and capabilities "pick"
    Then the request is accepted with status 201
    And a "ProcessPathCreated" event was published for path "PICK"

  @bdd
  Scenario: Revising an active path publishes ProcessPathUpdated
    Given a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    When the process path "PICK" is revised with matchPrefix "pick-zone-a" and capabilities "pick,hazmat"
    Then the request is accepted with status 200
    And a "ProcessPathUpdated" event was published for path "PICK"

  @bdd
  Scenario: Deactivating publishes ProcessPathDeactivated exactly once
    Given a process path "SLAM" is already defined with matchPrefix "slam" and capabilities "slam"
    When the process path "SLAM" is deactivated
    Then the deactivation is accepted with status 204
    And a "ProcessPathDeactivated" event was published for path "SLAM"
    When the process path "SLAM" is deactivated
    Then the deactivation is accepted with status 204
    And exactly one "ProcessPathDeactivated" event was published for path "SLAM"
