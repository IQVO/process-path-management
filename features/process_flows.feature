# Process flows (ADR 0042): the ordered stages of existing process paths a
# workload passes through, each derived from the ADR:
#   - docs/docs/adr/0042-process-flows.md, "1. ProcessFlow aggregate": a flow
#     has at least one stage; a stage is {path_id, process_type}; the path must
#     be an existing ACTIVE ProcessPath of this context's own catalogue; the
#     process type is one of the closed set; a path appears at most once; a
#     command that changes nothing raises no event and keeps the version.
#   - "2. Routes": PUT (define or revise, idempotent), GET, GET list, DELETE
#     (retire; a retired flow cannot be revised).
#   - "3. Events": full state with a version, startup republish of every
#     ACTIVE flow as ProcessFlowRevised.
#   - "4. Referential rule": deactivating a path an ACTIVE flow uses is a
#     409 path-in-active-flow naming the flows.
Feature: Process flows
  As an operator of the process path management service
  I want to declare the ordered stages a workload passes through
  So that the floor's topology has one owner and planning reads it from here

  Background:
    Given the Process Path Management service is running
    And a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    And a process path "REBIN" is already defined with matchPrefix "rebin" and capabilities "rebin"
    And a process path "PACK" is already defined with matchPrefix "pack" and capabilities "pack"

  @bdd
  Scenario: Defining a flow succeeds with 201 and publishes ProcessFlowDefined at version 1
    When the flow "outbound" named "Outbound" is put with stages "PICK:PICK,REBIN:REBIN,PACK:PACK"
    Then the request is accepted with status 201
    And the flow response reports flowId "outbound", status "ACTIVE", version 1 and stages "PICK:PICK,REBIN:REBIN,PACK:PACK"
    And a "ProcessFlowDefined" event was published for flow "outbound" at version 1

  @bdd
  Scenario: Revising a flow answers 200, bumps the version and publishes ProcessFlowRevised
    Given the flow "outbound" named "Outbound" is already put with stages "PICK:PICK,REBIN:REBIN,PACK:PACK"
    When the flow "outbound" named "Outbound" is put with stages "PICK:PICK,PACK:PACK"
    Then the request is accepted with status 200
    And the flow response reports flowId "outbound", status "ACTIVE", version 2 and stages "PICK:PICK,PACK:PACK"
    And a "ProcessFlowRevised" event was published for flow "outbound" at version 2

  @bdd
  Scenario: A PUT that changes nothing answers 200, raises no event and keeps the version
    Given the flow "outbound" named "Outbound" is already put with stages "PICK:PICK,PACK:PACK"
    When the flow "outbound" named "Outbound" is put with stages "PICK:PICK,PACK:PACK"
    Then the request is accepted with status 200
    And the flow response reports flowId "outbound", status "ACTIVE", version 1 and stages "PICK:PICK,PACK:PACK"
    And no "ProcessFlowRevised" event was published for flow "outbound"
    And exactly one "ProcessFlowDefined" event was published for flow "outbound"

  @bdd
  Scenario: The order of the stages is part of the definition
    Given the flow "outbound" named "Outbound" is already put with stages "PICK:PICK,PACK:PACK"
    When the flow "outbound" named "Outbound" is put with stages "PACK:PACK,PICK:PICK"
    Then the request is accepted with status 200
    And the flow response reports flowId "outbound", status "ACTIVE", version 2 and stages "PACK:PACK,PICK:PICK"

  @bdd
  Scenario: A flow is read back with its stages in order
    Given the flow "outbound" named "Outbound" is already put with stages "PICK:PICK,REBIN:REBIN,PACK:PACK"
    When the flow "outbound" is requested
    Then the request is accepted with status 200
    And the flow response reports flowId "outbound", status "ACTIVE", version 1 and stages "PICK:PICK,REBIN:REBIN,PACK:PACK"

  @bdd
  Scenario: Getting a flow that was never defined returns 404
    When the flow "ghost" is requested
    Then the request is rejected with status 404
    And the response problem reports type "flow-not-found"

  @bdd
  Scenario: The flows are listed, retired ones only on request
    Given the flow "a-flow" named "A" is already put with stages "PICK:PICK"
    And the flow "b-flow" named "B" is already put with stages "PACK:PACK"
    And the flow "b-flow" is already retired
    When the flows are listed
    Then the request is accepted with status 200
    And the flow list reports 1 flow
    When all flows are listed including retired ones
    Then the flow list reports 2 flows

  @bdd
  Scenario: A stage on a path that does not exist is rejected
    When the flow "outbound" named "Outbound" is put with stages "PICK:PICK,GHOST:PACK"
    Then the request is rejected with status 422
    And the response problem reports type "ineligible-stage-path"
    And no "ProcessFlowDefined" event was published for flow "outbound"

  @bdd
  Scenario: A stage on a deactivated path is rejected
    Given the process path "REBIN" is already deactivated
    When the flow "outbound" named "Outbound" is put with stages "PICK:PICK,REBIN:REBIN"
    Then the request is rejected with status 422
    And the response problem reports type "ineligible-stage-path"

  @bdd
  Scenario: A path may appear at most once in a flow
    When the flow "outbound" named "Outbound" is put with stages "PICK:PICK,PICK:PACK"
    Then the request is rejected with status 422
    And the response problem reports type "duplicate-stage-path"

  @bdd
  Scenario: A process type outside the closed set is rejected
    When the flow "outbound" named "Outbound" is put with stages "PICK:WELD"
    Then the request is rejected with status 422
    And the response problem reports type "invalid-process-type"

  @bdd
  Scenario: A flow needs at least one stage and a name
    When the flow "outbound" named "Outbound" is put with no stages
    Then the request is rejected with status 422
    And the response problem reports type "no-stages"
    When the flow "outbound" named "" is put with stages "PICK:PICK"
    Then the request is rejected with status 422
    And the response problem reports type "empty-flow-name"

  @bdd
  Scenario: Retiring a flow publishes ProcessFlowRetired with the full state and the next version
    Given the flow "outbound" named "Outbound" is already put with stages "PICK:PICK,PACK:PACK"
    When the flow "outbound" is retired
    Then the request is accepted with status 204
    And a "ProcessFlowRetired" event was published for flow "outbound" at version 2
    And the flow "outbound" now has status "RETIRED"

  @bdd
  Scenario: Retiring a retired flow again is a 204 and publishes nothing more
    Given the flow "outbound" named "Outbound" is already put with stages "PICK:PICK"
    And the flow "outbound" is already retired
    When the flow "outbound" is retired
    Then the request is accepted with status 204
    And exactly one "ProcessFlowRetired" event was published for flow "outbound"

  @bdd
  Scenario: Retiring a flow that was never defined returns 404
    When the flow "ghost" is retired
    Then the request is rejected with status 404
    And the response problem reports type "flow-not-found"

  @bdd
  Scenario: A retired flow cannot be revised
    Given the flow "outbound" named "Outbound" is already put with stages "PICK:PICK"
    And the flow "outbound" is already retired
    When the flow "outbound" named "Outbound" is put with stages "PICK:PICK,PACK:PACK"
    Then the request is rejected with status 422
    And the response problem reports type "flow-retired"

  @bdd
  Scenario: A path an active flow uses cannot be deactivated
    Given the flow "alpha" named "Alpha" is already put with stages "PICK:PICK,PACK:PACK"
    And the flow "beta" named "Beta" is already put with stages "PICK:PICK"
    When the process path "PICK" is deactivated
    Then the deactivation is rejected with status 409
    And the response problem reports type "path-in-active-flow"
    And the response detail names the flows "alpha, beta"
    And the process path "PICK" now has status "ACTIVE"
    And no "ProcessPathDeactivated" event was published for path "PICK"

  @bdd
  Scenario: Retiring the flows lifts the refusal
    Given the flow "alpha" named "Alpha" is already put with stages "PICK:PICK,PACK:PACK"
    And the flow "alpha" is already retired
    When the process path "PICK" is deactivated
    Then the deactivation is accepted with status 204

  @bdd
  Scenario: A path no flow uses is deactivated as before
    Given the flow "alpha" named "Alpha" is already put with stages "PICK:PICK"
    When the process path "REBIN" is deactivated
    Then the deactivation is accepted with status 204

  @bdd
  Scenario: A restart republishes every active flow as ProcessFlowRevised at its current version
    Given the flow "alpha" named "Alpha" is already put with stages "PICK:PICK"
    And the flow "alpha" named "Alpha 2" is already put with stages "PICK:PICK,PACK:PACK"
    And the flow "beta" named "Beta" is already put with stages "PACK:PACK"
    And the flow "beta" is already retired
    When the service is restarted
    Then a "ProcessFlowRevised" event was republished for flow "alpha" at version 2
    And no "ProcessFlowRevised" event was republished for flow "beta"
    And the restart republished 1 flow
