# Define-time invariants and capability declarations, each scenario
# derived from the project documentation:
#   - .claude/rules/domain-model.md, "Aggregates & invariants":
#     ProcessPath.Define requires a non-empty lower-case matchPrefix
#     (ErrEmptyMatchPrefix, ErrMatchPrefixNotLowercase) and non-empty
#     requiredCapabilities (ErrNoRequiredCapabilities) -- "a malformed
#     path definition must never be constructible".
#   - apis/openapi.yaml, POST /process-paths 422 response ("matchPrefix
#     is empty/not lower-case, requiredCapabilities is empty, or
#     destinationLocationRole is not one of Drop/WorkCenter/Shipping")
#     and the ProblemDetails schema (RFC 7807: type/title/status/detail).
#   - docs/docs/adr/0009-destination-location-role-on-process-path.md:
#     DestinationLocationRole is a closed Drop/WorkCenter/Shipping set
#     declared once at Define time; "a caller who typos or picks a
#     real-but-wrong facility-layout role (e.g. Storage) gets a clean
#     422 at Define time".
#   - docs/docs/adr/0010-fulfillment-capability-contract.md, "1.
#     ProcessPath gains two revisable capability facts": eligibility is
#     a required value object where "maxUnitsPerLine (nil = unbounded;
#     1 is how a singles path is declared)".
Feature: Define-time validation of process path definitions
  As an operator of the process path management service
  I want malformed path definitions rejected at Define time
  So that a malformed definition is never constructible and never published

  Background:
    Given the Process Path Management service is running

  @bdd
  Scenario: Defining a path with an empty matchPrefix is rejected
    When a process path "PICK" is defined with matchPrefix "" and capabilities "pick"
    Then the request is rejected with status 422
    And the response problem reports type "empty-match-prefix"

  @bdd
  Scenario: Defining a path with an upper-case matchPrefix is rejected
    When a process path "PICK" is defined with matchPrefix "Pick" and capabilities "pick"
    Then the request is rejected with status 422
    And the response problem reports type "match-prefix-not-lowercase"

  @bdd
  Scenario: Defining a path with no required capabilities is rejected
    When a process path "PICK" is defined with matchPrefix "pick" and no capabilities
    Then the request is rejected with status 422
    And the response problem reports type "no-required-capabilities"

  @bdd
  Scenario: Defining a path with an unrecognized destinationLocationRole is rejected
    When a process path "QC" is defined with matchPrefix "qc", capabilities "qc", and destinationLocationRole "Storage"
    Then the request is rejected with status 422
    And the response problem reports type "invalid-destination-location-role"

  @bdd
  Scenario: A declared destinationLocationRole is carried on the defined path
    When a process path "PACK" is defined with matchPrefix "pack", capabilities "pack", and destinationLocationRole "Drop"
    Then the request is accepted with status 201
    And the process path response reports pathId "PACK", matchPrefix "pack", and status "ACTIVE"
    And the process path response reports destinationLocationRole "Drop"

  @bdd
  Scenario: A singles path declares its eligibility at Define time
    When a process path "SINGLES" is defined with matchPrefix "singles", capabilities "pick", and maxUnitsPerLine 1
    Then the request is accepted with status 201
    And the process path response reports eligibility maxUnitsPerLine 1
