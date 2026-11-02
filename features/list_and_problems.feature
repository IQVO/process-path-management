# The listing surface and the shared 404 problem shape, each scenario derived
# from the project documentation:
#   - apis/openapi.yaml, listPaths: 200 ProcessPathPage {items, nextCursor}
#     (REST API guideline 3.3); 400 "The `all` query parameter is present but
#     not a boolean (e.g. `all=null`)", an invalid limit or cursor, or an
#     undeclared query parameter; the `all` parameter is `type: boolean` (so
#     only true/false are valid spellings).
#   - apis/openapi.yaml, getPath / revisePath / deactivatePath: 404 "No process
#     path exists with this id" with content application/problem+json and the
#     ProblemDetails schema (type, title, status, detail required).
Feature: Listing process paths and unknown-path problems
  As an operator or a consumer hydrating a cache
  I want list queries validated and not-found errors in one shape
  So that a client bug is told apart from an empty catalogue

  Background:
    Given the Process Path Management service is running

  @bdd
  Scenario: An empty catalogue lists as an empty page
    When the process paths are listed
    Then the request is accepted with status 200
    And the response body is an empty page
    And the process path list reports 0 paths

  @bdd
  Scenario: A paging parameter the collection does not declare is rejected, not ignored
    When the client sends GET "/process-paths?after=PICK"
    Then the request is rejected with status 400
    And the response problem reports type "unknown-query-parameter"
    When the client sends GET "/process-paths?offset=10"
    Then the request is rejected with status 400
    And the response problem reports type "unknown-query-parameter"

  @bdd
  Scenario: A limit outside 1 to 500 and a cursor this API did not issue are rejected
    When the client sends GET "/process-paths?limit=0"
    Then the request is rejected with status 400
    And the response problem reports type "invalid-limit"
    When the client sends GET "/process-paths?limit=501"
    Then the request is rejected with status 400
    And the response problem reports type "invalid-limit"
    When the client sends GET "/process-paths?cursor=not-a-cursor"
    Then the request is rejected with status 400
    And the response problem reports type "invalid-cursor"

  @bdd
  Scenario: An all query parameter that is not exactly true or false is rejected
    When the client sends GET "/process-paths?all=maybe"
    Then the request is rejected with status 400
    And the response problem reports type "invalid-query-parameter"
    And the response content type is "application/problem+json"
    When the client sends GET "/process-paths?all=F"
    Then the request is rejected with status 400
    And the response problem reports type "invalid-query-parameter"

  @bdd
  Scenario: Reading, revising and deactivating an unknown path all report path-not-found
    When the process path "MISSING" is requested
    Then the request is rejected with status 404
    And the response problem reports type "path-not-found"
    And the response content type is "application/problem+json"
    When the process path "MISSING" is revised with matchPrefix "missing" and capabilities "missing"
    Then the request is rejected with status 404
    And the response problem reports type "path-not-found"
    When the process path "MISSING" is deactivated
    Then the deactivation is rejected with status 404
    And the response problem reports type "path-not-found"
