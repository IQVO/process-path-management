# MCP behavioral evals for the process-path-management tool surface.
#
# Each scenario drives tools/call over the REAL Streamable HTTP handler
# with a connected SDK client — exactly the call a model host makes — and
# pins the structured result. Arguments are deliberately model-realistic:
# extra keys, wrong types, unknown ids. This adapter is read-only (there
# is no write tool and therefore no side-effect step to pin); every tool
# here serves the same read models the REST API serves.
#
# Derived from the tool contracts documented in:
#   - internal/adapters/inbound/mcp/tools.go (tool descriptions and
#     semantics: not-found is a tool-level error, activeOnly defaults to
#     true) and report_tool.go (from/to required, granularity 'day')
#   - docs/docs/mcp/governance-charter.md (§2 tool curation: intent-level
#     tools; §10.4 the eval gate these scenarios belong to)
#   - apis/openapi.yaml GET /paths/{id} and GET /paths — the same read
#     models the catalogue tools serve.

Feature: MCP tool behavioral evals
  The process-path-management MCP tools expose this bounded context to AI
  agents: the process-path catalogue (one path's full definition, the
  list of paths), a site's CPT schedule, and the catalogue-growth report.
  An agent relying on them must get the same semantics the REST API
  guarantees, through the schema-decoded argument path a model host
  actually uses.

  Background:
    Given the MCP server is running with the canonical eval state (PICK active, REBIN deactivated, sp1 CPT schedule)

  Scenario: A known path id returns the path's full definition
    When I call the tool "get_process_path" with argument "pathId" = "PICK"
    Then the tool call succeeds
    And the structured result field "pathId" is "PICK"
    And the structured result field "matchPrefix" is "pick"
    And the structured result field "direct" is true
    And the structured result field "status" is "ACTIVE"
    And the structured result field "active" is true
    And the structured result field "cycleTimeP95" is "2h0m0s"
    And the structured result field "requiredCapabilities" lists 1 entries

  Scenario: A deactivated path is still readable by id, flagged inactive
    Deactivation hides a path from the default list view but does not
    delete it: a get by its canonical id returns the definition with
    status DEACTIVATED so an agent can reason about retired paths.
    When I call the tool "get_process_path" with argument "pathId" = "REBIN"
    Then the tool call succeeds
    And the structured result field "pathId" is "REBIN"
    And the structured result field "status" is "DEACTIVATED"
    And the structured result field "active" is false

  Scenario: An unknown path id is a clean tool error
    When I call the tool "get_process_path" with argument "pathId" = "SLAM-NEVER-DEFINED"
    Then the tool call reports a problem mentioning "not found"

  Scenario: An empty path id is a clean tool error
    When I call the tool "get_process_path" with argument "pathId" = ""
    Then the tool call reports a problem mentioning "pathId"

  Scenario: The catalogue list defaults to active-only
    When I call the tool "list_process_paths" with no arguments
    Then the tool call succeeds
    And the structured result field "paths" lists 1 entries
    And the first entry of "paths" has "pathId" = "PICK"
    And the first entry of "paths" has "status" = "ACTIVE"

  Scenario: The catalogue list includes deactivated paths on request
    When I call the tool "list_process_paths" with argument "activeOnly" = false
    Then the tool call succeeds
    And the structured result field "paths" lists 2 entries

  Scenario: A site's CPT schedule returns timezone and cutoffs
    When I call the tool "get_cpt_schedule" with argument "siteId" = "sp1"
    Then the tool call succeeds
    And the structured result field "siteId" is "sp1"
    And the structured result field "timezone" is "America/Sao_Paulo"
    And the structured result field "cutoffs" lists 1 entries
    And the first entry of "cutoffs" has "cptId" = "sp1-1500"
    And the first entry of "cutoffs" has "localTime" = "15:00"
    And the first entry of "cutoffs" has "shipMethod" = "ground"

  Scenario: An unknown site's CPT schedule is a clean tool error
    When I call the tool "get_cpt_schedule" with argument "siteId" = "no-such-site"
    Then the tool call reports a problem mentioning "not found"

  Scenario: An empty site id is a clean tool error
    When I call the tool "get_cpt_schedule" with argument "siteId" = ""
    Then the tool call reports a problem mentioning "siteId"

  Scenario: The catalogue-growth report returns the reports service's rows
    When I call the tool "get_catalogue_growth_report" with arguments
      | from        | 2026-09-01T00:00:00Z |
      | to          | 2026-09-08T00:00:00Z |
      | granularity | day                  |
    Then the tool call succeeds
    And the structured result field "rows" lists 1 entries
    And the first entry of "rows" has "dayBucket" = "2026-09-01T00:00:00Z"
    And the first entry of "rows" has "pathsDefined" = 3

  Scenario: The catalogue-growth report requires its time window
    When I call the tool "get_catalogue_growth_report" with arguments
      | from        | 2026-09-01T00:00:00Z |
      | granularity | day                  |
    Then the tool call reports a problem mentioning "required"

  Scenario: Model chatter in the arguments is rejected, not ignored
    The typed tool schemas are strict (additionalProperties: false, the
    SDK default): a host forwarding stray model-generated keys gets a
    clean schema validation error rather than a silent ignore.
    When I call the tool "get_process_path" with arguments
      | pathId        | PICK                    |
      | model_chatter | pretty sure it's PICK   |
      | step          | 2                       |
    Then the tool call reports a problem mentioning "model_chatter"

  Scenario: A wrong-typed argument is rejected without coercion
    When I call the tool "get_process_path" with argument "pathId" = 42
    Then the tool call does not succeed silently

  Scenario: A wrong-typed boolean flag is rejected without coercion
    activeOnly is a boolean; a model answering "yes" must not be
    string-coerced to a truthy value.
    When I call the tool "list_process_paths" with argument "activeOnly" = "yes"
    Then the tool call does not succeed silently
