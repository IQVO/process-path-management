# CPTSchedule rules taken from the context documentation that no earlier
# scenario pins (warehouse-docs docs/contexts/process-path-management, in-repo
# copy docs/docs/ddd/*.md):
#   - aggregate-design-canvas.md, CPTSchedule: "no status enum; it exists or it
#     does not", Revise "changed raises CPTScheduleChanged" / "identical, no-op",
#     "Revise replaces timezone and cutoffs wholesale", the enforced invariants
#     (timezone non-empty and IANA; at least one cutoff; cptId unique within the
#     schedule; localTime strict HH:MM; days Mon..Sun; shipMethod non-empty;
#     eligiblePathIds non-empty and Active) and the corrective policy "invalid
#     schedule -> 422, nothing stored or published", "concurrent writer -> 409,
#     reload and retry".
#   - use-cases.md, DefineCPTSchedule, "Checks, in order": (1) per-cutoff
#     validation in the HTTP adapter, (2) every distinct eligible path id must
#     be an existing Active path (ErrIneligiblePathId), (3) timezone, at least
#     one cutoff, unique cptId, (4) the version-guarded upsert.
#   - eventstorming.md, Process 2 and Hotspots: "siteId is never validated";
#     "whenever a schedule changes, publish the full snapshot, never a diff".
#   - ubiquitous-language.md: CPT Id is "unique within a schedule", Site Id is
#     "never validated" against facility-layout, Local Time is strict HH:MM
#     24-hour, Day of Week is Mon..Sun.
Feature: CPT schedule rules from the aggregate design
  As an operator and as order-management deriving a delivery promise
  I want every documented CPT schedule rule enforced in its documented order
  So that a half-valid or dangling schedule is never stored or published

  Background:
    Given the Process Path Management service is running
    And a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    And a process path "PACK" is already defined with matchPrefix "pack" and capabilities "pack"

  # --- the cutoff invariants, one row each ---------------------------------------

  @bdd
  Scenario Outline: A cutoff that breaks a field invariant is refused with that field's problem and nothing is stored
    When the CPT schedule for site "sp1" is defined with a cutoff at "<localTime>" on days "<days>" via "<shipMethod>" eligible for paths "<paths>"
    Then the request is rejected with status 422
    And the response problem reports type "<problem>"
    And "CPTScheduleChanged" was published 0 times for "sp1"
    When the CPT schedule for site "sp1" is requested
    Then the request is rejected with status 404

    Examples:
      | localTime | days   | shipMethod | paths | problem              |
      |           | Mon    | ground     | PICK  | empty-local-time     |
      | 3pm       | Mon    | ground     | PICK  | invalid-local-time   |
      | 15:00:00  | Mon    | ground     | PICK  | invalid-local-time   |
      | 9:00      | Mon    | ground     | PICK  | invalid-local-time   |
      | 23:60     | Mon    | ground     | PICK  | invalid-local-time   |
      | 15.00     | Mon    | ground     | PICK  | invalid-local-time   |
      | 1a:00     | Mon    | ground     | PICK  | invalid-local-time   |
      | 15:00     | mon    | ground     | PICK  | invalid-day-of-week  |
      | 15:00     | Monday | ground     | PICK  | invalid-day-of-week  |
      | 15:00     | Mon    |            | PICK  | empty-ship-method    |
      | 15:00     | Mon    | ground     |       | no-eligible-path-ids |

  @bdd
  Scenario: A cutoff without a cptId is refused as empty-cpt-id
    When the client sends PUT "/sites/sp1/cpt-schedule" with body:
      """
      {"timezone":"America/Sao_Paulo","cutoffs":[{"cptId":"","localTime":"15:00","daysOfWeek":["Mon"],"shipMethod":"ground","eligiblePathIds":["PICK"]}]}
      """
    Then the request is rejected with status 422
    And the response problem reports type "empty-cpt-id"
    And "CPTScheduleChanged" was published 0 times for "sp1"

  @bdd
  Scenario Outline: The edges of the HH:MM clock and of the week are accepted and read back exactly
    When the CPT schedule for site "sp1" is defined with a cutoff at "<localTime>" on days "<days>" via "ground" eligible for paths "PICK"
    Then the request is accepted with status 200
    And the response field "cutoffs.0.localTime" is "<localTime>"
    And the response field "cutoffs.0.daysOfWeek" is "<days>"
    And "CPTScheduleChanged" was published 1 time for "sp1"

    Examples:
      | localTime | days                        |
      | 00:00     | Mon                         |
      | 23:59     | Sun                         |
      | 09:05     | Mon,Tue,Wed,Thu,Fri,Sat,Sun |

  @bdd
  Scenario Outline: A timezone is accepted only as an IANA zone name
    When the CPT schedule for site "sp1" is defined with timezone "<timezone>" and cutoff "sp1-1500" eligible for paths "PICK"
    Then the request is accepted with status 200
    And the response field "timezone" is "<timezone>"

    Examples:
      | timezone      |
      | UTC           |
      | Asia/Tokyo    |
      | Europe/London |

  @bdd
  Scenario Outline: A name that is not an IANA zone is refused as invalid-timezone and nothing is stored
    When the CPT schedule for site "sp1" is defined with timezone "<timezone>" and cutoff "sp1-1500" eligible for paths "PICK"
    Then the request is rejected with status 422
    And the response problem reports type "invalid-timezone"
    And "CPTScheduleChanged" was published 0 times for "sp1"

    Examples:
      | timezone         |
      | Sao_Paulo        |
      | America/Atlantis |

  # --- the documented order of the checks -------------------------------------------

  @bdd
  Scenario Outline: The checks run in the documented order, so the earlier problem is the one reported
    When the client sends PUT "/sites/sp1/cpt-schedule" with body:
      """
      <body>
      """
    Then the request is rejected with status 422
    And the response problem reports type "<problem>"
    And "CPTScheduleChanged" was published 0 times for "sp1"

    Examples:
      | body                                                                                                                                                                                     | problem            |
      | {"timezone":"America/Sao_Paulo","cutoffs":[{"cptId":"sp1-1500","localTime":"25:00","daysOfWeek":["Mon"],"shipMethod":"ground","eligiblePathIds":["GHOST"]}]}                             | invalid-local-time |
      | {"timezone":"Mars/Olympus","cutoffs":[{"cptId":"sp1-1500","localTime":"15:00","daysOfWeek":["Mon"],"shipMethod":"ground","eligiblePathIds":["GHOST"]}]}                                 | ineligible-path-id |
      | {"timezone":"America/Sao_Paulo","cutoffs":[{"cptId":"a","localTime":"15:00","daysOfWeek":["Mon"],"shipMethod":"ground","eligiblePathIds":["GHOST"]},{"cptId":"a","localTime":"16:00","daysOfWeek":["Mon"],"shipMethod":"ground","eligiblePathIds":["PICK"]}]} | ineligible-path-id |
      | {"timezone":"Mars/Olympus","cutoffs":[]}                                                                                                                                                 | invalid-timezone   |

  @bdd
  Scenario: One unknown path in any cutoff rejects the whole schedule, even when the first cutoff is valid
    When the client sends PUT "/sites/sp1/cpt-schedule" with body:
      """
      {"timezone":"America/Sao_Paulo","cutoffs":[{"cptId":"sp1-1500","localTime":"15:00","daysOfWeek":["Mon"],"shipMethod":"ground","eligiblePathIds":["PICK"]},{"cptId":"sp1-1800","localTime":"18:00","daysOfWeek":["Mon"],"shipMethod":"express","eligiblePathIds":["PACK","GHOST"]}]}
      """
    Then the request is rejected with status 422
    And the response problem reports type "ineligible-path-id"
    And "CPTScheduleChanged" was published 0 times for "sp1"
    When the CPT schedule for site "sp1" is requested
    Then the request is rejected with status 404

  # --- identity: one schedule per site ---------------------------------------------------

  @bdd
  Scenario: A cptId only has to be unique within its own site's schedule
    When the CPT schedule for site "sp1" is defined with timezone "America/Sao_Paulo" and cutoff "dc-1500" eligible for paths "PICK"
    Then the request is accepted with status 200
    When the CPT schedule for site "sp2" is defined with timezone "America/New_York" and cutoff "dc-1500" eligible for paths "PACK"
    Then the request is accepted with status 200
    When the CPT schedule for site "sp1" is requested
    Then the response field "timezone" is "America/Sao_Paulo"
    And the response field "cutoffs.0.eligiblePathIds" is "PICK"
    When the CPT schedule for site "sp2" is requested
    Then the response field "timezone" is "America/New_York"
    And the response field "cutoffs.0.eligiblePathIds" is "PACK"
    And "CPTScheduleChanged" was published 1 time for "sp1"
    And "CPTScheduleChanged" was published 1 time for "sp2"

  @bdd
  Scenario: A site id is never validated, so a schedule for a site nobody registered is accepted
    When the CPT schedule for site "ZZ-NOT-A-SITE-9" is defined with timezone "America/Sao_Paulo" and cutoff "zz-1500" eligible for paths "PICK"
    Then the request is accepted with status 200
    And a "CPTScheduleChanged" event was published for site "ZZ-NOT-A-SITE-9"

  @bdd
  Scenario: Two cutoffs may list the same path, and a schedule lists every cutoff in the order given
    When the client sends PUT "/sites/sp1/cpt-schedule" with body:
      """
      {"timezone":"America/Sao_Paulo","cutoffs":[{"cptId":"sp1-1500","localTime":"15:00","daysOfWeek":["Mon","Tue"],"shipMethod":"ground","eligiblePathIds":["PICK"]},{"cptId":"sp1-1800","localTime":"18:00","daysOfWeek":["Sat"],"shipMethod":"express","eligiblePathIds":["PICK","PACK"]}]}
      """
    Then the request is accepted with status 200
    When the CPT schedule for site "sp1" is requested
    Then the CPT schedule response lists cutoff ids "sp1-1500,sp1-1800"
    And the response field "cutoffs.0.daysOfWeek" is "Mon,Tue"
    And the response field "cutoffs.1.localTime" is "18:00"
    And the response field "cutoffs.1.shipMethod" is "express"
    And the response field "cutoffs.1.eligiblePathIds" is "PICK,PACK"
    When the "CPTScheduleChanged" event for "sp1" is published to the integration topic
    Then the CloudEvent field "data.cutoffs.1.cpt_id" is "sp1-1800"
    And the CloudEvent field "data.cutoffs.1.eligible_path_ids" is "PICK,PACK"

  # --- Revise: what counts as a change -----------------------------------------------------

  @bdd
  Scenario Outline: Changing any single schedule field is a real revision that publishes the full new snapshot
    Given the client has successfully sent PUT "/sites/sp1/cpt-schedule" with body:
      """
      {"timezone":"America/Sao_Paulo","cutoffs":[{"cptId":"sp1-1500","localTime":"15:00","daysOfWeek":["Mon","Tue"],"shipMethod":"ground","eligiblePathIds":["PICK"]}]}
      """
    When the client sends PUT "/sites/sp1/cpt-schedule" with body:
      """
      <body>
      """
    Then the request is accepted with status 200
    And "CPTScheduleChanged" was published 2 times for "sp1"
    And the "CPTScheduleChanged" event for "sp1" carries timezone "<timezone>" and cutoff ids "<ids>"

    Examples:
      | change         | body                                                                                                                                                                                                                  | timezone           | ids                |
      | timezone       | {"timezone":"America/New_York","cutoffs":[{"cptId":"sp1-1500","localTime":"15:00","daysOfWeek":["Mon","Tue"],"shipMethod":"ground","eligiblePathIds":["PICK"]}]}                                                       | America/New_York   | sp1-1500           |
      | cptId          | {"timezone":"America/Sao_Paulo","cutoffs":[{"cptId":"sp1-1600","localTime":"15:00","daysOfWeek":["Mon","Tue"],"shipMethod":"ground","eligiblePathIds":["PICK"]}]}                                                      | America/Sao_Paulo  | sp1-1600           |
      | local time     | {"timezone":"America/Sao_Paulo","cutoffs":[{"cptId":"sp1-1500","localTime":"16:00","daysOfWeek":["Mon","Tue"],"shipMethod":"ground","eligiblePathIds":["PICK"]}]}                                                      | America/Sao_Paulo  | sp1-1500           |
      | days of week   | {"timezone":"America/Sao_Paulo","cutoffs":[{"cptId":"sp1-1500","localTime":"15:00","daysOfWeek":["Mon","Tue","Wed"],"shipMethod":"ground","eligiblePathIds":["PICK"]}]}                                                | America/Sao_Paulo  | sp1-1500           |
      | ship method    | {"timezone":"America/Sao_Paulo","cutoffs":[{"cptId":"sp1-1500","localTime":"15:00","daysOfWeek":["Mon","Tue"],"shipMethod":"express","eligiblePathIds":["PICK"]}]}                                                     | America/Sao_Paulo  | sp1-1500           |
      | swapped day    | {"timezone":"America/Sao_Paulo","cutoffs":[{"cptId":"sp1-1500","localTime":"15:00","daysOfWeek":["Mon","Wed"],"shipMethod":"ground","eligiblePathIds":["PICK"]}]}                                                      | America/Sao_Paulo  | sp1-1500           |
      | swapped path   | {"timezone":"America/Sao_Paulo","cutoffs":[{"cptId":"sp1-1500","localTime":"15:00","daysOfWeek":["Mon","Tue"],"shipMethod":"ground","eligiblePathIds":["PACK"]}]}                                                      | America/Sao_Paulo  | sp1-1500           |
      | eligible paths | {"timezone":"America/Sao_Paulo","cutoffs":[{"cptId":"sp1-1500","localTime":"15:00","daysOfWeek":["Mon","Tue"],"shipMethod":"ground","eligiblePathIds":["PICK","PACK"]}]}                                              | America/Sao_Paulo  | sp1-1500           |
      | a second cutoff | {"timezone":"America/Sao_Paulo","cutoffs":[{"cptId":"sp1-1500","localTime":"15:00","daysOfWeek":["Mon","Tue"],"shipMethod":"ground","eligiblePathIds":["PICK"]},{"cptId":"sp1-1800","localTime":"18:00","daysOfWeek":["Mon"],"shipMethod":"ground","eligiblePathIds":["PICK"]}]} | America/Sao_Paulo | sp1-1500,sp1-1800 |
      | dropping a cutoff | {"timezone":"America/Sao_Paulo","cutoffs":[{"cptId":"sp1-1800","localTime":"18:00","daysOfWeek":["Mon"],"shipMethod":"ground","eligiblePathIds":["PICK"]}]}                                                         | America/Sao_Paulo  | sp1-1800           |

  @bdd
  Scenario: A no-op PUT leaves the schedule's timestamps alone and a real change moves only updatedAt
    Given the CPT schedule for site "sp1" is already defined with timezone "America/Sao_Paulo" and cutoff "sp1-1500" eligible for paths "PICK"
    And the clock reads "2026-09-07T09:00:00Z"
    When the CPT schedule for site "sp1" is defined with timezone "America/Sao_Paulo" and cutoff "sp1-1500" eligible for paths "PICK"
    Then the request is accepted with status 200
    And the response field "createdAt" is the timestamp "2026-09-06T00:00:00Z"
    And the response field "updatedAt" is the timestamp "2026-09-06T00:00:00Z"
    And "CPTScheduleChanged" was published 1 time for "sp1"
    When the CPT schedule for site "sp1" is defined with timezone "America/Sao_Paulo" and cutoff "sp1-1500" eligible for paths "PICK,PACK"
    Then the response field "createdAt" is the timestamp "2026-09-06T00:00:00Z"
    And the response field "updatedAt" is the timestamp "2026-09-07T09:00:00Z"
    And the "CPTScheduleChanged" event for "sp1" occurred at "2026-09-07T09:00:00Z"
