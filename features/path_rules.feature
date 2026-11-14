# ProcessPath rules taken from the context documentation that no earlier
# scenario pins (warehouse-docs docs/contexts/process-path-management, in-repo
# copy docs/docs/ddd/*.md):
#   - aggregate-design-canvas.md, ProcessPath: state transitions ("Revise -
#     changed or no-op", "Deactivate - idempotent no-op", "Revise returns
#     ErrPathDeactivated"), enforced invariants (matchPrefix lower-case and never
#     coerced, cycleTimeP95 strictly positive, `direct` and
#     `destinationLocationRole` immutable, an id is never re-defined, no lost
#     update), corrective policies (invalid input persists and publishes
#     nothing; 409 concurrent-modification: reload and retry; a CPT-listed path
#     is refused and the schedule is never pruned or republished).
#   - use-cases.md, DefinePath / RevisePath / DeactivatePath / ListPaths: the
#     ordered checks, "ProcessPathUpdated, only when matchPrefix, capabilities,
#     cycle time, eligibility or intake actually changed", `intake` absent or
#     null keeps the current one and `[]` clears it, ListPaths pages by id.
#   - ubiquitous-language.md: PathId is case-sensitive, MatchPrefix is validated
#     lower-case and stored as given, CycleTimeP95 is a positive Go duration,
#     Intake is a SET (distinct values, canonical order).
#   - domain-events.md: `direct` is absent from the payload when false,
#     ProcessPathUpdated carries the full current definition (not a diff).
#   - troubleshooting.md: every 4xx/5xx slug of the Writes table.
Feature: Process path rules from the aggregate design
  As an operator and as a consumer of the process-path catalogue
  I want every documented rule of the ProcessPath aggregate enforced at its boundary
  So that the catalogue never holds, or publishes, a definition the documents forbid

  Background:
    Given the Process Path Management service is running

  # --- identity ---------------------------------------------------------------

  @bdd
  Scenario: A path id is case-sensitive, so PICK and pick are two different paths
    Given a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    When the process path "pick" is requested
    Then the request is rejected with status 404
    And the response problem reports type "path-not-found"
    When a process path "pick" is defined with matchPrefix "pick" and capabilities "pick"
    Then the request is accepted with status 201
    When all process paths are listed including deactivated ones
    Then the process path list reports 2 paths

  @bdd
  Scenario: A refused duplicate define leaves the original definition and its single event untouched
    Given a process path "SLAM" is already defined with matchPrefix "slam" and capabilities "slam"
    When the client sends POST "/process-paths" with body:
      """
      {"pathId":"SLAM","matchPrefix":"slam-new","direct":false,"requiredCapabilities":["slam","hazmat"],"cycleTimeP95":"5h"}
      """
    Then the request is rejected with status 409
    And the response problem reports type "path-already-exists"
    And the process path "SLAM" still has matchPrefix "slam" and cycleTimeP95 "2h"
    And "ProcessPathCreated" was published 1 time for "SLAM"

  # --- matchPrefix: lower-case, never coerced ------------------------------------

  @bdd
  Scenario Outline: A lower-case matchPrefix is stored exactly as declared
    When a process path "PICK" is defined with matchPrefix "<matchPrefix>" and capabilities "pick"
    Then the request is accepted with status 201
    And the response field "matchPrefix" is "<matchPrefix>"

    Examples:
      | matchPrefix |
      | pick        |
      | pick-zone-a |
      | pick_b2     |
      | 2nd-floor   |

  @bdd
  Scenario Outline: A matchPrefix with any upper-case letter is refused, never lower-cased, and nothing is stored
    When a process path "PICK" is defined with matchPrefix "<matchPrefix>" and capabilities "pick"
    Then the request is rejected with status 422
    And the response problem reports type "match-prefix-not-lowercase"
    And no events were published
    When the process path "PICK" is requested
    Then the request is rejected with status 404

    Examples:
      | matchPrefix |
      | pickA       |
      | PICK        |
      | pick-Zone-a |
      | Épick       |

  # --- destinationLocationRole: a closed set ---------------------------------------

  @bdd
  Scenario Outline: Each destination role of the closed set is accepted
    When a process path "QC" is defined with matchPrefix "qc", capabilities "qc", and destinationLocationRole "<role>"
    Then the request is accepted with status 201
    And the process path response reports destinationLocationRole "<role>"

    Examples:
      | role       |
      | Drop       |
      | WorkCenter |
      | Shipping   |

  @bdd
  Scenario Outline: A destination role outside the closed set is refused whatever its spelling
    When a process path "QC" is defined with matchPrefix "qc", capabilities "qc", and destinationLocationRole "<role>"
    Then the request is rejected with status 422
    And the response problem reports type "invalid-destination-location-role"
    And no events were published
    When the process path "QC" is requested
    Then the request is rejected with status 404

    Examples:
      | role        |
      | drop        |
      | WORKCENTER  |
      | Work Center |
      | shipping    |

  # --- cycleTimeP95: a positive Go duration ------------------------------------------

  @bdd
  Scenario Outline: A positive Go duration is accepted and read back in its canonical form
    When a process path "PICK" is defined with matchPrefix "pick", capabilities "pick", and cycleTimeP95 "<declared>"
    Then the request is accepted with status 201
    And the response field "cycleTimeP95" is "<canonical>"
    When the "ProcessPathCreated" event for "PICK" is published to the integration topic
    Then the CloudEvent field "data.cycle_time_p95" is "<canonical>"

    Examples:
      | declared | canonical |
      | 90m      | 1h30m0s   |
      | 1ns      | 1ns       |
      | 36h      | 36h0m0s   |
      | 2h45m30s | 2h45m30s  |

  @bdd
  Scenario Outline: A cycleTimeP95 that is not a duration is refused as invalid-cycle-time-p95
    When a process path "PICK" is defined with matchPrefix "pick", capabilities "pick", and cycleTimeP95 "<declared>"
    Then the request is rejected with status 422
    And the response problem reports type "invalid-cycle-time-p95"
    And no events were published

    Examples:
      | declared  |
      |           |
      | 90        |
      | 2 hours   |
      | tomorrow  |

  @bdd
  Scenario: A missing cycleTimeP95 is the same invalid-cycle-time-p95 problem, not a default
    When the client sends POST "/process-paths" with body:
      """
      {"pathId":"PICK","matchPrefix":"pick","direct":true,"requiredCapabilities":["pick"]}
      """
    Then the request is rejected with status 422
    And the response problem reports type "invalid-cycle-time-p95"
    And no events were published

  # --- eligibility and direct ---------------------------------------------------------

  @bdd
  Scenario: A path declares its whole eligibility and every field reads back through GET
    Given the client has successfully sent POST "/process-paths" with body:
      """
      {"pathId":"SINGLES","matchPrefix":"singles","direct":true,"requiredCapabilities":["pick"],"cycleTimeP95":"90m","eligibility":{"maxUnitsPerLine":1,"requiredProductAttributes":["giftWrap","fragile"],"excludedProductAttributes":["hazmat"],"nonSortable":true}}
      """
    When the process path "SINGLES" is requested
    Then the request is accepted with status 200
    And the response field "eligibility.maxUnitsPerLine" is "1"
    And the response field "eligibility.requiredProductAttributes" is "giftWrap,fragile"
    And the response field "eligibility.excludedProductAttributes" is "hazmat"
    And the response field "eligibility.nonSortable" is "true"

  @bdd
  Scenario: A path declared without eligibility is fully permissive, never unbounded by accident
    When a process path "PICK" is defined with matchPrefix "pick" and capabilities "pick"
    Then the request is accepted with status 201
    And the response field "eligibility" is present
    And the response field "eligibility.maxUnitsPerLine" is absent
    And the response field "eligibility.requiredProductAttributes" is absent
    And the response field "eligibility.excludedProductAttributes" is absent
    And the response field "eligibility.nonSortable" is absent

  @bdd
  Scenario: A path defined with direct false reports false and omits direct from the Kafka payload
    When the client sends POST "/process-paths" with body:
      """
      {"pathId":"PACK","matchPrefix":"pack","direct":false,"requiredCapabilities":["pack"],"cycleTimeP95":"2h"}
      """
    Then the request is accepted with status 201
    And the response field "direct" is "false"
    When the "ProcessPathCreated" event for "PACK" is published to the integration topic
    Then the CloudEvent field "data.direct" is absent
    And the CloudEvent field "data.match_prefix" is "pack"

  # --- intake is a set ------------------------------------------------------------------

  @bdd
  Scenario: Every demand source is accepted and the set is kept in canonical order on REST and on the wire
    When the client sends POST "/process-paths" with body:
      """
      {"pathId":"ANY","matchPrefix":"any","direct":true,"requiredCapabilities":["pick"],"cycleTimeP95":"2h","intake":["STOCK_TRANSFER","SLOT_RELOCATION","REPLENISHMENT","INVENTORY_CONTROL","CUSTOMER_ORDER"]}
      """
    Then the request is accepted with status 201
    And the response field "intake" is "CUSTOMER_ORDER,INVENTORY_CONTROL,REPLENISHMENT,SLOT_RELOCATION,STOCK_TRANSFER"
    When the "ProcessPathCreated" event for "ANY" is published to the integration topic
    Then the CloudEvent field "data.intake" is "CUSTOMER_ORDER,INVENTORY_CONTROL,REPLENISHMENT,SLOT_RELOCATION,STOCK_TRANSFER"

  @bdd
  Scenario Outline: A blank, mis-cased or repeated demand source is refused at define and nothing is stored
    When the client sends POST "/process-paths" with body:
      """
      {"pathId":"PICK","matchPrefix":"pick","direct":true,"requiredCapabilities":["pick"],"cycleTimeP95":"2h","intake":<intake>}
      """
    Then the request is rejected with status 422
    And the response problem reports type "invalid-intake"
    And no events were published
    When the process path "PICK" is requested
    Then the request is rejected with status 404

    Examples:
      | intake                                           |
      | [""]                                             |
      | ["customer_order"]                               |
      | ["CUSTOMER_ORDER","STOCK_TRANSFER","CUSTOMER_ORDER"] |

  @bdd
  Scenario Outline: A bad intake on a revision is refused and the stored intake survives
    Given the client has successfully sent POST "/process-paths" with body:
      """
      {"pathId":"PICK","matchPrefix":"pick","direct":true,"requiredCapabilities":["pick"],"cycleTimeP95":"2h","intake":["CUSTOMER_ORDER"]}
      """
    When the client sends PUT "/process-paths/PICK" with body:
      """
      {"matchPrefix":"pick","requiredCapabilities":["pick"],"cycleTimeP95":"2h","intake":<intake>}
      """
    Then the request is rejected with status 422
    And the response problem reports type "invalid-intake"
    And "ProcessPathUpdated" was published 0 times for "PICK"
    When the process path "PICK" is requested
    Then the response field "intake" is "CUSTOMER_ORDER"

    Examples:
      | intake                                  |
      | ["ORDERS"]                              |
      | [""]                                    |
      | ["customer_order"]                      |
      | ["CUSTOMER_ORDER","CUSTOMER_ORDER"]     |

  @bdd
  Scenario: An intake of null on a revision keeps the current intake, like an absent one
    Given the client has successfully sent POST "/process-paths" with body:
      """
      {"pathId":"PICK","matchPrefix":"pick","direct":true,"requiredCapabilities":["pick"],"cycleTimeP95":"2h","intake":["CUSTOMER_ORDER"]}
      """
    When the client sends PUT "/process-paths/PICK" with body:
      """
      {"matchPrefix":"pick","requiredCapabilities":["pick"],"cycleTimeP95":"2h","intake":null}
      """
    Then the request is accepted with status 200
    And the response field "intake" is "CUSTOMER_ORDER"
    And "ProcessPathUpdated" was published 0 times for "PICK"

  # --- Revise: immutable fields ----------------------------------------------------------

  @bdd
  Scenario Outline: A revision cannot change an immutable field, and sending it is no change at all
    Given the client has successfully sent POST "/process-paths" with body:
      """
      {"pathId":"PICK","matchPrefix":"pick","direct":true,"requiredCapabilities":["pick"],"destinationLocationRole":"Drop","cycleTimeP95":"2h"}
      """
    When the client sends PUT "/process-paths/PICK" with body:
      """
      {"matchPrefix":"pick","requiredCapabilities":["pick"],"cycleTimeP95":"2h","<field>":<value>}
      """
    Then the request is accepted with status 200
    And the response field "<field>" is "<kept>"
    And "ProcessPathUpdated" was published 0 times for "PICK"

    Examples:
      | field                   | value      | kept |
      | direct                  | false      | true |
      | destinationLocationRole | "Shipping" | Drop |
      | pathId                  | "SLAM"     | PICK |

  # --- Revise: what counts as a change ---------------------------------------------------

  @bdd
  Scenario Outline: Each revisable field changed on its own is a real change that publishes the full new state
    Given the client has successfully sent POST "/process-paths" with body:
      """
      {"pathId":"PICK","matchPrefix":"pick","direct":true,"requiredCapabilities":["pick"],"cycleTimeP95":"2h","eligibility":{"maxUnitsPerLine":1,"requiredProductAttributes":["giftWrap"],"excludedProductAttributes":["hazmat"]},"intake":["CUSTOMER_ORDER"]}
      """
    And the clock reads "2026-09-07T12:00:00Z"
    When the client sends PUT "/process-paths/PICK" with body:
      """
      <body>
      """
    Then the request is accepted with status 200
    And the response field "<field>" is "<value>"
    And the response field "createdAt" is the timestamp "2026-09-06T00:00:00Z"
    And the response field "updatedAt" is the timestamp "2026-09-07T12:00:00Z"
    And "ProcessPathUpdated" was published 1 time for "PICK"

    Examples:
      | change                | body                                                                                                                                                                                                | field                                | value                         |
      | matchPrefix           | {"matchPrefix":"pick-zone-a","requiredCapabilities":["pick"],"cycleTimeP95":"2h","eligibility":{"maxUnitsPerLine":1,"requiredProductAttributes":["giftWrap"],"excludedProductAttributes":["hazmat"]},"intake":["CUSTOMER_ORDER"]} | matchPrefix                          | pick-zone-a                   |
      | capabilities          | {"matchPrefix":"pick","requiredCapabilities":["pick","hazmat"],"cycleTimeP95":"2h","eligibility":{"maxUnitsPerLine":1,"requiredProductAttributes":["giftWrap"],"excludedProductAttributes":["hazmat"]},"intake":["CUSTOMER_ORDER"]} | requiredCapabilities                 | pick,hazmat                   |
      | cycle time            | {"matchPrefix":"pick","requiredCapabilities":["pick"],"cycleTimeP95":"3h","eligibility":{"maxUnitsPerLine":1,"requiredProductAttributes":["giftWrap"],"excludedProductAttributes":["hazmat"]},"intake":["CUSTOMER_ORDER"]} | cycleTimeP95                         | 3h0m0s                        |
      | max units per line    | {"matchPrefix":"pick","requiredCapabilities":["pick"],"cycleTimeP95":"2h","eligibility":{"maxUnitsPerLine":2,"requiredProductAttributes":["giftWrap"],"excludedProductAttributes":["hazmat"]},"intake":["CUSTOMER_ORDER"]} | eligibility.maxUnitsPerLine          | 2                             |
      | unbounded units       | {"matchPrefix":"pick","requiredCapabilities":["pick"],"cycleTimeP95":"2h","eligibility":{"requiredProductAttributes":["giftWrap"],"excludedProductAttributes":["hazmat"]},"intake":["CUSTOMER_ORDER"]} | eligibility.requiredProductAttributes | giftWrap                      |
      | required attributes   | {"matchPrefix":"pick","requiredCapabilities":["pick"],"cycleTimeP95":"2h","eligibility":{"maxUnitsPerLine":1,"requiredProductAttributes":["giftWrap","fragile"],"excludedProductAttributes":["hazmat"]},"intake":["CUSTOMER_ORDER"]} | eligibility.requiredProductAttributes | giftWrap,fragile              |
      | excluded attributes   | {"matchPrefix":"pick","requiredCapabilities":["pick"],"cycleTimeP95":"2h","eligibility":{"maxUnitsPerLine":1,"requiredProductAttributes":["giftWrap"],"excludedProductAttributes":["hazmat","fragile"]},"intake":["CUSTOMER_ORDER"]} | eligibility.excludedProductAttributes | hazmat,fragile                |
      | non-sortable          | {"matchPrefix":"pick","requiredCapabilities":["pick"],"cycleTimeP95":"2h","eligibility":{"maxUnitsPerLine":1,"requiredProductAttributes":["giftWrap"],"excludedProductAttributes":["hazmat"],"nonSortable":true},"intake":["CUSTOMER_ORDER"]} | eligibility.nonSortable              | true                          |
      | swapped required attribute | {"matchPrefix":"pick","requiredCapabilities":["pick"],"cycleTimeP95":"2h","eligibility":{"maxUnitsPerLine":1,"requiredProductAttributes":["fragile"],"excludedProductAttributes":["hazmat"]},"intake":["CUSTOMER_ORDER"]} | eligibility.requiredProductAttributes | fragile                       |
      | swapped excluded attribute | {"matchPrefix":"pick","requiredCapabilities":["pick"],"cycleTimeP95":"2h","eligibility":{"maxUnitsPerLine":1,"requiredProductAttributes":["giftWrap"],"excludedProductAttributes":["fragile"]},"intake":["CUSTOMER_ORDER"]} | eligibility.excludedProductAttributes | fragile                       |
      | swapped capability    | {"matchPrefix":"pick","requiredCapabilities":["hazmat"],"cycleTimeP95":"2h","eligibility":{"maxUnitsPerLine":1,"requiredProductAttributes":["giftWrap"],"excludedProductAttributes":["hazmat"]},"intake":["CUSTOMER_ORDER"]} | requiredCapabilities                 | hazmat                        |
      | swapped demand source | {"matchPrefix":"pick","requiredCapabilities":["pick"],"cycleTimeP95":"2h","eligibility":{"maxUnitsPerLine":1,"requiredProductAttributes":["giftWrap"],"excludedProductAttributes":["hazmat"]},"intake":["STOCK_TRANSFER"]} | intake                               | STOCK_TRANSFER                |
      | intake                | {"matchPrefix":"pick","requiredCapabilities":["pick"],"cycleTimeP95":"2h","eligibility":{"maxUnitsPerLine":1,"requiredProductAttributes":["giftWrap"],"excludedProductAttributes":["hazmat"]},"intake":["CUSTOMER_ORDER","STOCK_TRANSFER"]} | intake                               | CUSTOMER_ORDER,STOCK_TRANSFER |

  @bdd
  Scenario Outline: A revision that restates the stored definition, however it is spelled, is a no-op
    Given the client has successfully sent POST "/process-paths" with body:
      """
      {"pathId":"PICK","matchPrefix":"pick","direct":true,"requiredCapabilities":["pick"],"cycleTimeP95":"2h","eligibility":{"maxUnitsPerLine":1,"requiredProductAttributes":["giftWrap"],"excludedProductAttributes":["hazmat"]},"intake":["CUSTOMER_ORDER","STOCK_TRANSFER"]}
      """
    And the clock reads "2026-09-07T12:00:00Z"
    When the client sends PUT "/process-paths/PICK" with body:
      """
      <body>
      """
    Then the request is accepted with status 200
    And the response field "updatedAt" is the timestamp "2026-09-06T00:00:00Z"
    And "ProcessPathUpdated" was published 0 times for "PICK"

    Examples:
      | spelling                    | body                                                                                                                                                                                                                       |
      | the same body               | {"matchPrefix":"pick","requiredCapabilities":["pick"],"cycleTimeP95":"2h","eligibility":{"maxUnitsPerLine":1,"requiredProductAttributes":["giftWrap"],"excludedProductAttributes":["hazmat"]},"intake":["CUSTOMER_ORDER","STOCK_TRANSFER"]} |
      | the intake in another order | {"matchPrefix":"pick","requiredCapabilities":["pick"],"cycleTimeP95":"2h","eligibility":{"maxUnitsPerLine":1,"requiredProductAttributes":["giftWrap"],"excludedProductAttributes":["hazmat"]},"intake":["STOCK_TRANSFER","CUSTOMER_ORDER"]} |
      | the cycle time in minutes   | {"matchPrefix":"pick","requiredCapabilities":["pick"],"cycleTimeP95":"120m","eligibility":{"maxUnitsPerLine":1,"requiredProductAttributes":["giftWrap"],"excludedProductAttributes":["hazmat"]},"intake":["CUSTOMER_ORDER","STOCK_TRANSFER"]} |
      | the canonical cycle time    | {"matchPrefix":"pick","requiredCapabilities":["pick"],"cycleTimeP95":"2h0m0s","eligibility":{"maxUnitsPerLine":1,"requiredProductAttributes":["giftWrap"],"excludedProductAttributes":["hazmat"]},"intake":["CUSTOMER_ORDER","STOCK_TRANSFER"]} |

  @bdd
  Scenario Outline: A rejected revision body is refused with its own problem, and the stored path and event stream stay as they were
    Given a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    When the client sends PUT "/process-paths/PICK" with body:
      """
      <body>
      """
    Then the request is rejected with status 422
    And the response problem reports type "<problem>"
    And the process path "PICK" still has matchPrefix "pick" and cycleTimeP95 "2h"
    And "ProcessPathUpdated" was published 0 times for "PICK"

    Examples:
      | body                                                                  | problem                    |
      | {"matchPrefix":"","requiredCapabilities":["pick"],"cycleTimeP95":"2h"} | empty-match-prefix         |
      | {"matchPrefix":"pick","requiredCapabilities":[],"cycleTimeP95":"2h"}   | no-required-capabilities   |
      | {"matchPrefix":"pick","requiredCapabilities":["pick"]}                 | invalid-cycle-time-p95     |
      | {"matchPrefix":"pick","requiredCapabilities":["pick"],"cycleTimeP95":"-1ns"} | invalid-cycle-time-p95 |

  # --- the published state of a revision ----------------------------------------------------

  @bdd
  Scenario: A revision publishes the full current definition, not a diff
    Given the client has successfully sent POST "/process-paths" with body:
      """
      {"pathId":"SINGLES","matchPrefix":"singles","direct":true,"requiredCapabilities":["pick"],"destinationLocationRole":"Shipping","cycleTimeP95":"90m","eligibility":{"maxUnitsPerLine":1,"nonSortable":true},"intake":["CUSTOMER_ORDER"]}
      """
    When the client sends PUT "/process-paths/SINGLES" with body:
      """
      {"matchPrefix":"singles-b","requiredCapabilities":["pick"],"cycleTimeP95":"90m","eligibility":{"maxUnitsPerLine":1,"nonSortable":true}}
      """
    Then the request is accepted with status 200
    When the "ProcessPathUpdated" event for "SINGLES" is published to the integration topic
    Then the CloudEvent field "type" is "com.warehouse.wes.process-path-management.processpath.ProcessPathUpdated"
    And the CloudEvent field "data.path_id" is "SINGLES"
    And the CloudEvent field "data.match_prefix" is "singles-b"
    And the CloudEvent field "data.direct" is "true"
    And the CloudEvent field "data.required_capabilities" is "pick"
    And the CloudEvent field "data.destination_location_role" is "Shipping"
    And the CloudEvent field "data.cycle_time_p95" is "1h30m0s"
    And the CloudEvent field "data.eligibility.max_units_per_line" is "1"
    And the CloudEvent field "data.eligibility.non_sortable" is "true"
    And the CloudEvent field "data.intake" is "CUSTOMER_ORDER"

  @bdd
  Scenario: Clearing the intake publishes an empty array and a permissive eligibility publishes an empty object
    Given the client has successfully sent POST "/process-paths" with body:
      """
      {"pathId":"PACK","matchPrefix":"pack","direct":true,"requiredCapabilities":["pack"],"cycleTimeP95":"2h","intake":["CUSTOMER_ORDER"],"eligibility":{"maxUnitsPerLine":3}}
      """
    When the client sends PUT "/process-paths/PACK" with body:
      """
      {"matchPrefix":"pack","requiredCapabilities":["pack"],"cycleTimeP95":"2h","intake":[]}
      """
    Then the request is accepted with status 200
    When the "ProcessPathUpdated" event for "PACK" is published to the integration topic
    Then the CloudEvent field "data.intake" is present
    And the CloudEvent field "data.intake" is ""
    And the CloudEvent field "data.eligibility" is present
    And the CloudEvent field "data.eligibility.max_units_per_line" is absent

  # --- Deactivate ---------------------------------------------------------------------------

  @bdd
  Scenario: A deactivated path stays readable with its definition, and a repeated deactivation changes nothing
    Given a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick,hazmat"
    And the clock reads "2026-09-07T08:00:00Z"
    And the process path "PICK" is already deactivated
    And the clock reads "2026-09-08T09:00:00Z"
    When the process path "PICK" is deactivated
    Then the deactivation is accepted with status 204
    When the process path "PICK" is requested
    Then the request is accepted with status 200
    And the response field "status" is "DEACTIVATED"
    And the response field "matchPrefix" is "pick"
    And the response field "requiredCapabilities" is "pick,hazmat"
    And the response field "createdAt" is the timestamp "2026-09-06T00:00:00Z"
    And the response field "updatedAt" is the timestamp "2026-09-07T08:00:00Z"
    And exactly one "ProcessPathDeactivated" event was published for path "PICK"
    And the "ProcessPathDeactivated" event for "PICK" occurred at "2026-09-07T08:00:00Z"

  @bdd
  Scenario: A refused deactivation neither prunes the schedule nor republishes it
    Given a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    And the CPT schedule for site "sp1" is already defined with timezone "America/Sao_Paulo" and cutoff "sp1-1500" eligible for paths "PICK"
    When the process path "PICK" is deactivated
    Then the deactivation is rejected with status 409
    And the response problem reports type "path-referenced-by-cpt-schedule"
    And no "ProcessPathDeactivated" event was published for path "PICK"
    And "CPTScheduleChanged" was published 1 time for "sp1"
    When the CPT schedule for site "sp1" is requested
    Then the request is accepted with status 200
    And the CPT schedule response lists cutoff ids "sp1-1500"
    And the response field "cutoffs.0.eligiblePathIds" is "PICK"

  @bdd
  Scenario: Deactivating a path no schedule lists leaves every schedule exactly as it was
    Given a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    And a process path "PACK" is already defined with matchPrefix "pack" and capabilities "pack"
    And the CPT schedule for site "sp1" is already defined with timezone "America/Sao_Paulo" and cutoff "sp1-1500" eligible for paths "PICK"
    When the process path "PACK" is deactivated
    Then the deactivation is accepted with status 204
    And "CPTScheduleChanged" was published 1 time for "sp1"
    When the CPT schedule for site "sp1" is requested
    Then the response field "cutoffs.0.eligiblePathIds" is "PICK"

  @bdd
  Scenario: A path listed by any cutoff of a schedule is protected, not only by the first one
    Given a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    And a process path "PACK" is already defined with matchPrefix "pack" and capabilities "pack"
    And the client has successfully sent PUT "/sites/sp1/cpt-schedule" with body:
      """
      {"timezone":"America/Sao_Paulo","cutoffs":[{"cptId":"sp1-1500","localTime":"15:00","daysOfWeek":["Mon"],"shipMethod":"ground","eligiblePathIds":["PICK"]},{"cptId":"sp1-1800","localTime":"18:00","daysOfWeek":["Mon"],"shipMethod":"express","eligiblePathIds":["PACK"]}]}
      """
    When the process path "PACK" is deactivated
    Then the deactivation is rejected with status 409
    And the response problem reports type "path-referenced-by-cpt-schedule"
    And the response problem detail mentions "sp1"
    And the process path "PACK" now has status "ACTIVE"

  # --- ListPaths: keyset pages in pathId order ------------------------------------------------

  @bdd
  Scenario Outline: A page holds at most limit paths in pathId order and the cursor exists only when more follow
    Given a process path "REBIN" is already defined with matchPrefix "rebin" and capabilities "rebin"
    And a process path "PACK" is already defined with matchPrefix "pack" and capabilities "pack"
    And a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    When the process path list is requested with "<query>"
    Then the request is accepted with status 200
    And the page lists path ids "<ids>"
    And the page has <cursor> next cursor

    Examples:
      | query     | ids              | cursor |
      | limit=1   | PACK             | a      |
      | limit=2   | PACK,PICK        | a      |
      | limit=3   | PACK,PICK,REBIN  | no     |
      | limit=500 | PACK,PICK,REBIN  | no     |
      |           | PACK,PICK,REBIN  | no     |

  @bdd
  Scenario: Following the cursor continues after the last path of the previous page and skips deactivated ones by default
    Given a process path "PACK" is already defined with matchPrefix "pack" and capabilities "pack"
    And a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    And a process path "REBIN" is already defined with matchPrefix "rebin" and capabilities "rebin"
    And the process path "PICK" is already deactivated
    When the process path list is requested with "limit=1"
    Then the page lists path ids "PACK"
    And the page has a next cursor
    When the next page is requested with "limit=1"
    Then the page lists path ids "REBIN"
    And the page has no next cursor
    When the process path list is requested with "all=true&limit=2"
    Then the page lists path ids "PACK,PICK"
    And the page has a next cursor
    When the next page is requested with "all=true&limit=2"
    Then the page lists path ids "REBIN"
    And the page has no next cursor

  @bdd
  Scenario Outline: A limit that is not exactly one integer from 1 to 500 is refused as invalid-limit
    When the process path list is requested with "<query>"
    Then the request is rejected with status 400
    And the response problem reports type "invalid-limit"

    Examples:
      | query         |
      | limit=        |
      | limit=ten     |
      | limit=1.5     |
      | limit=-1      |
      | limit=1&limit=2 |

  @bdd
  Scenario Outline: A cursor this API did not issue is refused, however well-formed it looks
    When the process path list is requested with the cursor "<cursor>"
    Then the request is rejected with status 400
    And the response problem reports type "invalid-cursor"

    Examples:
      | cursor                           |
      | bm90LWpzb24                      |
      | eyJzIjoiQSIsImkiOiJCIn0          |
      | eyJzIjoiUElDSyIsImkiOiIifQ       |
      | eyJzIjoiIiwiaSI6IiJ9             |

  # --- the API surface: bodies and routes -------------------------------------------------------

  @bdd
  Scenario Outline: A body that is empty, not valid JSON, has a null array element or carries a NUL is a 400 and changes nothing
    When the client sends <method> "<path>" with body:
      """
      <body>
      """
    Then the request is rejected with status 400
    And the response problem reports type "malformed-request-body"
    And the response content type is "application/problem+json"
    And no events were published

    Examples:
      | method | path                | body                                                                                      |
      | POST   | /process-paths      | {"pathId":                                                                                |
      | POST   | /process-paths      | {"pathId":"PICK","matchPrefix":"pick","requiredCapabilities":["pick",null],"cycleTimeP95":"2h"} |
      | POST   | /process-paths      | {"pathId":"PI\u0000CK","matchPrefix":"pick","requiredCapabilities":["pick"],"cycleTimeP95":"2h"} |
      | PUT    | /process-paths/PICK | not json at all                                                                           |
      | PUT    | /sites/sp1/cpt-schedule | {"timezone":"UTC","cutoffs":[{"cptId":                                                |
      | POST   | /process-paths      |                                                                                           |

  @bdd
  Scenario Outline: A request that no operation matches is the route-not-found problem, never a text 404
    When the client sends GET "<path>"
    Then the request is rejected with status 404
    And the response problem reports type "route-not-found"
    And the response content type is "application/problem+json"

    Examples:
      | path                                |
      | /nope                               |
      | /process-paths/PICK/history         |
      | /sites/sp1                          |
      | /sites/sp1/cpt-schedule/cutoffs     |
