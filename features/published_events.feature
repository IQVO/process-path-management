# The CloudEvents 1.0 wire form of the events this service publishes -- the
# Published Language five sibling services consume byte-for-byte. Each scenario
# drives a REST change, then encodes the recorded domain event with the real
# outbound encoder (the one the outbox and Kafka publisher use) and asserts the
# resulting message. Derived from the project documentation:
#   - apis/asyncapi.yaml: the CloudEvents envelope (source, type, subject, time,
#     dataschema `urn:warehouse:process-path-management:<events|analytics>:
#     <EventName>:v1`), ProcessPathData / EligibilityData / CPTScheduleData /
#     CutoffData payloads, "A fully permissive eligibility still appears as an
#     empty object {}, never an absent field", and ProcessPathDeactivatedData
#     ("required_capabilities/match_prefix/direct/cycle_time_p95/eligibility
#     are omitted ... on a deactivation").
#   - .claude/rules/cloudevents-envelope.md and .claude/rules/domain-model.md,
#     "Domain events": `type` = com.warehouse.wes.process-path-management.
#     <processpath|cptschedule>.<EventName>, `subject` = path_id / site_id (the
#     Kafka key).
#   - apis/asyncapi.yaml, analytics channel: "Same CloudEvents type, subject,
#     time and data as the integration occurrence; only dataschema differs".
Feature: Published events on the wire
  As a consumer of warehouse.process-path-management.events
  I want each change published as a valid CloudEvent with a stable payload
  So that my local cache can be built from the topic alone

  Background:
    Given the Process Path Management service is running

  @bdd
  Scenario: A defined path is published as a ProcessPathCreated CloudEvent
    When a process path "PICK" is defined with matchPrefix "pick" and capabilities "pick,hazmat"
    Then the request is accepted with status 201
    When the "ProcessPathCreated" event for "PICK" is published to the integration topic
    Then the Kafka key is "PICK"
    And the CloudEvent field "specversion" is "1.0"
    And the CloudEvent field "type" is "com.warehouse.wes.process-path-management.processpath.ProcessPathCreated"
    And the CloudEvent field "source" is "/warehouse/process-path-management"
    And the CloudEvent field "subject" is "PICK"
    And the CloudEvent field "time" is "2026-09-06T00:00:00Z"
    And the CloudEvent field "dataschema" is "urn:warehouse:process-path-management:events:ProcessPathCreated:v1"
    And the CloudEvent field "data.path_id" is "PICK"
    And the CloudEvent field "data.match_prefix" is "pick"
    And the CloudEvent field "data.direct" is "true"
    And the CloudEvent field "data.required_capabilities" is "pick,hazmat"
    And the CloudEvent field "data.cycle_time_p95" is "2h0m0s"
    And the CloudEvent field "data.destination_location_role" is absent
    And the CloudEvent field "data.eligibility" is present
    And the CloudEvent field "data.eligibility.max_units_per_line" is absent

  @bdd
  Scenario: A path with a destination role and narrowed eligibility carries both on the wire
    When the client sends POST "/process-paths" with body:
      """
      {"pathId":"SINGLES","matchPrefix":"singles","direct":true,"requiredCapabilities":["pick"],"destinationLocationRole":"Shipping","cycleTimeP95":"90m","eligibility":{"maxUnitsPerLine":1,"requiredProductAttributes":["giftWrap"],"excludedProductAttributes":["hazmat"],"nonSortable":true}}
      """
    Then the request is accepted with status 201
    When the "ProcessPathCreated" event for "SINGLES" is published to the integration topic
    Then the CloudEvent field "data.destination_location_role" is "Shipping"
    And the CloudEvent field "data.cycle_time_p95" is "1h30m0s"
    And the CloudEvent field "data.eligibility.max_units_per_line" is "1"
    And the CloudEvent field "data.eligibility.required_product_attributes" is "giftWrap"
    And the CloudEvent field "data.eligibility.excluded_product_attributes" is "hazmat"
    And the CloudEvent field "data.eligibility.non_sortable" is "true"

  @bdd
  Scenario: A deactivation is published carrying only the path id
    Given a process path "PACK" is already defined with matchPrefix "pack" and capabilities "pack"
    And the clock reads "2026-09-06T02:00:00Z"
    And the process path "PACK" is already deactivated
    When the "ProcessPathDeactivated" event for "PACK" is published to the integration topic
    Then the Kafka key is "PACK"
    And the CloudEvent field "type" is "com.warehouse.wes.process-path-management.processpath.ProcessPathDeactivated"
    And the CloudEvent field "subject" is "PACK"
    And the CloudEvent field "time" is "2026-09-06T02:00:00Z"
    And the CloudEvent field "data.path_id" is "PACK"
    And the CloudEvent field "data.match_prefix" is absent
    And the CloudEvent field "data.direct" is absent
    And the CloudEvent field "data.required_capabilities" is absent
    And the CloudEvent field "data.cycle_time_p95" is absent
    And the CloudEvent field "data.eligibility" is absent

  @bdd
  Scenario: A CPT schedule change is published as a full snapshot keyed by the site
    Given a process path "PICK" is already defined with matchPrefix "pick" and capabilities "pick"
    And a process path "PACK" is already defined with matchPrefix "pack" and capabilities "pack"
    And the CPT schedule for site "sp1" is already defined with timezone "America/Sao_Paulo" and cutoff "sp1-1500" eligible for paths "PICK,PACK"
    When the "CPTScheduleChanged" event for "sp1" is published to the integration topic
    Then the Kafka key is "sp1"
    And the CloudEvent field "type" is "com.warehouse.wes.process-path-management.cptschedule.CPTScheduleChanged"
    And the CloudEvent field "subject" is "sp1"
    And the CloudEvent field "dataschema" is "urn:warehouse:process-path-management:events:CPTScheduleChanged:v1"
    And the CloudEvent field "data.site_id" is "sp1"
    And the CloudEvent field "data.timezone" is "America/Sao_Paulo"
    And the CloudEvent field "data.cutoffs.0.cpt_id" is "sp1-1500"
    And the CloudEvent field "data.cutoffs.0.local_time" is "15:00"
    And the CloudEvent field "data.cutoffs.0.days_of_week" is "Mon,Tue,Wed,Thu,Fri"
    And the CloudEvent field "data.cutoffs.0.ship_method" is "ground"
    And the CloudEvent field "data.cutoffs.0.eligible_path_ids" is "PICK,PACK"

  @bdd
  Scenario: The analytics occurrence differs from the integration one only by its dataschema
    When a process path "PICK" is defined with matchPrefix "pick" and capabilities "pick"
    Then the request is accepted with status 201
    When the "ProcessPathCreated" event for "PICK" is published to the analytics topic
    Then the CloudEvent field "type" is "com.warehouse.wes.process-path-management.processpath.ProcessPathCreated"
    And the CloudEvent field "subject" is "PICK"
    And the CloudEvent field "time" is "2026-09-06T00:00:00Z"
    And the CloudEvent field "dataschema" is "urn:warehouse:process-path-management:analytics:ProcessPathCreated:v1"
    And the CloudEvent field "data.path_id" is "PICK"
    And the CloudEvent field "data.cycle_time_p95" is "2h0m0s"
