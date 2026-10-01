package cloudevents

// This service's published event catalogue (ADR 0016). These exact
// strings are a cross-service contract: fulfillment-execution,
// wes-work-planning, workforce-management and order-management dispatch on
// them byte-for-byte, so they are spelled out literally (not built via
// Type at init) to make any accidental rename show up in review. The
// cloudevents_test.go suite asserts each one equals Type(entity, name).
const (
	// EntityProcessPath is the `type` entity segment for ProcessPath* events.
	EntityProcessPath = "processpath"
	// EntityCPTSchedule is the `type` entity segment for CPTScheduleChanged.
	EntityCPTSchedule = "cptschedule"

	TypeProcessPathCreated     = "com.warehouse.wes.process-path-management.processpath.ProcessPathCreated"
	TypeProcessPathUpdated     = "com.warehouse.wes.process-path-management.processpath.ProcessPathUpdated"
	TypeProcessPathDeactivated = "com.warehouse.wes.process-path-management.processpath.ProcessPathDeactivated"
	TypeCPTScheduleChanged     = "com.warehouse.wes.process-path-management.cptschedule.CPTScheduleChanged"
)
