// Package memory provides in-memory implementations of the outbound
// ports. They are TEST DOUBLES for the unit and BDD tests: no cmd/**
// binary imports this package (internal/architecture
// TestRuntimeBinaryHasNoInMemoryAdapters); the shipped binaries run on real
// Postgres and real Kafka only. The production wall clock lives in
// internal/adapters/outbound/system.
package memory
