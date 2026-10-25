package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"

	outboundkafka "github.com/claudioed/process-path-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/process-path-management/internal/application/ports"
	"github.com/claudioed/process-path-management/internal/application/usecases"
)

// republishTimeout bounds the startup republish. It is one short transaction
// over a catalogue of tens of rows; the bound only stops an unreachable
// database from holding the boot (the pool already answered a ping).
const republishTimeout = 30 * time.Second

// catalogueRepublisher is what republishCatalogueAtStartup runs:
// usecases.RepublishCatalogue in production, a stub in the wiring tests.
type catalogueRepublisher interface {
	Execute(ctx context.Context) (usecases.RepublishResult, error)
}

// buildRepublishCatalogue wires the startup catalogue republish (ADR 0038).
//
// It publishes through the SAME transactional outbox the other use cases use
// (one outbox_events row per event, relayed by the same OutboxRelay), but
// with the INTEGRATION encoder only: a republish is not a catalogue change,
// and the analytics projector counts every ProcessPathUpdated as a path
// revision (ADR 0007), so mirroring the republish onto the analytics topic
// would inflate the "paths revised" report on every restart. Keeping it off
// the analytics stream leaves the wire contract of both topics unchanged.
func buildRepublishCatalogue(p *persistence, clock ports.Clock, metrics ports.CatalogueMetrics) *usecases.RepublishCatalogue {
	return &usecases.RepublishCatalogue{
		Repo:         p.repo,
		CPTSchedules: p.scheduleRepo,
		Publisher:    postgres.NewOutboxPublisher(p.pool, uuid.NewString, outboundkafka.IntegrationEncoder{}),
		Clock:        clock,
		UnitOfWork:   p.uow,
		Metrics:      metrics,
	}
}

// republishCatalogueAtStartup runs the republish once, after migrations and
// before the listener binds, and NEVER fails the boot: the service's own job
// (serving the catalogue API, relaying outbox rows) does not depend on it,
// and crash-looping a healthy pod over it would turn a Kafka-retention
// safety net into an availability risk. A failure is not silent, though: it
// is logged at ERROR and counted on
// process_path_management.catalogue.republished{outcome=failed}. The
// outcome is returned for tests.
func republishCatalogueAtStartup(ctx context.Context, logger *slog.Logger, uc catalogueRepublisher) bool {
	ctx, cancel := context.WithTimeout(ctx, republishTimeout)
	defer cancel()
	res, err := uc.Execute(ctx)
	if err != nil {
		logger.Error("startup catalogue republish failed: consumers that rebuild their path catalogue from the topic may be missing paths or schedules whose events expired from Kafka retention",
			"error", err)
		return false
	}
	if res.Omitted > 0 {
		logger.Error("startup catalogue republish truncated: the topic is still incomplete",
			"paths", res.Paths, "schedules", res.Schedules, "omitted", res.Omitted, "max_per_kind", usecases.MaxRepublishedPerKind)
		return false
	}
	logger.Info("startup catalogue republished", "topic", outboundkafka.Topic, "paths", res.Paths, "schedules", res.Schedules)
	return true
}
