// Package ports declares the driven (outbound) interfaces the application
// layer needs, implemented by adapters. No adapter/framework type ever
// appears here — the domain and application layers depend on nothing but
// these interfaces and the domain package itself.
package ports

import (
	"context"
	"time"

	"github.com/claudioed/process-path-management/internal/domain/cptschedule"
	"github.com/claudioed/process-path-management/internal/domain/processpath"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// ProcessPathRepo persists and retrieves ProcessPath aggregates, keyed by
// their PathId (the natural key — there is no separate surrogate id,
// matching the retired YAML file's own schema where `id` was already the
// identity).
type ProcessPathRepo interface {
	// Save upserts a ProcessPath. Used for both first Define and every
	// subsequent Revise/Deactivate — the aggregate's own Status field is
	// what distinguishes a live path from a deactivated one, not deletion.
	Save(ctx context.Context, p *processpath.ProcessPath) error
	FindByID(ctx context.Context, id shared.PathId) (*processpath.ProcessPath, error)
	// ListActive returns every path currently Active — the read model the
	// operator SPA's default view and each consumer's own boot-time/cache
	// hydration both use.
	ListActive(ctx context.Context) ([]*processpath.ProcessPath, error)
	// ListAll returns every path regardless of status, for the SPA's
	// "show deactivated too" view and audit purposes.
	ListAll(ctx context.Context) ([]*processpath.ProcessPath, error)
}

// CPTScheduleRepo persists and retrieves CPTSchedule aggregates, keyed by
// their SiteId (ADR 0010). One schedule per site — Save is an upsert,
// matching ProcessPathRepo's own convention.
type CPTScheduleRepo interface {
	Save(ctx context.Context, s *cptschedule.CPTSchedule) error
	FindBySiteID(ctx context.Context, siteId shared.SiteId) (*cptschedule.CPTSchedule, error)
}

// EventPublisher publishes a domain event raised by a use case. Mirrors
// the exact interface shape already used across the fleet (see
// labor-performance's ports.EventPublisher) so the outbound Kafka/log
// adapters can be lifted with minimal changes.
type EventPublisher interface {
	Publish(ctx context.Context, event shared.DomainEvent) error
}

// UnitOfWork brackets a use case's state change and the domain event it
// raises so both commit or neither does (ADR 0003, transactional outbox).
//
// Execute runs fn inside one atomic scope. Every Repo.Save and
// EventPublisher.Publish made with the ctx handed to fn is bound to that
// same scope: if fn returns an error the scope is rolled back and nothing
// — neither the aggregate row nor the outbox row — is visible afterwards.
//
// Adapters that have no transactional backing (the in-memory repo, the
// log publisher) satisfy this with a pass-through that simply calls fn;
// the use cases stay adapter-agnostic either way.
type UnitOfWork interface {
	Execute(ctx context.Context, fn func(ctx context.Context) error) error
}

// Clock abstracts wall-clock time so use cases and tests never call
// time.Now() directly — same convention used fleet-wide.
type Clock interface {
	Now() time.Time
}

// PathMetrics records DefinePath outcomes (standard-metrics convention,
// Tier 2) so the business signal — how often an operator's attempt to
// define a new process path actually takes effect versus gets rejected —
// is observable independently of HTTP traffic. Use cases treat a nil
// value as "not instrumented", mirroring inventory-storage's
// ports.ReservationMetrics and labor-performance's ports.StandardMetrics.
type PathMetrics interface {
	// PathDefinitionAccepted records a DefinePath call that persisted
	// successfully.
	PathDefinitionAccepted(ctx context.Context)
	// PathDefinitionRejected records a DefinePath call rejected for
	// invalid input or a duplicate PathId.
	PathDefinitionRejected(ctx context.Context)
}
