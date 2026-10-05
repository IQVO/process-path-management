package kafka

import (
	"context"

	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// FanOutPublisher forwards each domain event to several senders in turn.
// It is how the OLTP composition root publishes one event stream to both
// the integration topic and the analytics topic when running with Kafka
// but no Postgres (no outbox/transaction to bind them to) — the
// transactional-outbox path (postgres.OutboxPublisher) is used instead
// whenever a database is configured.
//
// Fan-out is fail-fast in sender order: the first error stops the fan-out
// and is returned, so an analytics-publish failure is never silently
// swallowed behind an earlier sender's success.
//
// Id semantics match postgres.OutboxPublisher's exactly (ADR 0016's
// dev-only fan-out edge): ONE CloudEvents id is minted per occurrence and
// handed to every target via PublishWithId, so the same occurrence lands
// on both topics under the same id and a consumer can correlate (and
// dedupe) across streams. Each target's own NewId is only used by its
// standalone Publish, never under this fan-out.
type FanOutPublisher struct {
	// Senders are the fan-out targets, in order.
	Senders []Sender
	// NewId mints the one CloudEvents id shared by every sender for a
	// single occurrence. Never nil — construct via NewSharedIdFanOut.
	NewId func() string
}

// Sender is the minimal shape a fan-out target needs: publish one domain
// event. Both *Publisher (integration, direct) and *AnalyticsPublisher
// satisfy it.
type Sender interface {
	Publish(ctx context.Context, event shared.DomainEvent) error
}

// IdAwareSender is a Sender that can publish one occurrence under a
// caller-minted CloudEvents id — the shape FanOutPublisher needs so ONE
// occurrence gets ONE id on every topic. Both *Publisher and
// *AnalyticsPublisher satisfy it.
type IdAwareSender interface {
	Sender
	PublishWithId(ctx context.Context, event shared.DomainEvent, eventId string) error
}

// NewSharedIdFanOut builds a FanOutPublisher over senders that mints ONE
// id per occurrence (newId) and publishes under it on every target,
// matching postgres.OutboxPublisher's semantics on the no-Postgres path.
func NewSharedIdFanOut(newId func() string, senders ...IdAwareSender) FanOutPublisher {
	targets := make([]Sender, 0, len(senders))
	for _, s := range senders {
		targets = append(targets, s)
	}
	return FanOutPublisher{Senders: targets, NewId: newId}
}

// Publish forwards event to every configured sender under the ONE id
// minted here for this occurrence, returning the first error encountered.
// A sender that cannot publish under a caller-minted id is refused loudly
// rather than allowed to silently break the shared-id contract.
func (f FanOutPublisher) Publish(ctx context.Context, event shared.DomainEvent) error {
	eventId := f.NewId()
	for _, p := range f.Senders {
		s, ok := p.(IdAwareSender)
		if !ok {
			return errNotIdAware{}
		}
		if err := s.PublishWithId(ctx, event, eventId); err != nil {
			return err
		}
	}
	return nil
}

// errNotIdAware reports a fan-out target that cannot publish under a
// caller-minted id; every target must share one id per occurrence (ADR
// 0016).
type errNotIdAware struct{}

func (errNotIdAware) Error() string {
	return "kafka: fan-out sender does not implement PublishWithId; every target must share one id per occurrence (ADR 0016)"
}
