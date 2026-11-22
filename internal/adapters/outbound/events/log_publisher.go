// Package events provides outbound EventPublisher implementations.
package events

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// LogPublisher publishes domain events by logging them as JSON. Used as
// the local-dev/test fallback and as one leg of the Kafka-backed
// composition root's fan-out.
type LogPublisher struct {
	logger *slog.Logger
}

// NewLogPublisher constructs a LogPublisher. A nil logger defaults to
// slog.Default().
func NewLogPublisher(logger *slog.Logger) *LogPublisher {
	if logger == nil {
		logger = slog.Default()
	}
	return &LogPublisher{logger: logger}
}

func (p *LogPublisher) Publish(ctx context.Context, event shared.DomainEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	p.logger.InfoContext(ctx, "domain event published",
		"event_name", sanitizeLog(event.EventName()),
		"payload", json.RawMessage(payload),
	)
	return nil
}
