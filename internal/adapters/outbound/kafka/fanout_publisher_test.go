package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// recordingFanOutWriter records every message written so a test can
// assert the CloudEvents id each message actually carries on the wire.
type recordingFanOutWriter struct {
	mu   sync.Mutex
	msgs []kafkago.Message
}

func (w *recordingFanOutWriter) WriteMessages(_ context.Context, msgs ...kafkago.Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.msgs = append(w.msgs, msgs...)
	return nil
}

// cloudEventID extracts the CloudEvents `id` from one recorded message's
// structured-mode JSON envelope.
func cloudEventID(t *testing.T, m kafkago.Message) string {
	t.Helper()
	var env struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(m.Value, &env); err != nil {
		t.Fatalf("decode cloudevent envelope: %v", err)
	}
	return env.ID
}

// TestNewSharedIdFanOut_SameIdOnEveryTopic pins ADR 0016's dev-only
// fan-out edge fix: on the no-Postgres direct path, ONE id is minted per
// occurrence and shared by both publishers, so the integration and
// analytics messages for the same occurrence carry the same CloudEvents
// id — matching postgres.OutboxPublisher's semantics on the outbox path.
// Per occurrence, the shared id mint is called exactly once; neither
// publisher's own NewId is consulted at all.
func TestNewSharedIdFanOut_SameIdOnEveryTopic(t *testing.T) {
	mints := 0
	sharedIds := make([]string, 0, 2)
	newId := func() string {
		mints++
		id := "shared-id-" + string(rune('0'+mints))
		sharedIds = append(sharedIds, id)
		return id
	}
	// If either publisher minted its OWN id, this sentinel would appear
	// on the wire and fail the assertions below.
	mustNotBeUsed := func() string { return "MUST-NOT-BE-USED" }

	integrationWriter := &recordingFanOutWriter{}
	analyticsWriter := &recordingFanOutWriter{}

	integration := &Publisher{Writer: integrationWriter, NewId: mustNotBeUsed}
	analytics := &AnalyticsPublisher{encoder: NewAnalyticsEncoder(mustNotBeUsed), writer: analyticsWriter}

	fanOut := NewSharedIdFanOut(newId, integration, analytics)

	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	first := shared.ProcessPathCreated{PathId: "PICK", At: now}
	second := shared.ProcessPathDeactivated{PathId: "PICK", At: now}
	for _, e := range []shared.DomainEvent{first, second} {
		if err := fanOut.Publish(context.Background(), e); err != nil {
			t.Fatalf("publish %T: %v", e, err)
		}
	}

	if mints != 2 {
		t.Fatalf("shared id minted %d times for 2 occurrences, want 2 (one per occurrence)", mints)
	}
	if len(integrationWriter.msgs) != 2 || len(analyticsWriter.msgs) != 2 {
		t.Fatalf("messages written: integration=%d analytics=%d, want 2 and 2", len(integrationWriter.msgs), len(analyticsWriter.msgs))
	}

	for i := range integrationWriter.msgs {
		integrationID := cloudEventID(t, integrationWriter.msgs[i])
		analyticsID := cloudEventID(t, analyticsWriter.msgs[i])
		if integrationID != analyticsID {
			t.Fatalf("occurrence %d: ids differ across topics: integration=%q analytics=%q", i, integrationID, analyticsID)
		}
		if integrationID != sharedIds[i] {
			t.Fatalf("occurrence %d: integration id %q is not the shared mint %q (a publisher minted its own)", i, integrationID, sharedIds[i])
		}
	}

	// The direct Publish paths still mint their own ids when used
	// standalone (PublishWithId is a fan-out-only seam): sanity-check one.
	if err := integration.Publish(context.Background(), first); err != nil {
		t.Fatalf("standalone publish: %v", err)
	}
	standalone := cloudEventID(t, integrationWriter.msgs[2])
	if standalone != "MUST-NOT-BE-USED" {
		t.Fatalf("standalone publish used id %q, want the publisher's own NewId", standalone)
	}
}

// TestNewSharedIdFanOut_StillFailFast keeps the fan-out's documented
// fail-fast semantics: the first sender's error stops the fan-out before
// any later sender is consulted.
func TestNewSharedIdFanOut_StillFailFast(t *testing.T) {
	boom := errors.New("fan-out sender failed")
	var calls int
	failing := idAwareFunc(func(context.Context, shared.DomainEvent, string) error { return boom })
	follow := idAwareFunc(func(context.Context, shared.DomainEvent, string) error {
		calls++
		return nil
	})

	fanOut := FanOutPublisher{Senders: []Sender{failing, follow}, NewId: func() string { return "x" }}
	if err := fanOut.Publish(context.Background(), shared.ProcessPathCreated{PathId: "PICK"}); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
	if calls != 0 {
		t.Fatalf("second sender called %d times after the first failed, want 0", calls)
	}
}

// TestFanOutPublisher_PlainSenderRefused pins the loud-refusal branch: a
// sender that cannot publish under a caller-minted id fails the fan-out
// instead of silently breaking the one-id-per-occurrence contract.
func TestFanOutPublisher_PlainSenderRefused(t *testing.T) {
	plain := SenderFunc(func(context.Context, shared.DomainEvent) error {
		t.Fatal("plain sender must never be called")
		return nil
	})
	fanOut := FanOutPublisher{Senders: []Sender{plain}, NewId: func() string { return "x" }}
	err := fanOut.Publish(context.Background(), shared.ProcessPathCreated{PathId: "PICK"})
	var want errNotIdAware
	if !errors.As(err, &want) {
		t.Fatalf("err = %v, want errNotIdAware", err)
	}
}

// SenderFunc adapts a function to the Sender interface for tests.
type SenderFunc func(ctx context.Context, event shared.DomainEvent) error

func (f SenderFunc) Publish(ctx context.Context, event shared.DomainEvent) error { return f(ctx, event) }

// idAwareFunc adapts a function to the IdAwareSender interface for tests.
type idAwareFunc func(ctx context.Context, event shared.DomainEvent, eventId string) error

func (f idAwareFunc) Publish(ctx context.Context, event shared.DomainEvent) error {
	return f(ctx, event, "unused")
}

func (f idAwareFunc) PublishWithId(ctx context.Context, event shared.DomainEvent, eventId string) error {
	return f(ctx, event, eventId)
}
