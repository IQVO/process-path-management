//go:build integration

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// ADR 0038, the incident this change closes: consumers rebuild their path
// catalogue by replaying the integration topic from the earliest offset, and
// the broker's retention had expired the creation events of paths seeded long
// ago. The test seeds a catalogue through the real entry point, then DELETES
// the integration topic (the strongest form of "retention expired every
// event"), restarts the service on the same database and asserts the new topic
// holds exactly the current state again: one ProcessPathUpdated per ACTIVE
// path and one CPTScheduleChanged per schedule, sorted, nothing for the
// deactivated path, and nothing added to the analytics stream.

func seedCatalogueForRepublish(t *testing.T, s *service) {
	t.Helper()
	s.call(http.MethodPost, "/process-paths", definePathBody("SLAM-1", "slam-a"), idemKey("rp-slam-1"), http.StatusCreated)
	s.call(http.MethodPost, "/process-paths", definePathBody("PICK-1", "pick-a"), idemKey("rp-pick-1"), http.StatusCreated)
	s.call(http.MethodPut, "/process-paths/PICK-1", reviseBody, nil, http.StatusOK)
	s.call(http.MethodPost, "/process-paths", definePathBody("PACK-1", "pack-a"), idemKey("rp-pack-1"), http.StatusCreated)
	s.call(http.MethodDelete, "/process-paths/PACK-1", "", nil, http.StatusNoContent)
	s.call(http.MethodPut, "/sites/SP1/cpt-schedule", cptBody, nil, http.StatusOK)
	s.call(http.MethodPut, "/sites/BA1/cpt-schedule", cptBody, nil, http.StatusOK)
}

// deleteTopic removes topic and waits until the broker no longer lists it.
func deleteTopic(t *testing.T, brokers []string, topic string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := (&kafkago.Client{Addr: kafkago.TCP(brokers...)}).DeleteTopics(ctx, &kafkago.DeleteTopicsRequest{Topics: []string{topic}}); err != nil {
		t.Fatalf("delete topic %s: %v", topic, err)
	}
	waitFor(t, 30*time.Second, "the topic to disappear", func() bool { return !topicExists(t, brokers, topic) })
}

type republishedPayload struct {
	PathID      string `json:"path_id"`
	MatchPrefix string `json:"match_prefix"`
	SiteID      string `json:"site_id"`
}

func TestStartupRepublishesTheCurrentCatalogueAfterRetentionExpiry(t *testing.T) {
	_, dbURL := startE2EPostgres(t)
	_, brokers := startE2EKafka(t)
	e := newE2EEnv(t, dbURL, brokers)
	db := newPGView(t, dbURL)
	for _, topic := range []string{integrationTopic, analyticsTopic} {
		createTopic(t, brokers, topic)
	}

	s := e.start(t)
	seedCatalogueForRepublish(t, s)
	waitFor(t, 60*time.Second, "the relay to drain the seeding", func() bool { return db.unpublished() == 0 })
	consume(t, brokers, integrationTopic, 7)
	s.stop()

	// Retention "expires" every event of the integration topic.
	deleteTopic(t, brokers, integrationTopic)
	createTopic(t, brokers, integrationTopic)

	s = e.start(t)
	defer s.stop()
	waitFor(t, 60*time.Second, "the relay to drain the republish", func() bool { return db.unpublished() == 0 })

	msgs := consume(t, brokers, integrationTopic, 4)
	noMoreMessages(t, brokers, integrationTopic, 4)
	// Sorted by path id, then by site id: paths first, then schedules.
	assertEnvelope(t, msgs[0], "processpath", "ProcessPathUpdated", "events", "PICK-1")
	assertEnvelope(t, msgs[1], "processpath", "ProcessPathUpdated", "events", "SLAM-1")
	assertEnvelope(t, msgs[2], "cptschedule", "CPTScheduleChanged", "events", "BA1")
	assertEnvelope(t, msgs[3], "cptschedule", "CPTScheduleChanged", "events", "SP1")

	// The payload is the FULL CURRENT state (PICK-1 was revised), not its
	// creation state.
	var pick republishedPayload
	if err := json.Unmarshal(decodeEnvelope(t, msgs[0]).Data, &pick); err != nil || pick.MatchPrefix != "pick-b" {
		t.Fatalf("republished PICK-1 data = %s (%v), want the revised match_prefix pick-b", decodeEnvelope(t, msgs[0]).Data, err)
	}

	// Seven events at seeding time; the republish added NOTHING to analytics.
	noMoreMessages(t, brokers, analyticsTopic, 7)
	if n := db.count(`SELECT count(*) FROM outbox_events WHERE topic = $1`, analyticsTopic); n != 7 {
		t.Fatalf("analytics outbox rows = %d, want 7 (the republish must not touch the analytics stream)", n)
	}
}
