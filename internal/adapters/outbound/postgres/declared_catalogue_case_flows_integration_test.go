//go:build integration

// ADR 0038: the declared FULL_CASE, CROSS_DOCK and BREAK_CASE entries
// (catalogue/paths.json) defined end to end against a real Postgres and a
// real Kafka broker (both started by testcontainers helpers of this
// package): each aggregate is persisted with its eligibility, destination
// role and intake, the outbox relay publishes one ProcessPathCreated per
// path on warehouse.process-path-management.events keyed by the path id, and
// the bytes decode with the payload shape the catalogue consumers read.
package postgres_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/process-path-management/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/process-path-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/process-path-management/internal/application/usecases"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// caseFlowEntry is one POST /process-paths body of catalogue/paths.json,
// including the fields the case flows add to the earlier entries.
type caseFlowEntry struct {
	PathId                  string   `json:"pathId"`
	MatchPrefix             string   `json:"matchPrefix"`
	Direct                  bool     `json:"direct"`
	RequiredCapabilities    []string `json:"requiredCapabilities"`
	DestinationLocationRole string   `json:"destinationLocationRole"`
	CycleTimeP95            string   `json:"cycleTimeP95"`
	Eligibility             struct {
		RequiredProductAttributes []string `json:"requiredProductAttributes"`
	} `json:"eligibility"`
	Intake []string `json:"intake"`
}

// caseFlowWire is the consumer-facing payload, field for field.
type caseFlowWire struct {
	PathId                  string   `json:"path_id"`
	MatchPrefix             string   `json:"match_prefix"`
	Direct                  bool     `json:"direct"`
	RequiredCapabilities    []string `json:"required_capabilities"`
	DestinationLocationRole string   `json:"destination_location_role,omitempty"`
	CycleTimeP95            string   `json:"cycle_time_p95"`
	Eligibility             struct {
		RequiredProductAttributes []string `json:"required_product_attributes"`
	} `json:"eligibility"`
	Intake []string `json:"intake"`
}

func loadCaseFlowEntries(t *testing.T, ids ...string) []caseFlowEntry {
	t.Helper()
	raw, err := os.ReadFile("../../../../catalogue/paths.json")
	if err != nil {
		t.Fatalf("read catalogue: %v", err)
	}
	var doc struct {
		Paths []json.RawMessage `json:"paths"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("decode catalogue: %v", err)
	}
	var out []caseFlowEntry
	for _, id := range ids {
		found := false
		for _, rawEntry := range doc.Paths {
			var e caseFlowEntry
			if err := json.Unmarshal(rawEntry, &e); err != nil {
				t.Fatalf("decode entry: %v", err)
			}
			if e.PathId == id {
				out = append(out, e)
				found = true
			}
		}
		if !found {
			t.Fatalf("catalogue/paths.json does not declare %s", id)
		}
	}
	return out
}

func defineCaseFlowEntry(t *testing.T, ctx context.Context, define *usecases.DefinePath, e caseFlowEntry) {
	t.Helper()
	cycle, err := time.ParseDuration(e.CycleTimeP95)
	if err != nil {
		t.Fatalf("%s cycleTimeP95 %q: %v", e.PathId, e.CycleTimeP95, err)
	}
	caps := make([]shared.Capability, len(e.RequiredCapabilities))
	for i, c := range e.RequiredCapabilities {
		caps[i] = shared.Capability(c)
	}
	intake, err := shared.ParseIntake(e.Intake)
	if err != nil {
		t.Fatalf("%s intake %v: %v", e.PathId, e.Intake, err)
	}
	role, err := shared.ParseDestinationLocationRole(e.DestinationLocationRole)
	if err != nil {
		t.Fatalf("%s role %q: %v", e.PathId, e.DestinationLocationRole, err)
	}
	elig := shared.NewEligibility(nil, e.Eligibility.RequiredProductAttributes, nil, false)
	if _, err := define.Execute(ctx, shared.PathId(e.PathId), e.MatchPrefix, e.Direct, caps, role, cycle, elig, intake); err != nil {
		t.Fatalf("define %s: %v", e.PathId, err)
	}
}

// TestDeclaredCatalogue_CaseFlowsArePersistedPublishedAndDecodable defines
// the three declared entries through the real use case, outbox and relay,
// then reads the integration topic back (bounded by a context deadline).
func TestDeclaredCatalogue_CaseFlowsArePersistedPublishedAndDecodable(t *testing.T) {
	pool := outboxDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	brokers := startKafka(t, ctx, outboundkafka.Topic, outboundkafka.AnalyticsTopic)

	newID := uuid.NewString
	define := &usecases.DefinePath{
		Repo: postgres.NewProcessPathRepo(pool),
		Publisher: postgres.NewOutboxPublisher(pool, newID,
			outboundkafka.IntegrationEncoder{}, outboundkafka.NewAnalyticsEncoder(newID)),
		Clock:      fixedClock{t: time.Now().UTC().Truncate(time.Microsecond)},
		UnitOfWork: postgres.NewUnitOfWork(pool),
	}
	entries := loadCaseFlowEntries(t, "FULL_CASE", "CROSS_DOCK", "BREAK_CASE")
	for _, e := range entries {
		defineCaseFlowEntry(t, ctx, define, e)
	}

	repo := postgres.NewProcessPathRepo(pool)
	for _, e := range entries {
		found, err := repo.FindByID(ctx, shared.PathId(e.PathId))
		if err != nil || found == nil || !found.IsActive() {
			t.Fatalf("find %s: %v %v", e.PathId, found, err)
		}
		if !slices.Equal(found.Intake().Strings(), e.Intake) {
			t.Fatalf("%s persisted intake = %v, want %v", e.PathId, found.Intake().Strings(), e.Intake)
		}
		if got := found.Eligibility().RequiredProductAttributes(); !slices.Equal(got, e.Eligibility.RequiredProductAttributes) {
			t.Fatalf("%s persisted required attributes = %v, want %v", e.PathId, got, e.Eligibility.RequiredProductAttributes)
		}
		if string(found.DestinationLocationRole()) != e.DestinationLocationRole {
			t.Fatalf("%s persisted destination role = %q, want %q", e.PathId, found.DestinationLocationRole(), e.DestinationLocationRole)
		}
	}
	// ListActive is what order-management's selection consumes through the events.
	active, err := repo.ListActive(ctx)
	if err != nil || len(active) != 3 {
		t.Fatalf("ListActive = %d paths, err %v, want 3", len(active), err)
	}

	sink := outboundkafka.NewPublisher(brokers, uuid.NewString)
	t.Cleanup(func() { _ = sink.Close() })
	relay := postgres.NewOutboxRelay(pool, sink, slog.Default(), postgres.WithMaxAttempts(20))
	if n, err := relay.RelayOnce(ctx); err != nil || n != 6 {
		t.Fatalf("relay: n=%d err=%v (want an integration and an analytics row per path)", n, err)
	}

	readCtx, readCancel := context.WithTimeout(ctx, 90*time.Second)
	defer readCancel()
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers, Topic: outboundkafka.Topic, StartOffset: kafkago.FirstOffset, MaxWait: 2 * time.Second,
	})
	defer func() { _ = reader.Close() }()
	got := map[string]caseFlowWire{}
	for len(got) < 3 {
		msg, err := reader.ReadMessage(readCtx)
		if err != nil {
			t.Fatalf("read integration topic: %v", err)
		}
		evt, err := cloudevents.Decode(msg.Value)
		if err != nil {
			t.Fatalf("relayed value is not a CloudEvent: %v", err)
		}
		var data caseFlowWire
		if err := evt.DataAs(&data); err != nil {
			t.Fatalf("payload does not decode: %v", err)
		}
		if evt.Type() != cloudevents.TypeProcessPathCreated || evt.Subject() != data.PathId || string(msg.Key) != data.PathId {
			t.Fatalf("type/subject/key = %s/%s/%s for %s", evt.Type(), evt.Subject(), msg.Key, data.PathId)
		}
		got[data.PathId] = data
	}
	for _, e := range entries {
		w, ok := got[e.PathId]
		if !ok {
			t.Fatalf("%s was not published", e.PathId)
		}
		if w.MatchPrefix != e.MatchPrefix || !w.Direct || !slices.Equal(w.RequiredCapabilities, e.RequiredCapabilities) ||
			w.DestinationLocationRole != e.DestinationLocationRole || !slices.Equal(w.Intake, e.Intake) ||
			!slices.Equal(w.Eligibility.RequiredProductAttributes, e.Eligibility.RequiredProductAttributes) {
			t.Fatalf("decoded %s payload = %+v, want it to match the declared entry %+v", e.PathId, w, e)
		}
	}
	if got["CROSS_DOCK"].DestinationLocationRole != "Shipping" || got["FULL_CASE"].DestinationLocationRole != "" {
		t.Fatalf("destination roles = %q / %q, want Shipping / absent", got["CROSS_DOCK"].DestinationLocationRole, got["FULL_CASE"].DestinationLocationRole)
	}
}
