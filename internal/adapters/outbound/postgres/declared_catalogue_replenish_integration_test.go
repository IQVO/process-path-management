//go:build integration

// ADR 0036: the declared REPLENISH entry (catalogue/paths.json) defined end
// to end against a real Postgres and a real Kafka broker (testcontainers,
// never KAFKA_BROKERS or localhost): the aggregate is persisted with the
// REPLENISHMENT intake, the outbox relay publishes ProcessPathCreated on
// warehouse.process-path-management.events keyed REPLENISH, and the bytes
// decode with the same payload shape wes-work-planning's catalogue consumer
// (internal/adapters/outbound/kafkacatalog, pathData) and
// fulfillment-execution apply.
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
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/process-path-management/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/process-path-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/process-path-management/internal/application/usecases"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

const declaredCatalogueFile = "../../../../catalogue/paths.json"

// declaredEntry is one POST /process-paths body of catalogue/paths.json.
type declaredEntry struct {
	PathId               string   `json:"pathId"`
	MatchPrefix          string   `json:"matchPrefix"`
	Direct               bool     `json:"direct"`
	RequiredCapabilities []string `json:"requiredCapabilities"`
	CycleTimeP95         string   `json:"cycleTimeP95"`
	Intake               []string `json:"intake"`
}

// wesCatalogueData is a copy of the payload struct wes-work-planning's
// kafkacatalog consumer decodes (pathData), kept field for field so a wire
// change that would break that consumer breaks this test first.
type wesCatalogueData struct {
	PathId                  string   `json:"path_id"`
	MatchPrefix             string   `json:"match_prefix"`
	Direct                  bool     `json:"direct"`
	RequiredCapabilities    []string `json:"required_capabilities"`
	DestinationLocationRole string   `json:"destination_location_role,omitempty"`
	Intake                  []string `json:"intake,omitempty"`
}

func loadDeclaredEntry(t *testing.T, id string) declaredEntry {
	t.Helper()
	raw, err := os.ReadFile(declaredCatalogueFile)
	if err != nil {
		t.Fatalf("read %s: %v", declaredCatalogueFile, err)
	}
	var doc struct {
		Paths []declaredEntry `json:"paths"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("decode %s: %v", declaredCatalogueFile, err)
	}
	for _, e := range doc.Paths {
		if e.PathId == id {
			return e
		}
	}
	t.Fatalf("%s does not declare %s", declaredCatalogueFile, id)
	return declaredEntry{}
}

func defineDeclaredEntry(t *testing.T, ctx context.Context, pool *pgxpool.Pool, e declaredEntry) {
	t.Helper()
	cycle, err := time.ParseDuration(e.CycleTimeP95)
	if err != nil {
		t.Fatalf("cycleTimeP95 %q: %v", e.CycleTimeP95, err)
	}
	caps := make([]shared.Capability, len(e.RequiredCapabilities))
	for i, c := range e.RequiredCapabilities {
		caps[i] = shared.Capability(c)
	}
	intake, err := shared.ParseIntake(e.Intake)
	if err != nil {
		t.Fatalf("intake %v: %v", e.Intake, err)
	}
	newID := uuid.NewString
	define := &usecases.DefinePath{
		Repo: postgres.NewProcessPathRepo(pool),
		Publisher: postgres.NewOutboxPublisher(pool, newID,
			outboundkafka.IntegrationEncoder{}, outboundkafka.NewAnalyticsEncoder(newID)),
		Clock:      fixedClock{t: time.Now().UTC().Truncate(time.Microsecond)},
		UnitOfWork: postgres.NewUnitOfWork(pool),
	}
	if _, err := define.Execute(ctx, shared.PathId(e.PathId), e.MatchPrefix, e.Direct, caps,
		shared.DestinationLocationRoleUnset, cycle, shared.Eligibility{}, intake); err != nil {
		t.Fatalf("define %s: %v", e.PathId, err)
	}
}

func assertReplenishPersisted(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	repo := postgres.NewProcessPathRepo(pool)
	found, err := repo.FindByID(ctx, "REPLENISH")
	if err != nil || found == nil {
		t.Fatalf("find REPLENISH: %v %v", found, err)
	}
	if !found.Intake().Equal(shared.MustIntake(shared.DemandSourceReplenishment)) || !found.IsActive() {
		t.Fatalf("persisted REPLENISH intake=%v active=%v, want [REPLENISHMENT] active", found.Intake().Strings(), found.IsActive())
	}
	var stored []string
	if err := pool.QueryRow(ctx, `SELECT intake FROM process_paths WHERE id = 'REPLENISH'`).Scan(&stored); err != nil {
		t.Fatalf("read intake column: %v", err)
	}
	if !slices.Equal(stored, []string{"REPLENISHMENT"}) {
		t.Fatalf("stored intake column = %v, want [REPLENISHMENT]", stored)
	}
	assertListedWithIntake(t, repo, "REPLENISH", shared.MustIntake(shared.DemandSourceReplenishment))
}

func assertDecodableByTheCatalogueConsumers(t *testing.T, value []byte) {
	t.Helper()
	evt, err := cloudevents.Decode(value)
	if err != nil {
		t.Fatalf("relayed value is not a CloudEvent: %v", err)
	}
	if evt.Type() != cloudevents.TypeProcessPathCreated || evt.Subject() != "REPLENISH" {
		t.Fatalf("type/subject = %s/%s, want %s/REPLENISH", evt.Type(), evt.Subject(), cloudevents.TypeProcessPathCreated)
	}
	var data wesCatalogueData
	if err := evt.DataAs(&data); err != nil {
		t.Fatalf("wes-work-planning pathData cannot decode the payload: %v", err)
	}
	if data.PathId != "REPLENISH" || data.MatchPrefix != "replenish" || !data.Direct ||
		!slices.Equal(data.RequiredCapabilities, []string{"replenish"}) ||
		data.DestinationLocationRole != "" || !slices.Equal(data.Intake, []string{"REPLENISHMENT"}) {
		t.Fatalf("decoded REPLENISH payload = %+v", data)
	}
}

// TestDeclaredCatalogue_ReplenishIsPersistedPublishedAndDecodable defines
// the declared REPLENISH entry through the real use case, outbox and relay,
// then reads the integration topic back (bounded by a context deadline).
func TestDeclaredCatalogue_ReplenishIsPersistedPublishedAndDecodable(t *testing.T) {
	pool := outboxDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	brokers := startKafka(t, ctx, outboundkafka.Topic, outboundkafka.AnalyticsTopic)

	defineDeclaredEntry(t, ctx, pool, loadDeclaredEntry(t, "REPLENISH"))
	assertReplenishPersisted(t, ctx, pool)

	sink := outboundkafka.NewPublisher(brokers, uuid.NewString)
	t.Cleanup(func() { _ = sink.Close() })
	relay := postgres.NewOutboxRelay(pool, sink, slog.Default(), postgres.WithMaxAttempts(20))
	if n, err := relay.RelayOnce(ctx); err != nil || n != 2 {
		t.Fatalf("relay: n=%d err=%v (want the integration and the analytics row)", n, err)
	}

	readCtx, readCancel := context.WithTimeout(ctx, 90*time.Second)
	defer readCancel()
	msg := readOne(t, readCtx, brokers, outboundkafka.Topic)
	if string(msg.Key) != "REPLENISH" {
		t.Fatalf("Kafka key = %q, want REPLENISH", msg.Key)
	}
	assertDecodableByTheCatalogueConsumers(t, msg.Value)
}
