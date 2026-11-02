//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// The end-to-end proof that the shipped binary is really wired (ADR 0037): the
// test boots the REAL entry point (run, in-process) against testcontainers
// Postgres and Kafka with nothing but the environment a deployment sets, and
// drives it only through REST. It reads the effects from Postgres and from the
// two produced topics (integration and analytics), replays a write to watch
// the idempotency, restarts the service on the same state, and finally takes
// the dependencies away to watch /readyz follow them.

const (
	integrationTopic = "warehouse.process-path-management.events"
	analyticsTopic   = "warehouse.process-path-management.analytics"

	typePrefix = "com.warehouse.wes.process-path-management."

	// containerStartTimeout bounds a testcontainers start.
	containerStartTimeout = 120 * time.Second
)

func startE2EPostgres(t *testing.T) (*tcpostgres.PostgresContainer, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), containerStartTimeout)
	defer cancel()
	c, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("pathmgmt_e2e"), tcpostgres.WithUsername("e2e"), tcpostgres.WithPassword("e2e"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(60*time.Second)))
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(c) })
	url, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres dsn: %v", err)
	}
	return c, url
}

func startE2EKafka(t *testing.T) (*tckafka.KafkaContainer, []string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), containerStartTimeout)
	defer cancel()
	c, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1", tckafka.WithClusterID(fmt.Sprintf("process-path-management-e2e-%d", time.Now().UnixNano())))
	if err != nil {
		t.Fatalf("start kafka: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(c) })
	brokers, err := c.Brokers(ctx)
	if err != nil {
		t.Fatalf("kafka brokers: %v", err)
	}
	return c, brokers
}

// freeAddr reserves a loopback port for the service's listener.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// topicExists asks the broker for the topic through a metadata request that
// cannot create it (kafka-go's Client.Metadata never auto-creates).
func topicExists(t *testing.T, brokers []string, topic string) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := (&kafkago.Client{Addr: kafkago.TCP(brokers...)}).Metadata(ctx, &kafkago.MetadataRequest{Topics: []string{topic}})
	if err != nil {
		t.Fatalf("metadata: %v", err)
	}
	for _, tp := range resp.Topics {
		if tp.Name == topic && tp.Error == nil {
			return true
		}
	}
	return false
}

// createTopic creates a one-partition topic and waits for its leader.
func createTopic(t *testing.T, brokers []string, topic string) {
	t.Helper()
	conn, err := kafkago.Dial("tcp", brokers[0])
	if err != nil {
		t.Fatalf("dial broker: %v", err)
	}
	defer conn.Close()
	if err := conn.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}); err != nil {
		t.Fatalf("create topic %s: %v", topic, err)
	}
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		if parts, err := conn.ReadPartitions(topic); err == nil && len(parts) == 1 && parts[0].Leader.ID != 0 {
			return
		}
	}
	t.Fatalf("topic %s never got a leader", topic)
}

// consume reads exactly n messages of the one-partition topic from the start,
// bounded by a deadline.
func consume(t *testing.T, brokers []string, topic string, n int) []kafkago.Message {
	t.Helper()
	r := kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: topic, Partition: 0, MinBytes: 1, MaxBytes: 1 << 20, MaxWait: 200 * time.Millisecond})
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := r.SetOffset(kafkago.FirstOffset); err != nil {
		t.Fatal(err)
	}
	var out []kafkago.Message
	for len(out) < n {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			t.Fatalf("read %s: got %d of %d messages: %v", topic, len(out), n, err)
		}
		out = append(out, m)
	}
	return out
}

// noMoreMessages fails if the topic holds more than want messages after a
// settling period (nothing was duplicated).
func noMoreMessages(t *testing.T, brokers []string, topic string, want int) {
	t.Helper()
	r := kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: topic, Partition: 0, MinBytes: 1, MaxBytes: 1 << 20, MaxWait: 200 * time.Millisecond})
	defer r.Close()
	if err := r.SetOffset(kafkago.FirstOffset); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	got := 0
	for {
		if _, err := r.ReadMessage(ctx); err != nil {
			break
		}
		got++
	}
	if got != want {
		t.Fatalf("%s holds %d messages, want exactly %d", topic, got, want)
	}
}

// service is one running instance of the real entry point.
type service struct {
	t      *testing.T
	base   string
	cancel context.CancelFunc
	done   chan error
}

// e2eEnv is the environment a deployment sets.
type e2eEnv struct {
	dbURL   string
	brokers []string
	addr    string
}

func newE2EEnv(t *testing.T, dbURL string, brokers []string) e2eEnv {
	t.Helper()
	return e2eEnv{dbURL: dbURL, brokers: brokers, addr: freeAddr(t)}
}

func (e e2eEnv) apply(t *testing.T) {
	t.Helper()
	t.Setenv("DATABASE_URL", e.dbURL)
	t.Setenv("MIGRATIONS_DATABASE_URL", "")
	t.Setenv("MIGRATIONS_PATH", "../../migrations")
	t.Setenv("KAFKA_BROKERS", strings.Join(e.brokers, ","))
	t.Setenv("EVENT_PUBLISHER", "kafka")
	t.Setenv("HTTP_ADDR", e.addr)
	t.Setenv("OUTBOX_RELAY_INTERVAL", "1s")
	t.Setenv("LOG_LEVEL", "warn")
}

// start boots run in-process and waits until the listener answers /healthz.
func (e e2eEnv) start(t *testing.T) *service {
	t.Helper()
	e.apply(t)
	ctx, cancel := context.WithCancel(context.Background())
	s := &service{t: t, base: "http://" + e.addr, cancel: cancel, done: make(chan error, 1)}
	go func() { s.done <- run(ctx) }()
	t.Cleanup(s.stopQuietly)
	waitFor(t, 90*time.Second, "the service to listen on "+e.addr, func() bool {
		select {
		case err := <-s.done:
			t.Fatalf("run returned while booting: %v", err)
		default:
		}
		status, _ := s.try(http.MethodGet, "/healthz", "", nil)
		return status == http.StatusOK
	})
	return s
}

// stop cancels run and requires a clean shutdown within a bound.
func (s *service) stop() {
	s.t.Helper()
	s.cancel()
	select {
	case err := <-s.done:
		if err != nil {
			s.t.Fatalf("shutdown returned %v", err)
		}
	case <-time.After(60 * time.Second):
		s.t.Fatal("run did not return after the shutdown")
	}
	s.done <- nil // let the cleanup find it stopped
}

func (s *service) stopQuietly() {
	s.cancel()
	select {
	case <-s.done:
	case <-time.After(60 * time.Second):
	}
}

// try performs a request and returns -1 when the connection fails.
func (s *service) try(method, path, body string, headers map[string]string) (int, string) {
	req, err := http.NewRequestWithContext(context.Background(), method, s.base+path, strings.NewReader(body))
	if err != nil {
		return -1, err.Error()
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return -1, err.Error()
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(raw)
}

func (s *service) call(method, path, body string, headers map[string]string, want int) string {
	s.t.Helper()
	status, raw := s.try(method, path, body, headers)
	if status != want {
		s.t.Fatalf("%s %s = %d %s, want %d", method, path, status, raw, want)
	}
	return raw
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}

// pgView reads the service's own database.
type pgView struct {
	t    *testing.T
	pool *pgxpool.Pool
}

func newPGView(t *testing.T, url string) pgView {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pgView{t: t, pool: pool}
}

func (v pgView) count(query string, args ...any) int {
	v.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var n int
	if err := v.pool.QueryRow(ctx, query, args...).Scan(&n); err != nil {
		v.t.Fatalf("%s: %v", query, err)
	}
	return n
}

func (v pgView) unpublished() int {
	return v.count(`SELECT count(*) FROM outbox_events WHERE published_at IS NULL`)
}

// envelope holds the CloudEvents context attributes of a published message,
// plus its data.
type envelope struct {
	SpecVersion     string          `json:"specversion"`
	ID              string          `json:"id"`
	Source          string          `json:"source"`
	Type            string          `json:"type"`
	Subject         string          `json:"subject"`
	Time            string          `json:"time"`
	DataContentType string          `json:"datacontenttype"`
	DataSchema      string          `json:"dataschema"`
	Data            json.RawMessage `json:"data"`
}

func decodeEnvelope(t *testing.T, m kafkago.Message) envelope {
	t.Helper()
	var e envelope
	if err := json.Unmarshal(m.Value, &e); err != nil {
		t.Fatalf("not a CloudEvent: %v: %s", err, m.Value)
	}
	return e
}

// assertEnvelope checks every context attribute of a message of this service.
// entity is the aggregate segment of the type ("processpath", "cptschedule").
func assertEnvelope(t *testing.T, m kafkago.Message, entity, eventName, stream, subject string) envelope {
	t.Helper()
	e := decodeEnvelope(t, m)
	wantType := typePrefix + entity + "." + eventName
	schema := fmt.Sprintf("urn:warehouse:process-path-management:%s:%s:v1", stream, eventName)
	if e.SpecVersion != "1.0" || e.Type != wantType || e.Source != "/warehouse/process-path-management" ||
		e.DataSchema != schema || e.DataContentType != "application/json" || e.Subject != subject || e.ID == "" {
		t.Fatalf("envelope = %+v, want 1.0 %s from /warehouse/process-path-management (%s) about %s", e, wantType, schema, subject)
	}
	if _, err := time.Parse(time.RFC3339Nano, e.Time); err != nil {
		t.Fatalf("time %q is not RFC 3339: %v", e.Time, err)
	}
	if string(m.Key) != subject {
		t.Fatalf("kafka key = %q, want %q", m.Key, subject)
	}
	var ct string
	for _, h := range m.Headers {
		if h.Key == "content-type" {
			ct = string(h.Value)
		}
	}
	if ct != "application/cloudevents+json; charset=UTF-8" {
		t.Fatalf("content-type header = %q", ct)
	}
	return e
}

func readyz(s *service) (int, string) { return s.try(http.MethodGet, "/readyz", "", nil) }

func waitReadyzStatus(t *testing.T, s *service, want int, what string) string {
	t.Helper()
	var body string
	waitFor(t, 60*time.Second, what, func() bool {
		var status int
		status, body = readyz(s)
		return status == want
	})
	return body
}

// Phase 1: the topics do not exist yet (a fresh cluster before anything
// published). Readiness needs Postgres plus a broker answering metadata, NOT
// topics, so the service is ready at once, and it creates no topic itself.
func phaseTopicsMissing(t *testing.T, s *service, brokers []string) {
	t.Helper()
	body := waitReadyzStatus(t, s, http.StatusOK, "/readyz to be 200 although no topic exists")
	if !strings.Contains(body, "ready") {
		t.Fatalf("readyz = %s", body)
	}
	for _, topic := range []string{integrationTopic, analyticsTopic} {
		if topicExists(t, brokers, topic) {
			t.Fatalf("topic %s exists: the service must never create topics itself", topic)
		}
	}
	// The topics appear as the cluster broker would create them on first write.
	for _, topic := range []string{integrationTopic, analyticsTopic} {
		createTopic(t, brokers, topic)
	}
	if status, body := readyz(s); status != http.StatusOK {
		t.Fatalf("readyz after the topics appeared = %d %s, want 200", status, body)
	}
}

func definePathBody(id, prefix string) string {
	return fmt.Sprintf(`{"pathId":%q,"matchPrefix":%q,"direct":true,"requiredCapabilities":["pick"],"cycleTimeP95":"2h"}`, id, prefix)
}

func idemKey(k string) map[string]string { return map[string]string{"Idempotency-Key": k} }

const (
	reviseBody = `{"matchPrefix":"pick-b","requiredCapabilities":["pick","hazmat"],"cycleTimeP95":"3h"}`
	cptBody    = `{"timezone":"America/Sao_Paulo","cutoffs":[{"cptId":"cpt-1","localTime":"18:00","daysOfWeek":["Mon","Tue"],"shipMethod":"GROUND","eligiblePathIds":["PICK-1"]}]}`
)

// Phase 2: the write API (REST). Two paths, one revision, one deactivation and
// one CPT schedule: five domain events.
func phaseWrites(t *testing.T, s *service) {
	t.Helper()
	body := s.call(http.MethodPost, "/process-paths", definePathBody("PICK-1", "pick-a"), idemKey("e2e-pick-1"), http.StatusCreated)
	if !strings.Contains(body, `"PICK-1"`) {
		t.Fatalf("POST /process-paths = %s", body)
	}
	s.call(http.MethodPut, "/process-paths/PICK-1", reviseBody, nil, http.StatusOK)
	if got := s.call(http.MethodGet, "/process-paths/PICK-1", "", nil, http.StatusOK); !strings.Contains(got, "pick-b") || !strings.Contains(got, "hazmat") {
		t.Fatalf("GET /process-paths/PICK-1 after the revision = %s", got)
	}
	s.call(http.MethodPut, "/sites/SP1/cpt-schedule", cptBody, nil, http.StatusOK)

	s.call(http.MethodPost, "/process-paths", definePathBody("PACK-1", "pack-a"), idemKey("e2e-pack-1"), http.StatusCreated)
	s.call(http.MethodDelete, "/process-paths/PACK-1", "", nil, http.StatusNoContent)
	if got := s.call(http.MethodGet, "/process-paths", "", nil, http.StatusOK); !strings.Contains(got, "PICK-1") || strings.Contains(got, "PACK-1") {
		t.Fatalf("GET /process-paths (active only) = %s, want PICK-1 and not the deactivated PACK-1", got)
	}
}

// Phase 3: the rows are in Postgres, and the relay drained every one of them.
func phaseRows(t *testing.T, db pgView) {
	t.Helper()
	for _, q := range []string{
		`SELECT count(*) FROM process_paths WHERE id = 'PICK-1' AND match_prefix = 'pick-b' AND status = 'ACTIVE'`,
		`SELECT count(*) FROM process_paths WHERE id = 'PACK-1' AND status = 'DEACTIVATED'`,
	} {
		if n := db.count(q); n != 1 {
			t.Fatalf("%s = %d, want 1", q, n)
		}
	}
	// Every domain event is enqueued onto BOTH topics (integration and
	// analytics) in the transaction of the write: 5 events, 10 rows.
	if n := db.count(`SELECT count(*) FROM outbox_events`); n != 10 {
		t.Fatalf("outbox rows = %d, want 10", n)
	}
	waitFor(t, 60*time.Second, "the relay to publish every outbox row", func() bool { return db.unpublished() == 0 })
	for _, topic := range []string{integrationTopic, analyticsTopic} {
		if n := db.count(`SELECT count(*) FROM outbox_events WHERE topic = $1`, topic); n != 5 {
			t.Fatalf("outbox rows for %s = %d, want 5", topic, n)
		}
	}
}

// Phase 4: the CloudEvents on both output topics, byte-exact envelope, in the
// order of the writes (one partition per topic).
func phaseTopics(t *testing.T, brokers []string) {
	t.Helper()
	type want struct{ entity, name, subject string }
	wants := []want{
		{"processpath", "ProcessPathCreated", "PICK-1"},
		{"processpath", "ProcessPathUpdated", "PICK-1"},
		{"cptschedule", "CPTScheduleChanged", "SP1"},
		{"processpath", "ProcessPathCreated", "PACK-1"},
		{"processpath", "ProcessPathDeactivated", "PACK-1"},
	}
	for topic, stream := range map[string]string{integrationTopic: "events", analyticsTopic: "analytics"} {
		msgs := consume(t, brokers, topic, len(wants))
		for i, w := range wants {
			assertEnvelope(t, msgs[i], w.entity, w.name, stream, w.subject)
		}
	}

	created := decodeEnvelope(t, consume(t, brokers, integrationTopic, 1)[0])
	var data struct {
		PathID               string   `json:"path_id"`
		MatchPrefix          string   `json:"match_prefix"`
		Direct               bool     `json:"direct"`
		RequiredCapabilities []string `json:"required_capabilities"`
		CycleTimeP95         string   `json:"cycle_time_p95"`
		Intake               []string `json:"intake"`
	}
	if err := json.Unmarshal(created.Data, &data); err != nil || data.PathID != "PICK-1" || data.MatchPrefix != "pick-a" ||
		!data.Direct || len(data.RequiredCapabilities) != 1 || data.RequiredCapabilities[0] != "pick" || data.CycleTimeP95 != "2h0m0s" || data.Intake == nil {
		t.Fatalf("ProcessPathCreated data = %s (%v)", created.Data, err)
	}
}

// Phase 5: replaying a creation under the same Idempotency-Key returns the
// stored response and adds no row and no event; the same key with another
// body, a creation without a key and a NUL in the body are refused.
func phaseReplay(t *testing.T, s *service, db pgView, brokers []string) {
	t.Helper()
	replayBody := definePathBody("PICK-1", "pick-a")
	// The original creation is stored under e2e-pick-1: the replay returns it.
	first := s.call(http.MethodPost, "/process-paths", replayBody, idemKey("e2e-pick-1"), http.StatusCreated)
	if first == "" {
		t.Fatal("empty replay response")
	}
	if body := s.call(http.MethodPost, "/process-paths", definePathBody("PICK-1", "pick-z"), idemKey("e2e-pick-1"), http.StatusConflict); !strings.Contains(body, "idempotency-key-reused") {
		t.Fatalf("the same key with another body = %s, want idempotency-key-reused", body)
	}
	if body := s.call(http.MethodPost, "/process-paths", replayBody, nil, http.StatusBadRequest); !strings.Contains(body, "idempotency-key-required") {
		t.Fatalf("a creation without a key = %s, want idempotency-key-required", body)
	}
	if body := s.call(http.MethodPost, "/process-paths", `{"pathId":"PI\u0000CK","matchPrefix":"pick","requiredCapabilities":["pick"],"cycleTimeP95":"2h"}`, idemKey("e2e-nul"), http.StatusBadRequest); !strings.Contains(body, "malformed-request-body") {
		t.Fatalf("a NUL in the body = %s, want malformed-request-body (never a 500)", body)
	}
	if n := db.count(`SELECT count(*) FROM idempotency_keys WHERE key = 'e2e-nul'`); n != 0 {
		t.Fatalf("the NUL request consumed its Idempotency-Key (%d rows)", n)
	}
	// A creation of an existing id under a fresh key is the domain conflict.
	s.call(http.MethodPost, "/process-paths", replayBody, idemKey("e2e-pick-dup"), http.StatusConflict)

	time.Sleep(3 * time.Second) // let a (wrong) second publication show up
	if n := db.count(`SELECT count(*) FROM process_paths`); n != 2 {
		t.Fatalf("process_paths rows after the replay = %d, want 2", n)
	}
	if n := db.count(`SELECT count(*) FROM outbox_events`); n != 10 {
		t.Fatalf("outbox rows after the replay = %d, want still 10", n)
	}
	noMoreMessages(t, brokers, integrationTopic, 5)
	noMoreMessages(t, brokers, analyticsTopic, 5)
}

// Phase 6: restart the real entry point in-process on the same database and
// broker: nothing is lost, no event is published twice, and the boot
// republishes the current catalogue (ADR 0038) onto the integration topic
// ONLY: the one ACTIVE path (PICK-1; PACK-1 is deactivated) as a
// ProcessPathUpdated and the one CPT schedule (SP1), before the new write.
func phaseRestart(t *testing.T, e e2eEnv, s *service, db pgView, brokers []string) *service {
	t.Helper()
	s.stop()
	s2 := e.start(t)
	if got := s2.call(http.MethodGet, "/process-paths/PICK-1", "", nil, http.StatusOK); !strings.Contains(got, "pick-b") {
		t.Fatalf("after the restart GET /process-paths/PICK-1 = %s", got)
	}
	s2.call(http.MethodPost, "/process-paths", definePathBody("REBIN-1", "rebin-a"), idemKey("e2e-rebin-1"), http.StatusCreated)
	waitFor(t, 60*time.Second, "the relay to publish after the restart", func() bool { return db.unpublished() == 0 })
	msgs := consume(t, brokers, integrationTopic, 8)
	seen := map[string]bool{}
	for _, m := range msgs {
		id := decodeEnvelope(t, m).ID
		if seen[id] {
			t.Fatalf("the CloudEvents id %s was published twice", id)
		}
		seen[id] = true
	}
	noMoreMessages(t, brokers, integrationTopic, 8)
	// The analytics stream counts catalogue CHANGES: the republish is not one.
	noMoreMessages(t, brokers, analyticsTopic, 6)
	assertEnvelope(t, msgs[5], "processpath", "ProcessPathUpdated", "events", "PICK-1")
	assertEnvelope(t, msgs[6], "cptschedule", "CPTScheduleChanged", "events", "SP1")
	assertEnvelope(t, msgs[7], "processpath", "ProcessPathCreated", "events", "REBIN-1")
	return s2
}

// Phase 7: /readyz follows the dependencies. Postgres goes first (Kafka still
// up): only postgres is named. Then Kafka goes: kafka is named too. /healthz
// stays liveness and answers 200 throughout.
func phaseDependenciesGoAway(t *testing.T, s *service, pg *tcpostgres.PostgresContainer, kc *tckafka.KafkaContainer) {
	t.Helper()
	stopTimeout := 10 * time.Second
	if err := pg.Stop(context.Background(), &stopTimeout); err != nil {
		t.Fatalf("stop postgres: %v", err)
	}
	body := waitReadyzStatus(t, s, http.StatusServiceUnavailable, "/readyz to turn 503 once Postgres is gone")
	if !strings.Contains(body, "dependency-unavailable") || !strings.Contains(body, "postgres") || strings.Contains(body, "kafka") {
		t.Fatalf("readyz with Postgres down = %s, want dependency-unavailable naming only postgres", body)
	}
	if err := kc.Stop(context.Background(), &stopTimeout); err != nil {
		t.Fatalf("stop kafka: %v", err)
	}
	waitFor(t, 60*time.Second, "/readyz to name kafka as well", func() bool {
		_, body := readyz(s)
		return strings.Contains(body, "postgres") && strings.Contains(body, "kafka")
	})
	if status, body := s.try(http.MethodGet, "/healthz", "", nil); status != http.StatusOK {
		t.Fatalf("healthz with every dependency down = %d %s, want 200 (liveness only)", status, body)
	}
}

func TestRunOnRealPostgresAndKafka(t *testing.T) {
	pg, dbURL := startE2EPostgres(t)
	kc, brokers := startE2EKafka(t)
	e := newE2EEnv(t, dbURL, brokers)
	db := newPGView(t, dbURL)

	s := e.start(t)
	phaseTopicsMissing(t, s, brokers)
	phaseWrites(t, s)
	phaseRows(t, db)
	phaseTopics(t, brokers)
	phaseReplay(t, s, db, brokers)
	s = phaseRestart(t, e, s, db, brokers)
	phaseDependenciesGoAway(t, s, pg, kc)
}
