package main_test

// Wave-2 step definitions for the godog suite. They extend the vocabulary in
// features_test.go with:
//
//   - raw request steps (malformed bodies, docstring bodies) and generic
//     JSON-field assertions on the last response;
//   - assertions on the domain events the recording publisher captured;
//   - the CloudEvents wire form of those events (the Published Language five
//     sibling services consume byte-for-byte), built by the real outbound
//     Encoder / AnalyticsEncoder;
//   - the analytics path: the same analytics-topic messages are fed to the
//     real AnalyticsConsumer (the exact entry point the Kafka loop uses) and
//     the resulting read model is asserted through the reports REST router.
//
// Facts that arrive over Kafka are never REST-reachable, so they are seeded
// through the consumer's own HandleMessage and asserted only via REST.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cucumber/godog"

	inboundhttp "github.com/claudioed/process-path-management/internal/adapters/inbound/http"
	inboundkafka "github.com/claudioed/process-path-management/internal/adapters/inbound/kafka"
	"github.com/claudioed/process-path-management/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/analyticsstore"
	outboundkafka "github.com/claudioed/process-path-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/process-path-management/internal/domain/cptschedule"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// settableClock is the scenarios' clock: it starts at a fixed instant in the
// past and only moves when a step says so.
type settableClock struct {
	mu sync.Mutex
	t  time.Time
}

func newSettableClock(t time.Time) *settableClock { return &settableClock{t: t} }

func (c *settableClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *settableClock) set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = t
}

// snapshot returns a copy of every event published so far, in order.
func (p *recordingPublisher) snapshot() []shared.DomainEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]shared.DomainEvent(nil), p.events...)
}

// wave2 is the per-scenario state of the wave-2 steps; it embeds the shared
// world so every step can reuse its server, clock and last response.
type wave2 struct {
	*world

	// wire is the CloudEvent (structured-mode JSON, decoded) selected by the
	// last "is published to the ... topic" step, and wireKey its Kafka key.
	wire    map[string]any
	wireKey string

	pipeline *analyticsPipeline
}

func (w2 *wave2) reset() {
	w2.wire = nil
	w2.wireKey = ""
	w2.closePipeline()
}

func (w2 *wave2) closePipeline() {
	if w2.pipeline != nil {
		w2.pipeline.server.Close()
		w2.pipeline = nil
	}
}

// --- generic requests -----------------------------------------------------

// send performs a request with a raw (possibly malformed) body and records
// the response as the "last" one.
func (w2 *wave2) send(method, path, body string) error {
	var reader io.Reader = http.NoBody
	if body != "" {
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(context.Background(), method, w2.server.URL+path, reader)
	if err != nil {
		return fmt.Errorf("build %s %s: %w", method, path, err)
	}
	req.Header.Set("Content-Type", "application/json")
	return w2.record(w2.server.Client(), req)
}

func (w2 *wave2) record(client *http.Client, req *http.Request) error {
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", req.Method, req.URL, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read %s %s response: %w", req.Method, req.URL, err)
	}
	w2.lastStatus, w2.lastBody, w2.lastHeader = resp.StatusCode, raw, resp.Header
	return nil
}

func (w2 *wave2) clientSends(method, path string) error { return w2.send(method, path, "") }

func (w2 *wave2) clientSendsWithBody(method, path string, body *godog.DocString) error {
	return w2.send(method, path, body.Content)
}

func (w2 *wave2) clockReads(instant string) error {
	t, err := time.Parse(time.RFC3339, instant)
	if err != nil {
		return fmt.Errorf("parse clock %q: %w", instant, err)
	}
	w2.clock.set(t)
	return nil
}

func (w2 *wave2) definePathWithCycleTime(pathId, matchPrefix, capabilitiesCSV, cycleTimeP95 string) error {
	return w2.do(http.MethodPost, "/process-paths", map[string]any{
		"pathId":               pathId,
		"matchPrefix":          matchPrefix,
		"direct":               true,
		"requiredCapabilities": splitCapabilities(capabilitiesCSV),
		"cycleTimeP95":         cycleTimeP95,
	})
}

// defineCutoffSchedule PUTs a one-cutoff schedule whose cutoff fields come
// straight from the scenario, so a validation boundary can be pinned per row.
// An empty days/paths cell means an empty list.
func (w2 *wave2) defineCutoffSchedule(siteId, localTime, daysCSV, shipMethod, pathsCSV string) error {
	return w2.do(http.MethodPut, "/sites/"+siteId+"/cpt-schedule", map[string]any{
		"timezone": "America/Sao_Paulo",
		"cutoffs": []any{map[string]any{
			"cptId":           siteId + "-1500",
			"localTime":       localTime,
			"daysOfWeek":      csvOrEmpty(daysCSV),
			"shipMethod":      shipMethod,
			"eligiblePathIds": csvOrEmpty(pathsCSV),
		}},
	})
}

func csvOrEmpty(csv string) []string {
	if csv == "" {
		return []string{}
	}
	return splitCapabilities(csv)
}

// --- generic response assertions ---------------------------------------------

// lookupField resolves a dotted path ("eligibility.maxUnitsPerLine",
// "cutoffs.0.cpt_id") inside decoded JSON.
func lookupField(root any, path string) (any, bool) {
	cur := root
	for _, seg := range strings.Split(path, ".") {
		switch node := cur.(type) {
		case map[string]any:
			next, ok := node[seg]
			if !ok {
				return nil, false
			}
			cur = next
		case []any:
			i, err := strconv.Atoi(seg)
			if err != nil || i < 0 || i >= len(node) {
				return nil, false
			}
			cur = node[i]
		default:
			return nil, false
		}
	}
	return cur, true
}

// renderValue prints a decoded JSON value the way scenarios write it:
// strings verbatim, numbers/booleans plainly, arrays comma-joined.
func renderValue(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case []any:
		parts := make([]string, len(t))
		for i, item := range t {
			parts[i] = renderValue(item)
		}
		return strings.Join(parts, ",")
	default:
		return fmt.Sprint(t)
	}
}

func fieldIs(root map[string]any, path, want string) error {
	v, ok := lookupField(root, path)
	if !ok {
		return fmt.Errorf("no field %q in %v", path, root)
	}
	if got := renderValue(v); got != want {
		return fmt.Errorf("field %q is %q, want %q", path, got, want)
	}
	return nil
}

func fieldAbsent(root map[string]any, path string) error {
	if v, ok := lookupField(root, path); ok {
		return fmt.Errorf("field %q is present (%v), want it omitted", path, v)
	}
	return nil
}

func (w2 *wave2) responseFieldIs(path, want string) error {
	obj, err := w2.decodeLastObject()
	if err != nil {
		return err
	}
	return fieldIs(obj, path, want)
}

func (w2 *wave2) responseFieldAbsent(path string) error {
	obj, err := w2.decodeLastObject()
	if err != nil {
		return err
	}
	return fieldAbsent(obj, path)
}

func (w2 *wave2) responseFieldIsTimestamp(path, want string) error {
	obj, err := w2.decodeLastObject()
	if err != nil {
		return err
	}
	v, ok := lookupField(obj, path)
	if !ok {
		return fmt.Errorf("no field %q in %s", path, string(w2.lastBody))
	}
	got, err := time.Parse(time.RFC3339, renderValue(v))
	if err != nil {
		return fmt.Errorf("field %q is not an RFC 3339 timestamp: %w", path, err)
	}
	wantT, err := time.Parse(time.RFC3339, want)
	if err != nil {
		return fmt.Errorf("parse expected timestamp %q: %w", want, err)
	}
	if !got.Equal(wantT) {
		return fmt.Errorf("field %q is %s, want %s", path, got, wantT)
	}
	return nil
}

// responseFieldEqualsDuration compares durations by value, so the wire's
// canonical "1h30m0s" matches a scenario's "90m".
func (w2 *wave2) responseFieldEqualsDuration(path, want string) error {
	obj, err := w2.decodeLastObject()
	if err != nil {
		return err
	}
	v, ok := lookupField(obj, path)
	if !ok {
		return fmt.Errorf("no field %q in %s", path, string(w2.lastBody))
	}
	return durationsEqual(renderValue(v), want)
}

func durationsEqual(got, want string) error {
	g, err := time.ParseDuration(got)
	if err != nil {
		return fmt.Errorf("%q is not a Go duration: %w", got, err)
	}
	wt, err := time.ParseDuration(want)
	if err != nil {
		return fmt.Errorf("parse expected duration %q: %w", want, err)
	}
	if g != wt {
		return fmt.Errorf("duration is %s, want %s", g, wt)
	}
	return nil
}

func (w2 *wave2) responseContentTypeIs(want string) error {
	got := w2.lastHeader.Get("Content-Type")
	if mediaType, _, _ := strings.Cut(got, ";"); strings.TrimSpace(mediaType) != want {
		return fmt.Errorf("content type is %q, want %q", got, want)
	}
	return nil
}

func (w2 *wave2) problemDetailMentions(text string) error {
	detail, err := w2.problemDetail()
	if err != nil {
		return err
	}
	if !strings.Contains(detail, text) {
		return fmt.Errorf("problem detail %q does not mention %q", detail, text)
	}
	return nil
}

func (w2 *wave2) problemDetailDoesNotMention(text string) error {
	detail, err := w2.problemDetail()
	if err != nil {
		return err
	}
	if strings.Contains(detail, text) {
		return fmt.Errorf("problem detail %q mentions %q, want it absent", detail, text)
	}
	return nil
}

func (w2 *wave2) problemDetail() (string, error) {
	obj, err := w2.decodeLastObject()
	if err != nil {
		return "", err
	}
	detail, _ := obj["detail"].(string)
	return detail, nil
}

func (w2 *wave2) responseBodyIsEmptyPage() error {
	if got := strings.TrimSpace(string(w2.lastBody)); got != `{"items":[]}` {
		return fmt.Errorf(`response body is %s, want {"items":[]}`, got)
	}
	return nil
}

func (w2 *wave2) pathStillHas(pathId, matchPrefix, cycleTimeP95 string) error {
	if err := w2.getPath(pathId); err != nil {
		return err
	}
	if w2.lastStatus != http.StatusOK {
		return fmt.Errorf("get %s: want 200, got %d", pathId, w2.lastStatus)
	}
	if err := w2.responseFieldIs("matchPrefix", matchPrefix); err != nil {
		return err
	}
	return w2.responseFieldEqualsDuration("cycleTimeP95", cycleTimeP95)
}

func (w2 *wave2) cptScheduleListsCutoffIds(csv string) error {
	obj, err := w2.decodeLastObject()
	if err != nil {
		return err
	}
	cutoffs, _ := obj["cutoffs"].([]any)
	ids := make([]string, len(cutoffs))
	for i, c := range cutoffs {
		cutoff, _ := c.(map[string]any)
		ids[i], _ = cutoff["cptId"].(string)
	}
	if got := strings.Join(ids, ","); got != csv {
		return fmt.Errorf("cutoff ids are %q, want %q", got, csv)
	}
	return nil
}

// --- domain events ------------------------------------------------------------

// eventIdentity is an event's name and aggregate id (PathId, or SiteId for the
// CPT schedule).
func eventIdentity(e shared.DomainEvent) (name, id string) {
	switch ev := e.(type) {
	case shared.ProcessPathCreated:
		return ev.EventName(), string(ev.PathId)
	case shared.ProcessPathUpdated:
		return ev.EventName(), string(ev.PathId)
	case shared.ProcessPathDeactivated:
		return ev.EventName(), string(ev.PathId)
	case cptschedule.CPTScheduleChanged:
		return ev.EventName(), string(ev.SiteId)
	}
	return e.EventName(), ""
}

func (w2 *wave2) eventsFor(name, id string) []shared.DomainEvent {
	var out []shared.DomainEvent
	for _, e := range w2.events.snapshot() {
		if n, k := eventIdentity(e); n == name && k == id {
			out = append(out, e)
		}
	}
	return out
}

func (w2 *wave2) latestEvent(name, id string) (shared.DomainEvent, error) {
	matches := w2.eventsFor(name, id)
	if len(matches) == 0 {
		return nil, fmt.Errorf("no %q event was published for %q", name, id)
	}
	return matches[len(matches)-1], nil
}

// pathEventView is the definition carried by ProcessPathCreated/Updated.
type pathEventView struct {
	matchPrefix  string
	capabilities string
	cycleTimeP95 time.Duration
	eligibility  shared.Eligibility
}

func pathEventDefinition(e shared.DomainEvent) (pathEventView, error) {
	switch ev := e.(type) {
	case shared.ProcessPathCreated:
		return pathEventView{ev.MatchPrefix, joinCapabilities(ev.RequiredCapabilities), ev.CycleTimeP95, ev.Eligibility}, nil
	case shared.ProcessPathUpdated:
		return pathEventView{ev.MatchPrefix, joinCapabilities(ev.RequiredCapabilities), ev.CycleTimeP95, ev.Eligibility}, nil
	}
	return pathEventView{}, fmt.Errorf("%T carries no path definition", e)
}

func joinCapabilities(caps []shared.Capability) string {
	parts := make([]string, len(caps))
	for i, c := range caps {
		parts[i] = string(c)
	}
	return strings.Join(parts, ",")
}

func (w2 *wave2) pathEventCarriesDefinition(name, pathId, matchPrefix, capabilitiesCSV, cycleTimeP95 string) error {
	ev, err := w2.latestEvent(name, pathId)
	if err != nil {
		return err
	}
	def, err := pathEventDefinition(ev)
	if err != nil {
		return err
	}
	if def.matchPrefix != matchPrefix || def.capabilities != capabilitiesCSV {
		return fmt.Errorf("%s carries matchPrefix %q and capabilities %q, want %q and %q",
			name, def.matchPrefix, def.capabilities, matchPrefix, capabilitiesCSV)
	}
	return durationsEqual(def.cycleTimeP95.String(), cycleTimeP95)
}

func (w2 *wave2) pathEventCarriesMaxUnitsPerLine(name, pathId string, want int) error {
	ev, err := w2.latestEvent(name, pathId)
	if err != nil {
		return err
	}
	def, err := pathEventDefinition(ev)
	if err != nil {
		return err
	}
	got := def.eligibility.MaxUnitsPerLine()
	if got == nil || *got != want {
		return fmt.Errorf("%s carries maxUnitsPerLine %v, want %d", name, got, want)
	}
	return nil
}

func (w2 *wave2) eventOccurredAt(name, id, instant string) error {
	ev, err := w2.latestEvent(name, id)
	if err != nil {
		return err
	}
	want, err := time.Parse(time.RFC3339, instant)
	if err != nil {
		return fmt.Errorf("parse %q: %w", instant, err)
	}
	if !ev.OccurredAt().Equal(want) {
		return fmt.Errorf("%s occurred at %s, want %s", name, ev.OccurredAt(), want)
	}
	return nil
}

func (w2 *wave2) cptEventCarries(siteId, timezone, cutoffIdsCSV string) error {
	ev, err := w2.latestEvent("CPTScheduleChanged", siteId)
	if err != nil {
		return err
	}
	changed, ok := ev.(cptschedule.CPTScheduleChanged)
	if !ok {
		return fmt.Errorf("%T is not a CPTScheduleChanged", ev)
	}
	ids := make([]string, len(changed.Cutoffs))
	for i, c := range changed.Cutoffs {
		ids[i] = c.CptId
	}
	if changed.Timezone != timezone || strings.Join(ids, ",") != cutoffIdsCSV {
		return fmt.Errorf("CPTScheduleChanged carries timezone %q and cutoffs %v, want %q and %q",
			changed.Timezone, ids, timezone, cutoffIdsCSV)
	}
	return nil
}

func (w2 *wave2) publishedTimes(name string, want int, id string) error {
	if got := len(w2.eventsFor(name, id)); got != want {
		return fmt.Errorf("%q was published %d times for %q, want %d", name, got, id, want)
	}
	return nil
}

func (w2 *wave2) noEventsPublished() error {
	if n := len(w2.events.snapshot()); n != 0 {
		return fmt.Errorf("%d events were published, want none", n)
	}
	return nil
}

// --- CloudEvents wire form ------------------------------------------------------

// publishedToTopic encodes the latest matching domain event with the real
// outbound encoder for the named stream, validates the result with the
// service's own CloudEvents decoder, and keeps it for the field assertions.
func (w2 *wave2) publishedToTopic(name, id, stream string) error {
	ev, err := w2.latestEvent(name, id)
	if err != nil {
		return err
	}
	var enc outboundkafka.Encoded
	if stream == "analytics" {
		enc, err = (&outboundkafka.AnalyticsEncoder{}).Encode(ev, "evt-1")
	} else {
		enc, err = outboundkafka.Encode(ev, "evt-1")
	}
	if err != nil {
		return fmt.Errorf("encode %s: %w", name, err)
	}
	if _, err := cloudevents.Decode(enc.Value); err != nil {
		return fmt.Errorf("%s is not a valid CloudEvents 1.0 event: %w", name, err)
	}
	var root map[string]any
	if err := json.Unmarshal(enc.Value, &root); err != nil {
		return fmt.Errorf("decode %s envelope: %w", name, err)
	}
	w2.wire, w2.wireKey = root, enc.Key
	return nil
}

func (w2 *wave2) cloudEventFieldIs(path, want string) error {
	if w2.wire == nil {
		return errors.New("no CloudEvent selected: add an \"is published to the ... topic\" step first")
	}
	return fieldIs(w2.wire, path, want)
}

func (w2 *wave2) cloudEventFieldAbsent(path string) error {
	if w2.wire == nil {
		return errors.New("no CloudEvent selected: add an \"is published to the ... topic\" step first")
	}
	return fieldAbsent(w2.wire, path)
}

func (w2 *wave2) cloudEventFieldPresent(path string) error {
	if w2.wire == nil {
		return errors.New("no CloudEvent selected: add an \"is published to the ... topic\" step first")
	}
	if _, ok := lookupField(w2.wire, path); !ok {
		return fmt.Errorf("CloudEvent field %q is absent, want it present", path)
	}
	return nil
}

func (w2 *wave2) kafkaKeyIs(want string) error {
	if w2.wireKey != want {
		return fmt.Errorf("kafka key is %q, want %q", w2.wireKey, want)
	}
	return nil
}

// --- analytics projector and its report ----------------------------------------------

// seenEvents is the consumer's in-memory idempotency gate (the role the
// Postgres ConsumedEventsRepo plays in the cluster).
type seenEvents struct{ seen map[string]bool }

func (s *seenEvents) MarkProcessed(_ context.Context, eventId string) (bool, error) {
	if s.seen[eventId] {
		return false, nil
	}
	s.seen[eventId] = true
	return true, nil
}

// analyticsPipeline is the analytics side wired in memory: the real
// AnalyticsConsumer over a MemoryStore, served by the real reports router.
type analyticsPipeline struct {
	consumer *inboundkafka.AnalyticsConsumer
	server   *httptest.Server
	now      time.Time // what the store's freshness clock reads

	messages [][]byte // analytics-topic messages produced so far
	encoded  int      // how many published domain events are already encoded
	lastErr  error    // result of the last single-message delivery
}

func newAnalyticsPipeline() *analyticsPipeline {
	p := &analyticsPipeline{now: time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)}
	store := analyticsstore.NewMemoryStore()
	store.Now = func() time.Time { return p.now }
	p.consumer = &inboundkafka.AnalyticsConsumer{
		Projection: store,
		Processed:  &seenEvents{seen: map[string]bool{}},
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	p.server = httptest.NewServer(inboundhttp.NewReportsRouter(&inboundhttp.ReportsHandlers{Store: store}, nil))
	return p
}

func (w2 *wave2) projectorIsRunning() error {
	w2.closePipeline()
	w2.pipeline = newAnalyticsPipeline()
	return nil
}

func (w2 *wave2) requirePipeline() (*analyticsPipeline, error) {
	if w2.pipeline == nil {
		return nil, errors.New("the analytics projector is not running: add the Given step first")
	}
	return w2.pipeline, nil
}

func (w2 *wave2) storeClockReads(instant string) error {
	p, err := w2.requirePipeline()
	if err != nil {
		return err
	}
	p.now, err = time.Parse(time.RFC3339, instant)
	return err
}

// deliverPublishedEvents encodes every domain event published since the last
// delivery as its analytics-topic message and hands it to the consumer.
func (w2 *wave2) deliverPublishedEvents() error {
	p, err := w2.requirePipeline()
	if err != nil {
		return err
	}
	events := w2.events.snapshot()
	for ; p.encoded < len(events); p.encoded++ {
		eventId := fmt.Sprintf("evt-%d", p.encoded+1)
		enc, err := (&outboundkafka.AnalyticsEncoder{}).Encode(events[p.encoded], eventId)
		if err != nil {
			return fmt.Errorf("encode analytics message %s: %w", eventId, err)
		}
		p.messages = append(p.messages, enc.Value)
		if err := p.consumer.HandleMessage(context.Background(), enc.Value); err != nil {
			return fmt.Errorf("consume %s: %w", eventId, err)
		}
	}
	return nil
}

// redeliverEveryMessage replays every analytics message already delivered, as
// Kafka's at-least-once delivery may.
func (w2 *wave2) redeliverEveryMessage() error {
	p, err := w2.requirePipeline()
	if err != nil {
		return err
	}
	for i, msg := range p.messages {
		if err := p.consumer.HandleMessage(context.Background(), msg); err != nil {
			return fmt.Errorf("redeliver message %d: %w", i+1, err)
		}
	}
	return nil
}

// deliverRetiredFlatEnvelope feeds the consumer the pre-CloudEvents flat
// envelope (ADR 0016: "never parse a legacy shape").
func (w2 *wave2) deliverRetiredFlatEnvelope() error {
	p, err := w2.requirePipeline()
	if err != nil {
		return err
	}
	raw, err := json.Marshal(map[string]any{
		"event_id": "legacy-1", "event_type": "ProcessPathCreated",
		"occurred_at": "2026-09-06T00:00:00Z", "source": "process-path-management",
		"schema_version": 1, "data": map[string]any{"path_id": "PICK"},
	})
	if err != nil {
		return err
	}
	p.lastErr = p.consumer.HandleMessage(context.Background(), raw)
	return nil
}

func (w2 *wave2) projectorRejectedNotCloudEvent() error {
	p, err := w2.requirePipeline()
	if err != nil {
		return err
	}
	if !errors.Is(p.lastErr, cloudevents.ErrNotCloudEvent) {
		return fmt.Errorf("consumer returned %v, want an ErrNotCloudEvent rejection", p.lastErr)
	}
	return nil
}

func (w2 *wave2) getReport(path string) error {
	p, err := w2.requirePipeline()
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, p.server.URL+path, http.NoBody)
	if err != nil {
		return err
	}
	return w2.record(p.server.Client(), req)
}

func (w2 *wave2) reportRequestedBetween(from, to string) error {
	return w2.getReport("/reports/catalogue-growth?from=" + from + "&to=" + to)
}

func (w2 *wave2) reportFreshnessRequested() error {
	return w2.getReport("/reports/catalogue-growth/freshness")
}

func (w2 *wave2) reportRows() ([]any, error) {
	obj, err := w2.decodeLastObject()
	if err != nil {
		return nil, err
	}
	rows, ok := obj["rows"].([]any)
	if !ok {
		return nil, fmt.Errorf("report has no rows array (null or missing): %s", string(w2.lastBody))
	}
	return rows, nil
}

func (w2 *wave2) reportHasRows(want int) error {
	rows, err := w2.reportRows()
	if err != nil {
		return err
	}
	if len(rows) != want {
		return fmt.Errorf("report has %d rows, want %d: %s", len(rows), want, string(w2.lastBody))
	}
	return nil
}

// reportRowCounts finds the row for one UTC day and compares its counters.
// Row order is deliberately not asserted: the runbook does not specify it.
func (w2 *wave2) reportRowCounts(day string, defined, revised, deactivated int) error {
	rows, err := w2.reportRows()
	if err != nil {
		return err
	}
	for _, r := range rows {
		row, _ := r.(map[string]any)
		if row["dayBucket"] != day {
			continue
		}
		got := fmt.Sprintf("%v/%v/%v", row["pathsDefined"], row["pathsRevised"], row["pathsDeactivated"])
		if want := fmt.Sprintf("%d/%d/%d", defined, revised, deactivated); got != want {
			return fmt.Errorf("row %s counts defined/revised/deactivated %s, want %s", day, got, want)
		}
		return nil
	}
	return fmt.Errorf("report has no row for %s: %s", day, string(w2.lastBody))
}

func (w2 *wave2) freshnessLagIs(want float64) error {
	obj, err := w2.decodeLastObject()
	if err != nil {
		return err
	}
	if got, _ := obj["lagSeconds"].(float64); got != want {
		return fmt.Errorf("lagSeconds is %v, want %v", obj["lagSeconds"], want)
	}
	return nil
}

// --- registration ------------------------------------------------------------------------

func registerWave2Steps(sc *godog.ScenarioContext, w *world) {
	w2 := &wave2{world: w}

	sc.Before(func(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
		w2.reset()
		return ctx, nil
	})
	sc.After(func(ctx context.Context, _ *godog.Scenario, _ error) (context.Context, error) {
		w2.closePipeline()
		return ctx, nil
	})

	w2.registerRequestSteps(sc)
	w2.registerResponseSteps(sc)
	w2.registerEventSteps(sc)
	w2.registerWireSteps(sc)
	w2.registerProjectorSteps(sc)
}

func (w2 *wave2) registerRequestSteps(sc *godog.ScenarioContext) {
	sc.Step(`^the client sends (GET|POST|PUT|DELETE) "([^"]*)"$`, w2.clientSends)
	sc.Step(`^the client sends (GET|POST|PUT|DELETE) "([^"]*)" with body:$`, w2.clientSendsWithBody)
	sc.Step(`^the client defines the declared catalogue entry "([^"]*)"$`, w2.definesDeclaredCatalogueEntry)
	sc.Step(`^the clock reads "([^"]*)"$`, w2.clockReads)
	sc.Step(`^a process path "([^"]*)" is defined with matchPrefix "([^"]*)", capabilities "([^"]*)", and cycleTimeP95 "([^"]*)"$`, w2.definePathWithCycleTime)
	sc.Step(`^the CPT schedule for site "([^"]*)" is defined with a cutoff at "([^"]*)" on days "([^"]*)" via "([^"]*)" eligible for paths "([^"]*)"$`, w2.defineCutoffSchedule)
}

func (w2 *wave2) registerResponseSteps(sc *godog.ScenarioContext) {
	sc.Step(`^the response field "([^"]*)" is "([^"]*)"$`, w2.responseFieldIs)
	sc.Step(`^the response field "([^"]*)" is absent$`, w2.responseFieldAbsent)
	sc.Step(`^the response field "([^"]*)" is the timestamp "([^"]*)"$`, w2.responseFieldIsTimestamp)
	sc.Step(`^the response field "([^"]*)" equals the duration "([^"]*)"$`, w2.responseFieldEqualsDuration)
	sc.Step(`^the response content type is "([^"]*)"$`, w2.responseContentTypeIs)
	sc.Step(`^the response problem detail mentions "([^"]*)"$`, w2.problemDetailMentions)
	sc.Step(`^the response problem detail does not mention "([^"]*)"$`, w2.problemDetailDoesNotMention)
	sc.Step(`^the response body is an empty page$`, w2.responseBodyIsEmptyPage)
	sc.Step(`^the process path "([^"]*)" still has matchPrefix "([^"]*)" and cycleTimeP95 "([^"]*)"$`, w2.pathStillHas)
	sc.Step(`^the CPT schedule response lists cutoff ids "([^"]*)"$`, w2.cptScheduleListsCutoffIds)
}

func (w2 *wave2) registerEventSteps(sc *godog.ScenarioContext) {
	sc.Step(`^the "([^"]*)" event for "([^"]*)" carries matchPrefix "([^"]*)", capabilities "([^"]*)", and cycleTimeP95 "([^"]*)"$`, w2.pathEventCarriesDefinition)
	sc.Step(`^the "([^"]*)" event for "([^"]*)" carries eligibility maxUnitsPerLine (\d+)$`, w2.pathEventCarriesMaxUnitsPerLine)
	sc.Step(`^the "([^"]*)" event for "([^"]*)" occurred at "([^"]*)"$`, w2.eventOccurredAt)
	sc.Step(`^the "CPTScheduleChanged" event for "([^"]*)" carries timezone "([^"]*)" and cutoff ids "([^"]*)"$`, w2.cptEventCarries)
	sc.Step(`^"([^"]*)" was published (\d+) times? for "([^"]*)"$`, w2.publishedTimes)
	sc.Step(`^no events were published$`, w2.noEventsPublished)
}

func (w2 *wave2) registerWireSteps(sc *godog.ScenarioContext) {
	sc.Step(`^the "([^"]*)" event for "([^"]*)" is published to the (integration|analytics) topic$`, w2.publishedToTopic)
	sc.Step(`^the CloudEvent field "([^"]*)" is "([^"]*)"$`, w2.cloudEventFieldIs)
	sc.Step(`^the CloudEvent field "([^"]*)" is absent$`, w2.cloudEventFieldAbsent)
	sc.Step(`^the CloudEvent field "([^"]*)" is present$`, w2.cloudEventFieldPresent)
	sc.Step(`^the Kafka key is "([^"]*)"$`, w2.kafkaKeyIs)
}

func (w2 *wave2) registerProjectorSteps(sc *godog.ScenarioContext) {
	sc.Step(`^the analytics projector is running$`, w2.projectorIsRunning)
	sc.Step(`^the analytics store clock reads "([^"]*)"$`, w2.storeClockReads)
	sc.Step(`^the published events are delivered to the analytics projector$`, w2.deliverPublishedEvents)
	sc.Step(`^the analytics topic redelivers every message$`, w2.redeliverEveryMessage)
	sc.Step(`^the analytics topic delivers a message in the retired flat envelope$`, w2.deliverRetiredFlatEnvelope)
	sc.Step(`^the projector rejected the message as not a CloudEvent$`, w2.projectorRejectedNotCloudEvent)
	sc.Step(`^the catalogue growth report is requested from "([^"]*)" to "([^"]*)"$`, w2.reportRequestedBetween)
	sc.Step(`^the catalogue growth freshness is requested$`, w2.reportFreshnessRequested)
	sc.Step(`^the report has (\d+) rows?$`, w2.reportHasRows)
	sc.Step(`^the report row for day "([^"]*)" counts (\d+) defined, (\d+) revised, and (\d+) deactivated$`, w2.reportRowCounts)
	sc.Step(`^the freshness lag is (\d+) seconds?$`, func(seconds int) error { return w2.freshnessLagIs(float64(seconds)) })
}
