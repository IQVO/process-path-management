// Package kafka provides the outbound adapter that publishes
// process-path-management domain events onto Kafka, satisfying
// ports.EventPublisher. This is the ONLY way fulfillment-execution,
// wes-work-planning, and workforce-management learn about a process-path
// change once this service replaces the static YAML catalogue they used
// to boot-load — see this repo's README for the full migration story.
package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/process-path-management/internal/domain/cptschedule"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// Topic is the topic this service publishes every ProcessPath* event to.
// A single topic (not one per event type) matches the fleet's existing
// convention (e.g. warehouse.fulfillment.events carries several event
// types, filtered by consumers on event_type) — consumers that only care
// about, say, deactivations still see every message, but the filter cost
// is negligible against the operational simplicity of one topic per
// bounded context.
const Topic = "warehouse.process-path-management.events"

// Source identifies this service in the "source" field of every envelope
// it publishes.
const Source = "process-path-management"

// The event types published on Topic — this service's own past-tense
// domain events, verbatim. Any other event_type appearing on this topic
// would be a bug in this publisher, not something a consumer should ever
// need to guard against.
const (
	EventTypeProcessPathCreated     = "ProcessPathCreated"
	EventTypeProcessPathUpdated     = "ProcessPathUpdated"
	EventTypeProcessPathDeactivated = "ProcessPathDeactivated"
	EventTypeCPTScheduleChanged     = "CPTScheduleChanged"
)

// Envelope is the CloudEvents-like wrapper shared across all
// warehouse-systems services (see e.g. fulfillment-execution's own
// kafka.Envelope) — Data is left as `any` here (rather than a fixed
// struct like fulfillment-execution's single-event-type Envelope)
// because this service publishes three distinct event shapes onto the
// same topic.
type Envelope struct {
	EventId    string    `json:"event_id"`
	EventType  string    `json:"event_type"`
	OccurredAt time.Time `json:"occurred_at"`
	Source     string    `json:"source"`
	Data       any       `json:"data"`
}

// ProcessPathData is the payload shape for ALL THREE ProcessPath* event
// types on this topic. RequiredCapabilities is omitted (not
// empty-arrayed) on a ProcessPathDeactivated event, since a deactivation
// carries no definition data — only the PathId and the fact that it
// happened. DestinationLocationRole is likewise omitted (not
// empty-stringed) on any event for a path that never declared one — a
// path with no destination role carries no such field on the wire (ADR
// 0006).
//
// CycleTimeP95 and Eligibility are the fulfillment capability contract
// (ADR 0010), additive on ProcessPathCreated/Updated. CycleTimeP95 is
// encoded as a Go duration string (e.g. "2h0m0s") rather than a bare
// number, so its unit is unambiguous on the wire without a separate
// units field.
type ProcessPathData struct {
	PathId                  string           `json:"path_id"`
	MatchPrefix             string           `json:"match_prefix,omitempty"`
	Direct                  bool             `json:"direct,omitempty"`
	RequiredCapabilities    []string         `json:"required_capabilities,omitempty"`
	DestinationLocationRole string           `json:"destination_location_role,omitempty"`
	CycleTimeP95            string           `json:"cycle_time_p95,omitempty"`
	Eligibility             *EligibilityData `json:"eligibility,omitempty"`
}

// EligibilityData is the wire shape of shared.Eligibility. Omitted from
// ProcessPathData entirely (via the pointer + omitempty above) on a
// ProcessPathDeactivated event, matching the same "no definition data on
// a deactivation" discipline the other fields already follow.
type EligibilityData struct {
	MaxUnitsPerLine           *int     `json:"max_units_per_line,omitempty"`
	RequiredProductAttributes []string `json:"required_product_attributes,omitempty"`
	ExcludedProductAttributes []string `json:"excluded_product_attributes,omitempty"`
	NonSortable               bool     `json:"non_sortable,omitempty"`
}

// CPTScheduleData is the payload shape for CPTScheduleChanged (ADR
// 0010) — a full snapshot of the schedule, matching the "self-sufficient
// event" convention ProcessPathCreated/Updated already follow.
type CPTScheduleData struct {
	SiteId   string       `json:"site_id"`
	Timezone string       `json:"timezone"`
	Cutoffs  []CutoffData `json:"cutoffs"`
}

// CutoffData is the wire shape of one cptschedule.CutoffSnapshot.
type CutoffData struct {
	CptId           string   `json:"cpt_id"`
	LocalTime       string   `json:"local_time"`
	DaysOfWeek      []string `json:"days_of_week"`
	ShipMethod      string   `json:"ship_method"`
	EligiblePathIds []string `json:"eligible_path_ids"`
}

// Writer is the subset of *kafkago.Writer the Publisher needs, so tests
// can substitute a fake without a live broker.
type Writer interface {
	WriteMessages(ctx context.Context, msgs ...kafkago.Message) error
}

// Publisher publishes process-path-management domain events onto Kafka.
// It satisfies ports.EventPublisher.
type Publisher struct {
	Writer Writer
	NewId  func() string
}

// NewPublisher constructs a Publisher writing to brokers. The underlying
// Writer carries NO fixed topic: this Publisher doubles as the outbox
// relay's Sink (ADR 0007), and the relay may hand it rows for either the
// integration topic (Topic) or the analytics topic (AnalyticsTopic) in
// the same pass, so the topic must travel per-message via Encoded.Topic
// rather than being pinned on the writer.
// Balancer is kafkago.Hash (FNV-1a over Message.Key), not LeastBytes: the
// mere presence of a non-nil Key does NOT by itself give "same key always
// maps to the same partition" with kafka-go — the Writer's Balancer alone
// decides partition placement, and LeastBytes routes purely by cumulative
// byte volume written per partition, ignoring Key's content entirely.
// Every event Encode/Publish emit is already keyed by the aggregate's
// PathId/SiteId (see Encode below); Hash is what actually turns that key
// into a same-aggregate-same-partition guarantee once a topic has more
// than one partition (warehouse-infra PR #42 took every business topic,
// including this one, from 1 to 8 — see ADR 0013 and order-management's
// companion ADR 0027, which found and fixed the identical LeastBytes
// mismatch).
func NewPublisher(brokers []string, newId func() string) *Publisher {
	return &Publisher{
		Writer: &kafkago.Writer{
			Addr:                   kafkago.TCP(brokers...),
			Balancer:               &kafkago.Hash{},
			AllowAutoTopicCreation: true,
		},
		NewId: newId,
	}
}

// Encoded is the wire form of one domain event: the topic it belongs on
// (so a multi-topic outbox/relay can route it correctly — ADR 0007), the
// partition key (the PathId, so every event for the same path lands on
// the same partition and a replaying consumer sees a given path's
// Created/Updated/Deactivated in publish order), and the JSON-marshalled
// envelope. It is the unit the transactional outbox (postgres.OutboxPublisher)
// stores and the outbox relay later hands to a Sink, so the direct and
// outbox paths can never disagree about what a message looks like.
type Encoded struct {
	Topic     string
	EventId   string
	EventType string
	Key       string
	Value     []byte
}

// Encoder turns a domain event into its Kafka wire form for one topic,
// without sending it. Both the integration publisher (this file) and the
// analytics publisher (analytics_publisher.go) implement it, so
// postgres.NewOutboxPublisher can fan a single event out to several
// topics inside one transaction (ADR 0007).
type Encoder interface {
	Encode(event shared.DomainEvent, eventId string) (Encoded, error)
}

// IntegrationEncoder adapts the package-level Encode function (this
// service's ONE integration topic, warehouse.process-path-management.events)
// to the Encoder interface, so it can sit alongside the analytics encoder
// in an OutboxPublisher's encoder list.
type IntegrationEncoder struct{}

// Encode implements Encoder by delegating to the package-level Encode.
func (IntegrationEncoder) Encode(event shared.DomainEvent, eventId string) (Encoded, error) {
	return Encode(event, eventId)
}

// Encode translates a domain event into its Kafka wire form on Topic.
// eventId is the envelope's event_id — callers supply it so the outbox can
// persist the same id it will later publish under, making redelivery
// detectable by consumers.
func Encode(event shared.DomainEvent, eventId string) (Encoded, error) {
	var (
		key  string
		data any
		typ  string
	)
	switch e := event.(type) {
	case shared.ProcessPathCreated:
		key = string(e.PathId)
		typ = EventTypeProcessPathCreated
		data = ProcessPathData{
			PathId:                  key,
			MatchPrefix:             e.MatchPrefix,
			Direct:                  e.Direct,
			RequiredCapabilities:    capabilitiesToStrings(e.RequiredCapabilities),
			DestinationLocationRole: string(e.DestinationLocationRole),
			CycleTimeP95:            e.CycleTimeP95.String(),
			Eligibility:             eligibilityToData(e.Eligibility),
		}
	case shared.ProcessPathUpdated:
		key = string(e.PathId)
		typ = EventTypeProcessPathUpdated
		data = ProcessPathData{
			PathId:                  key,
			MatchPrefix:             e.MatchPrefix,
			Direct:                  e.Direct,
			RequiredCapabilities:    capabilitiesToStrings(e.RequiredCapabilities),
			DestinationLocationRole: string(e.DestinationLocationRole),
			CycleTimeP95:            e.CycleTimeP95.String(),
			Eligibility:             eligibilityToData(e.Eligibility),
		}
	case shared.ProcessPathDeactivated:
		key = string(e.PathId)
		typ = EventTypeProcessPathDeactivated
		data = ProcessPathData{PathId: key}
	case cptschedule.CPTScheduleChanged:
		key = string(e.SiteId)
		typ = EventTypeCPTScheduleChanged
		data = CPTScheduleData{
			SiteId:   key,
			Timezone: e.Timezone,
			Cutoffs:  cutoffsToData(e.Cutoffs),
		}
	default:
		// An event type this publisher does not know how to serialize.
		// Every event this service's use cases raise today is one of
		// the four above; a future new event type must be added here
		// explicitly rather than silently dropped.
		return Encoded{}, fmt.Errorf("kafka: unknown event type %T", event)
	}

	env := Envelope{
		EventId:    eventId,
		EventType:  typ,
		OccurredAt: event.OccurredAt(),
		Source:     Source,
		Data:       data,
	}
	payload, err := json.Marshal(env)
	if err != nil {
		return Encoded{}, fmt.Errorf("kafka: marshal envelope: %w", err)
	}
	return Encoded{Topic: Topic, EventId: eventId, EventType: typ, Key: key, Value: payload}, nil
}

// Publish forwards event onto Kafka directly (no outbox), keyed by
// PathId. This is the EVENT_PUBLISHER=kafka path used when the service
// runs without Postgres; with a database configured the composition root
// wires the transactional outbox instead and this publisher only serves
// as the relay's sink via Send (ADR 0003).
func (p *Publisher) Publish(ctx context.Context, event shared.DomainEvent) error {
	enc, err := Encode(event, p.NewId())
	if err != nil {
		return err
	}
	return p.Send(ctx, enc)
}

// Send writes one already-encoded message to enc.Topic. The underlying
// Writer carries no fixed topic of its own (see NewPublisher) so a single
// Publisher instance can relay outbox rows for both the integration topic
// and the analytics topic (ADR 0007) — the topic travels with the
// message, not with the writer.
func (p *Publisher) Send(ctx context.Context, enc Encoded) error {
	msg := kafkago.Message{Topic: enc.Topic, Key: []byte(enc.Key), Value: enc.Value}
	if err := p.Writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("kafka: publish %s: %w", enc.EventType, err)
	}
	return nil
}

// Close releases the underlying Kafka writer.
func (p *Publisher) Close() error {
	if w, ok := p.Writer.(*kafkago.Writer); ok {
		return w.Close()
	}
	return nil
}

func capabilitiesToStrings(caps []shared.Capability) []string {
	out := make([]string, len(caps))
	for i, c := range caps {
		out[i] = string(c)
	}
	return out
}

// eligibilityToData maps a shared.Eligibility value object onto its wire
// shape. Eligibility{} (the fully permissive zero value) still produces
// a non-nil *EligibilityData with every field omitted by omitempty — the
// pointer only becomes nil for a ProcessPathDeactivated event, which
// never constructs one at all (see Encode's switch above).
func eligibilityToData(e shared.Eligibility) *EligibilityData {
	return &EligibilityData{
		MaxUnitsPerLine:           e.MaxUnitsPerLine(),
		RequiredProductAttributes: e.RequiredProductAttributes(),
		ExcludedProductAttributes: e.ExcludedProductAttributes(),
		NonSortable:               e.NonSortable(),
	}
}

// cutoffsToData maps a CPTScheduleChanged event's cutoff snapshots onto
// their wire shape.
func cutoffsToData(cutoffs []cptschedule.CutoffSnapshot) []CutoffData {
	out := make([]CutoffData, 0, len(cutoffs))
	for _, c := range cutoffs {
		out = append(out, CutoffData{
			CptId:           c.CptId,
			LocalTime:       c.LocalTime,
			DaysOfWeek:      weekdaysToStrings(c.DaysOfWeek),
			ShipMethod:      c.ShipMethod,
			EligiblePathIds: pathIdsToStrings(c.EligiblePathIds),
		})
	}
	return out
}

func weekdaysToStrings(ds []cptschedule.Weekday) []string {
	out := make([]string, len(ds))
	for i, d := range ds {
		out[i] = string(d)
	}
	return out
}

func pathIdsToStrings(ids []shared.PathId) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = string(id)
	}
	return out
}
