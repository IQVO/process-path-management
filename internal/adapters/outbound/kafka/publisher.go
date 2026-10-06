// Package kafka provides the outbound adapter that publishes
// process-path-management domain events onto Kafka, satisfying
// ports.EventPublisher. This is the ONLY way fulfillment-execution,
// wes-work-planning, and workforce-management learn about a process-path
// change once this service replaces the static YAML catalogue they used
// to boot-load — see this repo's README for the full migration story.
package kafka

import (
	"context"
	"fmt"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/process-path-management/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/process-path-management/internal/domain/cptschedule"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// Topic is the topic this service publishes every integration event to.
// A single topic (not one per event type) matches the fleet's existing
// convention: consumers dispatch on the CloudEvents `type` attribute
// (ADR 0016) and ignore types they do not handle, so one topic per bounded
// context stays operationally simple.
const Topic = "warehouse.process-path-management.events"

// Every message this package produces is a CloudEvents 1.0 event in
// structured content mode (ADR 0016), built exclusively via
// internal/adapters/kafka/cloudevents. The `type` strings this service
// publishes are the cloudevents.Type* constants — an exact cross-service
// contract consumed by four sibling services.

// ProcessPathData is the payload shape for ALL THREE ProcessPath* event
// types on this topic. RequiredCapabilities is omitted (not
// empty-arrayed) on a ProcessPathDeactivated event, since a deactivation
// carries no definition data — only the PathId and the fact that it
// DestinationLocationRole is likewise omitted (not
// empty-stringed) on any event for a path that never declared one — a
// path with no destination role carries no such field on the wire (ADR
// 0009).
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

// SetNewId overrides the id-minting function used by the standalone
// Publish path.
func (p *Publisher) SetNewId(newId func() string) { p.NewId = newId }

// PublishWithId publishes event onto Kafka under a caller-minted
// CloudEvents id — the form FanOutPublisher uses so ONE occurrence gets
// ONE id on every topic (ADR 0016's no-Postgres fan-out edge). It
// implements IdAwareSender.
func (p *Publisher) PublishWithId(ctx context.Context, event shared.DomainEvent, eventId string) error {
	enc, err := Encode(event, eventId)
	if err != nil {
		return err
	}
	enc.Trace = TraceContextFrom(ctx)
	return p.Send(ctx, enc)
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
			BatchTimeout:           syncWriterBatchTimeout,
			RequiredAcks:           syncWriterRequiredAcks,
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
// Created/Updated/Deactivated in publish order), and the structured-mode
// CloudEvents JSON (ADR 0016). EventId is the CloudEvents `id` and
// EventType the full CloudEvents `type`. It is the unit the transactional outbox (postgres.OutboxPublisher)
// stores and the outbox relay later hands to a Sink, so the direct and
// outbox paths can never disagree about what a message looks like.
type Encoded struct {
	Topic     string
	EventId   string
	EventType string
	Key       string
	Value     []byte
	// Trace is the W3C trace context of the operation that raised the
	// event (ADR 0027). Zero when none was active. It is NOT part of
	// Value: it rides in the traceparent/tracestate Kafka headers, so the
	// CloudEvent body is unchanged and stays stable across redelivery.
	Trace TraceContext
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

// Encode translates a domain event into its Kafka wire form on Topic: a
// CloudEvents 1.0 event with dataschema
// urn:warehouse:process-path-management:events:<EventName>:v1. eventId is
// the CloudEvents `id` — callers supply it so the outbox can persist the
// same id it will later publish under, making redelivery detectable by
// consumers.
func Encode(event shared.DomainEvent, eventId string) (Encoded, error) {
	return encodeFor(event, eventId, Topic, cloudevents.StreamEvents)
}

// encodeFor is the ONE place a domain event becomes a CloudEvent, shared
// by the integration Encode and the AnalyticsEncoder so the two streams can
// never disagree on type, subject, time or payload — only topic and
// dataschema differ.
func encodeFor(event shared.DomainEvent, eventId, topic, stream string) (Encoded, error) {
	m, err := mapEvent(event)
	if err != nil {
		return Encoded{}, err
	}
	value, err := cloudevents.New(cloudevents.Spec{
		ID:        eventId,
		Entity:    m.entity,
		EventName: m.name,
		Subject:   m.key,
		Time:      event.OccurredAt(),
		Stream:    stream,
		Version:   1,
		Data:      m.data,
	})
	if err != nil {
		return Encoded{}, fmt.Errorf("kafka: encode %s: %w", m.name, err)
	}
	return Encoded{
		Topic:     topic,
		EventId:   eventId,
		EventType: cloudevents.Type(m.entity, m.name),
		Key:       m.key,
		Value:     value,
	}, nil
}

// mapped is a domain event's CloudEvents coordinates plus its payload.
type mapped struct {
	entity string
	name   string
	key    string
	data   any
}

// mapEvent maps a domain event to its entity/name, aggregate key (the
// Kafka key AND the CloudEvents subject) and `data` payload.
func mapEvent(event shared.DomainEvent) (mapped, error) {
	switch e := event.(type) {
	case shared.ProcessPathCreated:
		key := string(e.PathId)
		return mapped{cloudevents.EntityProcessPath, "ProcessPathCreated", key, ProcessPathData{
			PathId:                  key,
			MatchPrefix:             e.MatchPrefix,
			Direct:                  e.Direct,
			RequiredCapabilities:    capabilitiesToStrings(e.RequiredCapabilities),
			DestinationLocationRole: string(e.DestinationLocationRole),
			CycleTimeP95:            e.CycleTimeP95.String(),
			Eligibility:             eligibilityToData(e.Eligibility),
		}}, nil
	case shared.ProcessPathUpdated:
		key := string(e.PathId)
		return mapped{cloudevents.EntityProcessPath, "ProcessPathUpdated", key, ProcessPathData{
			PathId:                  key,
			MatchPrefix:             e.MatchPrefix,
			Direct:                  e.Direct,
			RequiredCapabilities:    capabilitiesToStrings(e.RequiredCapabilities),
			DestinationLocationRole: string(e.DestinationLocationRole),
			CycleTimeP95:            e.CycleTimeP95.String(),
			Eligibility:             eligibilityToData(e.Eligibility),
		}}, nil
	case shared.ProcessPathDeactivated:
		key := string(e.PathId)
		return mapped{cloudevents.EntityProcessPath, "ProcessPathDeactivated", key, ProcessPathData{PathId: key}}, nil
	case cptschedule.CPTScheduleChanged:
		key := string(e.SiteId)
		return mapped{cloudevents.EntityCPTSchedule, "CPTScheduleChanged", key, CPTScheduleData{
			SiteId:   key,
			Timezone: e.Timezone,
			Cutoffs:  cutoffsToData(e.Cutoffs),
		}}, nil
	default:
		// An event type this publisher does not know how to serialize.
		// Every event this service's use cases raise today is one of
		// the four above; a future new event type must be added here
		// explicitly rather than silently dropped.
		return mapped{}, fmt.Errorf("kafka: unknown event type %T", event)
	}
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
	enc.Trace = TraceContextFrom(ctx)
	return p.Send(ctx, enc)
}

// Send writes one already-encoded message to enc.Topic. The underlying
// Writer carries no fixed topic of its own (see NewPublisher) so a single
// Publisher instance can relay outbox rows for both the integration topic
// and the analytics topic (ADR 0007) — the topic travels with the
// message, not with the writer.
func (p *Publisher) Send(ctx context.Context, enc Encoded) error {
	msg := kafkago.Message{
		Topic:   enc.Topic,
		Key:     []byte(enc.Key),
		Value:   enc.Value,
		Headers: enc.messageHeaders(),
	}
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
