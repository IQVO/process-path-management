-- W3C trace context on outbox rows (ADR 0027). The request's traceparent /
-- tracestate are captured at enqueue time and carried through the relay to
-- the Kafka headers apis/asyncapi.yaml promises. Both columns are nullable
-- and additive: rows enqueued before this migration, or outside any traced
-- operation, simply publish without trace headers.
ALTER TABLE outbox_events ADD COLUMN traceparent TEXT;
ALTER TABLE outbox_events ADD COLUMN tracestate TEXT;
