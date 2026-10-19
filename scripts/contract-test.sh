#!/usr/bin/env bash
# Contract-test the REST API with Schemathesis (property-based testing
# against apis/openapi.yaml): builds the service, boots it against the
# Postgres named by DATABASE_URL and the Kafka broker named by KAFKA_BROKERS
# on a loopback port, waits for /readyz, generates valid AND invalid requests
# for every operation, and asserts the responses conform to the spec (status
# codes, content types, response schemas; negative data rejected).
#
# Mirrors the `contract` job in .github/workflows/ci.yml — same pinned
# Schemathesis version, same flags — so a local pass means a CI pass.
#
# This service has no in-memory mode (ADR 0037): cmd/pathmgmt requires
# DATABASE_URL (it migrates and serves from Postgres) and KAFKA_BROKERS (the
# outbox relay and /readyz). CI provides a postgres:16 and a Kafka service
# container; for a local run, start disposable ones first (the broker must
# advertise the address you pass in KAFKA_BROKERS):
#
#   docker run -d --rm --name ppm-contract-pg -p 18099:5432 \
#     -e POSTGRES_USER=pathmgmt -e POSTGRES_PASSWORD=pathmgmt \
#     -e POSTGRES_DB=pathmgmt postgres:16
#   docker run -d --rm --name ppm-contract-kafka -p 19099:9092 \
#     -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:29092,PLAINTEXT_HOST://localhost:19099 \
#     confluentinc/confluent-local:7.6.1
#   export DATABASE_URL='postgres://pathmgmt:pathmgmt@localhost:18099/pathmgmt?sslmode=disable'
#   export KAFKA_BROKERS='localhost:19099'
#
# (credentials/URL shape mirror docker-compose.yml and the CI `contract`
# job's postgres service).
#
# Requires `st` on PATH:
#   python3 -m pip install --user 'schemathesis==4.28.0'
set -euo pipefail

SCHEMATHESIS_VERSION="4.28.0"
PORT="${CONTRACT_PORT:-18085}"
BASE_URL="http://127.0.0.1:${PORT}"
MAX_EXAMPLES="${CONTRACT_MAX_EXAMPLES:-100}"

if ! command -v st >/dev/null 2>&1; then
  echo "schemathesis (st) is not installed (or not on PATH)."
  echo "Install the exact version CI pins:"
  echo "  python3 -m pip install --user 'schemathesis==${SCHEMATHESIS_VERSION}'"
  exit 1
fi

if [ -z "${DATABASE_URL:-}" ]; then
  echo "DATABASE_URL is not set — this service has no in-memory mode"
  echo "(cmd/pathmgmt requires Postgres and applies migrations on boot)."
  echo "Start a disposable one and export DATABASE_URL, e.g.:"
  echo "  docker run -d --rm --name ppm-contract-pg -p 18099:5432 \\"
  echo "    -e POSTGRES_USER=pathmgmt -e POSTGRES_PASSWORD=pathmgmt \\"
  echo "    -e POSTGRES_DB=pathmgmt postgres:16"
  echo "  export DATABASE_URL='postgres://pathmgmt:pathmgmt@localhost:18099/pathmgmt?sslmode=disable'"
  exit 1
fi

if [ -z "${KAFKA_BROKERS:-}" ]; then
  echo "KAFKA_BROKERS is not set — this service has no log sink and no optional relay"
  echo "(cmd/pathmgmt requires a real Kafka broker; /readyz depends on it)."
  echo "Start a disposable one and export KAFKA_BROKERS, e.g.:"
  echo "  docker run -d --rm --name ppm-contract-kafka -p 19099:9092 \\"
  echo "    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:29092,PLAINTEXT_HOST://localhost:19099 \\"
  echo "    confluentinc/confluent-local:7.6.1"
  echo "  export KAFKA_BROKERS='localhost:19099'"
  exit 1
fi

cd "$(dirname "$0")/.."
BIN="$(mktemp -d)/pathmgmt"
go build -o "$BIN" ./cmd/pathmgmt

# The old in-memory boot started EMPTY (no seed data), and so does this one:
# the migrations create the schema and insert no rows, so Schemathesis still
# starts from an empty catalogue and creates what it needs itself. The
# declared catalogue (catalogue/paths.json) is NOT needed: `make
# seed-catalogue` is a separate step that applies it to a RUNNING service
# over REST, and neither the old nor the new boot ran it.
LOG_LEVEL="${LOG_LEVEL:-warn}" HTTP_ADDR="127.0.0.1:${PORT}" "$BIN" &
SERVER_PID=$!
trap 'kill "$SERVER_PID" 2>/dev/null || true' EXIT

# Wait for /readyz: it is 200 only while Postgres answers a ping AND the
# broker answers a metadata request (up to ~60s: migrations + broker).
for _ in $(seq 1 300); do
  if curl -sf "${BASE_URL}/readyz" >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done
curl -sf "${BASE_URL}/readyz" >/dev/null # fail loudly if it never became ready

# Two checks are excluded, each because the property it asserts is
# deliberately NOT a property of this API — everything the OpenAPI
# schema CAN express (minLength/pattern/enum/minItems/format,
# documented statuses, content types, response schemas, rejection of
# schema-invalid data, resource readability after creation) stays
# enforced by the remaining default checks:
#
# positive_data_acceptance — the impl correctly 422s schema-valid data
#   that violates semantics OpenAPI 3.0.3 cannot express, each enforced
#   and tested elsewhere:
#   * matchPrefix must be Unicode lower-case — BDD features/define_validation.feature
#     ("upper-case matchPrefix is rejected") + TestDefine_RejectsUppercaseMatchPrefix;
#   * cycleTimeP95 must be a POSITIVE Go duration string — BDD
#     features/revise_and_events.feature ("malformed cycleTimeP95 is rejected")
#     + TestDefine_RejectsZeroCycleTimeP95 / ...RejectsNegativeCycleTimeP95;
#   * timezone must be a recognized IANA zone — BDD features/cpt_schedule.feature
#     ("unrecognized timezone is rejected") + TestDefine_RejectsUnrecognizedTimezone;
#   * revisePath requires the target path be Active — BDD features/revise.feature
#     ("A deactivated path rejects a revision") + TestRevise_OnDeactivatedPath_ReturnsErrPathDeactivated;
#   * eligiblePathIds must reference Active paths — BDD features/cpt_schedule.feature
#     ("unknown"/"deactivated" cutoff-path scenarios) +
#     TestDefineCPTSchedule_IneligiblePathId_RejectsWithoutPublishing.
#
# use_after_free — DELETE /process-paths/{pathId} is a documented
#   idempotent SOFT retirement, not a hard delete: the spec documents
#   GET as returning the path "regardless of status (Active or
#   Deactivated)", so a 200 with status DEACTIVATED after a DELETE is
#   by design — BDD features/deactivate.feature + unit
#   TestDeactivate_IsIdempotent.
#
# Idempotency-Key (ADR 0011, enforced in every deployment now): the hooks
# file gives every request that carries the header a fresh UUID, like a real
# client sending one per logical submit (see scripts/contract_hooks.py).
#
# --generation-allow-x00 false: the schemas cannot say "no U+0000", and
# PostgreSQL text cannot hold one. The service answers a NUL in a body with
# 400 and in a path with 404 (internal/adapters/inbound/http/nul.go, pinned by
# TestRejectNUL and TestNUL_IsAClientErrorAndDoesNotConsumeTheKey), which
# Schemathesis would report as "API rejected schema-compliant request".
export SCHEMATHESIS_HOOKS="scripts/contract_hooks.py"
st run apis/openapi.yaml \
  --url "${BASE_URL}" \
  --max-examples "${MAX_EXAMPLES}" \
  --workers 4 \
  --generation-allow-x00 false \
  --exclude-checks positive_data_acceptance,use_after_free
