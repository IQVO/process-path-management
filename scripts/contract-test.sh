#!/usr/bin/env bash
# Contract-test the REST API with Schemathesis (property-based testing
# against apis/openapi.yaml): builds the service, boots it with its
# in-memory adapters on a loopback port, waits for /healthz, generates
# valid AND invalid requests for every operation, and asserts the
# responses conform to the spec (status codes, content types, response
# schemas; negative data rejected).
#
# Mirrors the `contract` job in .github/workflows/ci.yml — same pinned
# Schemathesis version, same flags — so a local pass means a CI pass.
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

cd "$(dirname "$0")/.."
BIN="$(mktemp -d)/pathmgmt"
go build -o "$BIN" ./cmd/pathmgmt

HTTP_ADDR="127.0.0.1:${PORT}" "$BIN" &
SERVER_PID=$!
trap 'kill "$SERVER_PID" 2>/dev/null || true' EXIT

# Wait for the server to report healthy (up to ~10s).
for _ in $(seq 1 50); do
  if curl -sf "${BASE_URL}/healthz" >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done
curl -sf "${BASE_URL}/healthz" >/dev/null # fail loudly if it never came up

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
st run apis/openapi.yaml \
  --url "${BASE_URL}" \
  --max-examples "${MAX_EXAMPLES}" \
  --workers 4 \
  --exclude-checks positive_data_acceptance,use_after_free
