// Package idempotency holds the ONE retention value both sides of the
// idempotency-key mechanism share: how long a committed idempotency_keys
// row may answer a replay. It exists as its own small package — sibling
// to, not nested inside, internal/adapters/inbound or
// internal/adapters/outbound — for the same reason internal/pgtx does:
// the replay read guard (internal/adapters/inbound/http's middleware)
// and the retention purge (internal/adapters/outbound/postgres's
// sweeper, ADR 0018) must use the SAME constant and the SAME configured
// value without either adapter importing the other's package (which
// internal/architecture's fitness tests forbid in both directions).
//
// cmd/pathmgmt reads IDEMPOTENCY_KEY_TTL once and passes the resolved
// value to BOTH the sweeper and the middleware, so the read guard and
// the purge can never disagree.
package idempotency

import "time"

// KeyTTL is how long a committed idempotency_keys row answers replays:
// ADR 0011's 24-hour window. A row older than KeyTTL must never answer a
// replay — not even in the window between its expiry and the ADR 0018
// sweeper's next pass — which is why the replay read carries this exact
// constant and the sweeper deletes on it (as
// postgres.DefaultIdempotencyKeyTTL, its alias here).
const KeyTTL = 24 * time.Hour
