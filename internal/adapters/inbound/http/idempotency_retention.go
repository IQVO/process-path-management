// The replay-side half of idempotency-key retention (ADR 0043's
// 2026-11-22 addendum): the replay read carries an age guard with the
// SAME TTL the ADR 0018 sweeper purges on (internal/idempotency.KeyTTL,
// wired from cmd's IDEMPOTENCY_KEY_TTL), so a row can never answer a
// replay between its expiry and the next sweep. When the guard rejects a
// row — expired but not yet swept, or already swept away mid-request —
// the key is TAKEN OVER: the expired row is deleted and a fresh one
// inserted in ONE transaction, and the request executes the real handler
// (runFreshRequest) as if the key were brand new.
package http

import (
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/process-path-management/internal/idempotency"
)

// maxTakeoverAttempts bounds the acquire-retry loop below. Every analyzed
// interleaving resolves in two passes (a lost takeover is followed by a
// replay of the winner's committed row); the bound exists only so the
// request can never spin, and exceeding it — unreachable in practice —
// fails loudly instead of looping.
const maxTakeoverAttempts = 3

// idempotencyConfig is RequireIdempotencyKey's configuration.
type idempotencyConfig struct {
	// ttl is how old a stored answer may be and still answer a replay;
	// the same value the sweeper purges on. <= 0 disables the age guard
	// (and, by the ADR 0018 knob's contract, retention itself), which is
	// the pre-addendum behaviour: a stored answer replays until swept.
	ttl time.Duration
}

// IdempotencyOption configures RequireIdempotencyKey beyond its pool.
type IdempotencyOption func(*idempotencyConfig)

// WithIdempotencyKeyTTL sets how old a stored answer may be and still
// answer a replay. cmd/pathmgmt passes the SAME housekeepingDurationEnv
// value the sweeper gets, so the read guard and the purge can never
// disagree; d <= 0 disables the guard.
func WithIdempotencyKeyTTL(d time.Duration) IdempotencyOption {
	return func(c *idempotencyConfig) { c.ttl = d }
}

// replayOrTakeOverExpired drives everything that happens after the key
// INSERT found an existing row: the guarded replay read answers a LIVE
// row (the pre-addendum behaviour, byte for byte); an ABSENT one —
// expired before the sweep got to it, or swept away mid-request — is
// taken over and executed, looping a bounded number of times because a
// lost takeover race always ends with the winner's committed row in
// place for the next read.
func replayOrTakeOverExpired(w http.ResponseWriter, r *http.Request, pool *pgxpool.Pool, cfg idempotencyConfig, key, requestHash string, next http.Handler) {
	for range maxTakeoverAttempts {
		if replayCachedResponse(w, r, pool, key, requestHash, cfg.ttl) {
			return // a live row answered: replayed, 409 reused, or a 500
		}
		if takeOverExpiredKey(w, r, pool, cfg, key, requestHash, next) {
			return // the key was acquired and the request executed
		}
	}
	writeProblem(w, http.StatusInternalServerError, problemInternalError,
		"the idempotency key could not be acquired after "+strconv.Itoa(maxTakeoverAttempts)+
			" attempts — this should be impossible", r.URL.Path)
}

// takeOverExpiredKey acquires a key whose stored row the age guard
// rejected: it deletes the expired row (only if it is still expired) and
// inserts a fresh one, in ONE transaction, then runs the request through
// runFreshRequest on that transaction. It reports whether the request
// was handled; false means the key is owned by a concurrent takeover and
// the caller should read (and replay) the winner's row instead.
//
// The expired row is DELETED and a NEW row inserted rather than updated
// in place, deliberately: the sweeper's DELETE targets rows by ctid from
// its own snapshot, and an in-place refresh would let a sweep that
// already selected the old row delete the refreshed answer a moment
// after it commits. A new row is invisible to that sweep by construction
// (see the ADR 0043 addendum), so the purge needed no change.
//
// Concurrency is the same argument as the fresh-key path in
// idempotency.go, transplanted: Postgres' unique-index lock serializes
// same-key writers, so a takeover that finds the row already replaced
// (its DELETE matched nothing, its INSERT conflicted) rolls back having
// written nothing, and the winner's committed row — outcome included,
// per the no-null-status-code invariant — answers the loser's next read.
func takeOverExpiredKey(w http.ResponseWriter, r *http.Request, pool *pgxpool.Pool, cfg idempotencyConfig, key, requestHash string, next http.Handler) bool {
	ctx := r.Context()
	tx, err := pool.Begin(ctx)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, problemInternalError, err.Error(), r.URL.Path)
		return true
	}

	// Delete only-if-expired: the exact complement of the replay read's
	// guard (created_at > now() - ttl answers; <= now() - ttl is
	// replaceable). A LIVE row matches nothing and the INSERT below
	// conflicts with it instead, which is the "lost the race" signal.
	if _, err := tx.Exec(ctx, `
		DELETE FROM idempotency_keys
		WHERE key = $1 AND created_at <= now() - ($2 * interval '1 second')
	`, key, cfg.ttl.Seconds()); err != nil {
		_ = tx.Rollback(ctx)
		writeProblem(w, http.StatusInternalServerError, problemInternalError, err.Error(), r.URL.Path)
		return true
	}

	tag, err := tx.Exec(ctx, `
		INSERT INTO idempotency_keys (key, method, path, request_hash)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (key) DO NOTHING
	`, key, r.Method, r.URL.Path, requestHash)
	if err != nil {
		_ = tx.Rollback(ctx)
		writeProblem(w, http.StatusInternalServerError, problemInternalError, err.Error(), r.URL.Path)
		return true
	}
	if tag.RowsAffected() == 0 {
		// A concurrent request owns a live row for this key (it took the
		// key over, or re-inserted after a sweep). This transaction wrote
		// nothing; read and replay the winner's answer instead.
		_ = tx.Rollback(ctx)
		return false
	}

	runFreshRequest(w, r, ctx, tx, key, next)
	return true
}

// defaultIdempotencyConfig is the middleware's configuration when no
// option overrides it: the ONE shared TTL constant.
func defaultIdempotencyConfig() idempotencyConfig {
	return idempotencyConfig{ttl: idempotency.KeyTTL}
}

// idempotencyOptions maps Server.IdempotencyKeyTTL onto middleware
// options: zero keeps the shared default, positive sets it explicitly,
// negative disables the guard (retention disabled — the pre-addendum
// behaviour, one IDEMPOTENCY_KEY_TTL knob for sweeper and read alike).
func (s *Server) idempotencyOptions() []IdempotencyOption {
	switch {
	case s.IdempotencyKeyTTL > 0:
		return []IdempotencyOption{WithIdempotencyKeyTTL(s.IdempotencyKeyTTL)}
	case s.IdempotencyKeyTTL < 0:
		return []IdempotencyOption{WithIdempotencyKeyTTL(-1)}
	default:
		return nil
	}
}
