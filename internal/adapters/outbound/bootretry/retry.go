// Package bootretry provides a single, reusable startup-dial retry used
// by every one of this service's four composition roots (cmd/pathmgmt,
// cmd/pathmgmt-projector, cmd/pathmgmt-reports, cmd/mcp).
//
// Fleet-wide known condition: every injected pod's FIRST outbound TCP
// dial (Postgres here) fails with "read: connection reset by peer"
// ~10s after the app starts, because Istio 1.30's native sidecars do not
// finish warming up their outbound listener before the app's own first
// connection attempt (holdApplicationUntilProxyStarts is a no-op for
// native sidecars). A single attempt turns that transient, one-time
// condition into a fail-fast os.Exit(1) and a kubelet-driven
// CrashLoopBackOff; the second start (after the sidecar has warmed up)
// is always clean.
//
// The retry below is NOT a weakening of this fleet's fail-closed
// convention: once the attempt budget is exhausted it still refuses to
// boot, and it still reports the real last error rather than a generic
// timeout. It only stops treating a sidecar warm-up race as a permanent
// failure. Mirrors network-fulfillment's cmd/netfulfil retry/
// retryWithDelay (the fleet's shipped, tested reference for this exact
// condition) so the fix is not re-derived differently per repo.
package bootretry

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// DefaultRetries and DefaultDelay bound the startup retry budget: with
// exponential backoff (1+2+4+8+16s) the total is ~31s, comfortably past
// the ~10s first-dial reset and still far inside a startupProbe's own
// tolerance, so a genuinely unreachable database still fails the pod
// rather than hanging it.
const (
	DefaultRetries = 5
	DefaultDelay   = time.Second
)

// Do runs op with DefaultRetries attempts and exponential backoff
// starting at DefaultDelay, logging a warning between attempts. what
// names the operation for that log line and the final error.
func Do(ctx context.Context, logger *slog.Logger, what string, op func() error) error {
	return DoWithDelay(ctx, logger, what, DefaultDelay, op)
}

// DoWithDelay is Do with the base delay injected, so tests can exercise
// the give-up path without sleeping out the real ~31s budget.
//
// On success it returns nil immediately (a healthy boot never pays the
// backoff). On exhaustion it returns the LAST error, wrapped so
// errors.Is still finds the real cause — a permanent failure (e.g. bad
// credentials) must keep reporting what actually went wrong, never a
// generic "gave up" message. A cancelled ctx abandons the retry rather
// than sleeping out the remaining budget.
func DoWithDelay(ctx context.Context, logger *slog.Logger, what string, base time.Duration, op func() error) error {
	if logger == nil {
		logger = slog.Default()
	}
	delay := base
	var err error
	for attempt := 1; attempt <= DefaultRetries; attempt++ {
		if err = op(); err == nil {
			if attempt > 1 {
				logger.Info("succeeded after retry", "op", what, "attempt", attempt)
			}
			return nil
		}
		if attempt == DefaultRetries {
			break
		}
		logger.Warn("retrying", "op", what, "attempt", attempt, "in", delay, "err", err)
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s: %w", what, ctx.Err())
		case <-time.After(delay):
		}
		delay *= 2
	}
	return fmt.Errorf("%s (after %d attempts): %w", what, DefaultRetries, err)
}
