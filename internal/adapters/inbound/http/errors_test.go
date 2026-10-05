package http

import (
	"net/http"
	"testing"

	"github.com/claudioed/process-path-management/internal/application/ports"
	"github.com/claudioed/process-path-management/internal/application/usecases"
)

// TestStatusFor_ConcurrentModification_Returns409 pins the ADR 0017 HTTP
// contract: a repo Save that loses the optimistic-concurrency race surfaces
// as 409 with its own RFC 7807 category, distinct from the natural-key
// path-already-exists 409 so a client can tell "re-fetch and retry" apart
// from a domain-rule rejection. Unit-tested here because the in-memory
// repos used by the router tests never version-guard; the sentinel itself
// is proven to fire against real Postgres by version_integration_test.go.
func TestStatusFor_ConcurrentModification_Returns409(t *testing.T) {
	if got := statusFor(ports.ErrConcurrentModification); got != http.StatusConflict {
		t.Fatalf("want 409 for ErrConcurrentModification, got %d", got)
	}
	info := problemFor(ports.ErrConcurrentModification)
	if info.slug != "concurrent-modification" {
		t.Fatalf("want problem category concurrent-modification, got %q", info.slug)
	}
	// It must remain DISTINCT from the natural-key conflict category.
	if info.slug == problemFor(usecases.ErrPathAlreadyExists).slug {
		t.Fatal("concurrent-modification must not collapse into path-already-exists")
	}
}
