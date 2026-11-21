package http

import (
	"net/http"
	"net/http/httptest"
)

// replayClass says what the Idempotency-Key middleware does with a problem
// answer when the same request is retried under the same key (the fleet rule
// .claude/rules/fleet/idempotency-and-outbox.md, standard in
// warehouse-harness-template docs/idempotency-replay-classes.md). It is
// declared ON the catalogue entry of each problem type, next to its slug and
// title; there is no default and no list of slugs in the middleware.
type replayClass string

const (
	// classFinal: the answer is a function of the request alone. Stored and
	// replayed.
	classFinal replayClass = "FINAL"
	// classOutcome: the command COMMITTED state and then answered an error
	// that must be remembered. Stored, and its writes commit with the stored
	// answer (one transaction).
	classOutcome replayClass = "OUTCOME"
	// classRetryable: nothing committed and the answer depends on state that
	// can change. Not stored: the transaction rolls back, key row included,
	// and a retry executes again.
	classRetryable replayClass = "RETRYABLE"
)

// validReplayClass reports whether c is one of the three classes; "" is
// "unclassified".
func validReplayClass(c replayClass) bool {
	return c == classFinal || c == classOutcome || c == classRetryable
}

// problemType is one problem type of the catalogue as the tests and the ADR
// table see it.
type problemType struct {
	Slug   string
	Status int
	Class  replayClass
}

// allProblemTypes is the whole catalogue: every typed error's problem in
// problemCatalog and flowProblems, plus the standalone problems of
// standaloneProblems (errors.go), one entry per (slug, status). Several
// malformed-request-body literals share one slug.
func allProblemTypes() []problemType {
	var out []problemType
	seen := map[problemType]bool{}
	add := func(status int, info problemInfo) {
		pt := problemType{Slug: info.slug, Status: status, Class: info.class}
		if !seen[pt] {
			seen[pt] = true
			out = append(out, pt)
		}
	}
	for _, e := range problemCatalog() {
		add(statusFor(e.err), e.info)
	}
	for _, e := range flowProblems {
		add(e.status, e.info)
	}
	for _, e := range standaloneProblems {
		add(e.status, e.info)
	}
	return out
}

// replayMarker is implemented by the capturing writer the idempotency
// middleware hands the handler. The production error writer marks it with the
// class of the problem it answers.
type replayMarker interface {
	markReplayClass(replayClass)
}

// markReplayClass marks w when it is the middleware's capture; any other
// writer (a route without the middleware, a test recorder) has nothing to
// mark.
func markReplayClass(w http.ResponseWriter, class replayClass) {
	if m, ok := w.(replayMarker); ok {
		m.markReplayClass(class)
	}
}

// keyCapture is the recorder the idempotency middleware runs the handler
// against: an httptest.ResponseRecorder that also carries the replay class
// the production error writer marked on it.
type keyCapture struct {
	*httptest.ResponseRecorder
	class replayClass
}

func (c *keyCapture) markReplayClass(class replayClass) { c.class = class }

// storable reports whether the captured answer is final for the request: a
// 2xx is stored, a 5xx never is, and every other answer is stored only when
// its problem type declares FINAL or OUTCOME. A refusal that declares no
// class (or a misspelt one) is released like RETRYABLE: a released refusal
// costs one more execution, a wrongly stored one pins a stale refusal for the
// TTL (the sweeper keeps rows 24h, ADR 0018).
func (c *keyCapture) storable() bool {
	switch {
	case c.Code >= http.StatusOK && c.Code < http.StatusMultipleChoices:
		return true
	case c.Code >= http.StatusInternalServerError:
		return false
	default:
		return c.class == classFinal || c.class == classOutcome
	}
}
