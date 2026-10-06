package ports

import "errors"

// ErrConcurrentModification is returned by a repo Save when the row it
// targeted was committed by a different writer between this caller's
// load and its Save (ADR 0017, optimistic concurrency via a version
// column). The caller must re-load the aggregate and re-apply its
// change; the HTTP adapter maps this to 409 concurrent-modification so
// a client can tell "re-fetch and retry" apart from a domain-rule
// rejection.
var ErrConcurrentModification = errors.New("aggregate was concurrently modified; reload and retry")

// ErrAlreadyExists is returned by a repo Create when a row with the same
// natural key already exists. Create is insert-only, so it never
// overwrites the existing row; the use case maps this to its own
// domain-level "already exists" error (HTTP 409 path-already-exists).
var ErrAlreadyExists = errors.New("aggregate already exists")
