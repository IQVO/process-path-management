package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestAwaitStop_ConsumerLoopExitIsAnError pins gap fix D13: when the
// analytics consumer loop returns on its own (a commit failure, a DLQ
// publish failure, a broker error), the projector must not keep serving
// /healthz and /readyz as if nothing happened. awaitStop turns that exit
// into an error, which run() returns, so main exits non-zero and
// Kubernetes restarts the pod.
func TestAwaitStop_ConsumerLoopExitIsAnError(t *testing.T) {
	cause := errors.New("commit offsets: broker down")
	exit := make(chan error, 1)
	exit <- cause

	err := awaitStop(context.Background(), exit)
	if !errors.Is(err, errConsumerStopped) {
		t.Fatalf("awaitStop = %v, want errConsumerStopped", err)
	}
	if !errors.Is(err, cause) {
		t.Fatalf("awaitStop = %v, want it to wrap the consumer's own error", err)
	}
}

// A consumer loop that returns nil without having been cancelled is just
// as dead: it is still an error.
func TestAwaitStop_ConsumerLoopReturningNilIsStillAnError(t *testing.T) {
	exit := make(chan error, 1)
	exit <- nil

	if err := awaitStop(context.Background(), exit); !errors.Is(err, errConsumerStopped) {
		t.Fatalf("awaitStop = %v, want errConsumerStopped", err)
	}
}

// A signal (SIGTERM from Kubernetes) is the normal way out: no error.
func TestAwaitStop_SignalIsAGracefulStop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := awaitStop(ctx, make(chan error)); err != nil {
		t.Fatalf("awaitStop after a signal = %v, want nil", err)
	}
}

type fakeCloser struct{ closed bool }

func (f *fakeCloser) Close() error { f.closed = true; return nil }

type fakePool struct{ closed bool }

func (f *fakePool) Close() { f.closed = true }

// TestShutdown_FlipsReadinessAndReleasesEverything proves the shutdown
// sequence the consumer-exit path shares with SIGTERM: readiness flips to
// not-ready (so /readyz answers 503), the consumer and the pool are closed.
func TestShutdown_FlipsReadinessAndReleasesEverything(t *testing.T) {
	readiness := &readinessGate{}
	consumer := &fakeCloser{}
	pool := &fakePool{}
	consumerDone := make(chan struct{})
	close(consumerDone)
	cancelled := false

	rt := projectorRuntime{
		logger:         quietLogger(),
		srv:            &http.Server{ReadHeaderTimeout: time.Second},
		readiness:      readiness,
		cancelConsumer: func() { cancelled = true },
		consumerDone:   consumerDone,
		consumer:       consumer,
		pool:           pool,
	}
	if err := rt.shutdown(); err != nil {
		t.Fatalf("shutdown = %v", err)
	}
	if !cancelled || !consumer.closed || !pool.closed {
		t.Fatalf("cancelled=%v consumerClosed=%v poolClosed=%v, want all true", cancelled, consumer.closed, pool.closed)
	}

	rec := httptest.NewRecorder()
	newAdminMux(readiness).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz after shutdown = %d, want 503", rec.Code)
	}
}

// TestNewLogger_AcceptsWarningAlias pins D22: LOG_LEVEL=warning behaves
// like warn.
func TestNewLogger_AcceptsWarningAlias(t *testing.T) {
	for _, level := range []string{"warn", "warning", "WARNING"} {
		l := newLogger(level)
		if l.Enabled(context.Background(), slog.LevelInfo) || !l.Enabled(context.Background(), slog.LevelWarn) {
			t.Fatalf("LOG_LEVEL=%q: want warn level", level)
		}
	}
}
