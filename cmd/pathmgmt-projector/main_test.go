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

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
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

// TestSetupTelemetry_InstallsTheSDKProviders pins gap fix D21: the
// projector calls telemetry.Setup like cmd/pathmgmt, so the OTEL_* values
// the chart injects are used (runtime metrics, the W3C propagator, and the
// trace-aware slog handler). An unreachable Collector must not fail boot.
func TestSetupTelemetry_InstallsTheSDKProviders(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "127.0.0.1:1")
	t.Setenv("OTEL_SERVICE_NAME", "")

	serviceName, shutdown, err := setupTelemetry(context.Background(), quietLogger())
	if err != nil {
		t.Fatalf("setupTelemetry: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = shutdown(ctx)
	})
	if serviceName != defaultServiceName {
		t.Fatalf("service name = %q, want the default %q", serviceName, defaultServiceName)
	}
	if _, ok := otel.GetTracerProvider().(*sdktrace.TracerProvider); !ok {
		t.Fatalf("global tracer provider = %T, want the SDK provider telemetry.Setup installs", otel.GetTracerProvider())
	}
}
