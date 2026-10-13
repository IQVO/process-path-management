package main

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestSetupTelemetry_InstallsTheSDKProviders pins gap fix D21: the
// reports reader calls telemetry.Setup like cmd/pathmgmt, so the OTEL_*
// values the chart injects are used (runtime metrics, the W3C propagator,
// and the trace-aware slog handler). An unreachable Collector must not
// fail boot.
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
