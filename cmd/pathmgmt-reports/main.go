// Command pathmgmt-reports is the READER composition root of the
// process-path-management "Process Path Catalogue Growth & Change" data
// product. It opens the analytical Postgres database over a read-only
// pool and serves the catalogue-growth report and its freshness over
// REST (apis/openapi-reports.yaml). It writes nothing: the writer
// (cmd/pathmgmt-projector) is a separate deployable and owns the schema
// (ADR 0007, mirroring facility-layout's ADR-0010).
//
// Gap fix D21: like cmd/pathmgmt and cmd/mcp, this process calls
// telemetry.Setup (OTLP traces + metrics, Go runtime metrics, the W3C
// propagator) and logs through the trace-aware slog handler, so the
// OTEL_* / SERVICE_VERSION / ENVIRONMENT values the chart injects are
// used.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	inboundhttp "github.com/claudioed/process-path-management/internal/adapters/inbound/http"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/bootretry"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/telemetry"
)

// defaultServiceName is the OTel service.name when OTEL_SERVICE_NAME is
// unset; the chart sets the same value.
const defaultServiceName = "pathmgmt-reports"

// telemetryFlushTimeout bounds the final export on shutdown, matching
// cmd/pathmgmt and cmd/mcp.
const telemetryFlushTimeout = 5 * time.Second

// errMissingAnalyticsURL is returned when ANALYTICS_DATABASE_URL is unset.
var errMissingAnalyticsURL = errors.New("ANALYTICS_DATABASE_URL is required")

func main() {
	if err := run(); err != nil {
		slog.Error("pathmgmt-reports exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	logger := newLogger(getenv("LOG_LEVEL", "info"))
	slog.SetDefault(logger)

	rootCtx := context.Background()

	_, shutdownTelemetry, err := setupTelemetry(rootCtx, logger)
	if err != nil {
		return err
	}
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), telemetryFlushTimeout)
		defer cancel()
		if err := shutdownTelemetry(flushCtx); err != nil {
			logger.Warn("telemetry flush failed on shutdown", "error", err)
		}
	}()

	httpAddr := getenv("HTTP_ADDR", ":8092")
	analyticsURL := os.Getenv("ANALYTICS_DATABASE_URL")
	if analyticsURL == "" {
		return errMissingAnalyticsURL
	}

	// Read-only pool: even a bug in the reader cannot mutate the read
	// model, on top of the read-only database role
	// ANALYTICS_DATABASE_URL should use.
	pool, err := analyticsstore.NewReadOnlyPool(rootCtx, analyticsURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	// NewReadOnlyPool does not itself establish a connection, so without
	// this the first real failure would surface inside a request rather
	// than at boot. Retried because in this fleet EVERY injected pod's
	// first outbound TCP dial (Postgres here) fails with "read:
	// connection reset by peer" ~10s after the app starts (Istio 1.30
	// native sidecars still warming up their outbound listener). This
	// does not weaken the fail-closed rule: once the budget is exhausted
	// it still refuses to boot, reporting the real cause.
	if err := bootretry.Do(rootCtx, logger, "ping analytics database", func() error {
		return pool.Ping(rootCtx)
	}); err != nil {
		return err
	}

	handlers := &inboundhttp.ReportsHandlers{Store: analyticsstore.NewPostgresReport(pool)}
	router := inboundhttp.NewReportsRouter(handlers, logger)

	srv := &http.Server{Addr: httpAddr, Handler: router, ReadHeaderTimeout: 5 * time.Second}

	go func() {
		logger.Info("reports server listening", "addr", httpAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("reports server failed", "error", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(ctx)
}

// setupTelemetry configures OTel exactly like cmd/pathmgmt (gap fix D21)
// and logs the "telemetry configured" line. Export is non-blocking: an
// unreachable Collector only drops telemetry. It returns the resolved
// service name and the flush function the caller defers.
func setupTelemetry(ctx context.Context, logger *slog.Logger) (string, func(context.Context) error, error) {
	serviceName := getenv("OTEL_SERVICE_NAME", defaultServiceName)
	otlpEndpoint := getenv("OTEL_EXPORTER_OTLP_ENDPOINT", telemetry.DefaultOTLPEndpoint)
	serviceVersion := getenv("SERVICE_VERSION", "dev")
	shutdown, err := telemetry.Setup(ctx, serviceName, serviceVersion, otlpEndpoint)
	if err != nil {
		return "", nil, err
	}
	logger.Info("telemetry configured",
		"service_name", serviceName,
		"service_version", serviceVersion,
		"environment", telemetry.Environment(),
		"otlp_endpoint", otlpEndpoint,
	)
	return serviceName, shutdown, nil
}

// newLogger builds the process-wide JSON slog logger, wrapped so any
// *Context log call made while a span is active carries trace_id/span_id
// (same convention as cmd/pathmgmt). LOG_LEVEL maps
// debug|info|warn|warning|error (case-insensitive); anything else is info.
func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn", "warning":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(telemetry.NewTraceHandler(
		slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}),
	))
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
