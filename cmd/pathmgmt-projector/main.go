// Command pathmgmt-projector is the WRITER composition root of the
// process-path-management "Process Path Catalogue Growth & Change" data
// product. It consumes the analytics Kafka topic, projects each
// catalogue-change event into the analytical Postgres database via the
// idempotent PostgresProjection, and serves only a health/readiness
// endpoint on an admin port. It is the single writer of the analytical
// database and serves no reports; the reader (cmd/pathmgmt-reports) is a
// separate deployable (ADR 0007, mirroring facility-layout's ADR-0010).
//
// Gap fix D21: like cmd/pathmgmt and cmd/mcp, this process calls
// telemetry.Setup (OTLP traces + metrics, Go runtime metrics, the W3C
// propagator) and logs through the trace-aware slog handler, so the
// OTEL_* / SERVICE_VERSION / ENVIRONMENT values the chart injects are
// used. The analytics consumer itself still opens no spans.
//
// ADR 0012 hardens this composition root's graceful shutdown
// (mirroring order-management's ADR-0025 §graceful shutdown): a new
// GET /readyz (distinct from the pre-existing /healthz liveness probe)
// flips to not-ready FIRST, before anything else stops; the analytics
// consumer's context is cancelled and its Run goroutine is AWAITED
// (bounded) so an in-flight message finishes its own commit/DLQ publish
// before this process exits, instead of the pre-existing fire-and-forget
// cancel; and the pgx pool is closed LAST, after both the admin server
// and the consumer have stopped touching it.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	inboundkafka "github.com/claudioed/process-path-management/internal/adapters/inbound/kafka"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/bootretry"
	outboundkafka "github.com/claudioed/process-path-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/telemetry"
)

// defaultServiceName is the OTel service.name when OTEL_SERVICE_NAME is
// unset; the chart sets the same value.
const defaultServiceName = "pathmgmt-projector"

// telemetryFlushTimeout bounds the final export on shutdown, matching
// cmd/pathmgmt and cmd/mcp.
const telemetryFlushTimeout = 5 * time.Second

// errMissingAnalyticsURL is returned when ANALYTICS_DATABASE_URL is
// unset: the projector is the writer of the analytical database and
// cannot start without it.
var errMissingAnalyticsURL = errors.New("ANALYTICS_DATABASE_URL is required")

func main() {
	if err := run(); err != nil {
		slog.Error("pathmgmt-projector exited with error", "error", err)
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

	adminAddr := getenv("ADMIN_ADDR", ":8091")
	analyticsURL := os.Getenv("ANALYTICS_DATABASE_URL")
	if analyticsURL == "" {
		return errMissingAnalyticsURL
	}
	kafkaBrokers := strings.Split(getenv("KAFKA_BROKERS", "localhost:9092"), ",")
	migrationsPath := getenv("ANALYTICS_MIGRATIONS_PATH", "migrations/analytics")

	// pool.Close() is called explicitly at the END of this function's
	// graceful-shutdown sequence (NOT deferred here) so it is guaranteed
	// to run LAST, after the admin server and the analytics consumer
	// have both already stopped touching it (ADR-0012 §graceful
	// shutdown).
	pool, err := openAnalyticsPool(rootCtx, logger, analyticsURL, migrationsPath)
	if err != nil {
		return err
	}

	projection := analyticsstore.NewPostgresProjection(pool)
	consumed := analyticsstore.NewConsumedEventsRepo(pool)
	consumer := inboundkafka.NewAnalyticsConsumer(kafkaBrokers, outboundkafka.AnalyticsTopic, projection, consumed, logger)

	// readiness gates GET /readyz (ADR-0012 §graceful shutdown,
	// mirroring order-management's ADR-0025). The zero value is
	// ready; SetNotReady is called as the FIRST step of the shutdown
	// sequence below, before the admin server itself stops accepting
	// connections.
	readiness := &readinessGate{}

	srv := &http.Server{Addr: adminAddr, Handler: newAdminMux(readiness), ReadHeaderTimeout: 5 * time.Second}

	go func() {
		logger.Info("projector admin server listening", "addr", adminAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("projector admin server failed", "error", err)
		}
	}()

	consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	// consumerExit carries the analytics consumer's Run result the moment
	// the loop returns; consumerDone closes right after, once the Run
	// goroutine has returned — including having committed (or
	// dead-lettered and committed) whatever message it was mid-handling
	// when cancelConsumer was called — so graceful shutdown can wait for
	// a REAL stop, not just fire-and-forget the cancel.
	consumerExit := make(chan error, 1)
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		logger.Info("analytics consumer starting", "topic", outboundkafka.AnalyticsTopic, "group", inboundkafka.AnalyticsConsumerGroup, "brokers", kafkaBrokers)
		consumerExit <- consumer.Run(consumerCtx)
	}()

	stopCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Gap fix D13: the projector stops serving ready when the consumer
	// loop exits. Before, a dead loop was only logged while /healthz and
	// /readyz kept answering 200, so the pod looked healthy and projected
	// nothing. Now a consumer exit runs the same shutdown sequence as a
	// SIGTERM (readiness flips first) and run() returns an error, so main
	// exits non-zero and Kubernetes restarts the pod.
	runErr := awaitStop(stopCtx, consumerExit)
	if runErr != nil {
		logger.Error("analytics consumer loop exited; stopping the projector so it is restarted", "error", runErr)
	}

	rt := projectorRuntime{
		logger:         logger,
		srv:            srv,
		readiness:      readiness,
		cancelConsumer: cancelConsumer,
		consumerDone:   consumerDone,
		consumer:       consumer,
		pool:           pool,
	}
	return errors.Join(runErr, rt.shutdown())
}

// openAnalyticsPool runs the analytical migrations (the projector owns the
// schema) and opens and pings the pool. Both dials are retried, because in
// this fleet EVERY injected pod's first outbound TCP dial (Postgres here)
// fails with "read: connection reset by peer" ~10s after the app starts
// (Istio 1.30 native sidecars still warming up their outbound listener). A
// single attempt turns that transient condition into CrashLoopBackOff. This
// does not weaken the fail-closed rule: once the budget is exhausted it
// still refuses to boot, reporting the real cause. NewPool does not itself
// establish a connection, so without the ping the first real failure would
// surface inside the projection loop rather than at boot.
func openAnalyticsPool(ctx context.Context, logger *slog.Logger, analyticsURL, migrationsPath string) (*pgxpool.Pool, error) {
	if err := bootretry.Do(ctx, logger, "run analytics migrations", func() error {
		return postgres.RunMigrations(analyticsURL, migrationsPath)
	}); err != nil {
		return nil, err
	}
	pool, err := analyticsstore.NewPool(ctx, analyticsURL)
	if err != nil {
		return nil, err
	}
	if err := bootretry.Do(ctx, logger, "ping analytics database", func() error {
		return pool.Ping(ctx)
	}); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

// errConsumerStopped is returned by run() when the analytics consumer loop
// exits on its own, without a shutdown signal (gap fix D13).
var errConsumerStopped = errors.New("analytics consumer loop exited")

// awaitStop blocks until either stopCtx is done (SIGINT/SIGTERM, the
// normal way out: nil) or the consumer loop exits on its own, which is
// always an error: a projector whose loop has stopped projects nothing,
// so it must not keep reporting ready.
func awaitStop(stopCtx context.Context, consumerExit <-chan error) error {
	select {
	case <-stopCtx.Done():
		return nil
	case err := <-consumerExit:
		if err == nil {
			return errConsumerStopped
		}
		return fmt.Errorf("%w: %w", errConsumerStopped, err)
	}
}

// projectorRuntime holds what the shutdown sequence stops, in order.
type projectorRuntime struct {
	logger         *slog.Logger
	srv            *http.Server
	readiness      *readinessGate
	cancelConsumer context.CancelFunc
	consumerDone   <-chan struct{}
	consumer       io.Closer
	pool           interface{ Close() }
}

// shutdown runs the graceful shutdown (ADR-0012 §graceful shutdown,
// mirroring order-management's ADR-0025), in order:
//
//  1. Flip readiness to not-ready FIRST, before anything else stops.
//  2. Stop accepting new admin-server connections and drain in-flight
//     requests, bounded by a 10s shutdown context.
//  3. Stop the analytics consumer's loop cleanly: cancel its context (no
//     new message is fetched/handled after this) and wait, bounded by the
//     SAME context, for its goroutine to actually finish in-flight work (a
//     message already being handled commits its offset, or dead-letters
//     and commits, before Run returns) rather than merely asking it to
//     stop and moving on. When the loop already exited (D13) this returns
//     at once.
//  4. Close the consumer's Kafka reader/DLQ writer and the pgx pool LAST,
//     after both the admin server and the consumer have already stopped
//     touching them.
func (rt projectorRuntime) shutdown() error {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rt.readiness.setNotReady()

	shutdownErr := rt.srv.Shutdown(shutdownCtx)

	rt.cancelConsumer()
	select {
	case <-rt.consumerDone:
	case <-shutdownCtx.Done():
		rt.logger.Warn("analytics consumer did not stop before the shutdown deadline")
	}
	if err := rt.consumer.Close(); err != nil {
		rt.logger.Error("error closing analytics consumer", "error", err)
	}
	rt.pool.Close()

	return shutdownErr
}

// readinessGate is a minimal, process-wide, thread-safe readiness gate
// for this admin-only binary — mirrors inboundhttp.Readiness's shape
// (see that type's doc comment) without importing the HTTP inbound
// adapter package, which this binary otherwise has no reason to depend
// on (it serves a bare net/http mux, not the chi API router).
type readinessGate struct {
	notReady int32
}

func (g *readinessGate) setNotReady() {
	atomic.StoreInt32(&g.notReady, 1)
}

func (g *readinessGate) ready() bool {
	return atomic.LoadInt32(&g.notReady) == 0
}

func (g *readinessGate) handle(w http.ResponseWriter, _ *http.Request) {
	if !g.ready() {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"status":"not_ready"}`))
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ready"}`))
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

// newAdminMux builds the projector's admin endpoints: /healthz is a pure
// liveness signal; /readyz mirrors the readiness gate so a Kubernetes
// readinessProbe observes the shutdown flip (ADR-0012 §graceful shutdown).
func newAdminMux(readiness *readinessGate) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/readyz", readiness.handle)
	return mux
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
