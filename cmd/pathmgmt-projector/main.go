// Command pathmgmt-projector is the WRITER composition root of the
// process-path-management "Process Path Catalogue Growth & Change" data
// product. It consumes the analytics Kafka topic, projects each
// catalogue-change event into the analytical Postgres database via the
// idempotent PostgresProjection, and serves only a health/readiness
// endpoint on an admin port. It is the single writer of the analytical
// database and serves no reports; the reader (cmd/pathmgmt-reports) is a
// separate deployable (ADR 0007, mirroring facility-layout's ADR-0010).
//
// Consistent with the rest of the analytics pipeline, this process is
// trace-free: process-path-management has no observability/OTel package
// for it.
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
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	inboundkafka "github.com/claudioed/process-path-management/internal/adapters/inbound/kafka"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/bootretry"
	outboundkafka "github.com/claudioed/process-path-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/postgres"
)

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

	adminAddr := getenv("ADMIN_ADDR", ":8091")
	analyticsURL := os.Getenv("ANALYTICS_DATABASE_URL")
	if analyticsURL == "" {
		return errMissingAnalyticsURL
	}
	kafkaBrokers := strings.Split(getenv("KAFKA_BROKERS", "localhost:9092"), ",")
	migrationsPath := getenv("ANALYTICS_MIGRATIONS_PATH", "migrations/analytics")

	// The projector owns the analytical schema: run its migrations on
	// start. Retried, because in this fleet EVERY injected pod's first
	// outbound TCP dial (Postgres here) fails with "read: connection
	// reset by peer" ~10s after the app starts (Istio 1.30 native
	// sidecars still warming up their outbound listener). A single
	// attempt turns that transient condition into CrashLoopBackOff. This
	// does not weaken the fail-closed rule: once the budget is exhausted
	// it still refuses to boot, reporting the real cause.
	if err := bootretry.Do(rootCtx, logger, "run analytics migrations", func() error {
		return postgres.RunMigrations(analyticsURL, migrationsPath)
	}); err != nil {
		return err
	}

	pool, err := analyticsstore.NewPool(rootCtx, analyticsURL)
	if err != nil {
		return err
	}
	// pool.Close() is called explicitly at the END of this function's
	// graceful-shutdown sequence (NOT deferred here) so it is guaranteed
	// to run LAST, after the admin server and the analytics consumer
	// have both already stopped touching it (ADR-0012 §graceful
	// shutdown).
	// NewPool does not itself establish a connection, so without this the
	// first real failure would surface inside the projection loop rather
	// than at boot.
	if err := bootretry.Do(rootCtx, logger, "ping analytics database", func() error {
		return pool.Ping(rootCtx)
	}); err != nil {
		pool.Close()
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

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/readyz", readiness.handle)
	srv := &http.Server{Addr: adminAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	go func() {
		logger.Info("projector admin server listening", "addr", adminAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("projector admin server failed", "error", err)
		}
	}()

	consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	// consumerDone closes once the analytics consumer's Run goroutine
	// has returned — including having committed (or dead-lettered and
	// committed) whatever message it was mid-handling when
	// cancelConsumer was called — so graceful shutdown can wait for a
	// REAL stop, not just fire-and-forget the cancel.
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		logger.Info("analytics consumer starting", "topic", outboundkafka.AnalyticsTopic, "group", inboundkafka.AnalyticsConsumerGroup, "brokers", kafkaBrokers)
		if err := consumer.Run(consumerCtx); err != nil {
			logger.Error("analytics consumer stopped", "error", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Graceful shutdown (ADR-0012 §graceful shutdown, mirroring
	// order-management's ADR-0025), in order:
	//
	//  1. Flip readiness to not-ready FIRST, before anything else
	//     stops.
	//  2. Stop accepting new admin-server connections and drain
	//     in-flight requests, bounded by shutdownCtx.
	//  3. Stop the analytics consumer's loop cleanly: cancel its
	//     context (no new message is fetched/handled after this) and
	//     wait, bounded by the SAME shutdownCtx, for its goroutine to
	//     actually finish in-flight work (a message already being
	//     handled commits its offset, or dead-letters and commits,
	//     before Run returns) rather than merely asking it to stop and
	//     moving on.
	//  4. Close the pgx pool and the consumer's Kafka reader/DLQ
	//     writer LAST, after both the admin server and the consumer
	//     have already stopped touching them.
	readiness.setNotReady()

	shutdownErr := srv.Shutdown(shutdownCtx)

	cancelConsumer()
	select {
	case <-consumerDone:
	case <-shutdownCtx.Done():
		logger.Warn("analytics consumer did not stop before the shutdown deadline")
	}
	if err := consumer.Close(); err != nil {
		logger.Error("error closing analytics consumer", "error", err)
	}
	pool.Close()

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

// newLogger builds a JSON slog logger at the given level. The analytics
// processes log structured JSON but do not wire OTel, so this is a plain
// handler.
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
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
