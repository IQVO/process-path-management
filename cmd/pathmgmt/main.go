// Command pathmgmt is the composition root for the Process Path
// Management service: it wires config from the environment to adapters,
// use cases, and the HTTP router, then serves it.
//
// Unlike most of this fleet's services, this one has no inbound Kafka
// consumer — it is the SOURCE of the process-path published language,
// not a consumer of anyone else's events. Its only Kafka role is the
// outbound publisher.
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

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	inboundhttp "github.com/claudioed/process-path-management/internal/adapters/inbound/http"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/bootretry"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/events"
	outboundkafka "github.com/claudioed/process-path-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/memory"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/telemetry"
	"github.com/claudioed/process-path-management/internal/application/ports"
	"github.com/claudioed/process-path-management/internal/application/usecases"
)

// version is the service version reported as the OTel `service.version`
// resource attribute. Overridable at build time
// (-ldflags "-X main.version=1.2.3"), else SERVICE_VERSION, else "dev".
var version = ""

func main() {
	if err := run(); err != nil {
		slog.Error("service exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	logger := newLogger(getenv("LOG_LEVEL", "info"))
	slog.SetDefault(logger)

	ctx := context.Background()

	serviceName := getenv("OTEL_SERVICE_NAME", inboundhttp.DefaultServiceName)
	otlpEndpoint := getenv("OTEL_EXPORTER_OTLP_ENDPOINT", telemetry.DefaultOTLPEndpoint)
	shutdownTelemetry, err := telemetry.Setup(ctx, serviceName, serviceVersion(), otlpEndpoint)
	if err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownTelemetry(shutdownCtx); err != nil {
			logger.Warn("telemetry shutdown did not flush cleanly", "error", err)
		}
	}()
	logger.Info("telemetry configured",
		"service_name", serviceName,
		"service_version", serviceVersion(),
		"environment", telemetry.Environment(),
		"otlp_endpoint", otlpEndpoint,
	)

	httpAddr := getenv("HTTP_ADDR", ":8080")
	databaseURL := os.Getenv("DATABASE_URL")
	migrationsPath := getenv("MIGRATIONS_PATH", "migrations")

	persistence, err := buildPersistence(ctx, databaseURL, migrationsPath, logger)
	if err != nil {
		return err
	}
	defer persistence.close()
	repo := persistence.repo

	publisher, relay, closePublisher := buildEventPublisher(persistence, logger)
	defer closePublisher()
	clock := memory.SystemClock{}

	pathMetrics, err := telemetry.NewPathMetrics()
	if err != nil {
		return err
	}

	server := &inboundhttp.Server{
		DefinePath:        &usecases.DefinePath{Repo: repo, Publisher: publisher, Clock: clock, UnitOfWork: persistence.uow, Metrics: pathMetrics},
		RevisePath:        &usecases.RevisePath{Repo: repo, Publisher: publisher, Clock: clock, UnitOfWork: persistence.uow},
		DeactivatePath:    &usecases.DeactivatePath{Repo: repo, Publisher: publisher, Clock: clock, UnitOfWork: persistence.uow},
		GetPath:           &usecases.GetPath{Repo: repo},
		ListPaths:         &usecases.ListPaths{Repo: repo},
		DefineCPTSchedule: &usecases.DefineCPTSchedule{Repo: persistence.scheduleRepo, ProcessPathRepo: repo, Publisher: publisher, Clock: clock, UnitOfWork: persistence.uow},
		GetCPTSchedule:    &usecases.GetCPTSchedule{Repo: persistence.scheduleRepo},
	}

	httpServer := &http.Server{
		Addr:              httpAddr,
		Handler:           inboundhttp.NewRouter(server, logger, serviceName),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("http server listening", "addr", httpAddr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	stopCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// The outbox relay (ADR 0003) runs alongside the HTTP server in the
	// same process, draining outbox_events onto Kafka. It is only wired
	// when both Postgres and the kafka publisher are configured.
	relayDone := make(chan struct{})
	relayCtx, stopRelay := context.WithCancel(stopCtx)
	defer stopRelay()
	if relay != nil {
		go func() {
			defer close(relayDone)
			logger.Info("outbox relay running", "topic", outboundkafka.Topic)
			if err := relay.Run(relayCtx); err != nil && !errors.Is(err, context.Canceled) {
				errCh <- err
			}
		}()
	} else {
		close(relayDone)
	}

	select {
	case err := <-errCh:
		return err
	case <-stopCtx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = httpServer.Shutdown(shutdownCtx)
	// Let the relay finish its in-flight pass so an event committed by a
	// request that completed just before shutdown is not stranded until
	// the next pod boots.
	stopRelay()
	select {
	case <-relayDone:
	case <-shutdownCtx.Done():
		logger.Warn("outbox relay did not stop before the shutdown deadline")
	}
	return err
}

// newLogger builds the process-wide structured logger, wrapped so any
// *Context log call made while a span is active carries trace_id/span_id.
func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
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

// serviceVersion resolves service.version: build-time ldflags first, then
// SERVICE_VERSION, then "dev".
func serviceVersion() string {
	if version != "" {
		return version
	}
	return getenv("SERVICE_VERSION", "dev")
}

// persistence is what buildPersistence wires: the repo the use cases
// read/write through, the Postgres pool (nil when running in-memory),
// and the UnitOfWork that brackets Save + Publish (nil when in-memory,
// which the use cases treat as "run them back to back").
type persistence struct {
	repo         ports.ProcessPathRepo
	scheduleRepo ports.CPTScheduleRepo
	pool         *pgxpool.Pool
	uow          ports.UnitOfWork
	close        func()
}

// buildPersistence wires the outbound ProcessPathRepo and CPTScheduleRepo
// (ADR 0010) together, since they always share the same storage mode
// (both in-memory, or both Postgres over the same pool) — there is no
// scenario in this service where one is backed by Postgres and the other
// is not. With no DATABASE_URL set, the service runs fully functional
// against in-memory repos (no Postgres required for local dev / smoke
// tests) — same fallback convention as every other service in this fleet.
func buildPersistence(ctx context.Context, databaseURL, migrationsPath string, logger *slog.Logger) (*persistence, error) {
	if databaseURL == "" {
		logger.Info("DATABASE_URL not set, using in-memory ProcessPathRepo and CPTScheduleRepo")
		return &persistence{
			repo:         memory.NewProcessPathRepo(),
			scheduleRepo: memory.NewCPTScheduleRepo(),
			close:        func() {},
		}, nil
	}

	// Retried, because in this fleet EVERY injected pod's first outbound
	// TCP dial (Postgres here) fails with "read: connection reset by
	// peer" ~10s after the app starts (Istio 1.30 native sidecars still
	// warming up their outbound listener). A single attempt turns that
	// transient condition into CrashLoopBackOff. The retry is NOT a
	// weakening of the fail-closed rule: once the budget is exhausted
	// this still refuses to boot, reporting the real cause.
	if err := bootretry.Do(ctx, logger, "run migrations", func() error {
		return postgres.RunMigrations(databaseURL, migrationsPath)
	}); err != nil {
		return nil, err
	}
	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	// ParseConfig/NewWithConfig do not themselves establish a connection,
	// so without this the first real failure would surface inside a
	// request rather than at boot.
	if err := bootretry.Do(ctx, logger, "ping database", func() error {
		return pool.Ping(ctx)
	}); err != nil {
		pool.Close()
		return nil, err
	}
	return &persistence{
		repo:         postgres.NewProcessPathRepo(pool),
		scheduleRepo: postgres.NewCPTScheduleRepo(pool),
		pool:         pool,
		uow:          postgres.NewUnitOfWork(pool),
		close:        pool.Close,
	}, nil
}

// buildEventPublisher wires the outbound event publisher, returning it,
// the outbox relay to run alongside the HTTP server (nil when there is
// none), and a close function.
//
// The default is the log publisher, so a local dev run with no Kafka is
// still fully functional. Setting EVENT_PUBLISHER=kafka publishes onto
// warehouse.process-path-management.events, which is what
// fulfillment-execution, wes-work-planning, and workforce-management
// consume to keep their own local path caches current.
//
// With BOTH Postgres and kafka configured the use cases publish into the
// transactional outbox (ADR 0003) and the relay forwards rows to Kafka;
// the store and the topic can no longer diverge. With kafka but no
// Postgres (in-memory dev runs) events go straight to the broker as
// before — there is no transaction to bind them to.
func buildEventPublisher(p *persistence, logger *slog.Logger) (ports.EventPublisher, *postgres.OutboxRelay, func()) {
	if !strings.EqualFold(getenv("EVENT_PUBLISHER", "log"), "kafka") {
		return events.NewLogPublisher(logger), nil, func() {}
	}

	brokers := strings.Split(getenv("KAFKA_BROKERS", "localhost:9092"), ",")
	kafkaPublisher := outboundkafka.NewPublisher(brokers, uuid.NewString)
	closeKafka := func() {
		if err := kafkaPublisher.Close(); err != nil {
			logger.Error("error closing kafka publisher", "error", err)
		}
	}

	if p.pool == nil {
		logger.Info("kafka event publishing enabled (direct, no outbox: DATABASE_URL not set)",
			"brokers", brokers, "topic", outboundkafka.Topic, "analytics_topic", outboundkafka.AnalyticsTopic)
		analyticsPublisher := outboundkafka.NewAnalyticsDirectPublisher(brokers, uuid.NewString)
		fanOut := outboundkafka.FanOutPublisher{kafkaPublisher, analyticsPublisher}
		closeBoth := func() {
			closeKafka()
			if err := analyticsPublisher.Close(); err != nil {
				logger.Error("error closing analytics kafka publisher", "error", err)
			}
		}
		return fanOut, nil, closeBoth
	}

	relay := postgres.NewOutboxRelay(p.pool, kafkaPublisher, logger,
		postgres.WithInterval(durationEnv("OUTBOX_RELAY_INTERVAL", time.Second)))
	logger.Info("kafka event publishing enabled (transactional outbox)",
		"brokers", brokers, "topic", outboundkafka.Topic, "analytics_topic", outboundkafka.AnalyticsTopic)
	// Every domain event is enqueued onto BOTH the integration topic
	// (unchanged, ADR 0003) and the new analytics topic (ADR 0007) in the
	// same transaction as the aggregate write, so the two streams can
	// never diverge from what actually happened.
	newId := uuid.NewString
	outboxPublisher := postgres.NewOutboxPublisher(p.pool, newId,
		outboundkafka.IntegrationEncoder{},
		outboundkafka.NewAnalyticsEncoder(newId),
	)
	return outboxPublisher, relay, closeKafka
}

// durationEnv parses key as a time.Duration, falling back on absence or a
// malformed value (logged by the caller's startup line rather than
// failing the boot: the relay interval is a tuning knob, not a contract).
func durationEnv(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return fallback
	}
	return d
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
