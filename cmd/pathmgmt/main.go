// Command pathmgmt is the composition root for the Process Path
// Management service: it wires config from the environment to adapters,
// use cases, and the HTTP router, then serves it.
//
// Unlike most of this fleet's services, this one has no inbound Kafka
// consumer — it is the SOURCE of the process-path published language,
// not a consumer of anyone else's events. Its only Kafka role is the
// outbound publisher.
//
// Postgres and Kafka are the ONLY runtime (ADR 0037): there is no
// in-memory mode, no log sink, no direct-to-broker path and no optional
// relay. A missing mandatory variable is a boot error naming it, raised
// before any connection is opened and before the listener binds.
//
// Environment:
//
//	DATABASE_URL                  REQUIRED. Postgres DSN
//	MIGRATIONS_DATABASE_URL       direct DSN for the boot migrations (default DATABASE_URL)
//	KAFKA_BROKERS                 REQUIRED. Comma-separated brokers
//	EVENT_PUBLISHER               kafka (default). log is REJECTED at boot
//	HTTP_ADDR                     listen address (default :8080)
//	OUTBOX_RELAY_INTERVAL         relay poll interval (default 1s)
//	OUTBOX_MAX_ATTEMPTS           failures before the relay excludes a row (default 20, 0 = unlimited)
//	HOUSEKEEPING_INTERVAL, IDEMPOTENCY_KEY_TTL, OUTBOX_RETENTION   sweeper knobs (ADR-0018)
//	OTEL_EXPORTER_OTLP_ENDPOINT, LOG_LEVEL, SERVICE_VERSION
//
// /readyz is the drain gate AND a Postgres ping AND a Kafka metadata check
// (a broker answers a request that names no topic: reachability only, never
// topics), cached for a few seconds. /healthz is liveness only. The service
// never creates topics through an admin API: the cluster broker
// auto-creates them on first write.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	inboundhttp "github.com/claudioed/process-path-management/internal/adapters/inbound/http"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/bootretry"
	outboundkafka "github.com/claudioed/process-path-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/system"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/telemetry"
	"github.com/claudioed/process-path-management/internal/application/ports"
	"github.com/claudioed/process-path-management/internal/application/usecases"
)

// version is the service version reported as the OTel `service.version`
// resource attribute. Overridable at build time
// (-ldflags "-X main.version=1.2.3"), else SERVICE_VERSION, else "dev".
var version = ""

func main() {
	if err := run(context.Background()); err != nil {
		slog.Error("service exited with error", "error", err)
		os.Exit(1)
	}
}

// run boots the service and serves until SIGINT/SIGTERM or until ctx is
// cancelled (the in-process integration test stops it that way), then shuts
// down in the fleet order. It reads the whole configuration from the
// environment.
func run(ctx context.Context) error {
	logger := newLogger(getenv("LOG_LEVEL", "info"))
	slog.SetDefault(logger)

	// Fail fast, before telemetry, any connection and the listener: a
	// missing mandatory variable is a boot error that names it.
	cfg, err := loadBootConfig()
	if err != nil {
		return err
	}

	serviceName, shutdownTelemetry, err := setupTelemetry(ctx, logger)
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

	httpAddr := getenv("HTTP_ADDR", ":8080")
	// cfg.migrationsDatabaseURL is MIGRATIONS_DATABASE_URL, a DIRECT
	// (non-pooled, session-mode) Postgres connection string used ONLY for
	// the golang-migrate startup step below — everything else (the pgxpool
	// this process serves requests through) keeps using cfg.databaseURL
	// unchanged. See buildPersistence's doc comment for the full "why":
	// golang-migrate's postgres driver takes a session-scoped
	// `SELECT pg_advisory_lock($1)` to serialize concurrent migration
	// runs, which PgBouncer's transaction-pooling mode does not support
	// (warehouse-infra's PgBouncer rollout, PR #43; this fallback closes
	// the fleet-wide bug that rollout introduced — see ADR
	// 0015-migrations-direct-postgres-connection.md). It falls back to
	// DATABASE_URL when unset.
	persistence, err := buildPersistence(ctx, cfg.databaseURL, cfg.migrationsDatabaseURL, cfg.migrationsPath, logger)
	if err != nil {
		return err
	}
	defer persistence.close()
	repo := persistence.repo

	publisher, relay, closePublisher := buildEventPublisher(persistence, cfg.kafkaBrokers, logger)
	defer closePublisher()
	clock := system.Clock{}

	// Outbox lag gauge (ADR 0018): the age of the oldest unpublished
	// outbox_events row, 0 when drained — the operational signal for
	// "the relay is behind", closing ADR 0003's deferred follow-up. The
	// registration is unregistered before the pool closes (deferred
	// here runs before buildPersistence's pool.Close below it).
	reg, err := postgres.RegisterOutboxLagGauge(persistence.pool)
	if err != nil {
		// A failed metric registration must never take the
		// service down; the gauge is observability, not a
		// dependency.
		logger.Warn("outbox lag gauge registration failed", "error", err)
	} else {
		defer func() { _ = reg.Unregister() }()
	}

	// Housekeeping sweeper (ADR 0018): bounds the two append-only tables
	// (idempotency_keys, published outbox_events) so they do not grow
	// without limit. Runs alongside the HTTP server in the same process
	// and stops with the same signal context.
	sweeper := buildSweeper(persistence.pool, logger)

	pathMetrics, err := telemetry.NewPathMetrics()
	if err != nil {
		return err
	}

	// readiness gates GET /readyz (ADR-0012 §graceful shutdown, ADR 0037,
	// mirroring order-management's ADR-0025): the drain gate, flipped to
	// not-ready as the FIRST step of the shutdown sequence below, before
	// the HTTP server itself stops accepting connections, AND a Postgres
	// ping AND a Kafka metadata check, so a Kubernetes readinessProbe only
	// routes traffic to a pod whose dependencies answer.
	readiness := inboundhttp.NewReadiness(0, 0,
		inboundhttp.Dependency{Name: "postgres", Check: persistence.pool.Ping},
		inboundhttp.Dependency{Name: "kafka", Check: outboundkafka.NewBrokerCheck(cfg.kafkaBrokers).Check},
	)

	server := &inboundhttp.Server{
		DefinePath:        &usecases.DefinePath{Repo: repo, Publisher: publisher, Clock: clock, UnitOfWork: persistence.uow, Metrics: pathMetrics},
		RevisePath:        &usecases.RevisePath{Repo: repo, Publisher: publisher, Clock: clock, UnitOfWork: persistence.uow},
		DeactivatePath:    &usecases.DeactivatePath{Repo: repo, Publisher: publisher, Clock: clock, UnitOfWork: persistence.uow, CPTSchedules: persistence.scheduleRepo},
		GetPath:           &usecases.GetPath{Repo: repo},
		ListPaths:         &usecases.ListPaths{Repo: repo},
		DefineCPTSchedule: &usecases.DefineCPTSchedule{Repo: persistence.scheduleRepo, ProcessPathRepo: repo, Publisher: publisher, Clock: clock, UnitOfWork: persistence.uow},
		GetCPTSchedule:    &usecases.GetCPTSchedule{Repo: persistence.scheduleRepo},
		// IdempotencyPool is the SAME pool buildPersistence opened against
		// DATABASE_URL: the Idempotency-Key check on POST /process-paths is
		// always on (see Server.IdempotencyPool and ADR 0011).
		IdempotencyPool: persistence.pool,
		Readiness:       readiness,
	}

	httpServer := &http.Server{
		Addr:              httpAddr,
		Handler:           inboundhttp.NewRouter(server, logger, serviceName),
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Re-enqueue the current catalogue once (ADR 0038) so a consumer that
	// rebuilds its cache by replaying the topic never depends on Kafka
	// retention. The relay is not running yet; it drains these rows as
	// soon as serveHTTP starts it. Never fatal.
	republishCatalogueAtStartup(ctx, logger, buildRepublishCatalogue(persistence, clock, pathMetrics))

	return serveHTTP(ctx, logger, httpServer, relay, sweeper, readiness)
}

// setupTelemetry configures OTel traces/metrics/logs and emits the
// "telemetry configured" startup line. It returns the resolved service
// name (the HTTP router needs the same value) and the telemetry shutdown
// function; the caller defers the flush itself so it stays last in the
// caller's defer stack, running after every adapter has closed.
func setupTelemetry(ctx context.Context, logger *slog.Logger) (string, func(context.Context) error, error) {
	serviceName := getenv("OTEL_SERVICE_NAME", inboundhttp.DefaultServiceName)
	otlpEndpoint := getenv("OTEL_EXPORTER_OTLP_ENDPOINT", telemetry.DefaultOTLPEndpoint)
	shutdown, err := telemetry.Setup(ctx, serviceName, serviceVersion(), otlpEndpoint)
	if err != nil {
		return "", nil, err
	}
	logger.Info("telemetry configured",
		"service_name", serviceName,
		"service_version", serviceVersion(),
		"environment", telemetry.Environment(),
		"otlp_endpoint", otlpEndpoint,
	)
	return serviceName, shutdown, nil
}

// serveHTTP runs the HTTP server, the outbox relay and the housekeeping
// sweeper until SIGINT or SIGTERM (or until ctx is cancelled), then shuts
// them down: the server drains in-flight requests within a 10s budget while
// the relay is allowed to finish its in-flight pass. It returns the first
// listener/relay error, or the HTTP shutdown error after a
// signal-triggered stop.
func serveHTTP(ctx context.Context, logger *slog.Logger, httpServer *http.Server, relay *postgres.OutboxRelay, sweeper *postgres.Sweeper, readiness *inboundhttp.Readiness) error {
	errCh := make(chan error, 1)
	go func() {
		logger.Info("http server listening", "addr", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	stopCtx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// The outbox relay (ADR 0003) runs alongside the HTTP server in the
	// same process, draining outbox_events onto Kafka. It is always on:
	// the service has no other way to publish.
	relayDone := make(chan struct{})
	relayCtx, stopRelay := context.WithCancel(stopCtx)
	defer stopRelay()
	go func() {
		defer close(relayDone)
		logger.Info("outbox relay running", "topic", outboundkafka.Topic, "analytics_topic", outboundkafka.AnalyticsTopic)
		if err := relay.Run(relayCtx); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
		}
	}()

	// The housekeeping sweeper (ADR 0018) runs on the same signal
	// context. Unlike the relay it never returns a non-nil error (a
	// failed pass is logged and retried on the next tick), so it does
	// not feed errCh; it just needs to stop touching the pool before
	// the deferred pool.Close runs, which the shared stopCtx gives us.
	go func() {
		logger.Info("housekeeping sweeper running")
		_ = sweeper.Run(stopCtx)
	}()

	select {
	case err := <-errCh:
		return err
	case <-stopCtx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Graceful shutdown (ADR-0012 §graceful shutdown, mirroring
	// order-management's ADR-0025), in order:
	//
	//  1. Flip readiness to not-ready FIRST, before anything else
	//     stops — a Kubernetes readinessProbe polling /readyz needs a
	//     window to observe this and stop routing NEW traffic to this
	//     pod before step 2 below ever closes the listener.
	//  2. Stop accepting new HTTP connections and drain in-flight
	//     requests, bounded by shutdownCtx.
	//  3. Stop the outbox relay cleanly: cancel its context (no new
	//     row is picked up after this) and wait, bounded by the SAME
	//     shutdownCtx, for its goroutine to actually finish its
	//     in-flight pass rather than merely asking it to stop and
	//     moving on.
	//  4. Only THEN do the deferred closePublisher/persistence.close
	//     calls (registered earlier in this function, so by defer's
	//     LIFO order closePublisher runs first and persistence.close
	//     — which closes the pgx pool — runs LAST of all, after the
	//     relay has already stopped touching it).
	readiness.SetNotReady()

	err := httpServer.Shutdown(shutdownCtx)
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

// serviceVersion resolves service.version: build-time ldflags first, then
// SERVICE_VERSION, then "dev".
func serviceVersion() string {
	if version != "" {
		return version
	}
	return getenv("SERVICE_VERSION", "dev")
}

// persistence is what buildPersistence wires: the Postgres repos the use
// cases read/write through, the pool, and the UnitOfWork that brackets
// Save + Publish.
type persistence struct {
	repo         ports.ProcessPathRepo
	scheduleRepo ports.CPTScheduleRepo
	pool         *pgxpool.Pool
	uow          ports.UnitOfWork
	close        func()
}

// buildPersistence wires the outbound ProcessPathRepo and CPTScheduleRepo
// (ADR 0010) together over the same Postgres pool. An empty databaseURL is
// a boot error (errNoDatabase): there is no in-memory fallback (ADR 0037).
//
// migrationsDatabaseURL is used ONLY for the golang-migrate step below —
// the pgxpool opened just after it (and used for every subsequent
// request) always uses databaseURL. They are deliberately different
// connection strings in a PgBouncer-fronted environment: golang-migrate's
// postgres driver takes a session-scoped `SELECT pg_advisory_lock($1)` to
// serialize concurrent migration runs across replicas starting at the
// same time, and PgBouncer's transaction-pooling mode (this fleet's
// pool_mode for every OLTP DATABASE_URL, warehouse-infra PR #43) does not
// support session-scoped state — each statement in one logical client
// session can land on a different physical backend connection, so the
// advisory lock never behaves as a real mutex. Losing replicas crash-loop
// with `pq: unnamed prepared statement does not exist` / `pq: canceling
// statement due to statement timeout` until one wins the race. See ADR
// 0015-migrations-direct-postgres-connection.md for the full incident and
// fix (mirroring order-management's ADR-0029). Callers pass
// MIGRATIONS_DATABASE_URL when set (warehouse-infra PR #44 now provisions
// it as a direct, non-pooled DSN alongside DATABASE_URL for all 9 OLTP
// services, this one included) or fall back to databaseURL itself for any
// environment that doesn't provision the split (local dev, CI integration
// tests) — byte-identical to this function's behavior before this
// parameter existed in that case.
func buildPersistence(ctx context.Context, databaseURL, migrationsDatabaseURL, migrationsPath string, logger *slog.Logger) (*persistence, error) {
	if databaseURL == "" {
		return nil, errNoDatabase
	}

	// Retried, because in this fleet EVERY injected pod's first outbound
	// TCP dial (Postgres here) fails with "read: connection reset by
	// peer" ~10s after the app starts (Istio 1.30 native sidecars still
	// warming up their outbound listener). A single attempt turns that
	// transient condition into CrashLoopBackOff. The retry is NOT a
	// weakening of the fail-closed rule: once the budget is exhausted
	// this still refuses to boot, reporting the real cause.
	if err := bootretry.Do(ctx, logger, "run migrations", func() error {
		return postgres.RunMigrations(migrationsDatabaseURL, migrationsPath)
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
// the outbox relay to run alongside the HTTP server, and a close function.
//
// The only publisher is the Kafka-backed transactional outbox (ADR 0003,
// ADR 0037): the use cases publish into outbox_events in the same
// transaction as the aggregate write and the relay forwards the rows onto
// warehouse.process-path-management.events (which fulfillment-execution,
// wes-work-planning and workforce-management consume to keep their own
// local path caches current) and onto the analytics topic. The store and the
// topics can never diverge. There is no log sink and no direct-to-broker
// mode.
func buildEventPublisher(p *persistence, brokers []string, logger *slog.Logger) (ports.EventPublisher, *postgres.OutboxRelay, func()) {
	kafkaPublisher := outboundkafka.NewPublisher(brokers, uuid.NewString)
	closeKafka := func() {
		if err := kafkaPublisher.Close(); err != nil {
			logger.Error("error closing kafka publisher", "error", err)
		}
	}

	// OUTBOX_MAX_ATTEMPTS (ADR 0030, gap fix D12): after this many failed
	// sends a row is no longer claimed, so one poison row cannot block the
	// relay forever. 0 = unlimited.
	maxAttempts := nonNegativeIntEnv(outboxMaxAttemptsEnv, defaultOutboxMaxAttempts, logger)
	relay := postgres.NewOutboxRelay(p.pool, kafkaPublisher, logger,
		postgres.WithInterval(durationEnv("OUTBOX_RELAY_INTERVAL", time.Second)),
		postgres.WithMaxAttempts(maxAttempts))
	logger.Info("kafka event publishing enabled (transactional outbox)",
		"brokers", brokers, "topic", outboundkafka.Topic, "analytics_topic", outboundkafka.AnalyticsTopic,
		"outbox_max_attempts", maxAttempts)
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
// A zero or negative value also falls back: for OUTBOX_RELAY_INTERVAL it
// would be a busy loop, not a meaningful setting. The housekeeping knobs,
// where 0 means "disabled", use housekeepingDurationEnv instead.
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

// housekeepingDurationEnv parses one of the housekeeping sweeper's knobs
// (ADR 0018). Unlike durationEnv, a zero or negative duration is returned
// as is, because the sweeper reads it as "disabled": HOUSEKEEPING_INTERVAL=0
// turns the whole job off, IDEMPOTENCY_KEY_TTL=0 / OUTBOX_RETENTION=0 turn
// off that half (gap fix D15: the ADR, the chart and the sweeper's own doc
// comment always said so, but durationEnv silently replaced 0 with the
// default). Unset or unparseable values fall back.
func housekeepingDurationEnv(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}

// buildSweeper wires the housekeeping sweeper (ADR 0018) from the
// environment. Env vars are read HERE, in the composition root, never in
// the adapter. A 0/negative HOUSEKEEPING_INTERVAL disables the job; a
// 0/negative TTL disables the corresponding half. Safe across replicas
// (each DELETE targets explicitly-eligible rows only).
func buildSweeper(pool *pgxpool.Pool, logger *slog.Logger) *postgres.Sweeper {
	interval := housekeepingDurationEnv("HOUSEKEEPING_INTERVAL", postgres.DefaultSweepInterval)
	idempotencyTTL := housekeepingDurationEnv("IDEMPOTENCY_KEY_TTL", postgres.DefaultIdempotencyKeyTTL)
	outboxRetention := housekeepingDurationEnv("OUTBOX_RETENTION", postgres.DefaultOutboxRetention)
	logger.Info("housekeeping sweeper configured",
		"interval", interval.String(), "enabled", interval > 0,
		"idempotency_key_ttl", idempotencyTTL.String(), "outbox_retention", outboxRetention.String())
	return postgres.NewSweeper(pool,
		postgres.WithSweepInterval(interval),
		postgres.WithIdempotencyKeyTTL(idempotencyTTL),
		postgres.WithOutboxRetention(outboxRetention),
		postgres.WithSweeperLogger(logger),
	)
}

// OUTBOX_MAX_ATTEMPTS caps how many failed sends an outbox row may
// accumulate before the relay stops claiming it (ADR 0030). 0 = unlimited.
const (
	outboxMaxAttemptsEnv     = "OUTBOX_MAX_ATTEMPTS"
	defaultOutboxMaxAttempts = 20
)

// nonNegativeIntEnv reads a non-negative integer from key, falling back to
// fallback for an unset value and, with a warning, for an unparseable or
// negative one. 0 is a valid value (OUTBOX_MAX_ATTEMPTS uses it for
// "unlimited").
func nonNegativeIntEnv(key string, fallback int, logger *slog.Logger) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || n < 0 {
		logger.Warn("ignoring invalid integer env var", "key", key, "value", raw, "fallback", fallback)
		return fallback
	}
	return n
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
