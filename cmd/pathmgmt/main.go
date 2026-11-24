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
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/process-path-management/internal/adapters/outbound/bootretry"
	outboundkafka "github.com/claudioed/process-path-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/system"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/telemetry"
	"github.com/claudioed/process-path-management/internal/application/ports"
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
	defer telemetryFlush(logger, shutdownTelemetry)()

	httpAddr := getenv("HTTP_ADDR", ":8080")
	// cfg.migrationsDatabaseURL is MIGRATIONS_DATABASE_URL: a DIRECT
	// (non-pooled) Postgres DSN used only for the golang-migrate boot
	// step. See buildPersistence's doc comment for the full "why" (ADR
	// 0015-migrations-direct-postgres-connection.md).
	persistence, err := buildPersistence(ctx, cfg.databaseURL, cfg.migrationsDatabaseURL, cfg.migrationsPath, logger)
	if err != nil {
		return err
	}
	defer persistence.close()

	publisher, relay, closePublisher := buildEventPublisher(persistence, cfg.kafkaBrokers, logger)
	defer closePublisher()
	clock := system.Clock{}

	// Outbox lag gauge (ADR 0018): the age of the oldest unpublished
	// outbox_events row, 0 when drained. The unregister runs before the
	// pool closes (defer LIFO puts it ahead of buildPersistence's
	// pool.Close below it).
	defer registerOutboxLagGauge(persistence.pool, logger)()

	pathMetrics, err := telemetry.NewPathMetrics()
	if err != nil {
		return err
	}

	// ONE resolved TTL drives BOTH sides of idempotency-key retention
	// (ADR 0043's 2026-11-22 addendum): the sweeper's purge (wired in
	// serveHTTP) and the replay read's age guard on the Server — the env
	// is read once, here, so the two can never disagree.
	idempotencyKeyTTL := housekeepingDurationEnv("IDEMPOTENCY_KEY_TTL", postgres.DefaultIdempotencyKeyTTL)

	readiness := newReadiness(persistence.pool, cfg.kafkaBrokers)
	server := buildInboundServer(persistence, publisher, clock, pathMetrics, idempotencyKeyTTL, readiness)

	httpServer := newHTTPServer(httpAddr, serviceName, server, logger)

	// Re-enqueue the current catalogue once (ADR 0038) so a consumer
	// replaying the topic never depends on Kafka retention; never fatal.
	republishCatalogueAtStartup(ctx, logger, buildRepublishCatalogue(persistence, clock, pathMetrics))

	return serveHTTP(ctx, logger, httpServer, relay, persistence.pool, idempotencyKeyTTL, readiness)
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
	flowRepo     ports.ProcessFlowRepo
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
		flowRepo:     postgres.NewProcessFlowRepo(pool),
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

// idempotencyGuardTTL maps the housekeeping IDEMPOTENCY_KEY_TTL knob onto
// the middleware's age guard: a positive window passes through unchanged;
// a disabled retention (<= 0, nothing is ever swept) disables the guard
// too — ONE knob, both sides, so the read and the purge never disagree.
func idempotencyGuardTTL(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return -1
	}
	return ttl
}

// buildSweeper wires the housekeeping sweeper (ADR 0018) from the
// environment. Env vars are read HERE, in the composition root, never in
// the adapter. A 0/negative HOUSEKEEPING_INTERVAL disables the job; a
// 0/negative TTL disables the corresponding half. Safe across replicas
// (each DELETE targets explicitly-eligible rows only). idempotencyTTL
// comes in resolved (shared with the replay read's age guard) so the env
// is read exactly once.
func buildSweeper(pool *pgxpool.Pool, logger *slog.Logger, idempotencyTTL time.Duration) *postgres.Sweeper {
	interval := housekeepingDurationEnv("HOUSEKEEPING_INTERVAL", postgres.DefaultSweepInterval)
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
