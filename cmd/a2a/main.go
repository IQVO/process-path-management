// Command a2a is the composition root for the Process Path Management A2A
// agent: it wires env config to the same outbound ProcessPathRepo selection
// cmd/pathmgmt and cmd/mcp use, wires that repo into the SAME GetPath and
// ListPaths use case structs, then serves Agent2Agent JSON-RPC over HTTP with
// the Agent Card at the well-known path. It is a third, independent deployable
// alongside cmd/pathmgmt (the HTTP service) and cmd/mcp, per ADR 0039.
//
// It reads the same Postgres as cmd/pathmgmt: DATABASE_URL is mandatory and
// its absence is a boot error that names it (ADR 0037); there is no in-memory
// store. Like cmd/mcp it needs no Kafka: it is READ-ONLY, so it never writes,
// never enqueues an outbox row and has no EventPublisher or relay (only
// cmd/pathmgmt drains outbox_events). A missing or malformed mandatory
// variable is a boot error naming it, raised before telemetry, any connection
// and the listener.
//
// It calls no other bounded context and contains no outbound A2A client
// (AGENTS.md: no REST, no MCP, no gRPC, no live lookup, in either direction).
//
// Environment:
//
//	DATABASE_URL             REQUIRED. Postgres DSN
//	A2A_AGENT_URL            REQUIRED. Absolute http(s) URL of this agent's
//	                         JSON-RPC endpoint, advertised on the Agent Card
//	MIGRATIONS_DATABASE_URL  direct DSN for the boot migrations (default DATABASE_URL)
//	MIGRATIONS_PATH          migrations directory (default migrations)
//	A2A_ADDR                 listen address (default :8093)
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/process-path-management/internal/adapters/outbound/bootretry"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/telemetry"
	"github.com/claudioed/process-path-management/internal/application/ports"
)

// errNoDatabase is the boot error of a missing DATABASE_URL: cmd/a2a reads
// the same Postgres as cmd/pathmgmt and has no in-memory store (ADR 0037).
var errNoDatabase = errors.New("DATABASE_URL is required: this context runs on real Postgres")

// errNoAgentURL is the boot error for a missing A2A_AGENT_URL: the Agent Card
// must advertise the absolute URL clients dial (ADR 0039).
var errNoAgentURL = errors.New("A2A_AGENT_URL is required: the Agent Card must advertise the absolute URL of this agent's JSON-RPC endpoint (ADR 0039), e.g. http://<release>-a2a.<ns>.svc.cluster.local:8093/")

// bootConfig is the mandatory configuration of cmd/a2a: real Postgres and the
// URL the Agent Card advertises, with no default and no degraded mode behind
// either.
type bootConfig struct {
	databaseURL string
	agentURL    string
}

// checkAgentURL validates A2A_AGENT_URL: an absolute http(s) URL. The card is
// the discovery contract; a relative or schemeless URL is a misconfiguration
// every client would hit, so it is refused at boot, never silently served.
func checkAgentURL(raw string) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return errNoAgentURL
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("A2A_AGENT_URL=%q is not an absolute http(s) URL: the Agent Card must advertise the endpoint clients dial (ADR 0039)", raw)
	}
	return nil
}

// loadBootConfig reads and validates the mandatory environment. Every problem
// is reported at once, each naming its variable, so the process fails fast at
// boot, before telemetry, any connection and the listener.
func loadBootConfig() (bootConfig, error) {
	cfg := bootConfig{
		databaseURL: os.Getenv("DATABASE_URL"),
		agentURL:    strings.TrimSpace(os.Getenv("A2A_AGENT_URL")),
	}
	var errs []error
	if cfg.databaseURL == "" {
		errs = append(errs, errNoDatabase)
	}
	if err := checkAgentURL(cfg.agentURL); err != nil {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return bootConfig{}, errors.Join(errs...)
	}
	return cfg, nil
}

// telemetryFlushTimeout bounds the final export attempt on shutdown,
// matching cmd/pathmgmt. Without a deadline the exporter would retry
// against an unreachable Collector well past what an orchestrator will
// wait for.
const telemetryFlushTimeout = 5 * time.Second

func main() {
	if err := run(); err != nil {
		slog.Error("a2a agent exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	logger := newLogger(getenv("LOG_LEVEL", "info"))
	slog.SetDefault(logger)

	// Fail fast, before telemetry, any connection and the listener: this
	// binary reads the same Postgres as cmd/pathmgmt and has no in-memory
	// store (ADR 0037), so DATABASE_URL is mandatory, and the Agent Card needs
	// the absolute A2A_AGENT_URL (ADR 0039).
	cfg, err := loadBootConfig()
	if err != nil {
		return err
	}

	ctx := context.Background()

	// Same non-blocking telemetry setup as cmd/pathmgmt: an unreachable
	// Collector degrades to dropped telemetry, never a server that won't
	// start.
	serviceName := getenv("OTEL_SERVICE_NAME", "process-path-management-a2a")
	otlpEndpoint := getenv("OTEL_EXPORTER_OTLP_ENDPOINT", telemetry.DefaultOTLPEndpoint)
	shutdownTelemetry, err := telemetry.Setup(ctx, serviceName, getenv("SERVICE_VERSION", "dev"), otlpEndpoint)
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
	logger.Info("telemetry configured", "service_name", serviceName, "otlp_endpoint", otlpEndpoint)

	a2aAddr := getenv("A2A_ADDR", ":8093")
	databaseURL := cfg.databaseURL
	// See cmd/pathmgmt/main.go's identical fallback and buildRepo's doc
	// comment for the full "why" (session-scoped pg_advisory_lock vs
	// PgBouncer transaction-pooling incompatibility, ADR
	// 0015-migrations-direct-postgres-connection.md). This binary also
	// runs migrations on start (buildRepo below), so it needs the same
	// direct-connection split.
	migrationsDatabaseURL := getenv("MIGRATIONS_DATABASE_URL", databaseURL)
	migrationsPath := getenv("MIGRATIONS_PATH", "migrations")

	repo, _, closeAdapters, err := buildRepo(ctx, databaseURL, migrationsDatabaseURL, migrationsPath, logger)
	if err != nil {
		return err
	}
	defer closeAdapters()
	handler := newAgent(repo, cfg.agentURL, getenv("SERVICE_VERSION", "dev"))

	srv := &http.Server{
		Addr:              a2aAddr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("a2a agent listening (JSON-RPC over HTTP)", "addr", a2aAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	stopCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	select {
	case err := <-errCh:
		return err
	case <-stopCtx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

// buildRepo wires the Postgres ProcessPathRepo, exactly the selection
// cmd/pathmgmt makes. An empty databaseURL is a boot error (errNoDatabase):
// there is no in-memory fallback (ADR 0037).
//
// migrationsDatabaseURL is used ONLY for the golang-migrate step below,
// mirroring cmd/pathmgmt/main.go's buildPersistence exactly — see its
// doc comment for the full "why" a direct, non-pooled connection is
// needed here even though the pgxpool opened just after (databaseURL)
// stays on PgBouncer.
//
// The ONE pool this function opens is also returned, so the caller can
// share it with the CPTScheduleRepo (ADR 0014's connection arithmetic
// counts ONE MaxConns=10 pool per pod; cmd/a2a discards it because its two
// skills need only the ProcessPathRepo).
func buildRepo(ctx context.Context, databaseURL, migrationsDatabaseURL, migrationsPath string, logger *slog.Logger) (ports.ProcessPathRepo, *pgxpool.Pool, func(), error) {
	noop := func() {}

	if databaseURL == "" {
		return nil, nil, noop, errNoDatabase
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
		return nil, nil, noop, err
	}
	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		return nil, nil, noop, err
	}
	// ParseConfig/NewWithConfig do not themselves establish a connection,
	// so without this the first real failure would surface inside a
	// request rather than at boot.
	if err := bootretry.Do(ctx, logger, "ping database", func() error {
		return pool.Ping(ctx)
	}); err != nil {
		pool.Close()
		return nil, nil, noop, err
	}
	return postgres.NewProcessPathRepo(pool), pool, pool.Close, nil
}

// newLogger builds the process-wide structured logger, wrapped so any
// *Context log call made while a span is active carries trace_id/span_id
// — same convention as cmd/pathmgmt.
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
