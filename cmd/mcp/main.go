// Command mcp is the composition root for the Process Path Management
// MCP server: it wires env config to the same outbound ProcessPathRepo
// selection cmd/pathmgmt uses, wires that repo into the SAME GetPath and
// ListPaths use case structs cmd/pathmgmt uses, then serves MCP over
// Streamable HTTP. It is a second, independent deployable alongside
// cmd/pathmgmt (the HTTP service).
//
// It never writes: only the two existing read use cases are exposed, as
// read-only MCP tools.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	inboundmcp "github.com/claudioed/process-path-management/internal/adapters/inbound/mcp"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/bootretry"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/memory"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/telemetry"
	"github.com/claudioed/process-path-management/internal/application/ports"
	"github.com/claudioed/process-path-management/internal/application/usecases"
)

// telemetryFlushTimeout bounds the final export attempt on shutdown,
// matching cmd/pathmgmt. Without a deadline the exporter would retry
// against an unreachable Collector well past what an orchestrator will
// wait for.
const telemetryFlushTimeout = 5 * time.Second

func main() {
	if err := run(); err != nil {
		slog.Error("mcp server exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	logger := newLogger(getenv("LOG_LEVEL", "info"))
	slog.SetDefault(logger)

	ctx := context.Background()

	// Same non-blocking telemetry setup as cmd/pathmgmt: an unreachable
	// Collector degrades to dropped telemetry, never a server that won't
	// start.
	serviceName := getenv("OTEL_SERVICE_NAME", "process-path-management-mcp")
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

	mcpAddr := getenv("MCP_ADDR", ":8090")
	databaseURL := os.Getenv("DATABASE_URL")
	// See cmd/pathmgmt/main.go's identical fallback and buildRepo's doc
	// comment for the full "why" (session-scoped pg_advisory_lock vs
	// PgBouncer transaction-pooling incompatibility, ADR
	// 0015-migrations-direct-postgres-connection.md). This binary also
	// runs migrations on start (buildRepo below), so it needs the same
	// direct-connection split.
	migrationsDatabaseURL := getenv("MIGRATIONS_DATABASE_URL", databaseURL)
	migrationsPath := getenv("MIGRATIONS_PATH", "migrations")

	repo, pool, closeAdapters, err := buildRepo(ctx, databaseURL, migrationsDatabaseURL, migrationsPath, logger)
	if err != nil {
		return err
	}
	defer closeAdapters()
	scheduleRepo, err := buildCPTScheduleRepo(ctx, databaseURL, pool, logger)
	if err != nil {
		return err
	}
	// The MCP adapter reuses the SAME read use cases the HTTP adapter
	// uses: GetPath and ListPaths. It never writes, so no
	// EventPublisher/UnitOfWork/Clock is needed here.
	deps := inboundmcp.Deps{
		GetPath:        &usecases.GetPath{Repo: repo},
		ListPaths:      &usecases.ListPaths{Repo: repo},
		GetCPTSchedule: &usecases.GetCPTSchedule{Repo: scheduleRepo},
	}
	// When REPORTS_BASE_URL is set, the curated catalogue-growth report
	// tool is additionally registered, calling the pathmgmt-reports REST
	// service. Left unset, the MCP server serves only the two existing
	// read-only tools, unchanged.
	if reportsURL := os.Getenv("REPORTS_BASE_URL"); reportsURL != "" {
		deps.Reports = inboundmcp.NewReportsRESTClient(reportsURL, nil)
	}
	server := inboundmcp.NewServer(deps)

	handler := newRouter(inboundmcp.Handler(server))

	srv := &http.Server{
		Addr:              mcpAddr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("mcp server listening (Streamable HTTP)", "addr", mcpAddr)
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

// newRouter wraps the MCP handler in the process's HTTP surface:
//
//   - GET /healthz answers 200 {"status":"ok"}, so the Kubernetes
//     liveness/readiness probes (charts/.../mcp-deployment.yaml) have a
//     cheap target.
//   - The MCP Streamable HTTP endpoint is mounted at BOTH "/" and "/mcp"
//     (warehouse-ops-agent's *_MCP_ENDPOINT convention and the docs'
//     examples), so either URL works.
func newRouter(mcpHandler http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.Handle("/mcp", mcpHandler)
	mux.Handle("/mcp/", mcpHandler)
	mux.Handle("/", mcpHandler)
	return mux
}

// buildRepo wires the Postgres ProcessPathRepo when DATABASE_URL is set,
// or falls back to the in-memory repo for local development without a
// database — exactly the selection cmd/pathmgmt makes.
//
// migrationsDatabaseURL is used ONLY for the golang-migrate step below,
// mirroring cmd/pathmgmt/main.go's buildPersistence exactly — see its
// doc comment for the full "why" a direct, non-pooled connection is
// needed here even though the pgxpool opened just after (databaseURL)
// stays on PgBouncer.
//
// The ONE pool this function opens is also returned, so the caller can
// share it with buildCPTScheduleRepo (ADR 0014's connection arithmetic
// counts ONE MaxConns=10 pool per mcp pod, not two — this binary used to
// open a second pool for the CPT schedule repo, silently doubling the
// pod's ceiling to 20 connections). It is nil in the in-memory mode.
func buildRepo(ctx context.Context, databaseURL, migrationsDatabaseURL, migrationsPath string, logger *slog.Logger) (ports.ProcessPathRepo, *pgxpool.Pool, func(), error) {
	noop := func() {}

	if databaseURL == "" {
		logger.Info("DATABASE_URL not set, using in-memory ProcessPathRepo")
		return memory.NewProcessPathRepo(), nil, noop, nil
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

// buildCPTScheduleRepo wires the Postgres CPTScheduleRepo when
// DATABASE_URL is set, or falls back to the in-memory repo otherwise
// (ADR 0010) — mirroring buildRepo's own selection. Migrations have
// already run via buildRepo's own call to postgres.RunMigrations by the
// time this is invoked, so this does not re-run them.
//
// pool is buildRepo's ALREADY-PINGED pool (nil in the in-memory mode):
// both repos share it, holding this pod to ADR 0014's ONE MaxConns=10
// OLTP pool per mcp pod — the same single-pool shape
// cmd/pathmgmt/buildPersistence already uses for exactly the same two
// repos. Sharing also means no second first-dial bootretry is needed
// here: the one pool's Ping already proved the (shared) connection path.
func buildCPTScheduleRepo(ctx context.Context, databaseURL string, pool *pgxpool.Pool, logger *slog.Logger) (ports.CPTScheduleRepo, error) {
	if databaseURL == "" {
		logger.Info("DATABASE_URL not set, using in-memory CPTScheduleRepo")
		return memory.NewCPTScheduleRepo(), nil
	}
	return postgres.NewCPTScheduleRepo(pool), nil
}

// newLogger builds the process-wide structured logger, wrapped so any
// *Context log call made while a span is active carries trace_id/span_id
// — same convention as cmd/pathmgmt.
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

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
