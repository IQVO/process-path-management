// Wiring helpers of the pathmgmt composition root: the builders run()
// orchestrates (setupTelemetry, buildInboundServer, newReadiness,
// newHTTPServer, the outbox lag gauge) and the serve/shutdown half
// (serveHTTP, shutdownGracefully, telemetryFlush). Extracted from
// main.go so each file stays under the fleet's 300-line file-length
// budget (revive file-length-limit, see .golangci.yml).
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	inboundhttp "github.com/claudioed/process-path-management/internal/adapters/inbound/http"
	outboundkafka "github.com/claudioed/process-path-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/system"
	"github.com/claudioed/process-path-management/internal/adapters/outbound/telemetry"
	"github.com/claudioed/process-path-management/internal/application/ports"
	"github.com/claudioed/process-path-management/internal/application/usecases"
)

// newHTTPServer builds the process's HTTP server over the API router.
func newHTTPServer(addr, serviceName string, server *inboundhttp.Server, logger *slog.Logger) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           inboundhttp.NewRouter(server, logger, serviceName),
		ReadHeaderTimeout: 5 * time.Second,
	}
}

// buildInboundServer wires the use-case handlers the HTTP router serves.
// Every command use case shares the same repo, publisher, clock and
// UnitOfWork buildPersistence opened; IdempotencyPool is the SAME pool
// buildPersistence opened against DATABASE_URL: the Idempotency-Key
// check on POST /process-paths is always on (see Server.IdempotencyPool
// and ADR 0011).
func buildInboundServer(p *persistence, publisher ports.EventPublisher, clock system.Clock, pathMetrics *telemetry.PathMetrics, idempotencyKeyTTL time.Duration, readiness *inboundhttp.Readiness) *inboundhttp.Server {
	repo := p.repo
	return &inboundhttp.Server{
		DefinePath:        &usecases.DefinePath{Repo: repo, Publisher: publisher, Clock: clock, UnitOfWork: p.uow, Metrics: pathMetrics},
		RevisePath:        &usecases.RevisePath{Repo: repo, Publisher: publisher, Clock: clock, UnitOfWork: p.uow},
		DeactivatePath:    &usecases.DeactivatePath{Repo: repo, Publisher: publisher, Clock: clock, UnitOfWork: p.uow, CPTSchedules: p.scheduleRepo, Flows: p.flowRepo},
		GetPath:           &usecases.GetPath{Repo: repo},
		ListPaths:         &usecases.ListPaths{Repo: repo},
		DefineCPTSchedule: &usecases.DefineCPTSchedule{Repo: p.scheduleRepo, ProcessPathRepo: repo, Publisher: publisher, Clock: clock, UnitOfWork: p.uow},
		GetCPTSchedule:    &usecases.GetCPTSchedule{Repo: p.scheduleRepo},
		DefineProcessFlow: &usecases.DefineProcessFlow{Repo: p.flowRepo, ProcessPathRepo: repo, Publisher: publisher, Clock: clock, UnitOfWork: p.uow},
		RetireProcessFlow: &usecases.RetireProcessFlow{Repo: p.flowRepo, Publisher: publisher, Clock: clock, UnitOfWork: p.uow},
		GetProcessFlow:    &usecases.GetProcessFlow{Repo: p.flowRepo},
		ListProcessFlows:  &usecases.ListProcessFlows{Repo: p.flowRepo},
		IdempotencyPool:   p.pool,
		// The replay read's age guard uses the SAME resolved TTL the
		// sweeper got (ADR 0043's 2026-11-22 addendum): an expired key
		// row never answers a replay, even before the sweep deletes it.
		// A disabled retention (<= 0) disables the guard too.
		IdempotencyKeyTTL: idempotencyGuardTTL(idempotencyKeyTTL),
		Readiness:         readiness,
	}
}

// newReadiness gates GET /readyz (ADR-0012 §graceful shutdown, ADR 0037,
// mirroring order-management's ADR-0025): the drain gate, flipped to
// not-ready as the FIRST step of the shutdown sequence, before the HTTP
// server itself stops accepting connections, AND a Postgres ping AND a
// Kafka metadata check, so a Kubernetes readinessProbe only routes
// traffic to a pod whose dependencies answer.
func newReadiness(pool *pgxpool.Pool, brokers []string) *inboundhttp.Readiness {
	return inboundhttp.NewReadiness(0, 0,
		inboundhttp.Dependency{Name: "postgres", Check: pool.Ping},
		inboundhttp.Dependency{Name: "kafka", Check: outboundkafka.NewBrokerCheck(brokers).Check},
	)
}

// registerOutboxLagGauge registers the outbox lag gauge (ADR 0018): the
// age of the oldest unpublished outbox_events row, 0 when drained — the
// operational signal for "the relay is behind", closing ADR 0003's
// deferred follow-up. A failed metric registration must never take the
// service down; the gauge is observability, not a dependency, so on
// failure the returned closure is a no-op. run defers the returned
// closure, which runs before buildPersistence's pool.Close below it.
func registerOutboxLagGauge(pool *pgxpool.Pool, logger *slog.Logger) func() {
	reg, err := postgres.RegisterOutboxLagGauge(pool)
	if err != nil {
		logger.Warn("outbox lag gauge registration failed", "error", err)
		return func() {}
	}
	return func() { _ = reg.Unregister() }
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

// telemetryFlush adapts a telemetry shutdown function into the no-arg
// closure run defers, so the flush — bounded by a 5s budget, a failure
// only warned about — runs after every adapter close registered later in
// run's defer stack.
func telemetryFlush(logger *slog.Logger, shutdown func(context.Context) error) func() {
	return func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdown(flushCtx); err != nil {
			logger.Warn("telemetry shutdown did not flush cleanly", "error", err)
		}
	}
}

// serveHTTP runs the HTTP server, the outbox relay and the housekeeping
// sweeper until SIGINT or SIGTERM (or until ctx is cancelled), then shuts
// them down: the server drains in-flight requests within a 10s budget while
// the relay is allowed to finish its in-flight pass. It returns the first
// listener/relay error, or the HTTP shutdown error after a
// signal-triggered stop.
func serveHTTP(ctx context.Context, logger *slog.Logger, httpServer *http.Server, relay *postgres.OutboxRelay, pool *pgxpool.Pool, idempotencyKeyTTL time.Duration, readiness *inboundhttp.Readiness) error {
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

	// Housekeeping sweeper (ADR 0018): bounds the two append-only tables
	// (idempotency_keys, published outbox_events) so they do not grow
	// without limit. It runs alongside the HTTP server in the same
	// process and on the same signal context; unlike the relay it never
	// returns a non-nil error (a failed pass is logged and retried on the
	// next tick), so it does not feed errCh; it just needs to stop
	// touching the pool before the deferred pool.Close runs, which the
	// shared stopCtx gives us.
	sweeper := buildSweeper(pool, logger, idempotencyKeyTTL)
	go func() {
		logger.Info("housekeeping sweeper running")
		_ = sweeper.Run(stopCtx)
	}()

	select {
	case err := <-errCh:
		return err
	case <-stopCtx.Done():
	}

	// Graceful shutdown (ADR-0012 §graceful shutdown, mirroring
	// order-management's ADR-0025) — see shutdownGracefully.
	return shutdownGracefully(logger, httpServer, readiness, stopRelay, relayDone)
}

// shutdownGracefully runs the fleet's graceful shutdown order
// (ADR-0012 §graceful shutdown, mirroring order-management's ADR-0025),
// once serveHTTP has observed a stop signal or a worker error:
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
//     calls (registered earlier in run, so by defer's LIFO order
//     closePublisher runs first and persistence.close — which
//     closes the pgx pool — runs LAST of all, after the relay has
//     already stopped touching it).
func shutdownGracefully(logger *slog.Logger, httpServer *http.Server, readiness *inboundhttp.Readiness, stopRelay context.CancelFunc, relayDone <-chan struct{}) error {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

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
