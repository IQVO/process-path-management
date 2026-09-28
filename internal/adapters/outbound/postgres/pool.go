// Package postgres provides pgxpool-backed implementations of the outbound
// ports, plus a golang-migrate runner for the SQL migrations in
// /migrations.
package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// MaxConns is the OLTP pool's per-process connection ceiling, shared by
// both cmd/pathmgmt (the api Deployment, HPA-scalable up to
// charts/process-path-management values.yaml's autoscaling.api.maxReplicas,
// 4) and cmd/mcp (the mcp Deployment, fixed at 1 replica -- see that
// chart value's own doc comment for why it does not get an HPA).
//
// Sized against this shared Postgres instance's REAL max_connections
// (100, an unmodified Bitnami chart default -- warehouse-infra's
// Terraform has no override; the value order-management's ADR 0026
// confirmed live against the same deployed release, since this service
// shares the SAME Postgres instance, not a dedicated one): at the OLTP
// Deployment's HPA ceiling of 4 replicas, 4 * 10 = 40 connections, ~40%
// of the instance-wide ceiling for this ONE of up to 10 fleet services'
// OLTP path alone -- matching order-management's number exactly (ADR
// 0026 / PR #110, the fleet's Phase 3 reference), deliberately leaving
// headroom for the other 9 services (and this service's own mcp/
// projector/reports processes) sharing the same Postgres instance. See
// ADR 0014 for the full connection-budget accounting.
const MaxConns = 10

// StatementTimeout bounds how long a single query may hold a connection
// on the OLTP database before Postgres cancels it. process-path-
// management's OLTP queries are all single-aggregate reads/writes (one
// ProcessPath or CPTSchedule row, keyed by id) that normally complete
// in low milliseconds; 5s is generous headroom for lock contention or a
// slow disk without letting one runaway or blocked query hold a pool
// slot -- and therefore a bulkhead slot the HPA's replica math is
// sizing capacity around -- indefinitely. Matches order-management's
// OLTP value (ADR 0026). See ADR 0014.
const StatementTimeout = "5s"

// NewPool opens a connection pool against databaseURL, with MaxConns and
// StatementTimeout applied to every connection.
func NewPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	return NewPoolWithLimits(ctx, databaseURL, MaxConns, StatementTimeout)
}

// NewPoolWithLimits is NewPool's shared implementation, taking maxConns
// and statementTimeout explicitly so an integration test can drive a
// much shorter timeout directly -- proving the AfterConnect hook really
// applies the setting to every new connection, by triggering an actual
// cancellation -- without waiting out the real production value.
// Production callers should use NewPool.
func NewPoolWithLimits(ctx context.Context, databaseURL string, maxConns int32, statementTimeout string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, err
	}
	config.MaxConns = maxConns
	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, fmt.Sprintf("SET statement_timeout = '%s'", statementTimeout))
		return err
	}
	return pgxpool.NewWithConfig(ctx, config)
}
