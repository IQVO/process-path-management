// Package postgres's ProcessFlowRepo persists the ProcessFlow aggregate (ADR
// 0042): one row in process_flows per flow and its ordered stages in
// process_flow_stages. Save replaces the full stage set (delete then
// reinsert inside the caller's unit of work), matching the aggregate's own
// "revise replaces wholesale" semantics.
package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/process-path-management/internal/application/ports"
	"github.com/claudioed/process-path-management/internal/domain/processflow"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// ProcessFlowRepo is a pgxpool-backed implementation of ports.ProcessFlowRepo.
type ProcessFlowRepo struct {
	pool *pgxpool.Pool
}

// NewProcessFlowRepo constructs a ProcessFlowRepo over pool.
func NewProcessFlowRepo(pool *pgxpool.Pool) *ProcessFlowRepo {
	return &ProcessFlowRepo{pool: pool}
}

// Create inserts a new flow and its stages. An existing row (ACTIVE or
// RETIRED) writes nothing and returns ports.ErrAlreadyExists.
func (r *ProcessFlowRepo) Create(ctx context.Context, f *processflow.ProcessFlow) error {
	q := querierFrom(ctx, r.pool)
	tag, err := q.Exec(ctx, `
		INSERT INTO process_flows (flow_id, name, status, version, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (flow_id) DO NOTHING
	`, string(f.ID()), f.Name(), string(f.Status()), f.Version(), f.CreatedAt(), f.UpdatedAt())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ports.ErrAlreadyExists
	}
	return insertStages(ctx, q, f)
}

// Save applies f's change, version-guarded (ADR 0017): f carries the NEW
// version and the row only updates when the stored one is exactly one lower.
// ports.ErrConcurrentModification is returned otherwise (including a row that
// no longer exists).
func (r *ProcessFlowRepo) Save(ctx context.Context, f *processflow.ProcessFlow) error {
	q := querierFrom(ctx, r.pool)
	tag, err := q.Exec(ctx, `
		UPDATE process_flows
		   SET name = $2, status = $3, version = $4, updated_at = $5
		 WHERE flow_id = $1 AND version = $4 - 1
	`, string(f.ID()), f.Name(), string(f.Status()), f.Version(), f.UpdatedAt())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ports.ErrConcurrentModification
	}
	if _, err := q.Exec(ctx, `DELETE FROM process_flow_stages WHERE flow_id = $1`, string(f.ID())); err != nil {
		return err
	}
	return insertStages(ctx, q, f)
}

func insertStages(ctx context.Context, q querier, f *processflow.ProcessFlow) error {
	for i, s := range f.Stages() {
		if _, err := q.Exec(ctx, `
			INSERT INTO process_flow_stages (flow_id, position, path_id, process_type)
			VALUES ($1, $2, $3, $4)
		`, string(f.ID()), i, string(s.PathId()), string(s.ProcessType())); err != nil {
			return err
		}
	}
	return nil
}

// FindByID returns the flow, or nil, nil when there is none.
func (r *ProcessFlowRepo) FindByID(ctx context.Context, id processflow.FlowId) (*processflow.ProcessFlow, error) {
	q := querierFrom(ctx, r.pool)
	var (
		name, status         string
		version              int
		createdAt, updatedAt time.Time
	)
	err := q.QueryRow(ctx, `
		SELECT name, status, version, created_at, updated_at FROM process_flows WHERE flow_id = $1
	`, string(id)).Scan(&name, &status, &version, &createdAt, &updatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	stages, err := loadStages(ctx, q, []string{string(id)})
	if err != nil {
		return nil, err
	}
	return processflow.Rehydrate(id, name, stages[string(id)], processflow.Status(status), createdAt, updatedAt, version), nil
}

// ListPage is one keyset page ordered by flow_id, the natural key (unique, so
// it is its own tie-break), served by the primary-key index.
func (r *ProcessFlowRepo) ListPage(ctx context.Context, activeOnly bool, after processflow.FlowId, size int) ([]*processflow.ProcessFlow, error) {
	q := querierFrom(ctx, r.pool)
	rows, err := q.Query(ctx, `
		SELECT flow_id, name, status, version, created_at, updated_at
		  FROM process_flows
		 WHERE flow_id > $1 AND (NOT $2::boolean OR status = 'ACTIVE')
		 ORDER BY flow_id
		 LIMIT $3
	`, string(after), activeOnly, size)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type head struct {
		id, name, status     string
		version              int
		createdAt, updatedAt time.Time
	}
	var heads []head
	var ids []string
	for rows.Next() {
		var h head
		if err := rows.Scan(&h.id, &h.name, &h.status, &h.version, &h.createdAt, &h.updatedAt); err != nil {
			return nil, err
		}
		heads = append(heads, h)
		ids = append(ids, h.id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()

	stages, err := loadStages(ctx, q, ids)
	if err != nil {
		return nil, err
	}
	out := make([]*processflow.ProcessFlow, 0, len(heads))
	for _, h := range heads {
		out = append(out, processflow.Rehydrate(processflow.FlowId(h.id), h.name, stages[h.id], processflow.Status(h.status), h.createdAt, h.updatedAt, h.version))
	}
	return out, nil
}

// loadStages reads the ordered stages of every flow in ids in one query.
func loadStages(ctx context.Context, q querier, ids []string) (map[string][]processflow.Stage, error) {
	out := make(map[string][]processflow.Stage, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	rows, err := q.Query(ctx, `
		SELECT flow_id, path_id, process_type
		  FROM process_flow_stages
		 WHERE flow_id = ANY ($1)
		 ORDER BY flow_id, position
	`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var flowID, pathID, processType string
		if err := rows.Scan(&flowID, &pathID, &processType); err != nil {
			return nil, err
		}
		s, err := processflow.NewStage(shared.PathId(pathID), processflow.ProcessType(processType))
		if err != nil {
			return nil, err
		}
		out[flowID] = append(out[flowID], s)
	}
	return out, rows.Err()
}

// ListActiveFlowIDsReferencingPath returns, ascending, every ACTIVE flow with
// a stage on id. It runs inside the caller's transaction when one is bound to
// ctx (DeactivatePath's unit of work), so the answer is consistent with the
// deactivation it guards.
func (r *ProcessFlowRepo) ListActiveFlowIDsReferencingPath(ctx context.Context, id shared.PathId) ([]processflow.FlowId, error) {
	rows, err := querierFrom(ctx, r.pool).Query(ctx, `
		SELECT f.flow_id
		  FROM process_flows f
		  JOIN process_flow_stages s ON s.flow_id = f.flow_id
		 WHERE s.path_id = $1 AND f.status = 'ACTIVE'
		 ORDER BY f.flow_id
	`, string(id))
	if err != nil {
		return nil, err
	}
	return scanFlowIDs(rows)
}

// LockActiveForShare is SELECT ... FOR SHARE on every ACTIVE flow row in
// ascending id order (the stable lock order), returning the ids. FOR SHARE
// coexists with other share locks but conflicts with the UPDATE a concurrent
// revision's Save performs, so that revision either commits before this read
// (and is seen) or waits until the caller's unit of work ends (and its event
// is enqueued after the caller's). Outside a unit of work the locks release
// at once.
func (r *ProcessFlowRepo) LockActiveForShare(ctx context.Context) ([]processflow.FlowId, error) {
	rows, err := querierFrom(ctx, r.pool).Query(ctx, `
		SELECT flow_id FROM process_flows WHERE status = 'ACTIVE' ORDER BY flow_id FOR SHARE
	`)
	if err != nil {
		return nil, err
	}
	return scanFlowIDs(rows)
}

func scanFlowIDs(rows pgx.Rows) ([]processflow.FlowId, error) {
	defer rows.Close()
	var out []processflow.FlowId
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, processflow.FlowId(id))
	}
	return out, rows.Err()
}

var _ ports.ProcessFlowRepo = (*ProcessFlowRepo)(nil)
