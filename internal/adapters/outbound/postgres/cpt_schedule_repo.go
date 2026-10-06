// Package postgres's CPTScheduleRepo persists the CPTSchedule aggregate
// (ADR 0010): one row in cpt_schedules per site, and its cutoffs in the
// child table cpt_schedule_cutoffs. Save always replaces the full cutoff
// set (delete-then-reinsert within one statement batch) — matching the
// aggregate's own "Revise replaces wholesale, no partial update"
// semantics, so the child table can never accumulate stale rows from a
// cutoff that was removed on a later revision.
package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/process-path-management/internal/application/ports"
	"github.com/claudioed/process-path-management/internal/domain/cptschedule"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// CPTScheduleRepo is a pgxpool-backed implementation of
// ports.CPTScheduleRepo.
type CPTScheduleRepo struct {
	pool *pgxpool.Pool
}

// NewCPTScheduleRepo constructs a CPTScheduleRepo over pool.
func NewCPTScheduleRepo(pool *pgxpool.Pool) *CPTScheduleRepo {
	return &CPTScheduleRepo{pool: pool}
}

// Save upserts the schedule, version-guarded against a concurrent
// writer (ADR 0017): the cpt_schedules row only updates when its current
// version still matches s.Version(), and always advances by exactly one.
// ports.ErrConcurrentModification is returned on a mismatch. The cutoff
// child rows are replaced wholesale inside the same transaction scope —
// they carry no version of their own because they are only ever written
// through this full-replace Save, never mutated independently.
func (r *CPTScheduleRepo) Save(ctx context.Context, s *cptschedule.CPTSchedule) error {
	q := querierFrom(ctx, r.pool)

	tag, err := q.Exec(ctx, `
		INSERT INTO cpt_schedules (site_id, timezone, created_at, updated_at, version)
		VALUES ($1, $2, $3, $4, 1)
		ON CONFLICT (site_id) DO UPDATE
		  SET timezone   = EXCLUDED.timezone,
		      updated_at = EXCLUDED.updated_at,
		      version    = cpt_schedules.version + 1
		WHERE cpt_schedules.version = $5
	`, string(s.SiteId()), s.Timezone(), s.CreatedAt(), s.UpdatedAt(), s.Version())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		// The row exists (the ON CONFLICT arm) but its version no
		// longer matches what s was loaded at.
		return ports.ErrConcurrentModification
	}

	// Replace the cutoff set wholesale: delete every existing row for
	// this site, then reinsert the current set. This mirrors the
	// aggregate's own Revise semantics (a full replacement, never a
	// partial update) and keeps the child table from accumulating a row
	// for a cutoff a later revision removed.
	if _, err := q.Exec(ctx, `DELETE FROM cpt_schedule_cutoffs WHERE schedule_site_id = $1`, string(s.SiteId())); err != nil {
		return err
	}
	for _, c := range s.Cutoffs() {
		if _, err := q.Exec(ctx, `
			INSERT INTO cpt_schedule_cutoffs (schedule_site_id, cpt_id, local_time, days_of_week, ship_method, eligible_path_ids)
			VALUES ($1, $2, $3, $4, $5, $6)
		`, string(s.SiteId()), c.CptId(), c.LocalTime(), weekdaysToStrings(c.DaysOfWeek()), c.ShipMethod(), pathIdsToStrings(c.EligiblePathIds())); err != nil {
			return err
		}
	}
	return nil
}

func (r *CPTScheduleRepo) FindBySiteID(ctx context.Context, siteId shared.SiteId) (*cptschedule.CPTSchedule, error) {
	q := querierFrom(ctx, r.pool)

	var (
		timezone             string
		createdAt, updatedAt time.Time
		version              int
	)
	err := q.QueryRow(ctx, `
		SELECT timezone, created_at, updated_at, version FROM cpt_schedules WHERE site_id = $1
	`, string(siteId)).Scan(&timezone, &createdAt, &updatedAt, &version)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	rows, err := q.Query(ctx, `
		SELECT cpt_id, local_time, days_of_week, ship_method, eligible_path_ids
		FROM cpt_schedule_cutoffs
		WHERE schedule_site_id = $1
		ORDER BY cpt_id
	`, string(siteId))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cutoffs []cptschedule.Cutoff
	for rows.Next() {
		var (
			cptId, localTime, shipMethod string
			daysOfWeek, eligiblePathIds  []string
		)
		if err := rows.Scan(&cptId, &localTime, &daysOfWeek, &shipMethod, &eligiblePathIds); err != nil {
			return nil, err
		}
		c, err := cptschedule.NewCutoff(cptId, localTime, stringsToWeekdays(daysOfWeek), shipMethod, stringsToPathIds(eligiblePathIds))
		if err != nil {
			return nil, err
		}
		cutoffs = append(cutoffs, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return cptschedule.Rehydrate(siteId, timezone, cutoffs, createdAt, updatedAt, version), nil
}

// ListSiteIDsReferencingPath implements ports.CPTScheduleRepo: every
// site with at least one cutoff whose eligible_path_ids contains id, in
// ascending order. It runs inside the caller's transaction when one is
// bound to ctx (DeactivatePath's unit of work), so the answer is
// consistent with the deactivation it guards.
func (r *CPTScheduleRepo) ListSiteIDsReferencingPath(ctx context.Context, id shared.PathId) ([]shared.SiteId, error) {
	rows, err := querierFrom(ctx, r.pool).Query(ctx, `
		SELECT DISTINCT schedule_site_id
		FROM cpt_schedule_cutoffs
		WHERE $1 = ANY (eligible_path_ids)
		ORDER BY schedule_site_id
	`, string(id))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var sites []shared.SiteId
	for rows.Next() {
		var site string
		if err := rows.Scan(&site); err != nil {
			return nil, err
		}
		sites = append(sites, shared.SiteId(site))
	}
	return sites, rows.Err()
}

func weekdaysToStrings(ds []cptschedule.Weekday) []string {
	out := make([]string, len(ds))
	for i, d := range ds {
		out[i] = string(d)
	}
	return out
}

func stringsToWeekdays(ss []string) []cptschedule.Weekday {
	out := make([]cptschedule.Weekday, len(ss))
	for i, s := range ss {
		out[i] = cptschedule.Weekday(s)
	}
	return out
}

func pathIdsToStrings(ids []shared.PathId) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = string(id)
	}
	return out
}

func stringsToPathIds(ss []string) []shared.PathId {
	out := make([]shared.PathId, len(ss))
	for i, s := range ss {
		out[i] = shared.PathId(s)
	}
	return out
}
