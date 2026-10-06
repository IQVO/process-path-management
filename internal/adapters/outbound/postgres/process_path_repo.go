package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/process-path-management/internal/application/ports"
	"github.com/claudioed/process-path-management/internal/domain/processpath"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// ProcessPathRepo is a pgxpool-backed implementation of
// ports.ProcessPathRepo. RequiredCapabilities is stored as a Postgres
// text[] column — a real array type, not a serialized JSON blob, so it
// stays queryable (e.g. "which paths require hazmat") without a JSON
// operator. Eligibility (ADR 0010) is stored as jsonb: unlike
// requiredCapabilities it is a small, cohesive value object always
// read/written as a whole, never queried by an individual field, so a
// JSON column is the more direct fit — matching cpt_schedule_cutoffs'
// own eligible_path_ids array choice for the analogous reasoning in the
// other direction.
type ProcessPathRepo struct {
	pool *pgxpool.Pool
}

// NewProcessPathRepo constructs a ProcessPathRepo over pool.
func NewProcessPathRepo(pool *pgxpool.Pool) *ProcessPathRepo {
	return &ProcessPathRepo{pool: pool}
}

// Create inserts p as a brand-new row at version 1 and refuses to touch an
// existing one: ON CONFLICT (id) DO NOTHING means a concurrent define of
// the same PathId that committed first (or any existing row, Active or
// Deactivated) makes this affect zero rows, reported as
// ports.ErrAlreadyExists (HTTP 409 path-already-exists). Creation must be
// insert-only: routing it through Save's version-guarded upsert let a
// fresh aggregate (version 1) match the winner's row (also version 1) and
// overwrite it, so both racers published ProcessPathCreated. Two
// concurrent inserts of one id serialize on the primary-key index, so the
// loser blocks until the winner commits and then reliably sees the
// conflict.
func (r *ProcessPathRepo) Create(ctx context.Context, p *processpath.ProcessPath) error {
	tag, err := querierFrom(ctx, r.pool).Exec(ctx, `
		INSERT INTO process_paths (id, match_prefix, direct, required_capabilities, destination_location_role, cycle_time_p95, eligibility, status, created_at, updated_at, version)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 1)
		ON CONFLICT (id) DO NOTHING
	`, string(p.ID()), p.MatchPrefix(), p.Direct(), capabilitiesToStrings(p.RequiredCapabilities()), destinationLocationRoleToColumn(p.DestinationLocationRole()), durationToInterval(p.CycleTimeP95()), eligibilityToRow(p.Eligibility()), string(p.Status()), p.CreatedAt(), p.UpdatedAt())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ports.ErrAlreadyExists
	}
	return nil
}

// Save upserts a's current state, version-guarded against a concurrent
// writer (ADR 0017): on an existing row it only applies when the row's
// current version still matches p.Version(), and always advances the row
// by exactly one version. ports.ErrConcurrentModification is returned
// when the row exists but its version no longer matches — the caller
// must re-fetch and retry, not blindly re-Save the same in-memory
// aggregate. A fresh INSERT (no conflicting row) always succeeds and
// starts at version 1.
func (r *ProcessPathRepo) Save(ctx context.Context, p *processpath.ProcessPath) error {
	tag, err := querierFrom(ctx, r.pool).Exec(ctx, `
		INSERT INTO process_paths (id, match_prefix, direct, required_capabilities, destination_location_role, cycle_time_p95, eligibility, status, created_at, updated_at, version)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 1)
		ON CONFLICT (id) DO UPDATE
		  SET match_prefix          = EXCLUDED.match_prefix,
		      required_capabilities = EXCLUDED.required_capabilities,
		      cycle_time_p95        = EXCLUDED.cycle_time_p95,
		      eligibility           = EXCLUDED.eligibility,
		      status                = EXCLUDED.status,
		      updated_at            = EXCLUDED.updated_at,
		      version               = process_paths.version + 1
		WHERE process_paths.version = $11
	`, string(p.ID()), p.MatchPrefix(), p.Direct(), capabilitiesToStrings(p.RequiredCapabilities()), destinationLocationRoleToColumn(p.DestinationLocationRole()), durationToInterval(p.CycleTimeP95()), eligibilityToRow(p.Eligibility()), string(p.Status()), p.CreatedAt(), p.UpdatedAt(), p.Version())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		// The row exists (this is the ON CONFLICT arm; a plain INSERT
		// into an empty slot always affects exactly 1 row) but its
		// version no longer matches what p was loaded at.
		return ports.ErrConcurrentModification
	}
	return nil
}

func (r *ProcessPathRepo) FindByID(ctx context.Context, id shared.PathId) (*processpath.ProcessPath, error) {
	row := querierFrom(ctx, r.pool).QueryRow(ctx, `
		SELECT id, match_prefix, direct, required_capabilities, destination_location_role, cycle_time_p95, eligibility, status, created_at, updated_at, version
		FROM process_paths
		WHERE id = $1
	`, string(id))
	return scanProcessPath(row)
}

func (r *ProcessPathRepo) ListActive(ctx context.Context) ([]*processpath.ProcessPath, error) {
	return r.list(ctx, `
		SELECT id, match_prefix, direct, required_capabilities, destination_location_role, cycle_time_p95, eligibility, status, created_at, updated_at, version
		FROM process_paths
		WHERE status = 'ACTIVE'
		ORDER BY id
	`)
}

func (r *ProcessPathRepo) ListAll(ctx context.Context) ([]*processpath.ProcessPath, error) {
	return r.list(ctx, `
		SELECT id, match_prefix, direct, required_capabilities, destination_location_role, cycle_time_p95, eligibility, status, created_at, updated_at, version
		FROM process_paths
		ORDER BY id
	`)
}

func (r *ProcessPathRepo) list(ctx context.Context, query string) ([]*processpath.ProcessPath, error) {
	rows, err := querierFrom(ctx, r.pool).Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*processpath.ProcessPath
	for rows.Next() {
		p, err := scanProcessPath(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func scanProcessPath(row pgx.Row) (*processpath.ProcessPath, error) {
	var (
		id                      string
		matchPrefix             string
		direct                  bool
		requiredCapabilities    []string
		destinationLocationRole *string
		cycleTimeP95            pgtype.Interval
		eligibility             eligibilityRow
		status                  string
		createdAt, updatedAt    time.Time
		version                 int
	)
	err := row.Scan(&id, &matchPrefix, &direct, &requiredCapabilities, &destinationLocationRole, &cycleTimeP95, &eligibility, &status, &createdAt, &updatedAt, &version)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return processpath.Rehydrate(
		shared.PathId(id),
		matchPrefix,
		direct,
		stringsToCapabilities(requiredCapabilities),
		destinationLocationRoleFromColumn(destinationLocationRole),
		intervalToDuration(cycleTimeP95),
		eligibility.toDomain(),
		processpath.Status(status),
		createdAt,
		updatedAt,
		version,
	), nil
}

// destinationLocationRoleToColumn maps the domain's
// shared.DestinationLocationRoleUnset (empty string) to a real SQL NULL,
// not the empty-string value, so the column reads as "not declared"
// rather than an empty-but-present value.
func destinationLocationRoleToColumn(role shared.DestinationLocationRole) *string {
	if role == shared.DestinationLocationRoleUnset {
		return nil
	}
	v := string(role)
	return &v
}

// destinationLocationRoleFromColumn is the inverse of
// destinationLocationRoleToColumn: a NULL column value rehydrates to
// shared.DestinationLocationRoleUnset.
func destinationLocationRoleFromColumn(v *string) shared.DestinationLocationRole {
	if v == nil {
		return shared.DestinationLocationRoleUnset
	}
	return shared.DestinationLocationRole(*v)
}

func capabilitiesToStrings(caps []shared.Capability) []string {
	out := make([]string, len(caps))
	for i, c := range caps {
		out[i] = string(c)
	}
	return out
}

func stringsToCapabilities(ss []string) []shared.Capability {
	out := make([]shared.Capability, len(ss))
	for i, s := range ss {
		out[i] = shared.Capability(s)
	}
	return out
}

// durationToInterval converts a time.Duration into the Postgres INTERVAL
// wire form. This service only ever writes a duration it built itself
// (never a days/months-granularity value from elsewhere), so the whole
// value is carried in Microseconds and Days/Months are left zero —
// avoiding any lossy day/month normalization on the round trip.
func durationToInterval(d time.Duration) pgtype.Interval {
	return pgtype.Interval{Microseconds: int64(d / time.Microsecond), Valid: true}
}

// intervalToDuration is the inverse of durationToInterval. Days/Months
// are folded in using fixed 24h/30-day conversions in case a value ever
// arrives with those components populated (e.g. hand-edited via psql) —
// this service itself never writes them, but a read path should not
// silently drop data it did not write.
func intervalToDuration(iv pgtype.Interval) time.Duration {
	if !iv.Valid {
		return 0
	}
	d := time.Duration(iv.Microseconds) * time.Microsecond
	d += time.Duration(iv.Days) * 24 * time.Hour
	d += time.Duration(iv.Months) * 30 * 24 * time.Hour
	return d
}

// eligibilityRow is the jsonb wire shape for shared.Eligibility. Kept as
// its own type (not shared.Eligibility itself, which has unexported
// fields) — the same boundary-DTO discipline the HTTP adapter's dto.go
// already documents ("domain structs never cross this boundary").
type eligibilityRow struct {
	MaxUnitsPerLine           *int     `json:"maxUnitsPerLine,omitempty"`
	RequiredProductAttributes []string `json:"requiredProductAttributes,omitempty"`
	ExcludedProductAttributes []string `json:"excludedProductAttributes,omitempty"`
	NonSortable               bool     `json:"nonSortable,omitempty"`
}

func eligibilityToRow(e shared.Eligibility) eligibilityRow {
	return eligibilityRow{
		MaxUnitsPerLine:           e.MaxUnitsPerLine(),
		RequiredProductAttributes: e.RequiredProductAttributes(),
		ExcludedProductAttributes: e.ExcludedProductAttributes(),
		NonSortable:               e.NonSortable(),
	}
}

func (r eligibilityRow) toDomain() shared.Eligibility {
	return shared.NewEligibility(r.MaxUnitsPerLine, r.RequiredProductAttributes, r.ExcludedProductAttributes, r.NonSortable)
}
