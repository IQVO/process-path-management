//go:build integration

package postgres_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/process-path-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/process-path-management/internal/domain/processpath"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// TestPostgres_ListPage_KeysetWalkMatchesTheUnpagedOrder: keyset pages
// (WHERE id > $after ORDER BY id LIMIT n) walked page by page return every
// row once, in exactly ListAll's order, with no duplicate and no skip, for
// every page size, in both the audit and the active-only view.
func TestPostgres_ListPage_KeysetWalkMatchesTheUnpagedOrder(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	repo := postgres.NewProcessPathRepo(pool)

	ids := []shared.PathId{"PACK", "PICK", "PICK-1", "PICK-10", "PICK-2", "REBIN", "SLAM", "pick", "stow"}
	now := time.Now().UTC().Truncate(time.Microsecond)
	for i, id := range ids {
		p, err := processpath.Define(id, "p", true, []shared.Capability{"cap"}, shared.DestinationLocationRoleUnset, time.Hour, shared.Eligibility{}, shared.Intake{}, now)
		if err != nil {
			t.Fatalf("define %s: %v", id, err)
		}
		if i%3 == 0 { // PACK, PICK-10, SLAM: deactivated
			p.Deactivate(now)
		}
		if err := repo.Save(ctx, p); err != nil {
			t.Fatalf("save %s: %v", id, err)
		}
	}

	all, err := repo.ListAll(ctx)
	if err != nil {
		t.Fatalf("ListAll: %v", err)
	}
	active, err := repo.ListActive(ctx)
	if err != nil {
		t.Fatalf("ListActive: %v", err)
	}
	for _, view := range []struct {
		name       string
		activeOnly bool
		want       []*processpath.ProcessPath
	}{{"all", false, all}, {"active", true, active}} {
		for _, limit := range []int{1, 2, 3, 4, len(ids), len(ids) + 1} {
			var got []string
			var after shared.PathId
			for pages := 0; ; pages++ {
				if pages > len(ids)+1 {
					t.Fatalf("%s limit=%d: walk did not terminate", view.name, limit)
				}
				rows, err := repo.ListPage(ctx, view.activeOnly, after, limit+1) // limit+1: one extra row says "more"
				if err != nil {
					t.Fatalf("ListPage: %v", err)
				}
				for _, r := range rows[:min(len(rows), limit)] {
					got = append(got, string(r.ID()))
				}
				if len(rows) <= limit {
					break
				}
				after = rows[limit-1].ID()
			}
			var want []string
			for _, p := range view.want {
				want = append(want, string(p.ID()))
			}
			if strings.Join(got, ",") != strings.Join(want, ",") {
				t.Fatalf("%s limit=%d: walk = %v, want %v", view.name, limit, got, want)
			}
			if len(got) != len(view.want) {
				t.Fatalf("%s limit=%d: %d rows, want %d", view.name, limit, len(got), len(view.want))
			}
		}
	}
	if len(active) != len(ids)-3 || len(all) != len(ids) {
		t.Fatalf("setup: %d active / %d all, want %d / %d", len(active), len(all), len(ids)-3, len(ids))
	}
}
