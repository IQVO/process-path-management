package usecases

import (
	"context"

	"github.com/claudioed/process-path-management/internal/application/ports"
	"github.com/claudioed/process-path-management/internal/domain/processpath"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// GetPath returns one path by id, active or deactivated.
type GetPath struct {
	Repo ports.ProcessPathRepo
}

func (uc *GetPath) Execute(ctx context.Context, id shared.PathId) (*processpath.ProcessPath, error) {
	p, err := uc.Repo.FindByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if p == nil {
		return nil, ErrPathNotFound
	}
	return p, nil
}

// listAllPageSize is how many paths Execute reads per repository round trip
// while it gathers the complete list for the MCP and A2A adapters.
const listAllPageSize = 500

// ListPaths returns paths for the operator SPA and consumer read models.
// activeOnly=true is the default view (and what every consuming service's
// own cache hydration uses); activeOnly=false is the SPA's audit view.
type ListPaths struct {
	Repo ports.ProcessPathRepo
}

// ExecutePage is one keyset page of the list behind GET /process-paths: at
// most size paths ordered by id, strictly after `after` ("" is the first
// page). The REST adapter asks for limit+1 rows to learn whether a next
// page exists.
func (uc *ListPaths) ExecutePage(ctx context.Context, activeOnly bool, after shared.PathId, size int) ([]*processpath.ProcessPath, error) {
	return uc.Repo.ListPage(ctx, activeOnly, after, size)
}

// Execute returns the COMPLETE list, in id order, for the MCP and A2A
// adapters, whose tool schemas have no cursor: it walks the keyset pages
// until the last one, so a catalogue that outgrows one page is never
// silently truncated.
func (uc *ListPaths) Execute(ctx context.Context, activeOnly bool) ([]*processpath.ProcessPath, error) {
	var all []*processpath.ProcessPath
	var after shared.PathId
	for {
		rows, err := uc.ExecutePage(ctx, activeOnly, after, listAllPageSize+1)
		if err != nil {
			return nil, err
		}
		if len(rows) <= listAllPageSize {
			return append(all, rows...), nil
		}
		all = append(all, rows[:listAllPageSize]...)
		after = rows[listAllPageSize-1].ID()
	}
}
