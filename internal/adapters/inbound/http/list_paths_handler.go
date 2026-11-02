package http

import (
	"net/http"

	"github.com/claudioed/process-path-management/internal/adapters/inbound/http/page"
	"github.com/claudioed/process-path-management/internal/domain/processpath"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// handleListPaths backs GET /process-paths: one keyset page, {items,
// nextCursor}, in pathId order (REST API guideline section 3). The path id
// is unique, so the cursor carries it as both the sort key and the id.
//
// activeOnly is the default (?all=true opts into the audit view) — matches
// the retired YAML catalogue's own posture that every consumer's normal read
// is "the currently valid set", not everything that ever existed. The value
// must be a real boolean when present: a query like ?all=null is a client
// bug and gets a 400 problem+json, never silently coerced to "false".
func (s *Server) handleListPaths(w http.ResponseWriter, r *http.Request) {
	limit, pos, ok := pageParams(w, r, "all")
	if !ok {
		return
	}
	activeOnly := true
	if vals, present := r.URL.Query()["all"]; present {
		all, err := parseStrictBool(vals[0])
		if err != nil {
			writeProblem(w, http.StatusBadRequest, problemInfo{"invalid-query-parameter", "The 'all' query parameter must be a boolean (true or false)"}, "could not parse 'all' as a boolean: "+vals[0], r.URL.Path)
			return
		}
		activeOnly = !all
	}
	if pos != nil && pos.Sort != pos.ID {
		writeInvalidCursor(w, r)
		return
	}
	var after shared.PathId
	if pos != nil {
		after = shared.PathId(pos.ID)
	}

	paths, err := s.ListPaths.ExecutePage(r.Context(), activeOnly, after, page.FetchSize(limit))
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, pageOf(paths, limit, processPathPosition, toProcessPathResponse))
}

// processPathPosition is the keyset position of a path in GET
// /process-paths: its id, which is unique.
func processPathPosition(p *processpath.ProcessPath) page.Position {
	return page.Position{Sort: string(p.ID()), ID: string(p.ID())}
}
