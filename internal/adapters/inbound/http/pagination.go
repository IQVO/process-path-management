package http

import (
	"net/http"

	"github.com/claudioed/process-path-management/internal/adapters/inbound/http/page"
)

// The 400 problems of a paginated collection GET (REST API guideline 3.2, 3.4, 4.2).
// pageParams reads `limit` and `cursor`; filters lists the other query parameters the
// collection declares, and any further one is a 400 (a retired paging parameter such as
// `after` must fail loudly, not read page one forever). On a bad value it has already
// written the 400 problem and reports ok == false.
func pageParams(w http.ResponseWriter, r *http.Request, filters ...string) (limit int, pos *page.Position, ok bool) {
	if name := page.Unknown(r, filters...); name != "" {
		writeProblem(w, http.StatusBadRequest, problemUnknownParam, "unknown query parameter: "+name, r.URL.Path)
		return 0, nil, false
	}
	limit, err := page.Limit(r)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, problemInvalidLimit, err.Error(), r.URL.Path)
		return 0, nil, false
	}
	pos, err = page.After(r)
	if err != nil {
		writeInvalidCursor(w, r)
		return 0, nil, false
	}
	return limit, pos, true
}

func writeInvalidCursor(w http.ResponseWriter, r *http.Request) {
	writeProblem(w, http.StatusBadRequest, problemInvalidCursor, page.ErrInvalidCursor.Error(), r.URL.Path)
}

// pageOf maps one page of rows (fetched with page.FetchSize(limit)) to its
// response: the first `limit` rows through toDTO, plus the cursor after the
// last one when more rows exist.
func pageOf[T, D any](rows []T, limit int, at func(T) page.Position, toDTO func(T) D) page.Page[D] {
	pg := page.Of(rows, limit, at)
	out := page.Page[D]{Items: make([]D, 0, len(pg.Items)), NextCursor: pg.NextCursor}
	for _, row := range pg.Items {
		out.Items = append(out.Items, toDTO(row))
	}
	return out
}

// listQuery reads the query of a collection GET whose one filter is the
// boolean `all` (include the retired/deactivated members): the page size, the
// active-only narrowing (the default; ?all=true opts out) and the id the
// cursor points after ("" on the first page). On a bad value it has already
// written the 400 problem and reports ok == false. A cursor whose sort key is
// not its id was not issued by an id-ordered collection.
func listQuery(w http.ResponseWriter, r *http.Request) (limit int, activeOnly bool, after string, ok bool) {
	limit, pos, ok := pageParams(w, r, "all")
	if !ok {
		return 0, false, "", false
	}
	activeOnly = true
	if vals, present := r.URL.Query()["all"]; present {
		all, err := parseStrictBool(vals[0])
		if err != nil {
			writeProblem(w, http.StatusBadRequest, problemInvalidQueryParam, "could not parse 'all' as a boolean: "+vals[0], r.URL.Path)
			return 0, false, "", false
		}
		activeOnly = !all
	}
	if pos == nil {
		return limit, activeOnly, "", true
	}
	if pos.Sort != pos.ID {
		writeInvalidCursor(w, r)
		return 0, false, "", false
	}
	return limit, activeOnly, pos.ID, true
}
