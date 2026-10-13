package http

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/claudioed/process-path-management/internal/analytics/report"
)

// ReportsHandlers is the inbound HTTP adapter for the process-path-management
// "Process Path Catalogue Growth & Change" data product's READER. It depends
// only on the read-model port (report.ReportStore); it never touches the
// OLTP use cases or the writer.
type ReportsHandlers struct {
	Store report.ReportStore
}

// catalogueRowDTO is the wire shape of one report row. It is a dedicated
// DTO so the read-model struct (report.Row) never leaks onto the API.
type catalogueRowDTO struct {
	DayBucket        string `json:"dayBucket"`
	PathsDefined     int    `json:"pathsDefined"`
	PathsRevised     int    `json:"pathsRevised"`
	PathsDeactivated int    `json:"pathsDeactivated"`
}

// catalogueReportDTO is the wire shape of a catalogue-growth report
// response.
type catalogueReportDTO struct {
	Rows []catalogueRowDTO `json:"rows"`
}

// freshnessDTO is the wire shape of the freshness-lag response.
type freshnessDTO struct {
	LagSeconds float64 `json:"lagSeconds"`
}

// GetCatalogueGrowth serves GET /reports/catalogue-growth. from and to
// (RFC3339) are required; granularity is optional (defaults to day).
func (h *ReportsHandlers) GetCatalogueGrowth(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	from, ok := parseRequiredTime(w, r, q.Get("from"), "from")
	if !ok {
		return
	}
	to, ok := parseRequiredTime(w, r, q.Get("to"), "to")
	if !ok {
		return
	}

	granularity := report.GranularityDay
	if g := q.Get("granularity"); g != "" {
		if g != string(report.GranularityDay) {
			writeReportBadRequest(w, r, "granularity must be 'day'")
			return
		}
		granularity = report.Granularity(g)
	}

	rep, err := h.Store.Query(r.Context(), report.ReportQuery{
		From:        from,
		To:          to,
		Granularity: granularity,
	})
	if err != nil {
		writeReportInternal(w, r, err)
		return
	}

	dto := catalogueReportDTO{Rows: make([]catalogueRowDTO, 0, len(rep.Rows))}
	for _, row := range rep.Rows {
		dto.Rows = append(dto.Rows, catalogueRowDTO{
			DayBucket:        row.Key.DayBucket.UTC().Format(time.RFC3339),
			PathsDefined:     row.PathsDefined,
			PathsRevised:     row.PathsRevised,
			PathsDeactivated: row.PathsDeactivated,
		})
	}
	writeJSON(w, http.StatusOK, dto)
}

// GetCatalogueGrowthFreshness serves GET /reports/catalogue-growth/freshness.
func (h *ReportsHandlers) GetCatalogueGrowthFreshness(w http.ResponseWriter, r *http.Request) {
	lag, err := h.Store.FreshnessLag(r.Context())
	if err != nil {
		writeReportInternal(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, freshnessDTO{LagSeconds: lag.Seconds()})
}

// GetReportsHealthz serves GET /healthz for the reports service.
func (h *ReportsHandlers) GetReportsHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// parseRequiredTime parses an RFC3339 timestamp, writing an RFC 7807 400
// and returning ok=false when it is missing or malformed.
func parseRequiredTime(w http.ResponseWriter, r *http.Request, raw, name string) (time.Time, bool) {
	if raw == "" {
		writeReportBadRequest(w, r, "query parameter '"+name+"' is required (RFC3339)")
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		writeReportBadRequest(w, r, "query parameter '"+name+"' must be an RFC3339 timestamp")
		return time.Time{}, false
	}
	return t, true
}

// writeReportBadRequest writes the reports service's RFC 7807 400, reusing
// the service-wide problem writer.
func writeReportBadRequest(w http.ResponseWriter, r *http.Request, detail string) {
	writeProblem(w, http.StatusBadRequest,
		problemInfo{"invalid-report-query", "The report query is malformed or missing a required parameter"},
		detail, r.URL.Path)
}

// writeReportInternal writes the reports service's RFC 7807 500.
func writeReportInternal(w http.ResponseWriter, r *http.Request, err error) {
	writeProblem(w, http.StatusInternalServerError,
		problemInfo{"report-store-error", "The report could not be served"},
		err.Error(), r.URL.Path)
}

// NewReportsRouter builds the chi router for the pathmgmt-reports reader
// service. A nil logger falls back to slog.Default(). The router has no
// OTel middleware: the reports binary sets up the providers (ADR 0029) but
// opens no request spans.
func NewReportsRouter(h *ReportsHandlers, logger *slog.Logger) *chi.Mux {
	if logger == nil {
		logger = slog.Default()
	}

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(RequestLogger(logger))
	r.Use(middleware.Recoverer)

	r.Get("/healthz", h.GetReportsHealthz)

	r.Get("/reports/catalogue-growth", h.GetCatalogueGrowth)
	r.Get("/reports/catalogue-growth/freshness", h.GetCatalogueGrowthFreshness)

	return r
}
