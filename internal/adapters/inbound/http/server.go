package http

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riandyrn/otelchi"
	otelchimetric "github.com/riandyrn/otelchi/metric"

	"github.com/claudioed/process-path-management/internal/application/usecases"
	"github.com/claudioed/process-path-management/internal/domain/cptschedule"
	"github.com/claudioed/process-path-management/internal/domain/processpath"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// DefaultServiceName labels this service in logs, spans and metrics when
// the caller does not supply one. It matches the OTel resource's
// service.name.
const DefaultServiceName = "process-path-management"

// Server holds every use case the HTTP adapter depends on.
type Server struct {
	DefinePath     *usecases.DefinePath
	RevisePath     *usecases.RevisePath
	DeactivatePath *usecases.DeactivatePath
	GetPath        *usecases.GetPath
	ListPaths      *usecases.ListPaths
	// DefineCPTSchedule and GetCPTSchedule are optional (ADR 0010): a nil
	// value means the CPT schedule surface is not wired, and the
	// corresponding endpoints are simply never reached (they are always
	// registered on the router; nil use cases would only be a wiring
	// bug, not an expected runtime state — this mirrors how every other
	// use case field here is always populated by the composition root).
	DefineCPTSchedule *usecases.DefineCPTSchedule
	GetCPTSchedule    *usecases.GetCPTSchedule
	// IdempotencyPool, when non-nil, wires RequireIdempotencyKey onto
	// POST /process-paths (see idempotency.go). A nil pool means "no
	// transactional Postgres backing wired" (in-memory dev/test
	// configuration) — the idempotency middleware needs a real
	// pgxpool.Pool to begin its own transaction, so it is simply not
	// applied in that case, exactly this codebase's existing convention
	// for every other optional Postgres-backed capability (UnitOfWork,
	// the outbox relay).
	IdempotencyPool *pgxpool.Pool
	// Readiness backs GET /readyz (ADR-0012 §graceful shutdown,
	// mirroring order-management's ADR-0025). A nil Readiness (the
	// zero value, and every pre-existing caller/test) means /readyz
	// always reports ready — see Readiness's own doc comment.
	Readiness *Readiness
}

// NewRouter builds the chi router for this service's REST API. A nil
// logger defaults to slog.Default(); an empty serviceName defaults to
// DefaultServiceName.
//
// Middleware order matters here (fleet-standard-metrics ADR, Tier 1 item
// 2): otelchi runs before RequestLogger so the request context already
// carries a span by the time a line is logged. WithChiRoutes resolves
// the route pattern up front, so spans/metrics are labeled
// "/process-paths/{pathId}" rather than one distinct name per path id.
func NewRouter(s *Server, logger *slog.Logger, serviceName string) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	if serviceName == "" {
		serviceName = DefaultServiceName
	}

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(otelchi.Middleware(serviceName, otelchi.WithChiRoutes(r)))
	// Emits http.server.request.duration (seconds) per OTel HTTP semantic
	// conventions; no hand-rolled histogram needed.
	r.Use(otelchimetric.NewServerRequestDuration(otelchimetric.NewBaseConfig(serviceName)))
	r.Use(RequestLogger(logger))
	r.Use(middleware.Recoverer)
	r.Use(corsMiddleware())

	r.Get("/healthz", s.handleHealthz)
	r.Get("/readyz", s.handleReadyz)

	// POST /process-paths is route-scoped (r.With, not r.Use) behind
	// RequireIdempotencyKey — it is the one mutating endpoint that
	// creates a NEW resource. Its PathId is caller-supplied (not
	// server-generated), so a byte-identical retry would otherwise hit
	// DefinePath's own ErrPathAlreadyExists natural-key check and come
	// back as a confusing 409 Conflict instead of a clean idempotent
	// replay of the original 201 — the middleware fixes exactly that
	// case. The other mutating routes (PUT /process-paths/{pathId},
	// PUT /sites/{siteId}/cpt-schedule) are idempotent-by-PUT-semantics
	// already and are deliberately left unprotected for v1 (see the
	// ADR). IdempotencyPool nil (in-memory dev/test configuration, no
	// transactional Postgres backing) skips the middleware entirely,
	// mirroring every other optional Postgres-backed capability's nil
	// convention in this repo.
	if s.IdempotencyPool != nil {
		r.With(RequireIdempotencyKey(s.IdempotencyPool)).Post("/process-paths", s.handleDefinePath)
	} else {
		r.Post("/process-paths", s.handleDefinePath)
	}
	r.Get("/process-paths", s.handleListPaths)
	r.Get("/process-paths/{pathId}", s.handleGetPath)
	r.Put("/process-paths/{pathId}", s.handleRevisePath)
	r.Delete("/process-paths/{pathId}", s.handleDeactivatePath)

	r.Put("/sites/{siteId}/cpt-schedule", s.handleDefineCPTSchedule)
	r.Get("/sites/{siteId}/cpt-schedule", s.handleGetCPTSchedule)

	// Unroutable requests (e.g. an empty path-id segment, which chi's
	// trie never matches) get the same RFC 7807 shape as every other
	// error this API returns, instead of Go's default text/plain
	// "404 page not found" — the API's own documented 404 content type
	// is application/problem+json.
	r.NotFound(func(w http.ResponseWriter, r *http.Request) {
		writeProblem(w, http.StatusNotFound, problemInfo{"route-not-found", "No route matches this request"}, "the request path does not match any operation in this API", r.URL.Path)
	})

	return r
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleDefinePath(w http.ResponseWriter, r *http.Request) {
	var req defineProcessPathRequest
	if !decodeJSON(w, r, &req) {
		return
	}

	destinationLocationRole, err := shared.ParseDestinationLocationRole(req.DestinationLocationRole)
	if err != nil {
		writeError(w, r, err)
		return
	}
	cycleTimeP95, err := parseCycleTimeP95(req.CycleTimeP95)
	if err != nil {
		writeError(w, r, err)
		return
	}

	p, err := s.DefinePath.Execute(r.Context(), shared.PathId(req.PathId), req.MatchPrefix, req.Direct, toCapabilities(req.RequiredCapabilities), destinationLocationRole, cycleTimeP95, toEligibility(req.Eligibility))
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, toProcessPathResponse(p))
}

func (s *Server) handleListPaths(w http.ResponseWriter, r *http.Request) {
	// activeOnly is the default (?all=true opts into the audit view) —
	// matches the retired YAML catalogue's own posture that every
	// consumer's normal read is "the currently valid set", not
	// everything that ever existed. The value must be a real boolean
	// when present: a query like ?all=null is a client bug and gets a
	// 400 problem+json, never silently coerced to "false".
	activeOnly := true
	if vals, ok := r.URL.Query()["all"]; ok {
		all, err := parseStrictBool(vals[0])
		if err != nil {
			writeProblem(w, http.StatusBadRequest, problemInfo{"invalid-query-parameter", "The 'all' query parameter must be a boolean (true or false)"}, "could not parse 'all' as a boolean: "+vals[0], r.URL.Path)
			return
		}
		activeOnly = !all
	}

	paths, err := s.ListPaths.Execute(r.Context(), activeOnly)
	if err != nil {
		writeError(w, r, err)
		return
	}
	responses := make([]processPathResponse, len(paths))
	for i, p := range paths {
		responses[i] = toProcessPathResponse(p)
	}
	writeJSON(w, http.StatusOK, responses)
}

func (s *Server) handleGetPath(w http.ResponseWriter, r *http.Request) {
	pathId := shared.PathId(chi.URLParam(r, "pathId"))

	p, err := s.GetPath.Execute(r.Context(), pathId)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toProcessPathResponse(p))
}

func (s *Server) handleRevisePath(w http.ResponseWriter, r *http.Request) {
	pathId := shared.PathId(chi.URLParam(r, "pathId"))

	var req reviseProcessPathRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	cycleTimeP95, err := parseCycleTimeP95(req.CycleTimeP95)
	if err != nil {
		writeError(w, r, err)
		return
	}

	p, err := s.RevisePath.Execute(r.Context(), pathId, req.MatchPrefix, toCapabilities(req.RequiredCapabilities), cycleTimeP95, toEligibility(req.Eligibility))
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toProcessPathResponse(p))
}

func (s *Server) handleDeactivatePath(w http.ResponseWriter, r *http.Request) {
	pathId := shared.PathId(chi.URLParam(r, "pathId"))

	if err := s.DeactivatePath.Execute(r.Context(), pathId); err != nil {
		writeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleDefineCPTSchedule(w http.ResponseWriter, r *http.Request) {
	siteId := shared.SiteId(chi.URLParam(r, "siteId"))

	var req defineCPTScheduleRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	cutoffs, err := toCutoffs(req.Cutoffs)
	if err != nil {
		writeError(w, r, err)
		return
	}

	sched, err := s.DefineCPTSchedule.Execute(r.Context(), siteId, req.Timezone, cutoffs)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toCPTScheduleResponse(sched))
}

func (s *Server) handleGetCPTSchedule(w http.ResponseWriter, r *http.Request) {
	siteId := shared.SiteId(chi.URLParam(r, "siteId"))

	sched, err := s.GetCPTSchedule.Execute(r.Context(), siteId)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toCPTScheduleResponse(sched))
}

const timeFormat = time.RFC3339

func toProcessPathResponse(p *processpath.ProcessPath) processPathResponse {
	caps := p.RequiredCapabilities()
	strCaps := make([]string, len(caps))
	for i, c := range caps {
		strCaps[i] = string(c)
	}
	return processPathResponse{
		PathId:                  string(p.ID()),
		MatchPrefix:             p.MatchPrefix(),
		Direct:                  p.Direct(),
		RequiredCapabilities:    strCaps,
		DestinationLocationRole: string(p.DestinationLocationRole()),
		CycleTimeP95:            p.CycleTimeP95().String(),
		Eligibility:             toEligibilityResponse(p.Eligibility()),
		Status:                  string(p.Status()),
		CreatedAt:               p.CreatedAt().UTC().Format(timeFormat),
		UpdatedAt:               p.UpdatedAt().UTC().Format(timeFormat),
	}
}

func toEligibilityResponse(e shared.Eligibility) eligibilityResponse {
	return eligibilityResponse{
		MaxUnitsPerLine:           e.MaxUnitsPerLine(),
		RequiredProductAttributes: e.RequiredProductAttributes(),
		ExcludedProductAttributes: e.ExcludedProductAttributes(),
		NonSortable:               e.NonSortable(),
	}
}

// toEligibility maps the request DTO onto shared.Eligibility. A nil req
// (no "eligibility" key on the request body) maps to the fully
// permissive zero value (ADR 0010).
func toEligibility(req *eligibilityRequest) shared.Eligibility {
	if req == nil {
		return shared.Eligibility{}
	}
	return shared.NewEligibility(req.MaxUnitsPerLine, req.RequiredProductAttributes, req.ExcludedProductAttributes, req.NonSortable)
}

// parseCycleTimeP95 parses a Go duration string (e.g. "2h", "90m"). A
// malformed value is reported the same way the domain's own
// ErrInvalidCycleTime is (422) via statusFor/problemFor's default
// mapping, so callers see a consistent RFC 7807 shape either way.
func parseCycleTimeP95(s string) (time.Duration, error) {
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, processpath.ErrInvalidCycleTime
	}
	return d, nil
}

func toCutoffs(reqs []cutoffRequest) ([]cptschedule.Cutoff, error) {
	out := make([]cptschedule.Cutoff, 0, len(reqs))
	for _, r := range reqs {
		c, err := cptschedule.NewCutoff(r.CptId, r.LocalTime, toWeekdays(r.DaysOfWeek), r.ShipMethod, toPathIds(r.EligiblePathIds))
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, nil
}

func toWeekdays(ss []string) []cptschedule.Weekday {
	out := make([]cptschedule.Weekday, len(ss))
	for i, s := range ss {
		out[i] = cptschedule.Weekday(s)
	}
	return out
}

func toPathIds(ss []string) []shared.PathId {
	out := make([]shared.PathId, len(ss))
	for i, s := range ss {
		out[i] = shared.PathId(s)
	}
	return out
}

func toCPTScheduleResponse(s *cptschedule.CPTSchedule) cptScheduleResponse {
	cutoffs := s.Cutoffs()
	out := make([]cutoffResponse, 0, len(cutoffs))
	for _, c := range cutoffs {
		out = append(out, cutoffResponse{
			CptId:           c.CptId(),
			LocalTime:       c.LocalTime(),
			DaysOfWeek:      weekdaysToStrings(c.DaysOfWeek()),
			ShipMethod:      c.ShipMethod(),
			EligiblePathIds: pathIdsToStrings(c.EligiblePathIds()),
		})
	}
	return cptScheduleResponse{
		SiteId:    string(s.SiteId()),
		Timezone:  s.Timezone(),
		Cutoffs:   out,
		CreatedAt: s.CreatedAt().UTC().Format(timeFormat),
		UpdatedAt: s.UpdatedAt().UTC().Format(timeFormat),
	}
}

func weekdaysToStrings(ds []cptschedule.Weekday) []string {
	out := make([]string, len(ds))
	for i, d := range ds {
		out[i] = string(d)
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

func toCapabilities(ss []string) []shared.Capability {
	out := make([]shared.Capability, len(ss))
	for i, s := range ss {
		out[i] = shared.Capability(s)
	}
	return out
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dest any) bool {
	if err := json.NewDecoder(r.Body).Decode(dest); err != nil {
		writeProblem(w, http.StatusBadRequest, problemInfo{"malformed-request-body", "The request body is not valid JSON"}, err.Error(), r.URL.Path)
		return false
	}
	return true
}

// writeError writes a domain/application error as an RFC 7807
// (application/problem+json) response.
func writeError(w http.ResponseWriter, r *http.Request, err error) {
	writeProblem(w, statusFor(err), problemFor(err), err.Error(), r.URL.Path)
}

func writeProblem(w http.ResponseWriter, status int, info problemInfo, detail, instance string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(problemDetails{
		Type:     problemBaseURI + info.slug,
		Title:    info.title,
		Status:   status,
		Detail:   detail,
		Instance: instance,
	})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// corsMiddleware allows the operator-facing SPA (this service's own
// future path-mgmt-mfe remote, plus warehouse-console) to call this API
// directly from the browser. CORS_ALLOWED_ORIGINS overrides the
// local-dev default (comma-separated) for staging/prod deployments.
// Includes PUT/DELETE (unlike a read-mostly service's CORS policy)
// since operators mutate paths directly from the browser.
// Idempotency-Key is allowed because the SPA's POST /process-paths
// sends one (ADR 0011): without the header here the browser's preflight
// would reject the request before it ever reached the middleware.
func corsMiddleware() func(http.Handler) http.Handler {
	return cors.Handler(cors.Options{
		AllowedOrigins:   allowedOrigins(),
		AllowedMethods:   []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete},
		AllowedHeaders:   []string{"Content-Type", "Authorization", IdempotencyKeyHeader},
		AllowCredentials: false,
		MaxAge:           300,
	})
}

// allowedOrigins resolves the browser origins permitted to call this
// service, from CORS_ALLOWED_ORIGINS (comma-separated) or the local-dev
// default.
func allowedOrigins() []string {
	if v := os.Getenv("CORS_ALLOWED_ORIGINS"); v != "" {
		return strings.Split(v, ",")
	}
	return []string{"http://localhost:5173", "http://localhost:5189"}
}

// parseStrictBool accepts exactly "true" or "false" -- the only spellings
// of a JSON-Schema/OpenAPI `boolean` query parameter. strconv.ParseBool is
// far more lenient ("1", "t", "T", "F", "TRUE", ...), so GET
// /process-paths?all=F used to answer 200 for a value the published
// contract says is invalid (found by the Schemathesis contract job).
func parseStrictBool(v string) (bool, error) {
	switch v {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, fmt.Errorf("%q is not a boolean (want true or false)", v)
	}
}
