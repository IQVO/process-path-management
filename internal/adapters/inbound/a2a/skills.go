package a2a

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/claudioed/process-path-management/internal/application/usecases"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

// tracerName is the OTel instrumentation scope for A2A skill spans, mirroring
// the MCP adapter's mcp.tool spans.
const tracerName = "github.com/claudioed/process-path-management/internal/adapters/inbound/a2a"

// Skill ids advertised on the Agent Card (see Card). They are the MCP tool
// names of the same reads (ADR 0006), so one name identifies one question
// across both agent-facing surfaces.
const (
	skillGetProcessPath   = "get_process_path"
	skillListProcessPaths = "list_process_paths"
)

// skillRequest is the wire contract of every skill call: the first meaningful
// message part carries (as a Data part, or a Text part holding the same JSON)
// the skill id and its arguments. Strict on purpose: an agent client is
// machine-to-machine, so unknown fields are rejected, never ignored, and a
// misspelled name like "activeOnlyy" must not silently fall back to the
// active-only default. (Go's encoding/json matches field names
// case-insensitively, so "activeonly" still reaches activeOnly; only
// genuinely unknown names are refused.)
type skillRequest struct {
	Skill     string          `json:"skill"`
	Arguments json.RawMessage `json:"arguments"`
}

// getProcessPathArgs and listProcessPathsArgs are the per-skill argument
// shapes.
type getProcessPathArgs struct {
	PathId string `json:"pathId"`
}

type listProcessPathsArgs struct {
	// ActiveOnly defaults to true when omitted, matching the REST and MCP
	// default view.
	ActiveOnly *bool `json:"activeOnly"`
}

// answer maps one incoming message to a skill and returns the agent's reply
// Message. It is the whole executor body: this is a message-only agent, so a
// skill run is exactly one request mapped to one answer (ADR 0039).
func answer(ctx context.Context, deps Deps, ec *a2asrv.ExecutorContext) a2a.Event {
	req, problem := parseSkillRequest(ec)
	if problem != nil {
		return reply(ec, envelope{Skill: "", Problem: problem}, problem.summary())
	}

	ctx, span := otel.Tracer(tracerName).Start(ctx, "a2a.skill "+req.Skill)
	defer span.End()
	span.SetAttributes(attribute.String("a2a.skill", req.Skill))

	dto, problem := run(ctx, deps, req)
	if problem != nil {
		span.SetStatus(codes.Error, problem.Detail)
		return reply(ec, envelope{Skill: req.Skill, Problem: problem}, problem.summary())
	}
	return reply(ec, envelope{Skill: req.Skill, Data: dto}, dto.Summary())
}

// run dispatches one validated request to its skill.
func run(ctx context.Context, deps Deps, req skillRequest) (skillDTO, *problem) {
	switch req.Skill {
	case skillGetProcessPath:
		data, problem := deps.getProcessPath(ctx, req.Arguments)
		if problem != nil {
			return skillDTO{}, problem
		}
		return skillDTO{ProcessPath: &data, summary: summarizePath(data)}, nil
	case skillListProcessPaths:
		data, problem := deps.listProcessPaths(ctx, req.Arguments)
		if problem != nil {
			return skillDTO{}, problem
		}
		return skillDTO{ProcessPaths: &data, summary: summarizePaths(data)}, nil
	default:
		return skillDTO{}, problemUnknownSkill(req.Skill)
	}
}

// reply builds the agent Message for one skill run: a Data part carrying the
// structured envelope and a Text part carrying the one-line summary, bound to
// the caller's context (and task, when the client referenced one).
func reply(ec *a2asrv.ExecutorContext, env envelope, summary string) a2a.Event {
	return a2a.NewMessageForTask(a2a.MessageRoleAgent, ec, a2a.NewDataPart(env), a2a.NewTextPart(summary))
}

// parseSkillRequest extracts the skillRequest from the message's first
// meaningful part. A Data part (or a Text part holding the same JSON object)
// is accepted; anything else is a request-shape problem, answered, not
// raised, because a malformed delegation is a business answer for the calling
// agent, not a transport failure.
func parseSkillRequest(ec *a2asrv.ExecutorContext) (skillRequest, *problem) {
	msg := ec.Message
	if msg == nil || len(msg.Parts) == 0 {
		return skillRequest{}, problemMalformedRequest("the message carries no parts")
	}
	raw, err := partJSON(msg.Parts[0])
	if err != nil {
		return skillRequest{}, problemMalformedRequest(fmt.Sprintf("the first message part is not a skill request: %v", err))
	}
	var req skillRequest
	if err := decodeStrict(raw, &req); err != nil {
		return skillRequest{}, problemMalformedRequest(fmt.Sprintf("the skill request is not valid: %v", err))
	}
	if req.Skill == "" {
		return skillRequest{}, problemMalformedRequest(`the skill request carries no "skill" field`)
	}
	return req, nil
}

// partJSON renders one part's content as the JSON bytes of a skill request.
func partJSON(part *a2a.Part) ([]byte, error) {
	switch content := part.Content.(type) {
	case a2a.Data:
		return json.Marshal(content.Value)
	case a2a.Text:
		return []byte(string(content)), nil
	default:
		return nil, errors.New("expected a data part (or a text part holding the skill JSON)")
	}
}

// decodeStrict decodes one JSON document into dst, rejecting unknown fields
// and trailing data. Absent arguments decode as the empty object so each
// skill's own required-field checks produce the specific problem.
func decodeStrict(raw []byte, dst any) error {
	if len(bytes.TrimSpace(raw)) == 0 {
		raw = []byte("{}")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	if dec.More() {
		return errors.New("unexpected data after the JSON document")
	}
	return nil
}

// getProcessPath is the single-path read skill, through the same GetPath use
// case the REST and MCP adapters use.
func (d Deps) getProcessPath(ctx context.Context, rawArgs json.RawMessage) (processPathDTO, *problem) {
	var args getProcessPathArgs
	if err := decodeStrict(rawArgs, &args); err != nil {
		return processPathDTO{}, problemInvalidArgument(fmt.Errorf("arguments: %w", err))
	}
	if strings.TrimSpace(args.PathId) == "" {
		return processPathDTO{}, problemInvalidArgument(errors.New("pathId is required"))
	}
	p, err := d.GetPath.Execute(ctx, shared.PathId(args.PathId))
	if err != nil {
		return processPathDTO{}, problemFromUseCase(err, args.PathId)
	}
	return toProcessPathDTO(p), nil
}

// listProcessPaths is the catalogue read skill, through the same ListPaths use
// case the REST and MCP adapters use. activeOnly defaults to true.
func (d Deps) listProcessPaths(ctx context.Context, rawArgs json.RawMessage) (processPathsDTO, *problem) {
	var args listProcessPathsArgs
	if err := decodeStrict(rawArgs, &args); err != nil {
		return processPathsDTO{}, problemInvalidArgument(fmt.Errorf("arguments: %w", err))
	}
	activeOnly := true
	if args.ActiveOnly != nil {
		activeOnly = *args.ActiveOnly
	}
	paths, err := d.ListPaths.Execute(ctx, activeOnly)
	if err != nil {
		return processPathsDTO{}, problemFromUseCase(err, "")
	}
	dtos := make([]processPathDTO, 0, len(paths))
	for _, p := range paths {
		dtos = append(dtos, toProcessPathDTO(p))
	}
	return processPathsDTO{ActiveOnly: activeOnly, Paths: dtos}, nil
}

// problemFromUseCase maps a use case error to a problem answer, using the
// SAME classification as the REST adapter (internal/adapters/inbound/http
// statusFor and problemCatalog) for the errors a read can raise: a missing
// path is 404 path-not-found and anything unrecognised is 500 internal-error.
// (The REST adapter's 409 and 422 classes belong to writes this read-only
// agent does not offer.) The detail is adapter-owned text, so the shared
// error's "usecases:" prefix never reaches an agent client. Domain rejections
// are EXPECTED business outcomes and are answered as RFC 7807-shaped
// problems; anything unrecognised is still answered as a problem, so the
// calling agent always receives a structured, machine-readable result.
func problemFromUseCase(err error, pathId string) *problem {
	switch {
	case errors.Is(err, usecases.ErrPathNotFound):
		return &problem{Type: problemTypeBase + "/path-not-found", Title: "No process path exists with this id", Detail: fmt.Sprintf("no process path exists with pathId %q", pathId), Status: 404}
	default:
		slog.Error("a2a skill failed", "error", err)
		return &problem{Type: problemTypeBase + "/internal-error", Title: "An unexpected internal error occurred", Detail: "the skill failed; see server logs (trace id in the response metadata)", Status: 500}
	}
}
