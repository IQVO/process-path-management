import { useState, type FormEvent } from "react";
import { Card, DataTable, StatusPill, useFetch } from "@warehouse/ui-kit";
import { apiPost, apiPut, apiDelete, ApiError } from "../api";
import { PROCESS_PATH_API_BASE } from "../config";
import type { Eligibility, ProcessPath } from "../types";
import {
  CheckboxField,
  FormRow,
  InlineError,
  InlineSuccess,
  SubmitButton,
  TextField,
} from "../components/formkit";

/** Parses a comma-separated capability list into a trimmed, non-empty
 *  string array -- the form's one text input for what the DTO models as
 *  requiredCapabilities: []string. */
function parseCapabilities(raw: string): string[] {
  return raw
    .split(",")
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
}

/** Parses a comma-separated attribute list the same way
 *  parseCapabilities does, for eligibility.requiredProductAttributes /
 *  excludedProductAttributes. An empty input is the API's "none"
 *  (omitted array), not an error. */
function parseAttributes(raw: string): string[] {
  return parseCapabilities(raw);
}

/** Parses the optional maxUnitsPerLine input. Empty string = unbounded
 *  (omitted on the wire); otherwise a positive integer. Returns null
 *  when invalid so the caller can show the field's own error. */
function parseMaxUnitsPerLine(raw: string): number | null | undefined {
  const trimmed = raw.trim();
  if (trimmed === "") return undefined;
  const n = Number(trimmed);
  if (!Number.isInteger(n) || n <= 0) return null;
  return n;
}

/** Builds the request's eligibility object from the form's raw inputs.
 *  Returns null when maxUnitsPerLine is malformed (the caller surfaces
 *  the error); returns undefined when every field is left empty -- the
 *  API treats an absent object as the fully permissive zero value (ADR
 *  0010), which is exactly what an untouched form means. */
function buildEligibility(input: {
  maxUnitsPerLine: string;
  requiredAttributes: string;
  excludedAttributes: string;
  nonSortable: boolean;
}): Eligibility | null | undefined {
  const max = parseMaxUnitsPerLine(input.maxUnitsPerLine);
  if (max === null) return null;
  const required = parseAttributes(input.requiredAttributes);
  const excluded = parseAttributes(input.excludedAttributes);
  const anySet =
    max !== undefined ||
    required.length > 0 ||
    excluded.length > 0 ||
    input.nonSortable;
  if (!anySet) return undefined;
  return {
    maxUnitsPerLine: max ?? null,
    requiredProductAttributes: required,
    excludedProductAttributes: excluded,
    nonSortable: input.nonSortable,
  };
}

/** Renders a path's eligibility as a compact human string for the list
 *  view; the empty case is the permissive default. */
function eligibilitySummary(e: Eligibility | undefined): string {
  if (!e) return "permissive";
  const parts: string[] = [];
  if (e.maxUnitsPerLine != null) parts.push(`max ${e.maxUnitsPerLine}/line`);
  if (e.requiredProductAttributes?.length)
    parts.push(`requires ${e.requiredProductAttributes.join(", ")}`);
  if (e.excludedProductAttributes?.length)
    parts.push(`excludes ${e.excludedProductAttributes.join(", ")}`);
  if (e.nonSortable) parts.push("non-sortable");
  return parts.length > 0 ? parts.join(" · ") : "permissive";
}

/** Normalizes a Go duration string ("2h0m0s") for display by dropping
 *  the zero tail segments ("2h"), keeping whatever the API sent when it
 *  does not reduce cleanly; an absent value renders as "—". */
function formatCycleTimeP95(raw: string | undefined): string {
  if (!raw) return "—";
  const m = raw.match(/^([0-9]+h)(?:0m0s)?$/);
  return m ? m[1] : raw;
}

/**
 * The single screen for this remote: process-path-management is a flat
 * ProcessPath resource (no sub-hierarchy the way facility-layout's
 * Site->Zone->Aisle chain is), so one Card+DataTable pair covers the
 * whole use-case surface -- DefinePath (POST), RevisePath (PUT, inline
 * edit per row), DeactivatePath (DELETE, per-row action). Deactivation is
 * a soft delete: the row stays visible with status DEACTIVATED rather
 * than disappearing, matching the aggregate's own append-only lifecycle
 * (see this repo's CLAUDE.md).
 *
 * cycleTimeP95 (required) and eligibility (optional, default permissive)
 * are the fulfillment capability contract (ADR 0010): the server rejects
 * a define/revise without cycleTimeP95 with 422, so the form always
 * sends it -- defaulting to the catalogue's own 2h stand-in until the
 * operator changes it.
 */
export function ProcessPathsScreen() {
  const [refreshKey, setRefreshKey] = useState(0);
  const {
    data: paths,
    loading,
    error: listError,
  } = useFetch<ProcessPath[]>(`${PROCESS_PATH_API_BASE}/process-paths?_r=${refreshKey}`);

  // Define form state
  const [pathId, setPathId] = useState("");
  const [matchPrefix, setMatchPrefix] = useState("");
  const [direct, setDirect] = useState(true);
  const [capabilities, setCapabilities] = useState("");
  const [cycleTimeP95, setCycleTimeP95] = useState("2h");
  const [maxUnitsPerLine, setMaxUnitsPerLine] = useState("");
  const [requiredAttributes, setRequiredAttributes] = useState("");
  const [excludedAttributes, setExcludedAttributes] = useState("");
  const [nonSortable, setNonSortable] = useState(false);
  const [defineError, setDefineError] = useState<string | null>(null);
  const [defineSuccess, setDefineSuccess] = useState<string | null>(null);
  const [defining, setDefining] = useState(false);

  // Inline revise state: at most one row editable at a time, keyed by pathId.
  const [editingId, setEditingId] = useState<string | null>(null);
  const [editMatchPrefix, setEditMatchPrefix] = useState("");
  const [editCapabilities, setEditCapabilities] = useState("");
  const [editCycleTimeP95, setEditCycleTimeP95] = useState("");
  const [editMaxUnitsPerLine, setEditMaxUnitsPerLine] = useState("");
  const [editRequiredAttributes, setEditRequiredAttributes] = useState("");
  const [editExcludedAttributes, setEditExcludedAttributes] = useState("");
  const [editNonSortable, setEditNonSortable] = useState(false);
  const [reviseError, setReviseError] = useState<string | null>(null);
  const [revising, setRevising] = useState(false);

  const [deactivatingId, setDeactivatingId] = useState<string | null>(null);
  const [deactivateError, setDeactivateError] = useState<string | null>(null);

  async function onDefine(e: FormEvent) {
    e.preventDefault();
    setDefineError(null);
    setDefineSuccess(null);
    const eligibility = buildEligibility({
      maxUnitsPerLine,
      requiredAttributes,
      excludedAttributes,
      nonSortable,
    });
    if (eligibility === null) {
      setDefineError("maxUnitsPerLine must be a positive whole number (or empty for unbounded).");
      return;
    }
    setDefining(true);
    try {
      await apiPost<ProcessPath>("/process-paths", {
        pathId: pathId.trim(),
        matchPrefix: matchPrefix.trim(),
        direct,
        requiredCapabilities: parseCapabilities(capabilities),
        cycleTimeP95: cycleTimeP95.trim(),
        ...(eligibility !== undefined ? { eligibility } : {}),
      });
      setDefineSuccess(`Process path ${pathId.trim()} defined.`);
      setPathId("");
      setMatchPrefix("");
      setDirect(true);
      setCapabilities("");
      setCycleTimeP95("2h");
      setMaxUnitsPerLine("");
      setRequiredAttributes("");
      setExcludedAttributes("");
      setNonSortable(false);
      setRefreshKey((k) => k + 1);
    } catch (err) {
      setDefineError(err instanceof ApiError ? err.message : "Failed to define process path.");
    } finally {
      setDefining(false);
    }
  }

  function startEdit(p: ProcessPath) {
    setEditingId(p.pathId);
    setEditMatchPrefix(p.matchPrefix);
    setEditCapabilities(p.requiredCapabilities.join(", "));
    setEditCycleTimeP95(p.cycleTimeP95);
    setEditMaxUnitsPerLine(p.eligibility?.maxUnitsPerLine != null ? String(p.eligibility.maxUnitsPerLine) : "");
    setEditRequiredAttributes(p.eligibility?.requiredProductAttributes?.join(", ") ?? "");
    setEditExcludedAttributes(p.eligibility?.excludedProductAttributes?.join(", ") ?? "");
    setEditNonSortable(p.eligibility?.nonSortable ?? false);
    setReviseError(null);
  }

  function cancelEdit() {
    setEditingId(null);
    setReviseError(null);
  }

  async function onRevise(p: ProcessPath) {
    setReviseError(null);
    const eligibility = buildEligibility({
      maxUnitsPerLine: editMaxUnitsPerLine,
      requiredAttributes: editRequiredAttributes,
      excludedAttributes: editExcludedAttributes,
      nonSortable: editNonSortable,
    });
    if (eligibility === null) {
      setReviseError("maxUnitsPerLine must be a positive whole number (or empty for unbounded).");
      return;
    }
    setRevising(true);
    try {
      await apiPut<ProcessPath>(`/process-paths/${encodeURIComponent(p.pathId)}`, {
        matchPrefix: editMatchPrefix.trim(),
        requiredCapabilities: parseCapabilities(editCapabilities),
        cycleTimeP95: editCycleTimeP95.trim(),
        ...(eligibility !== undefined ? { eligibility } : {}),
      });
      setEditingId(null);
      setRefreshKey((k) => k + 1);
    } catch (err) {
      setReviseError(err instanceof ApiError ? err.message : "Failed to revise process path.");
    } finally {
      setRevising(false);
    }
  }

  async function onDeactivate(p: ProcessPath) {
    setDeactivateError(null);
    setDeactivatingId(p.pathId);
    try {
      await apiDelete(`/process-paths/${encodeURIComponent(p.pathId)}`);
      setRefreshKey((k) => k + 1);
    } catch (err) {
      setDeactivateError(
        err instanceof ApiError ? err.message : "Failed to deactivate process path.",
      );
    } finally {
      setDeactivatingId(null);
    }
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "var(--wh-space-5)" }}>
      <div>
        <h1 style={{ fontSize: "var(--wh-font-size-2xl)", margin: 0 }}>Process paths</h1>
        <p style={{ color: "var(--wh-color-text-muted)", marginTop: 4 }}>
          process-path-management · the fleet&apos;s declared process-path catalogue
        </p>
      </div>

      <Card title="Define a process path">
        <form onSubmit={onDefine} style={{ display: "flex", flexDirection: "column", gap: "var(--wh-space-3)" }}>
          <FormRow>
            <TextField
              label="Path ID"
              value={pathId}
              onChange={setPathId}
              placeholder="PICK"
              required
            />
            <TextField
              label="Match prefix"
              value={matchPrefix}
              onChange={setMatchPrefix}
              placeholder="pick"
              required
            />
            <TextField
              label="Required capabilities (comma-separated)"
              value={capabilities}
              onChange={setCapabilities}
              placeholder="pick, pick-heavy"
              required
            />
            <TextField
              label="Cycle time p95 (Go duration, e.g. 2h)"
              value={cycleTimeP95}
              onChange={setCycleTimeP95}
              placeholder="2h"
              required
            />
            <CheckboxField label="Direct" checked={direct} onChange={setDirect} />
            <SubmitButton
              disabled={defining || !pathId.trim() || !matchPrefix.trim() || !capabilities.trim() || !cycleTimeP95.trim()}
            >
              {defining ? "Defining…" : "Define path"}
            </SubmitButton>
          </FormRow>
          <FormRow>
            <TextField
              label="Max units per line (optional)"
              value={maxUnitsPerLine}
              onChange={setMaxUnitsPerLine}
              placeholder="1"
            />
            <TextField
              label="Required product attributes (optional)"
              value={requiredAttributes}
              onChange={setRequiredAttributes}
              placeholder="giftWrap"
            />
            <TextField
              label="Excluded product attributes (optional)"
              value={excludedAttributes}
              onChange={setExcludedAttributes}
              placeholder="hazmat"
            />
            <CheckboxField
              label="Non-sortable"
              checked={nonSortable}
              onChange={setNonSortable}
            />
          </FormRow>
          <InlineError message={defineError} />
          <InlineSuccess message={defineSuccess} />
        </form>
      </Card>

      <Card title="Declared paths">
        {listError && <InlineError message={listError.message} />}
        {deactivateError && <InlineError message={deactivateError} />}
        <DataTable
          rowKey={(p) => p.pathId}
          rows={paths ?? []}
          loading={loading}
          emptyState={<span>No process paths defined yet.</span>}
          columns={[
            { key: "pathId", header: "Path ID", render: (p) => p.pathId },
            {
              key: "matchPrefix",
              header: "Match prefix",
              render: (p) =>
                editingId === p.pathId ? (
                  <TextField
                    label=""
                    value={editMatchPrefix}
                    onChange={setEditMatchPrefix}
                    disabled={p.status === "DEACTIVATED"}
                  />
                ) : (
                  p.matchPrefix
                ),
            },
            { key: "direct", header: "Direct", render: (p) => (p.direct ? "Yes" : "No") },
            {
              key: "requiredCapabilities",
              header: "Required capabilities",
              render: (p) =>
                editingId === p.pathId ? (
                  <TextField
                    label=""
                    value={editCapabilities}
                    onChange={setEditCapabilities}
                    disabled={p.status === "DEACTIVATED"}
                  />
                ) : (
                  p.requiredCapabilities.join(", ")
                ),
            },
            {
              key: "destinationLocationRole",
              header: "Destination role",
              render: (p) => p.destinationLocationRole ?? "—",
            },
            {
              key: "cycleTimeP95",
              header: "Cycle time p95",
              render: (p) =>
                editingId === p.pathId ? (
                  <TextField
                    label=""
                    value={editCycleTimeP95}
                    onChange={setEditCycleTimeP95}
                    disabled={p.status === "DEACTIVATED"}
                  />
                ) : (
                  formatCycleTimeP95(p.cycleTimeP95)
                ),
            },
            {
              key: "eligibility",
              header: "Eligibility",
              render: (p) =>
                editingId === p.pathId ? (
                  <div style={{ display: "flex", flexDirection: "column", gap: "var(--wh-space-2)" }}>
                    <TextField
                      label="Max units/line"
                      value={editMaxUnitsPerLine}
                      onChange={setEditMaxUnitsPerLine}
                    />
                    <TextField
                      label="Required attributes"
                      value={editRequiredAttributes}
                      onChange={setEditRequiredAttributes}
                    />
                    <TextField
                      label="Excluded attributes"
                      value={editExcludedAttributes}
                      onChange={setEditExcludedAttributes}
                    />
                    <CheckboxField
                      label="Non-sortable"
                      checked={editNonSortable}
                      onChange={setEditNonSortable}
                    />
                  </div>
                ) : (
                  eligibilitySummary(p.eligibility)
                ),
            },
            { key: "status", header: "Status", render: (p) => <StatusPill status={p.status} size="sm" /> },
            {
              key: "actions",
              header: "Actions",
              render: (p) => {
                if (p.status === "DEACTIVATED") return null;
                if (editingId === p.pathId) {
                  return (
                    <div style={{ display: "flex", gap: "var(--wh-space-2)" }}>
                      <SubmitButton
                        type="button"
                        onClick={() => void onRevise(p)}
                        disabled={revising || !editMatchPrefix.trim() || !editCapabilities.trim() || !editCycleTimeP95.trim()}
                      >
                        {revising ? "Saving…" : "Save"}
                      </SubmitButton>
                      <SubmitButton type="button" onClick={cancelEdit} disabled={revising}>
                        Cancel
                      </SubmitButton>
                    </div>
                  );
                }
                return (
                  <div style={{ display: "flex", gap: "var(--wh-space-2)" }}>
                    <SubmitButton type="button" onClick={() => startEdit(p)}>
                      Revise
                    </SubmitButton>
                    <SubmitButton
                      type="button"
                      tone="danger"
                      onClick={() => void onDeactivate(p)}
                      disabled={deactivatingId === p.pathId}
                    >
                      {deactivatingId === p.pathId ? "Deactivating…" : "Deactivate"}
                    </SubmitButton>
                  </div>
                );
              },
            },
          ]}
        />
        <InlineError message={reviseError} />
      </Card>
    </div>
  );
}
