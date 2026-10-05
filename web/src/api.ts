import { PROCESS_PATH_API_BASE } from "./config";

/** RFC 7807 problem+json body every error response from
 *  process-path-management returns (see its own dto.go's problemDetails
 *  type). Mirrors facility-mfe's own ApiError shape byte-for-byte. */
export interface ProblemDetails {
  type: string;
  title: string;
  status: number;
  detail: string;
  instance?: string;
}

export class ApiError extends Error {
  problem: ProblemDetails | null;
  status: number;

  constructor(status: number, problem: ProblemDetails | null, fallbackMessage: string) {
    super(problem?.detail || problem?.title || fallbackMessage);
    this.status = status;
    this.problem = problem;
  }
}

async function parseProblemOrThrow(res: Response): Promise<void> {
  let problem: ProblemDetails | null = null;
  try {
    problem = (await res.json()) as ProblemDetails;
  } catch {
    // non-JSON error body -- fall through with problem = null
  }
  throw new ApiError(res.status, problem, `${res.status} ${res.statusText}`);
}

/**
 * POST call for defining a new process path (POST /process-paths).
 * Parses an RFC 7807 problem+json body on failure so the form can surface
 * the exact domain-error detail (e.g. "a path with this id already
 * exists") instead of a generic "request failed".
 *
 * Every call sends a fresh Idempotency-Key (ADR 0011), minted per submit:
 * a network retry of the SAME submit reuses the same key only within the
 * one apiPost invocation (fetch's own retry, if any), while every new
 * submit gets a new key — so an accidental double-click can never replay
 * a previous response, and against a Postgres-backed deployment the
 * request no longer 400s with idempotency-key-required.
 */
export async function apiPost<TResponse>(
  path: string,
  body: unknown,
): Promise<TResponse> {
  const headers: Record<string, string> = { "Content-Type": "application/json" };
  // crypto.randomUUID is available in every browser context the MFE runs
  // in (secure contexts + localhost dev; the fallback keeps arbitrary
  // non-secure hosts working, same approach the console shell uses).
  const key =
    typeof crypto !== "undefined" && "randomUUID" in crypto
      ? crypto.randomUUID()
      : `${Date.now()}-${Math.random().toString(36).slice(2)}`;
  headers["Idempotency-Key"] = key;
  const res = await fetch(`${PROCESS_PATH_API_BASE}${path}`, {
    method: "POST",
    headers,
    body: JSON.stringify(body),
  });
  if (!res.ok) await parseProblemOrThrow(res);
  if (res.status === 204) return undefined as TResponse;
  return (await res.json()) as TResponse;
}

/**
 * PUT call for revising an existing process path
 * (PUT /process-paths/{pathId}) -- matchPrefix and/or requiredCapabilities.
 * pathId and direct are immutable (see the aggregate's own doc comment),
 * so this never carries them.
 */
export async function apiPut<TResponse>(
  path: string,
  body: unknown,
): Promise<TResponse> {
  const res = await fetch(`${PROCESS_PATH_API_BASE}${path}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) await parseProblemOrThrow(res);
  if (res.status === 204) return undefined as TResponse;
  return (await res.json()) as TResponse;
}

/**
 * DELETE call for deactivating a process path
 * (DELETE /process-paths/{pathId}) -- a soft delete: the path remains
 * visible in the list with status DEACTIVATED, matching the aggregate's
 * own append-only lifecycle (see CLAUDE.md).
 */
export async function apiDelete(path: string): Promise<void> {
  const res = await fetch(`${PROCESS_PATH_API_BASE}${path}`, {
    method: "DELETE",
  });
  if (!res.ok) await parseProblemOrThrow(res);
}
