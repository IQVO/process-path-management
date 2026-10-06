import { describe, expect, it, vi, afterEach } from "vitest";
import { apiPost, apiPut, apiDelete, ApiError } from "./api";

describe("apiPost", () => {
  const originalFetch = globalThis.fetch;

  afterEach(() => {
    globalThis.fetch = originalFetch;
    vi.restoreAllMocks();
  });

  it("returns the parsed JSON body on success", async () => {
    globalThis.fetch = vi.fn().mockResolvedValue({
      ok: true,
      status: 201,
      json: async () => ({ pathId: "PICK", matchPrefix: "pick", status: "ACTIVE" }),
    }) as unknown as typeof fetch;

    const result = await apiPost("/process-paths", { pathId: "PICK" });
    expect(result).toEqual({ pathId: "PICK", matchPrefix: "pick", status: "ACTIVE" });
  });

  it("throws ApiError with the parsed RFC 7807 problem detail on failure", async () => {
    globalThis.fetch = vi.fn().mockResolvedValue({
      ok: false,
      status: 409,
      statusText: "Conflict",
      json: async () => ({
        type: "https://errors.process-path-management.warehouse-systems.dev/duplicate-path-id",
        title: "A process path with this id already exists",
        status: 409,
        detail: "a process path with this id already exists",
      }),
    }) as unknown as typeof fetch;

    await expect(apiPost("/process-paths", { pathId: "PICK" })).rejects.toThrow(ApiError);
    await expect(apiPost("/process-paths", { pathId: "PICK" })).rejects.toThrow(
      "a process path with this id already exists",
    );
  });

  it("falls back to statusText when the error body is not JSON", async () => {
    globalThis.fetch = vi.fn().mockResolvedValue({
      ok: false,
      status: 500,
      statusText: "Internal Server Error",
      json: async () => {
        throw new Error("not json");
      },
    }) as unknown as typeof fetch;

    await expect(apiPost("/process-paths", {})).rejects.toThrow("500 Internal Server Error");
  });

  it("sends a non-empty Idempotency-Key header, different per submit (ADR 0011)", async () => {
    const seenKeys: string[] = [];
    globalThis.fetch = vi.fn().mockImplementation((_input: RequestInfo | URL, init?: RequestInit) => {
      const headers = init?.headers as Record<string, string>;
      seenKeys.push(headers["Idempotency-Key"]);
      return Promise.resolve({
        ok: true,
        status: 201,
        json: async () => ({ pathId: "PICK" }),
      });
    }) as unknown as typeof fetch;

    await apiPost("/process-paths", { pathId: "PICK" });
    await apiPost("/process-paths", { pathId: "PACK" });

    expect(seenKeys).toHaveLength(2);
    for (const key of seenKeys) {
      expect(key).toBeTruthy();
    }
    expect(seenKeys[0]).not.toBe(seenKeys[1]);
  });
});

describe("apiPut", () => {
  const originalFetch = globalThis.fetch;

  afterEach(() => {
    globalThis.fetch = originalFetch;
    vi.restoreAllMocks();
  });

  it("returns the parsed JSON body on success", async () => {
    globalThis.fetch = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ pathId: "PICK", matchPrefix: "pick-v2", status: "ACTIVE" }),
    }) as unknown as typeof fetch;

    const result = await apiPut("/process-paths/PICK", { matchPrefix: "pick-v2" });
    expect(result).toEqual({ pathId: "PICK", matchPrefix: "pick-v2", status: "ACTIVE" });
  });

  it("throws ApiError on failure", async () => {
    globalThis.fetch = vi.fn().mockResolvedValue({
      ok: false,
      status: 404,
      statusText: "Not Found",
      json: async () => ({
        type: "https://errors.process-path-management.warehouse-systems.dev/unknown-path",
        title: "Unknown process path",
        status: 404,
        detail: "no process path with this id exists",
      }),
    }) as unknown as typeof fetch;

    await expect(apiPut("/process-paths/BOGUS", {})).rejects.toThrow(
      "no process path with this id exists",
    );
  });
});

describe("apiDelete", () => {
  const originalFetch = globalThis.fetch;

  afterEach(() => {
    globalThis.fetch = originalFetch;
    vi.restoreAllMocks();
  });

  it("resolves on a 204 No Content response", async () => {
    globalThis.fetch = vi.fn().mockResolvedValue({
      ok: true,
      status: 204,
    }) as unknown as typeof fetch;

    await expect(apiDelete("/process-paths/PICK")).resolves.toBeUndefined();
  });

  it("throws ApiError on failure", async () => {
    globalThis.fetch = vi.fn().mockResolvedValue({
      ok: false,
      status: 404,
      statusText: "Not Found",
      json: async () => ({
        type: "https://errors.process-path-management.warehouse-systems.dev/unknown-path",
        title: "Unknown process path",
        status: 404,
        detail: "no process path with this id exists",
      }),
    }) as unknown as typeof fetch;

    await expect(apiDelete("/process-paths/BOGUS")).rejects.toThrow(
      "no process path with this id exists",
    );
  });
});
