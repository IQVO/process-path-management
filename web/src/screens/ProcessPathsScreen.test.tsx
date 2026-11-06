import { describe, expect, it } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse, server } from "../test/mocks/server";
import { PROCESS_PATH_API_BASE } from "../config";
import { ProcessPathsScreen } from "./ProcessPathsScreen";

describe("ProcessPathsScreen", () => {
  it("lists process paths returned by GET /process-paths", async () => {
    server.use(
      http.get(`${PROCESS_PATH_API_BASE}/process-paths`, () =>
        HttpResponse.json({ items: [
          {
            pathId: "PICK",
            matchPrefix: "pick",
            direct: true,
            requiredCapabilities: ["pick-heavy"],
            destinationLocationRole: "Drop",
            cycleTimeP95: "2h0m0s",
            eligibility: { maxUnitsPerLine: 1 },
            status: "ACTIVE",
            createdAt: "2026-09-06T00:00:00Z",
            updatedAt: "2026-09-06T00:00:00Z",
          },
        ] }),
      ),
    );

    render(<ProcessPathsScreen />);

    expect(await screen.findByText("PICK")).toBeInTheDocument();
    expect(screen.getByText("pick")).toBeInTheDocument();
    expect(screen.getByText("pick-heavy")).toBeInTheDocument();
    expect(screen.getByText("Drop")).toBeInTheDocument();
    expect(screen.getByText("2h")).toBeInTheDocument();
    expect(screen.getByText("max 1/line")).toBeInTheDocument();
  });

  it("defines a new process path and shows a success message", async () => {
    server.use(
      http.get(`${PROCESS_PATH_API_BASE}/process-paths`, () => HttpResponse.json({ items: [] })),
      http.post(`${PROCESS_PATH_API_BASE}/process-paths`, async ({ request }) => {
        const body = (await request.json()) as {
          pathId: string;
          matchPrefix: string;
          cycleTimeP95: string;
          eligibility?: { nonSortable: boolean };
        };
        if (body.cycleTimeP95 !== "90m") {
          return HttpResponse.json(
            {
              type: "https://errors.process-path-management.warehouse-systems.dev/invalid-cycle-time-p95",
              title: "cycleTimeP95 must be a positive duration",
              status: 422,
              detail: `cycleTimeP95 must be a positive duration, got ${body.cycleTimeP95}`,
            },
            { status: 422 },
          );
        }
        return HttpResponse.json(
          {
            pathId: body.pathId,
            matchPrefix: body.matchPrefix,
            direct: true,
            requiredCapabilities: ["pack"],
            cycleTimeP95: body.cycleTimeP95,
            eligibility: body.eligibility ?? {},
            status: "ACTIVE",
            createdAt: "2026-09-06T00:00:00Z",
            updatedAt: "2026-09-06T00:00:00Z",
          },
          { status: 201 },
        );
      }),
    );

    render(<ProcessPathsScreen />);
    await userEvent.type(screen.getByLabelText("Path ID *"), "PACK");
    await userEvent.type(screen.getByLabelText("Match prefix *"), "pack");
    await userEvent.type(
      screen.getByLabelText("Required capabilities (comma-separated) *"),
      "pack",
    );
    const cycleInput = screen.getByLabelText(/Cycle time p95/);
    await userEvent.clear(cycleInput);
    await userEvent.type(cycleInput, "90m");
    await userEvent.click(screen.getByRole("checkbox", { name: "Non-sortable" }));
    await userEvent.click(screen.getByRole("button", { name: "Define path" }));

    await waitFor(() =>
      expect(screen.getByText("Process path PACK defined.")).toBeInTheDocument(),
    );
  });

  it("shows the RFC 7807 problem detail on a 409 conflict", async () => {
    server.use(
      http.get(`${PROCESS_PATH_API_BASE}/process-paths`, () => HttpResponse.json({ items: [] })),
      http.post(`${PROCESS_PATH_API_BASE}/process-paths`, () =>
        HttpResponse.json(
          {
            type: "https://errors.process-path-management.warehouse-systems.dev/duplicate-path-id",
            title: "A process path with this id already exists",
            status: 409,
            detail: "a process path with this id already exists",
          },
          { status: 409 },
        ),
      ),
    );

    render(<ProcessPathsScreen />);
    await userEvent.type(screen.getByLabelText("Path ID *"), "PICK");
    await userEvent.type(screen.getByLabelText("Match prefix *"), "pick");
    await userEvent.type(
      screen.getByLabelText("Required capabilities (comma-separated) *"),
      "pick",
    );
    await userEvent.click(screen.getByRole("button", { name: "Define path" }));

    expect(
      await screen.findByText("a process path with this id already exists"),
    ).toBeInTheDocument();
  });

  it("revises an existing path's match prefix and capabilities", async () => {
    let revised = false;
    server.use(
      http.get(`${PROCESS_PATH_API_BASE}/process-paths`, () =>
        HttpResponse.json({ items: [
          {
            pathId: "PICK",
            matchPrefix: revised ? "pick-v2" : "pick",
            direct: true,
            requiredCapabilities: revised ? ["pick", "pick-heavy"] : ["pick-standard"],
            cycleTimeP95: revised ? "3h0m0s" : "2h0m0s",
            status: "ACTIVE",
            createdAt: "2026-09-06T00:00:00Z",
            updatedAt: "2026-09-06T00:00:00Z",
          },
        ] }),
      ),
      http.put(`${PROCESS_PATH_API_BASE}/process-paths/PICK`, async ({ request }) => {
        const body = (await request.json()) as {
          matchPrefix: string;
          requiredCapabilities: string[];
          cycleTimeP95: string;
        };
        if (body.cycleTimeP95 !== "3h") {
          return HttpResponse.json(
            {
              type: "https://errors.process-path-management.warehouse-systems.dev/invalid-cycle-time-p95",
              title: "cycleTimeP95 must be a positive duration",
              status: 422,
              detail: `cycleTimeP95 must be a positive duration, got ${body.cycleTimeP95}`,
            },
            { status: 422 },
          );
        }
        revised = true;
        return HttpResponse.json({
          pathId: "PICK",
          matchPrefix: body.matchPrefix,
          direct: true,
          requiredCapabilities: body.requiredCapabilities,
          cycleTimeP95: body.cycleTimeP95,
          status: "ACTIVE",
          createdAt: "2026-09-06T00:00:00Z",
          updatedAt: "2026-09-06T01:00:00Z",
        });
      }),
    );

    render(<ProcessPathsScreen />);
    expect(await screen.findByText("pick")).toBeInTheDocument();

    await userEvent.click(screen.getByRole("button", { name: "Revise" }));
    const textboxes = screen.getAllByRole("textbox");
    const prefixInput = textboxes.find((el) => (el as HTMLInputElement).value === "pick")!;
    await userEvent.clear(prefixInput);
    await userEvent.type(prefixInput, "pick-v2");
    const cycleInput = textboxes.find((el) => (el as HTMLInputElement).value === "2h0m0s")!;
    await userEvent.clear(cycleInput);
    await userEvent.type(cycleInput, "3h");
    await userEvent.click(screen.getByRole("button", { name: "Save" }));

    await waitFor(() => expect(screen.getByText("pick-v2")).toBeInTheDocument());
  });

  it("deactivates a path and removes its per-row actions", async () => {
    let deactivated = false;
    server.use(
      http.get(`${PROCESS_PATH_API_BASE}/process-paths`, () =>
        HttpResponse.json({ items: [
          {
            pathId: "SLAM",
            matchPrefix: "slam",
            direct: true,
            requiredCapabilities: ["slam"],
            status: deactivated ? "DEACTIVATED" : "ACTIVE",
            createdAt: "2026-09-06T00:00:00Z",
            updatedAt: "2026-09-06T00:00:00Z",
          },
        ] }),
      ),
      http.delete(`${PROCESS_PATH_API_BASE}/process-paths/SLAM`, () => {
        deactivated = true;
        return new HttpResponse(null, { status: 204 });
      }),
    );

    render(<ProcessPathsScreen />);
    expect(await screen.findByText("SLAM")).toBeInTheDocument();

    await userEvent.click(screen.getByRole("button", { name: "Deactivate" }));

    await waitFor(() =>
      expect(screen.queryByRole("button", { name: "Deactivate" })).not.toBeInTheDocument(),
    );
    expect(screen.getByText("DEACTIVATED")).toBeInTheDocument();
  });
});

describe("ProcessPathsScreen reads GET /process-paths as {items,nextCursor}", () => {
  const path = (pathId: string) => ({
    pathId,
    matchPrefix: pathId.toLowerCase(),
    direct: true,
    requiredCapabilities: ["x"],
    cycleTimeP95: "2h0m0s",
    status: "ACTIVE",
    createdAt: "2026-09-06T00:00:00Z",
    updatedAt: "2026-09-06T00:00:00Z",
  });

  it("follows nextCursor and lists every page, with only declared query parameters", async () => {
    const seen: URL[] = [];
    server.use(
      http.get(`${PROCESS_PATH_API_BASE}/process-paths`, ({ request }) => {
        const url = new URL(request.url);
        seen.push(url);
        const cursor = url.searchParams.get("cursor");
        if (cursor === null) return HttpResponse.json({ items: [path("PACK"), path("PICK")], nextCursor: "c1" });
        if (cursor === "c1") return HttpResponse.json({ items: [path("REBIN")], nextCursor: "c2" });
        return HttpResponse.json({ items: [path("SLAM")] });
      }),
    );

    render(<ProcessPathsScreen />);

    for (const id of ["PACK", "PICK", "REBIN", "SLAM"]) {
      expect(await screen.findByText(id)).toBeInTheDocument();
    }
    expect(seen.map((u) => u.searchParams.get("cursor"))).toEqual([null, "c1", "c2"]);
    // The producer answers 400 unknown-query-parameter to anything but all/limit/cursor.
    for (const u of seen) {
      expect([...u.searchParams.keys()].filter((k) => !["all", "limit", "cursor"].includes(k))).toEqual([]);
    }
    // Deactivated paths stay visible on this screen, which needs the audit view.
    expect(seen[0].searchParams.get("all")).toBe("true");
  });

  it("refuses a 200 that is not a page: error shown, no 'No process paths defined yet'", async () => {
    server.use(
      http.get(`${PROCESS_PATH_API_BASE}/process-paths`, () => HttpResponse.json([{ pathId: "PICK" }])),
    );

    render(<ProcessPathsScreen />);

    expect(await screen.findByText(/not a \{items,nextCursor\} page/)).toBeInTheDocument();
    expect(screen.queryByText("No process paths defined yet.")).not.toBeInTheDocument();
  });

  it("a 400 unknown-query-parameter is shown, not read as an empty list", async () => {
    server.use(
      http.get(`${PROCESS_PATH_API_BASE}/process-paths`, () =>
        HttpResponse.json({ title: "Unknown query parameter", status: 400 }, { status: 400 }),
      ),
    );

    render(<ProcessPathsScreen />);

    expect(await screen.findByText(/400/)).toBeInTheDocument();
    expect(screen.queryByText("No process paths defined yet.")).not.toBeInTheDocument();
  });
});
