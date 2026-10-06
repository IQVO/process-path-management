---
name: how-to-add-a-frontend-remote
description: Add or change a micro-frontend remote under web/ (vite federation config in object form, /mfes/<context>/ base, remoteEntry, Docker/nginx packaging, console integration). Use when touching web/.
---

# How to add a frontend remote

Use when adding a new screen/feature to this repo's `web/` Module
Federation remote (`process-path-mfe`, dev port **5189**), or when
standing up a NEW remote for a bounded context that doesn't have one
yet. Read `web/vite.config.ts`, `web/src/config.ts`, and
`web/src/screens/ProcessPathsScreen.tsx` alongside this guide — they are
the real, currently-shipping example this guide walks.

## `process-path-mfe` is standalone-only — it is NOT wired into `warehouse-console`

Unlike most of the fleet's remotes, this one has no matching entry in
`warehouse-console`'s federation remotes config. It's meant to be opened
directly at `http://localhost:5189` for local dev, or served by its own
nginx pod at `http://localhost/mfes/process-path-management/` in the
cluster — not lazy-loaded inside the console shell today. If you're
adding a NEW remote for a different bounded context and want it to
behave differently (embedded in the console), that's a deliberate
divergence from this repo's own pattern — check
the `warehouse-console` repo's own guides for the shell-side contract
rather than assuming this repo's standalone posture is the default.

## `vite.config.ts` must stay in OBJECT form, always

```ts
const IS_BUILD = process.argv.includes("build");
const PUBLIC_BASE = IS_BUILD ? "/mfes/process-path-management/" : "/";

export default defineConfig({
  base: PUBLIC_BASE,
  plugins: [react(), federation({ name: "process_path_mfe", /* ... */ })],
  // ...
});
```

This is `web/vite.config.ts` verbatim (see its own inline comment
explaining why). Never convert this to the callback form
(`defineConfig(({ command }) => ({...}))`) to compute `base`
dynamically. `web/vitest.config.ts` does `mergeConfig(viteConfig,
defineConfig({...}))`, and Vite throws `Error: Cannot merge config in
form of callback` on a function export — killing the ENTIRE test suite
for a one-line config change, not just the new test.

## Runtime API base URL: two DIFFERENT patterns live in this repo's history — use the current one

`web/src/config.ts` resolves the API origin from
`window.__WAREHOUSE_CONFIG__.apiOrigin` (published by the console shell
at runtime, before any remote mounts) plus a fixed `API_PATH =
"/api/process-path-management"`, and THROWS in a production build if
that config is missing — a silent fallback would mean a deployed console
quietly talking to nothing:

```ts
const API_PATH = "/api/process-path-management";
const DEV_API_BASE = "http://localhost:8087";

export function resolveProcessPathApiBase(runtimeConfig, isProduction) {
  const apiOrigin = runtimeConfig.apiOrigin?.replace(/\/+$/, "");
  if (!apiOrigin) {
    if (isProduction) throw new Error("window.__WAREHOUSE_CONFIG__.apiOrigin is required in production");
    return DEV_API_BASE;
  }
  return `${apiOrigin}${API_PATH}`;
}
```

Only the `DEV_API_BASE` fallback (`http://localhost:8087`) is a fixed
constant — the production path is runtime-resolved, not hardcoded. If
you're auditing an older remote and find one with NO runtime resolution
at all (a bare `export const X_API_BASE = "http://localhost:8087"` and
nothing else), that's the OLDER, now-superseded pattern — bring it in
line with `config.ts`'s current shape rather than copying the old one.
Either way: to actually exercise this UI against a live cluster, the
process-path-management OLTP pod (not the `-mcp`/`-projector`/`-reports`
pods, if this service ever grows those) must be port-forwarded to
EXACTLY the port `DEV_API_BASE` names — the fleet-wide "service
selectors select far more than you think" pitfall means any wrong port
forward target here fails silently with empty data loads on the
catalogue screen, no obvious error.

## `@warehouse/ui-kit` is a sibling checkout, not a registry package

`web/package.json` depends on it via `"@warehouse/ui-kit":
"file:../../warehouse-ui-kit"`. Any domain-status color, chart, or
shared layout primitive belongs in that repo, not hand-rolled here — a
remote reinventing its own palette instead of consuming the shared
components is a bug, not a style choice.

## This remote DOES have a working `test` script — confirm any new remote does too

`web/package.json`'s `scripts.test` is `"vitest run"` — this repo is
already one of the fleet's correctly-configured remotes (several
sibling remotes historically shipped only build/dev/lint/preview and
silently never ran added tests). `web/src/config.test.ts`,
`web/src/api.test.ts`, and
`web/src/screens/ProcessPathsScreen.test.tsx` are the real, currently
passing examples — model a new test file's setup (MSW mock server via
`web/src/test/mocks/server.ts`, jsdom via `web/src/test/setup.ts`) on
those rather than starting from scratch. Confirm this repo's `web:` CI
job (`.github/workflows/ci.yml`) actually invokes `npm test` before
assuming a new test file runs in CI — it does today (`npm ci && npm run
lint && npx tsc -b && npm test && npm run build`, against a dual
checkout of this repo plus `claudioed/warehouse-ui-kit@develop`).

## The Docker build recipe (packaging as a deployable nginx workload)

See `web/Dockerfile` for the complete, working, heavily commented
recipe — copy it rather than re-deriving these traps from scratch for a
new remote:

1. **Base image must be `node:22-bookworm-slim`, not an alpine or newer
   node image.** `web/package-lock.json` was generated by npm 10 and
   carries no Linux entries for Vite's platform-specific optional
   dependencies. npm 11 rejects that lockfile outright with `EUSAGE
   "lock file out of sync"`; glibc (not musl) matches the published
   `*-gnu` bindings.
2. **Do NOT install from the committed lockfile, and do NOT reuse a host
   `node_modules`.** Both leave the native binding missing (`Cannot find
   native binding ... @rolldown/binding-linux-*`). The working recipe:
   `rm -rf node_modules package-lock.json` in the ui-kit build stage (it
   arrives as a named BuildKit context from a developer checkout), `COPY
   package.json ./` only, then `npm install --no-save`.
3. **`@warehouse/ui-kit` must be supplied as a named BuildKit context**,
   since it's a sibling checkout, not a registry dependency:
   ```bash
   docker build --build-context uikit=../../warehouse-ui-kit \
     -t warehouse/process-path-management-frontend:local .
   ```

## Wiring into `warehouse-infra`'s deploy

A new remote isn't deployed automatically — `warehouse-infra`'s
`local.services` map and this repo's own Helm chart
(`charts/process-path-management`, `frontend.enabled` value, check its
current default before assuming it's `false`) both need updating. See
`warehouse-infra`'s AGENTS.md for the Terraform side of this. Given this
remote's standalone (non-console-embedded) posture, also confirm whether
it needs a distinct routing entry from the six console-embedded remotes
before assuming the same wiring steps apply verbatim.

## Key commands

```bash
cd web
npm ci
npm run lint        # oxlint
npx tsc -b           # typecheck
npm test             # vitest run
npm run build         # tsc -b && vite build -> dist/
npm run dev           # vite --port 5189 (strictPort: true)
```

CI's `web:` job (`.github/workflows/ci.yml`) runs this exact sequence
against a dual checkout (this repo + `claudioed/warehouse-ui-kit@develop`)
— reproduce that locally by symlinking or checking out `warehouse-ui-kit`
as a real sibling if `npm ci` behaves differently than CI.
