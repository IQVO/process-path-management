---
paths:
  - "docs/**"
  - "apis/**"
  - ".github/workflows/**"
---

# Docs site, generated API reference and GitFlow

Moved out of `CLAUDE.md`; still authoritative.

Docs site (Docusaurus, generated OpenAPI reference pages):

```bash
cd docs && npm ci && npm run gen-api-docs pathmgmt   # regenerate docs/docs/api-reference/rest/* from apis/openapi.yaml
npm run build                                          # full site build, verifies no broken links
```

## Docs site and GitFlow (repo-specific — do not "fix" without checking first)

- GitFlow: `develop` is the working branch; `main` is release-only,
  synced by explicit fast-forward.
- `.github/workflows/docs.yml` triggers on **push to `develop`** with
  paths `docs/**` (not `main`, and not on every push) — this is
  deliberate for this repo (its GitHub Pages `github-pages` deployment
  environment only allows the `develop` branch), matching every other
  service's docs pipeline in this fleet. Do not "fix" this to trigger off
  `main`.
- `docs/package.json`'s `gen-api-docs` script (`docusaurus gen-api-docs
  pathmgmt`) regenerates `docs/docs/api-reference/rest/*.api.mdx` from
  `apis/openapi.yaml`. Whenever `apis/openapi.yaml` changes (new
  operation, changed request/response shape, changed
  summary/description), regenerate and commit the `.mdx`/`.json`
  companions — they are committed generated output, not hand-written.
  CI's `docs-api-drift` job runs `npm run clean-api-docs pathmgmt && npm
  run gen-api-docs pathmgmt` and fails on any diff.
