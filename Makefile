# Makefile — the local quality gate for process-path-management.
#
# Every target below mirrors a sensor in .github/workflows/ci.yml, so the
# same feedback CI gives you post-push is available locally, pre-commit.

GO                 ?= go
GOLANGCI_LINT      ?= golangci-lint
GOLANGCI_VERSION   := v2.13.1

COVERAGE_OUT       := coverage.out
COVERAGE_PKGS      := ./internal/domain/...,./internal/application/...,./internal/analytics/...
COVERAGE_THRESHOLD := 90

.DEFAULT_GOAL := help

.PHONY: help build vet fmt fmt-check lint test coverage bdd arch-test integration mutation mutation-fast api-lint vuln contract check check-all

help:
	@echo "process-path-management — local quality gate (targets mirror .github/workflows/ci.yml)"
	@echo ""
	@echo "  help              Print this list of targets (default target)"
	@echo "  build             go build ./..."
	@echo "  vet               go vet ./..."
	@echo "  fmt               gofmt -w . — format the tree in place"
	@echo "  fmt-check         Fail if gofmt -l . is non-empty (the CI-style check)"
	@echo "  lint              golangci-lint run ./... (pinned $(GOLANGCI_VERSION) in CI)"
	@echo "  test              go test ./... -race"
	@echo "  coverage          CI coverage command + the $(COVERAGE_THRESHOLD)% gate"
	@echo "  bdd               go test ./... -run TestFeatures -v (godog/Gherkin)"
	@echo "  arch-test         go test ./internal/architecture/... -v"
	@echo "  integration       go test -tags=integration ./... -race -count=1 (needs DATABASE_URL)"
	@echo "  mutation-fast     gremlins unleash ./internal/domain — CI's blocking job (only ~11 mutants here, so full-domain IS the fast subset)"
	@echo "  mutation          alias for mutation-fast (see .gremlins.yaml)"
	@echo "  api-lint          Spectral lint on openapi.yaml and asyncapi.yaml"
	@echo "  vuln              Known CVEs in the dependency graph and the Go stdlib"
	@echo ""
	@echo "  contract          scripts/contract-test.sh — Schemathesis property-based"
	@echo "                    contract tests vs apis/openapi.yaml (needs 'st' on PATH)"
	@echo ""
	@echo "  check             FAST bundle: fmt-check vet build lint test"
	@echo "  check-all         check + coverage + arch-test + bdd — run this before pushing"

build:
	$(GO) build ./...

vet:
	$(GO) vet ./...

fmt:
	gofmt -w .

fmt-check:
	@files=$$(gofmt -l .); \
	if [ -n "$$files" ]; then \
		echo "gofmt: the following files are not formatted:"; \
		echo "$$files" | sed 's/^/  /'; \
		echo "run 'make fmt' to fix them"; \
		exit 1; \
	fi; \
	echo "gofmt: clean"

lint:
	@if ! command -v $(GOLANGCI_LINT) >/dev/null 2>&1; then \
		echo "golangci-lint is not installed (or not on PATH)."; \
		echo "Install the exact version CI pins:"; \
		echo "  go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_VERSION)"; \
		exit 1; \
	fi
	$(GOLANGCI_LINT) run ./...

test:
	$(GO) test ./... -race

coverage:
	$(GO) test ./... -race -coverprofile=$(COVERAGE_OUT) -coverpkg=$(COVERAGE_PKGS)
	@COVERAGE=$$($(GO) tool cover -func=$(COVERAGE_OUT) | awk '/^total:/ {print $$3}' | tr -d '%'); \
	echo "Coverage: $${COVERAGE}% (gate: $(COVERAGE_THRESHOLD)%)"; \
	if awk -v c="$$COVERAGE" -v t="$(COVERAGE_THRESHOLD)" 'BEGIN { exit !(c < t) }'; then \
		echo "coverage $${COVERAGE}% is below the $(COVERAGE_THRESHOLD)% gate"; \
		exit 1; \
	fi

bdd:
	$(GO) test ./... -run TestFeatures -v

arch-test:
	$(GO) test ./internal/architecture/... -v

# Requires a running Postgres: docker compose up -d postgres, and
# DATABASE_URL pointed at it.
integration:
	$(GO) build -tags=integration ./...
	$(GO) vet -tags=integration ./...
	$(GO) test -tags=integration ./... -race -count=1

mutation-fast:
	@if ! command -v gremlins >/dev/null 2>&1; then \
		echo "gremlins is not installed."; \
		echo "install the version CI pins with:"; \
		echo "  go install github.com/go-gremlins/gremlins/cmd/gremlins@v0.6.0"; \
		exit 1; \
	fi
	gremlins unleash ./internal/domain --workers 1 --timeout-coefficient 30

# Alias: CI's only mutation job is called mutation-fast (this repo's whole
# domain is small enough — ~11 mutants — that the "fast" subset already IS
# the full domain run, unlike bigger repos that split mutation-fast/mutation).
mutation: mutation-fast

api-lint:
	@if ! command -v spectral >/dev/null 2>&1; then \
		echo "spectral is not installed."; \
		echo "install it with:"; \
		echo "  npm install -g @stoplight/spectral-cli"; \
		exit 1; \
	fi
	spectral lint apis/openapi.yaml --ruleset .spectral.yaml --fail-severity=warn
	spectral lint apis/asyncapi.yaml --ruleset .spectral.asyncapi.yaml --fail-severity=warn

vuln:
	@if ! command -v govulncheck >/dev/null 2>&1; then \
		echo "govulncheck is not installed."; \
		echo "install it with:"; \
		echo "  go install golang.org/x/vuln/cmd/govulncheck@latest"; \
		exit 1; \
	fi
	govulncheck ./...

# Property-based contract tests against apis/openapi.yaml — mirrors the
# `contract` CI job. Not part of check/check-all (needs the Python
# `schemathesis==4.28.0` tooling on PATH, like CI installs).
contract:
	./scripts/contract-test.sh

# The fast self-correction loop: run this after every change, before committing.
check: fmt-check vet build lint test

# The fuller gate a human runs before pushing.
check-all: check coverage arch-test bdd
