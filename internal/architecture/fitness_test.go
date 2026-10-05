// Package architecture holds fitness tests (the Go analogue of ArchUnit,
// via github.com/arch-go/arch-go) that enforce the hexagonal/ports-and-adapters
// dependency rule described in this project's README: dependencies point
// inward only, and inbound/outbound adapters never depend on each other.
package architecture

import (
	"bufio"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	archgo "github.com/arch-go/arch-go/api"
	"github.com/arch-go/arch-go/api/configuration"
)

// TestMCPAdapterDependencyRule encodes ADR-0008: the MCP inbound adapter is
// additive, never load-bearing for the OLTP composition root. It may depend
// only on the application and domain layers (never on outbound adapters,
// never on cmd), and — the direction that actually matters for keeping it
// additive — nothing else in this codebase may depend on it. If some other
// package started importing internal/adapters/inbound/mcp, the MCP surface
// would have silently become a dependency other code relies on rather than
// a pure inbound entrypoint cmd/mcp wires up and nothing else touches.
func TestMCPAdapterDependencyRule(t *testing.T) {
	moduleInfo := configuration.Load(modulePath)

	t.Run("mcp adapter depends only on application and domain", func(t *testing.T) {
		result := archgo.CheckArchitecture(moduleInfo, configuration.Config{
			DependenciesRules: []*configuration.DependenciesRule{
				{
					Package: "**.internal.adapters.inbound.mcp.**",
					ShouldOnlyDependsOn: &configuration.Dependencies{
						Internal: []string{
							"**.internal.adapters.inbound.mcp.**",
							"**.internal.application.**",
							"**.internal.domain.**",
						},
					},
				},
			},
		})

		assertArchGoPasses(t, result)
	})

	t.Run("nothing else depends on the mcp adapter", func(t *testing.T) {
		result := archgo.CheckArchitecture(moduleInfo, configuration.Config{
			DependenciesRules: []*configuration.DependenciesRule{
				{
					Package: "**.internal.**",
					ShouldNotDependsOn: &configuration.Dependencies{
						Internal: []string{"**.internal.adapters.inbound.mcp.**"},
					},
				},
			},
		})

		assertArchGoPasses(t, result)
	})
}

// assertArchGoPasses is a shared helper for the two-return-shape arch-go
// result (DependenciesRuleResult here; the file's other tests use their own
// assertPass with the same semantics — kept separate to avoid touching
// existing tests' helper functions).
func assertArchGoPasses(t *testing.T, result *archgo.Result) {
	t.Helper()

	if result.Pass {
		return
	}

	if result.DependenciesRuleResult != nil {
		for _, r := range result.DependenciesRuleResult.Results {
			if r.Passes {
				continue
			}
			for _, v := range r.Verifications {
				if v.Passes {
					continue
				}
				for _, d := range v.Details {
					t.Errorf("%s: %s", v.Package, d)
				}
			}
		}
	}

	t.FailNow()
}

// ---------------------------------------------------------------------
// Source-scanning fitness tests below. arch-go's DSL only expresses import
// graph shape; these invariants are about literal content (a string, a
// missing import) so they're enforced the same way internal/architecture/
// zerowrite does it in warehouse-ops-agent: walk the real .go files under
// internal/, skip generated/_test.go where noted, and fail with the exact
// file:line that violates the rule.
// ---------------------------------------------------------------------

// goFilesUnder returns every non-test .go file under root (relative to the
// module root), or every .go file including tests when includeTests is true.
func goFilesUnder(t *testing.T, root string, includeTests bool) []string {
	t.Helper()

	var files []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		if !includeTests && strings.HasSuffix(path, "_test.go") {
			return nil
		}
		files = append(files, path)
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	return files
}

// authPatternRE matches the literal shapes a reintroduced bearer/JWT/API-key
// auth middleware would contain in source. Deliberately narrow (case
// sensitive on "Bearer " and known JWT library import paths) so it doesn't
// false-positive on the CORS AllowedHeaders allowlist entry (which
// legitimately lists "Authorization" as a header name a browser may send)
// or on comment-only mentions of the fleet's auth revert — this test scans
// non-comment, non-test Go source only.
var authPatternRE = regexp.MustCompile(`"Bearer |golang-jwt/jwt|dgrijalva/jwt-go|lestrrat-go/jwx`)

// TestNoAuthMiddlewareReintroduced encodes the fleet-wide 2026-09-11 REST +
// MCP static-bearer-auth revert: every endpoint is deliberately
// unauthenticated pending a fresh auth-model decision. An agent
// "helpfully" re-adding a bearer/JWT middleware to internal/adapters/inbound
// should fail CI, not ship silently.
func TestNoAuthMiddlewareReintroduced(t *testing.T) {
	for _, path := range goFilesUnder(t, "../adapters/inbound", false) {
		f, err := os.Open(path)
		if err != nil {
			t.Fatalf("open %s: %v", path, err)
		}
		lineNo := 0
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			lineNo++
			line := scanner.Text()
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "//") {
				continue
			}
			if authPatternRE.MatchString(line) {
				t.Errorf("%s:%d: matches a reintroduced-auth pattern: %q — the fleet-wide static-bearer auth layer was deliberately reverted 2026-09-11 (unauthenticated pending a fresh auth-model decision); if this is intentional, update this test alongside the ADR documenting the new decision", path, lineNo, trimmed)
			}
		}
		f.Close()
		if err := scanner.Err(); err != nil {
			t.Fatalf("scan %s: %v", path, err)
		}
	}
}

// groupIDLiteralRE matches a kafka-go ReaderConfig's GroupID field being
// assigned a bare double-quoted string literal, e.g. `GroupID: "my-group"`.
// A symbol reference (`GroupID: AnalyticsConsumerGroup`, `GroupID: groupID`,
// `GroupID: uniqueConsumerGroup()`) never matches this.
var groupIDLiteralRE = regexp.MustCompile(`GroupID:\s*"[^"]+"`)

// TestKafkaConsumerGroupNeverHardcodedInline encodes a real, already-lived
// incident: wes-work-planning's OLTP consumer group was a hardcoded literal
// (`"wes-work-planning"`), which made a locally-run e2e-tests process join
// the SAME consumer group as the live in-cluster Deployment on the shared
// fleet Kafka broker — Kafka's rebalance protocol then handed the single
// partition to only one of the two group members, silently starving
// whichever process lost the race (fixed in wes-work-planning#67 by making
// the group id env-configurable). This test doesn't ban long-lived named
// consumer groups outright (this repo's own analytics AnalyticsConsumerGroup
// constant is a deliberate, correct exception — only one instance of that
// consumer ever runs, so there's nothing to collide with) — it bans the
// shape that caused the incident: a GroupID assigned directly from an
// inline string literal rather than through a named symbol (const, var, or
// function call) that a reviewer can trace back to its definition and
// reasoning.
func TestKafkaConsumerGroupNeverHardcodedInline(t *testing.T) {
	for _, path := range goFilesUnder(t, "..", false) {
		f, err := os.Open(path)
		if err != nil {
			t.Fatalf("open %s: %v", path, err)
		}
		lineNo := 0
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			lineNo++
			line := scanner.Text()
			if groupIDLiteralRE.MatchString(line) {
				t.Errorf("%s:%d: GroupID assigned an inline string literal: %q — use a named const/var or a function call (see this repo's AnalyticsConsumerGroup const for the single-instance pattern, or e.g. inventory-storage's uniqueConsumerGroup() for the per-process-unique pattern) so the group id's lifetime/uniqueness reasoning is traceable and a locally-run process can never silently join a live cluster's group", path, lineNo, strings.TrimSpace(line))
			}
		}
		f.Close()
		if err := scanner.Err(); err != nil {
			t.Fatalf("scan %s: %v", path, err)
		}
	}
}

// TestKafkaIntegrationTestsUseTestcontainers encodes the fleet-wide rule
// (corrected 2026-09-06): a Kafka-touching `-tags=integration` test MUST
// start its own broker via testcontainers-go/modules/kafka, never gate on
// os.Getenv("KAFKA_BROKERS") + t.Skip, and never hardcode localhost:9092.
// This fleet's CI `integration` job provisions Postgres only — a
// skip-gated Kafka test silently skips in CI and proves nothing there,
// while testcontainers is the only variant that actually exercises the
// Kafka assertions on a runner.
func TestKafkaIntegrationTestsUseTestcontainers(t *testing.T) {
	for _, path := range goFilesUnder(t, "..", true) {
		if !strings.HasSuffix(path, "_integration_test.go") {
			continue
		}

		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		content := string(src)

		if !integrationTestTouchesKafka(content) {
			continue
		}

		hasSkipGate, hasHardcodedBroker := scanKafkaIntegrationTestRules(t, path)

		assertKafkaIntegrationTestFollowsFleetRules(t, path, content, hasSkipGate, hasHardcodedBroker)
	}
}

// integrationTestTouchesKafka reports whether an integration test's source
// references this fleet's Kafka client or its broker config at all.
func integrationTestTouchesKafka(content string) bool {
	return strings.Contains(content, "segmentio/kafka-go") ||
		strings.Contains(content, "kafka.Reader") ||
		strings.Contains(content, "kafka.Writer") ||
		strings.Contains(content, "KAFKA_BROKERS")
}

// scanKafkaIntegrationTestRules inspects the file line-by-line so a comment
// that merely MENTIONS the banned shapes (e.g. explaining that a real broker
// via testcontainers is used specifically so the test needs no
// KAFKA_BROKERS/localhost:9092) doesn't false-positive. It reports whether
// the file gates on os.Getenv("KAFKA_BROKERS") and/or hardcodes
// localhost:9092.
func scanKafkaIntegrationTestRules(t *testing.T, path string) (hasSkipGate, hasHardcodedBroker bool) {
	t.Helper()

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "//") {
			continue
		}
		if strings.Contains(line, `os.Getenv("KAFKA_BROKERS")`) {
			hasSkipGate = true
		}
		if strings.Contains(line, "localhost:9092") {
			hasHardcodedBroker = true
		}
	}
	f.Close()
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan %s: %v", path, err)
	}
	return hasSkipGate, hasHardcodedBroker
}

// assertKafkaIntegrationTestFollowsFleetRules fails the test when a
// Kafka-touching integration test violates the testcontainers rule: gating
// on an env-provided broker, hardcoding localhost:9092, or not importing
// testcontainers-go/modules/kafka.
func assertKafkaIntegrationTestFollowsFleetRules(t *testing.T, path, content string, hasSkipGate, hasHardcodedBroker bool) {
	t.Helper()

	if hasSkipGate {
		t.Errorf("%s: gates on os.Getenv(\"KAFKA_BROKERS\") — this fleet's CI integration job provisions Postgres only, so a skip-gated Kafka test silently skips in CI and proves nothing there; start a real broker via testcontainers-go/modules/kafka instead", path)
	}
	if hasHardcodedBroker {
		t.Errorf("%s: hardcodes localhost:9092 — a fresh CI runner has no broker at that address; start one via testcontainers-go/modules/kafka instead", path)
	}
	if !strings.Contains(content, "testcontainers-go/modules/kafka") {
		t.Errorf("%s: touches Kafka but does not import github.com/testcontainers/testcontainers-go/modules/kafka — Kafka-touching integration tests in this fleet must start their own broker via testcontainers, never assume/skip on an external one", path)
	}
}

// TestNoSiblingContextOutboundCalls encodes this repo's stricter-than-fleet
// rule (see AGENTS.md's "Strategic classification": zero inbound dependency
// on any other bounded context, everything propagates only via Kafka
// events this service publishes). Unlike most of the fleet, which allows a
// synchronous HTTP client to a sibling context's REST or MCP surface,
// process-path-management bans that outright: no adapter may hold an
// HTTP CLIENT dependency, because there is no legitimate outbound
// synchronous call this service should ever make to another bounded
// context. (net/http the SERVER library appears legitimately across
// internal/adapters/inbound — the chi router, the MCP Streamable HTTP
// handler — which is why the scan matches client constructs, not the
// bare import.)
//
// The scan covers BOTH internal/adapters/outbound and
// internal/adapters/inbound: a sibling-context HTTP client could just as
// easily hide inside an inbound adapter (the MCP report tool's client
// did exactly that), and a scan that only reads outbound would never
// see it. Each file that legitimately holds an HTTP client must appear
// in sameContextHTTPClients with its reason; today that is exactly one:
// the MCP catalogue-growth report tool calling this context's OWN
// pathmgmt-reports service, which ADR 0007 §4 explicitly allows
// (same-context, not a sibling bounded context).
//
// httpClientConstructs are the textual shapes an HTTP client takes in
// this codebase: a field/parameter of type *http.Client, a client
// literal, or an outbound request builder.
var httpClientConstructs = []string{
	"*http.Client",
	"&http.Client{",
	"http.NewRequest",
	"http.NewRequestWithContext",
	"http.Get(",
	"http.Post(",
	"http.DefaultClient",
}

// containsHTTPClient reports whether src holds any HTTP-client construct.
func containsHTTPClient(src string) bool {
	for _, s := range httpClientConstructs {
		if strings.Contains(src, s) {
			return true
		}
	}
	return false
}

var sameContextHTTPClients = map[string]string{
	"adapters/inbound/mcp/report_tool.go": "calls this context's OWN pathmgmt-reports REST service (ADR 0007 §4: same-context reads are not sibling-context coupling)",
}

func TestNoSiblingContextOutboundCalls(t *testing.T) {
	for _, root := range []string{"../adapters/outbound", "../adapters/inbound"} {
		for _, path := range goFilesUnder(t, root, false) {
			src, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read %s: %v", path, err)
			}
			if !containsHTTPClient(string(src)) {
				continue
			}
			rel, err := filepath.Rel("..", path)
			if err != nil {
				t.Fatalf("rel %s: %v", path, err)
			}
			if reason, ok := sameContextHTTPClients[rel]; ok {
				t.Logf("%s: HTTP client allowed — %s", rel, reason)
				continue
			}
			t.Errorf("%s: holds an HTTP client under internal/adapters — process-path-management is a zero-inbound-dependency Open Host Service (see AGENTS.md): it must never issue a synchronous HTTP call to a sibling bounded context. Every cross-context integration here happens exclusively via Kafka events this service publishes. If this file calls only this context's own service, add it (with its reason) to sameContextHTTPClients in this test.", path)
		}
	}
}

// TestNoSiblingContextOutboundCalls_DetectsViolation proves the guard
// above fires: given a fixture that looks exactly like a smuggled
// sibling-context HTTP client, the detector must reject it. This is the
// guard's own test — a scanner that has never seen a violation prove
// anything is a scanner nobody trusts — and it fails the moment someone
// weakens containsHTTPClient or the allowlist lookup to a no-op.
func TestNoSiblingContextOutboundCalls_DetectsViolation(t *testing.T) {
	fixturePath := filepath.Join("adapters", "inbound", "sibling_context_client_fixture.go")
	if _, ok := sameContextHTTPClients[fixturePath]; ok {
		t.Fatalf("%s must NOT be in the allowlist for this test to mean anything", fixturePath)
	}

	// The exact shapes a smuggled client takes; every one must be caught.
	violations := []string{
		"package inbound\n\nimport \"net/http\"\n\nvar c *http.Client\n",
		"package inbound\n\nimport \"net/http\"\n\nvar c = &http.Client{Timeout: time.Second}\n",
		"package inbound\n\nimport \"net/http\"\n\nfunc f() { req, _ := http.NewRequest(http.MethodGet, url, nil); _ = req }\n",
		"package inbound\n\nimport \"net/http\"\n\nfunc f(ctx context.Context, url string) { req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil); _ = req }\n",
	}
	for i, src := range violations {
		if !containsHTTPClient(src) {
			t.Fatalf("violation shape %d was NOT detected — containsHTTPClient must match every client construct a smuggler would use", i)
		}
	}

	// And the allowlist lookup this test relies on: a detected client not
	// on the allowlist is the failing condition the real scan reports.
	if _, allowed := sameContextHTTPClients[fixturePath]; allowed {
		t.Fatal("fixture unexpectedly allowlisted")
	}

	// A clean file (server-only net/http use) must NOT be flagged.
	clean := "package inbound\n\nimport (\n	\"net/http\"\n)\n\nfunc Handler() http.Handler { return nil }\n"
	if containsHTTPClient(clean) {
		t.Fatal("server-only net/http use was flagged as a client — the detector is too broad and would drown the signal in false positives")
	}
}
