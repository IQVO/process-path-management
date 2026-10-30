package architecture

import (
	"os"
	"strings"
	"testing"

	archgo "github.com/arch-go/arch-go/api"
	"github.com/arch-go/arch-go/api/configuration"
)

// TestA2AAdapterDependencyRule encodes ADR 0039: the A2A inbound adapter is
// additive, never load-bearing. It may depend only on the application and
// domain layers (never on outbound adapters, never on cmd), and nothing else
// in this codebase may depend on it; cmd/a2a wires it up and nothing else
// touches it.
func TestA2AAdapterDependencyRule(t *testing.T) {
	moduleInfo := configuration.Load(modulePath)

	t.Run("a2a adapter depends only on application and domain", func(t *testing.T) {
		result := archgo.CheckArchitecture(moduleInfo, configuration.Config{
			DependenciesRules: []*configuration.DependenciesRule{
				{
					Package: "**.internal.adapters.inbound.a2a.**",
					ShouldOnlyDependsOn: &configuration.Dependencies{
						Internal: []string{
							"**.internal.adapters.inbound.a2a.**",
							"**.internal.application.**",
							"**.internal.domain.**",
						},
					},
				},
			},
		})

		assertArchGoPasses(t, result)
	})

	t.Run("nothing else depends on the a2a adapter", func(t *testing.T) {
		result := archgo.CheckArchitecture(moduleInfo, configuration.Config{
			DependenciesRules: []*configuration.DependenciesRule{
				{
					Package: "**.internal.**",
					ShouldNotDependsOn: &configuration.Dependencies{
						Internal: []string{"**.internal.adapters.inbound.a2a.**"},
					},
				},
			},
		})

		assertArchGoPasses(t, result)
	})
}

// a2aForbidden maps a construct that must never appear in the A2A adapter's
// non-test sources to the reason. This context's MCP charter is entirely
// read-only (ADR 0006) and AGENTS.md bans every call to a sibling context, so
// the A2A agent derived from it is read-only and holds no outbound client.
var a2aForbidden = map[string]string{
	"a2a-go/v2/a2aclient":  "an outbound A2A client (AGENTS.md: no call to a sibling context, ever; ADR 0039)",
	"a2aclient.":           "an outbound A2A client (AGENTS.md: no call to a sibling context, ever; ADR 0039)",
	"usecases.DefinePath":  "a write use case (the A2A agent is read-only like the MCP charter, ADR 0006)",
	"usecases.RevisePath":  "a write use case (the A2A agent is read-only like the MCP charter, ADR 0006)",
	"usecases.Deactivate":  "a write use case (the A2A agent is read-only like the MCP charter, ADR 0006)",
	"usecases.DefineCPT":   "a write use case (the A2A agent is read-only like the MCP charter, ADR 0006)",
	"usecases.Republish":   "a write use case (the A2A agent is read-only like the MCP charter, ADR 0006)",
	"ports.UnitOfWork":     "a transaction: a read-only agent never writes the outbox",
	"ports.EventPublisher": "an event publisher: a read-only agent publishes no fact",
}

func a2aForbiddenViolations(src string) []string {
	var out []string
	for needle, why := range a2aForbidden {
		if strings.Contains(src, needle) {
			out = append(out, needle+": "+why)
		}
	}
	return out
}

// TestA2AAdapterIsReadOnlyAndHasNoOutboundClient is the sensor behind the
// ADR 0039 boundaries: no write use case, no outbox access and no outbound A2A
// client in internal/adapters/inbound/a2a (test files excluded: the wire tests
// legitimately drive the agent with the SDK client).
func TestA2AAdapterIsReadOnlyAndHasNoOutboundClient(t *testing.T) {
	files := goFilesUnder(t, "../adapters/inbound/a2a", false)
	if len(files) == 0 {
		t.Fatal("no A2A adapter sources found; the sensor is looking in the wrong place")
	}
	for _, path := range files {
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		for _, v := range a2aForbiddenViolations(string(src)) {
			t.Errorf("%s uses %s", path, v)
		}
	}
}

// TestA2AReadOnlySensorBites proves the detector flags each forbidden shape,
// so the sensor above cannot pass vacuously.
func TestA2AReadOnlySensorBites(t *testing.T) {
	for needle := range a2aForbidden {
		if got := a2aForbiddenViolations("package a2a\n// " + needle + "\n"); len(got) == 0 {
			t.Errorf("the sensor does not flag %q", needle)
		}
	}
	if got := a2aForbiddenViolations("uc := &usecases.GetPath{}\nlist := &usecases.ListPaths{}"); len(got) != 0 {
		t.Errorf("false positive on the read use cases: %v", got)
	}
}
