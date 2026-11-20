package http

import "net/http"

// ProblemTypeForTest is one problem type of the catalogue, for the external
// test package.
type ProblemTypeForTest struct {
	Slug   string
	Status int
	Class  string
}

// ProblemTypesForTest is the whole catalogue (allProblemTypes), one entry per
// slug and status.
func ProblemTypesForTest() []ProblemTypeForTest {
	var out []ProblemTypeForTest
	for _, p := range allProblemTypes() {
		out = append(out, ProblemTypeForTest{Slug: p.Slug, Status: p.Status, Class: string(p.Class)})
	}
	return out
}

// RefuseForTest answers the problem type (slug, status) through the
// production error writer: a typed error of the catalogue through writeError,
// a standalone problem through writeProblem, so the class marking the
// middleware reads is the production one.
func RefuseForTest(w http.ResponseWriter, r *http.Request, slug string, status int) {
	for _, e := range problemCatalog() {
		if e.info.slug == slug && statusFor(e.err) == status {
			writeError(w, r, e.err)
			return
		}
	}
	for _, e := range flowProblems {
		if e.info.slug == slug && e.status == status {
			writeError(w, r, e.err)
			return
		}
	}
	for _, e := range standaloneProblems {
		if e.info.slug == slug && e.status == status {
			writeProblem(w, status, e.info, "refused for the test", r.URL.Path)
			return
		}
	}
	panic("no problem type " + slug)
}

// ProbeRefuseForTest answers a refusal of the given status whose replay class
// is class, through the production writer. No problem type of this service
// declares OUTCOME (ADR 0043), so the middleware's OUTCOME branch is proved
// with this probe.
func ProbeRefuseForTest(w http.ResponseWriter, r *http.Request, status int, class string) {
	writeProblem(w, status, problemInfo{"probe-refusal", "Probe refusal", replayClass(class)}, "refused for the test", r.URL.Path)
}
