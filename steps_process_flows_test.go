package main_test

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/cucumber/godog"

	"github.com/claudioed/process-path-management/internal/domain/processflow"
)

// flowStagesBody turns "PICK:PICK,PACK:PACK" (pathId:processType, in order)
// into the wire shape of the stages. An empty string is no stages.
func flowStagesBody(csv string) []map[string]string {
	out := []map[string]string{}
	if strings.TrimSpace(csv) == "" {
		return out
	}
	for _, pair := range strings.Split(csv, ",") {
		path, typ, _ := strings.Cut(strings.TrimSpace(pair), ":")
		out = append(out, map[string]string{"pathId": path, "processType": typ})
	}
	return out
}

func (w *world) putFlow(flowId, name, stagesCSV string) error {
	return w.do(http.MethodPut, "/process-flows/"+flowId, map[string]any{
		"name":   name,
		"stages": flowStagesBody(stagesCSV),
	})
}

func (w *world) flowAlreadyPut(flowId, name, stagesCSV string) error {
	if err := w.putFlow(flowId, name, stagesCSV); err != nil {
		return err
	}
	if w.lastStatus != http.StatusOK && w.lastStatus != http.StatusCreated {
		return fmt.Errorf("setup put flow %s: got %d: %s", flowId, w.lastStatus, string(w.lastBody))
	}
	return nil
}

func (w *world) retireFlow(flowId string) error {
	return w.do(http.MethodDelete, "/process-flows/"+flowId, nil)
}

func (w *world) flowAlreadyRetired(flowId string) error {
	if err := w.retireFlow(flowId); err != nil {
		return err
	}
	if w.lastStatus != http.StatusNoContent {
		return fmt.Errorf("setup retire flow %s: want 204, got %d: %s", flowId, w.lastStatus, string(w.lastBody))
	}
	return nil
}

func (w *world) flowResponseReports(flowId, status string, version int, stagesCSV string) error {
	obj, err := w.decodeLastObject()
	if err != nil {
		return err
	}
	if got, _ := obj["flowId"].(string); got != flowId {
		return fmt.Errorf("got flowId %q, want %q", got, flowId)
	}
	if got, _ := obj["status"].(string); got != status {
		return fmt.Errorf("got status %q, want %q", got, status)
	}
	if got, ok := obj["version"].(float64); !ok || int(got) != version {
		return fmt.Errorf("got version %v, want %d", obj["version"], version)
	}
	var got []string
	stages, _ := obj["stages"].([]any)
	for _, s := range stages {
		m, _ := s.(map[string]any)
		got = append(got, fmt.Sprintf("%v:%v", m["pathId"], m["processType"]))
	}
	if strings.Join(got, ",") != stagesCSV {
		return fmt.Errorf("got stages %q, want %q", strings.Join(got, ","), stagesCSV)
	}
	return nil
}

func (w *world) flowNowHasStatus(flowId, status string) error {
	if err := w.do(http.MethodGet, "/process-flows/"+flowId, nil); err != nil {
		return err
	}
	obj, err := w.decodeLastObject()
	if err != nil {
		return err
	}
	if got, _ := obj["status"].(string); got != status {
		return fmt.Errorf("got status %q, want %q", got, status)
	}
	return nil
}

func (w *world) flowListReports(count int) error {
	items, err := w.decodeLastItems()
	if err != nil {
		return err
	}
	if len(items) != count {
		return fmt.Errorf("got %d flows, want %d: %v", len(items), count, items)
	}
	return nil
}

// flowEventVersions lists the versions of the recorded events named name for
// flow id.
func flowEventVersions(p *recordingPublisher, name, id string) []int {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []int
	for _, e := range p.events {
		switch ev := e.(type) {
		case processflow.ProcessFlowDefined:
			if ev.EventName() == name && string(ev.FlowId) == id {
				out = append(out, ev.Version)
			}
		case processflow.ProcessFlowRevised:
			if ev.EventName() == name && string(ev.FlowId) == id {
				out = append(out, ev.Version)
			}
		case processflow.ProcessFlowRetired:
			if ev.EventName() == name && string(ev.FlowId) == id {
				out = append(out, ev.Version)
			}
		}
	}
	return out
}

func hasVersion(versions []int, v int) bool {
	for _, x := range versions {
		if x == v {
			return true
		}
	}
	return false
}

func (w *world) detailNamesFlows(names string) error {
	obj, err := w.decodeLastObject()
	if err != nil {
		return err
	}
	if detail, _ := obj["detail"].(string); !strings.Contains(detail, "["+strings.ReplaceAll(names, ", ", " ")+"]") {
		return fmt.Errorf("detail %q does not name the flows [%s]", detail, names)
	}
	return nil
}

func registerProcessFlowSteps(sc *godog.ScenarioContext, w *world) {
	sc.Step(`^the flow "([^"]*)" named "([^"]*)" is put with stages "([^"]*)"$`, w.putFlow)
	sc.Step(`^the flow "([^"]*)" named "([^"]*)" is put with no stages$`, func(id, name string) error { return w.putFlow(id, name, "") })
	sc.Step(`^the flow "([^"]*)" named "([^"]*)" is already put with stages "([^"]*)"$`, w.flowAlreadyPut)
	sc.Step(`^the flow "([^"]*)" is requested$`, func(id string) error { return w.do(http.MethodGet, "/process-flows/"+id, nil) })
	sc.Step(`^the flows are listed$`, func() error { return w.do(http.MethodGet, "/process-flows", nil) })
	sc.Step(`^all flows are listed including retired ones$`, func() error { return w.do(http.MethodGet, "/process-flows?all=true", nil) })
	sc.Step(`^the flow "([^"]*)" is retired$`, w.retireFlow)
	sc.Step(`^the flow "([^"]*)" is already retired$`, w.flowAlreadyRetired)

	sc.Step(`^the flow response reports flowId "([^"]*)", status "([^"]*)", version (\d+) and stages "([^"]*)"$`, w.flowResponseReports)
	sc.Step(`^the flow "([^"]*)" now has status "([^"]*)"$`, w.flowNowHasStatus)
	sc.Step(`^the flow list reports (\d+) flows?$`, w.flowListReports)
	sc.Step(`^the response detail names the flows "([^"]*)"$`, w.detailNamesFlows)

	sc.Step(`^a "([^"]*)" event was published for flow "([^"]*)" at version (\d+)$`, func(name, id string, v int) error {
		if got := flowEventVersions(w.events, name, id); !hasVersion(got, v) {
			return fmt.Errorf("%q events for flow %q have versions %v, want one at %d", name, id, got, v)
		}
		return nil
	})
	sc.Step(`^no "([^"]*)" event was published for flow "([^"]*)"$`, func(name, id string) error { return w.noEventWasPublishedFor(name, id) })
	sc.Step(`^exactly one "([^"]*)" event was published for flow "([^"]*)"$`, func(name, id string) error { return w.exactlyOneEventWasPublishedFor(name, id) })

	sc.Step(`^a "([^"]*)" event was republished for flow "([^"]*)" at version (\d+)$`, func(name, id string, v int) error {
		if got := flowEventVersions(w.rig.published, name, id); len(got) != 1 || got[0] != v {
			return fmt.Errorf("republished %q events for flow %q have versions %v, want exactly one at %d", name, id, got, v)
		}
		return nil
	})
	sc.Step(`^no "([^"]*)" event was republished for flow "([^"]*)"$`, func(name, id string) error {
		if got := flowEventVersions(w.rig.published, name, id); len(got) != 0 {
			return fmt.Errorf("got republished %q events for flow %q at %v, want none", name, id, got)
		}
		return nil
	})
	sc.Step(`^the restart republished (\d+) flows?$`, func(n int) error {
		if w.rig.last.Flows != n {
			return fmt.Errorf("republished %+v, want %d flows", w.rig.last, n)
		}
		return nil
	})
}
