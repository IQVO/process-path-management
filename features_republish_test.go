package main_test

import (
	"context"
	"fmt"

	"github.com/cucumber/godog"

	"github.com/claudioed/process-path-management/internal/application/usecases"
)

// republishRig is the startup catalogue republish (ADR 0038) wired over the
// scenario's own repositories, with its own recorder.
type republishRig struct {
	uc        *usecases.RepublishCatalogue
	published *recordingPublisher
	last      usecases.RepublishResult
}

func registerRepublishSteps(sc *godog.ScenarioContext, w *world) {
	sc.Step(`^the service is restarted$`, func() error {
		res, err := w.rig.uc.Execute(context.Background())
		w.rig.last = res
		return err
	})
	sc.Step(`^a "([^"]*)" event was republished for (?:path|site) "([^"]*)"$`, func(name, id string) error {
		if n := w.rig.published.countFor(name, id); n != 1 {
			return fmt.Errorf("got %d republished %q events for %q, want exactly 1", n, name, id)
		}
		return nil
	})
	sc.Step(`^no "([^"]*)" event was republished for path "([^"]*)"$`, func(name, id string) error {
		if n := w.rig.published.countFor(name, id); n != 0 {
			return fmt.Errorf("got %d republished %q events for %q, want 0", n, name, id)
		}
		return nil
	})
	sc.Step(`^the restart republished (\d+) paths? and (\d+) schedules?$`, func(paths, schedules int) error {
		if w.rig.last.Paths != paths || w.rig.last.Schedules != schedules {
			return fmt.Errorf("republished %+v, want %d paths and %d schedules", w.rig.last, paths, schedules)
		}
		return nil
	})
}
