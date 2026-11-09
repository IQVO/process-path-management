package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/claudioed/process-path-management/internal/application/usecases"
)

type stubRepublisher struct {
	res usecases.RepublishResult
	err error
}

func (s stubRepublisher) Execute(context.Context) (usecases.RepublishResult, error) {
	return s.res, s.err
}

func captureLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewTextHandler(&buf, nil)), &buf
}

// ADR 0038: a failed republish is never fatal and never silent: the boot
// continues, and the failure is logged at ERROR.
func TestRepublishAtStartup_FailureIsLoggedAtErrorAndNotFatal(t *testing.T) {
	logger, buf := captureLogger()
	ok := republishCatalogueAtStartup(context.Background(), logger, stubRepublisher{err: errors.New("db gone")})
	if ok {
		t.Fatal("a failed republish reported success")
	}
	out := buf.String()
	if !strings.Contains(out, "level=ERROR") || !strings.Contains(out, "startup catalogue republish failed") || !strings.Contains(out, "db gone") {
		t.Fatalf("log = %s, want an ERROR line carrying the cause", out)
	}
}

func TestRepublishAtStartup_TruncationIsLoggedAtError(t *testing.T) {
	logger, buf := captureLogger()
	ok := republishCatalogueAtStartup(context.Background(), logger, stubRepublisher{res: usecases.RepublishResult{Paths: 1000, Omitted: 3}})
	if ok {
		t.Fatal("a truncated republish reported success")
	}
	if out := buf.String(); !strings.Contains(out, "level=ERROR") || !strings.Contains(out, "omitted=3") {
		t.Fatalf("log = %s, want an ERROR line with omitted=3", out)
	}
}

func TestRepublishAtStartup_SuccessIsLoggedAtInfo(t *testing.T) {
	logger, buf := captureLogger()
	ok := republishCatalogueAtStartup(context.Background(), logger, stubRepublisher{res: usecases.RepublishResult{Paths: 7, Schedules: 2}})
	if !ok {
		t.Fatal("a clean republish reported failure")
	}
	out := buf.String()
	if strings.Contains(out, "level=ERROR") || !strings.Contains(out, "paths=7") || !strings.Contains(out, "schedules=2") {
		t.Fatalf("log = %s, want an INFO line with the counts and no ERROR", out)
	}
}

// The republish is bounded: the context handed to the use case carries a
// deadline, so an unreachable database cannot hold the boot.
func TestRepublishAtStartup_UseCaseRunsUnderADeadline(t *testing.T) {
	logger, _ := captureLogger()
	var hasDeadline bool
	republishCatalogueAtStartup(context.Background(), logger, deadlineProbe{&hasDeadline})
	if !hasDeadline {
		t.Fatal("the use case ran without a deadline")
	}
}

type deadlineProbe struct{ got *bool }

func (p deadlineProbe) Execute(ctx context.Context) (usecases.RepublishResult, error) {
	_, *p.got = ctx.Deadline()
	return usecases.RepublishResult{}, nil
}
