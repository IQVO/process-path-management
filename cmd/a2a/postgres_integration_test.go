//go:build integration

// Integration test for cmd/a2a over REAL Postgres: buildRepo (the same
// migrations + pool + ping the binary runs at boot) behind newAgent, the same
// composition run() serves, driven by the SDK's own client through the real
// router. It proves the A2A skills answer from Postgres-backed use cases, end
// to end, and that the read-only agent leaves the outbox empty.
//
// Postgres comes from testcontainers; never an external DATABASE_URL, never
// t.Skip.
package main

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/claudioed/process-path-management/internal/domain/processpath"
	"github.com/claudioed/process-path-management/internal/domain/shared"
)

func TestA2AReadsFromRealPostgresAndWritesNothing(t *testing.T) {
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("pathmgmt"),
		tcpostgres.WithUsername("pathmgmt"),
		tcpostgres.WithPassword("pathmgmt"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(90*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })
	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}

	repo, pool, closeAdapters, err := buildRepo(ctx, dsn, dsn, migrationsDirForTest(t), quietLogger())
	if err != nil {
		t.Fatalf("buildRepo: %v", err)
	}
	t.Cleanup(closeAdapters)

	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	pick, err := processpath.Define("PICK", "pick", true, []shared.Capability{"pick"}, shared.DestinationLocationRoleUnset, 2*time.Hour, shared.Eligibility{}, shared.Intake{}, now)
	if err != nil {
		t.Fatal(err)
	}
	rebin, err := processpath.Define("REBIN", "rebin", false, []shared.Capability{"rebin"}, shared.DestinationLocationRoleUnset, 2*time.Hour, shared.Eligibility{}, shared.Intake{}, now)
	if err != nil {
		t.Fatal(err)
	}
	rebin.Deactivate(now.Add(time.Hour))
	for _, p := range []*processpath.ProcessPath{pick, rebin} {
		if err := repo.Save(ctx, p); err != nil {
			t.Fatalf("seed %s: %v", p.ID(), err)
		}
	}

	srv := httptest.NewServer(newAgent(repo, testAgentURL, "itest"))
	t.Cleanup(srv.Close)
	client := clientFor(t, srv.URL+"/a2a")

	text := send(t, client, map[string]any{"skill": "get_process_path", "arguments": map[string]any{"pathId": "PICK"}})
	if !strings.Contains(text, "PICK: active, direct, matchPrefix pick, requires pick") {
		t.Fatalf("get_process_path = %q", text)
	}
	text = send(t, client, map[string]any{"skill": "list_process_paths", "arguments": map[string]any{}})
	if !strings.HasPrefix(text, "1 process paths (active only): PICK") {
		t.Fatalf("list_process_paths = %q", text)
	}
	text = send(t, client, map[string]any{"skill": "list_process_paths", "arguments": map[string]any{"activeOnly": false}})
	if !strings.HasPrefix(text, "2 process paths (including deactivated): ") {
		t.Fatalf("list_process_paths(all) = %q", text)
	}
	text = send(t, client, map[string]any{"skill": "get_process_path", "arguments": map[string]any{"pathId": "GHOST"}})
	if !strings.Contains(text, "(404)") {
		t.Fatalf("unknown path = %q, want an in-band 404 problem", text)
	}

	// The seeding above went through the repository directly, not the outbox;
	// a read-only agent adds no row.
	var rows int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM outbox_events").Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 0 {
		t.Fatalf("outbox_events has %d rows after read-only skills, want 0", rows)
	}
}

func clientFor(t *testing.T, endpoint string) *a2aclient.Client {
	t.Helper()
	card := &a2a.AgentCard{
		Name:                "process-path-management",
		SupportedInterfaces: []*a2a.AgentInterface{a2a.NewAgentInterface(endpoint, a2a.TransportProtocolJSONRPC)},
	}
	client, err := a2aclient.NewFromCard(context.Background(), card)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func send(t *testing.T, client *a2aclient.Client, req map[string]any) string {
	t.Helper()
	resp, err := client.SendMessage(context.Background(), &a2a.SendMessageRequest{Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewDataPart(req))})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	msg, ok := resp.(*a2a.Message)
	if !ok || len(msg.Parts) < 2 {
		raw, _ := json.Marshal(resp)
		t.Fatalf("reply = %s", raw)
	}
	text, _ := msg.Parts[1].Content.(a2a.Text)
	return string(text)
}
