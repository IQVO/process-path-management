//go:build integration

package postgres_test

import (
	"path/filepath"
	"runtime"
	"testing"
)

// migrationsDir resolves /migrations relative to this test file, so the
// test works regardless of the working directory `go test` is invoked
// from.
func migrationsDir(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("unable to resolve test file path")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..", "migrations")
}
