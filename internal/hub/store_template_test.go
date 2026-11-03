package hub

// Test-only store template: migrate once per test binary, then hand each
// test its own file copy. Every test keeps an isolated database file; the
// template itself is read-only after the single migration, so parallel
// tests never share state. Production code is untouched, and migration
// coverage stays in internal/eventstore, which keeps opening fresh files.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/eventstore"
)

var (
	hubTemplateDir  string
	hubTemplateOnce sync.Once
	hubTemplatePath string
	hubTemplateErr  error
)

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "gifford-hub-template-*")
	if err != nil {
		fmt.Fprintln(os.Stderr, "hub template dir:", err)
		os.Exit(1)
	}
	hubTemplateDir = dir
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

// testStorePath copies the once-migrated template database to dir/name and
// returns that path for eventstore.Open. Call it once per database file.
func testStorePath(t *testing.T, dir, name string) string {
	t.Helper()
	hubTemplateOnce.Do(func() {
		path := filepath.Join(hubTemplateDir, "template.db")
		store, err := eventstore.Open(context.Background(), path)
		if err != nil {
			hubTemplateErr = err
			return
		}
		if err := store.Close(); err != nil {
			hubTemplateErr = err
			return
		}
		hubTemplatePath = path
	})
	require.NoError(t, hubTemplateErr)
	dst := filepath.Join(dir, name)
	raw, err := os.ReadFile(hubTemplatePath)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(dst, raw, 0o600))
	return dst
}
