package federation

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
	federationTemplateDir  string
	federationTemplateOnce sync.Once
	federationTemplatePath string
	federationTemplateErr  error
)

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "gifford-federation-template-*")
	if err != nil {
		fmt.Fprintln(os.Stderr, "federation template dir:", err)
		os.Exit(1)
	}
	federationTemplateDir = dir
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

// testFederationTemplatePath copies the once-migrated template database
// into dir/name and returns that path for eventstore.Open. Call it once
// per database file.
func testFederationTemplatePath(t *testing.T, dir, name string) string {
	t.Helper()
	federationTemplateOnce.Do(func() {
		path := filepath.Join(federationTemplateDir, "template.db")
		store, err := eventstore.Open(context.Background(), path)
		if err != nil {
			federationTemplateErr = err
			return
		}
		if err := store.Close(); err != nil {
			federationTemplateErr = err
			return
		}
		federationTemplatePath = path
	})
	require.NoError(t, federationTemplateErr)
	dst := filepath.Join(dir, name)
	raw, err := os.ReadFile(federationTemplatePath)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(dst, raw, 0o600))
	return dst
}
