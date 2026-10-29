package capture

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// publishDir creates agent/session under the root.
func publishDir(t *testing.T, data string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Join(data, "captures", "agent", "session"), 0o700))
}

func TestPublishNoReplace(t *testing.T) {
	t.Parallel()
	data, root := testRoot(t)
	publishDir(t, data)
	require.NoError(t, publishFile(root, "agent/session", "session.json", []byte(`{"a":1}`), true))
	got, err := os.ReadFile(filepath.Join(data, "captures", "agent", "session", "session.json"))
	require.NoError(t, err)
	require.Equal(t, `{"a":1}`, string(got))
	require.NoError(t, os.Mkdir(filepath.Join(data, "captures", "agent", "other"), 0o700))
	require.NoError(t, publishFile(root, "agent/other", "session.json", []byte(`{"b":1}`), true))

	// A second publication keeps the existing file and leaves a remnant.
	err = publishFile(root, "agent/session", "session.json", []byte(`{"a":2}`), true)
	require.ErrorIs(t, err, errNameExists)
	got, err = os.ReadFile(filepath.Join(data, "captures", "agent", "session", "session.json"))
	require.NoError(t, err)
	require.Equal(t, `{"a":1}`, string(got))
	n, err := countRemnants(root, "agent/session", "session.json")
	require.NoError(t, err)
	require.Equal(t, 1, n)

	// A third leaves a second remnant; a fourth is refused.
	require.ErrorIs(t, publishFile(root, "agent/session", "session.json", []byte(`{"a":3}`), true), errNameExists)
	require.ErrorIs(t, publishFile(root, "agent/session", "session.json", []byte(`{"a":4}`), true), ErrUnsafePath)
}

func TestPublishReplace(t *testing.T) {
	t.Parallel()
	data, root := testRoot(t)
	publishDir(t, data)
	require.NoError(t, publishFile(root, "agent/session", "quota.json", []byte(`{"r":1}`), false))
	require.NoError(t, publishFile(root, "agent/session", "quota.json", []byte(`{"r":2}`), false))
	got, err := os.ReadFile(filepath.Join(data, "captures", "agent", "session", "quota.json"))
	require.NoError(t, err)
	require.Equal(t, `{"r":2}`, string(got))
	n, err := countRemnants(root, "agent/session", "quota.json")
	require.NoError(t, err)
	require.Equal(t, 0, n)
}

func TestCheckNoReplace(t *testing.T) {
	t.Parallel()
	_, root := testRoot(t)
	require.NoError(t, checkNoReplace(root))
}

// TestCheckNoReplaceRefusesSymlink proves a dangling claims.lock
// symlink is refused before any open: no target file appears in the
// other session's directory.
func TestCheckNoReplaceRefusesSymlink(t *testing.T) {
	t.Parallel()
	data, root := testRoot(t)
	makeSession(t, root, testAgent, testSessionA)
	target := filepath.Join(data, "captures", testAgent, testSessionA, "stop.request")
	require.NoError(t, os.Symlink(testAgent+"/"+testSessionA+"/stop.request", filepath.Join(data, "captures", "claims.lock")))
	require.ErrorIs(t, checkNoReplace(root), ErrUnsafePath)
	if _, err := os.Lstat(target); !os.IsNotExist(err) {
		t.Fatal("checkNoReplace created the symlink target")
	}
}
