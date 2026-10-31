package runtimelock

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAcquireIsExclusiveAndReusable(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	first, err := Acquire(dir)
	require.NoError(t, err)

	_, err = Acquire(dir)
	require.ErrorIs(t, err, ErrHeld)

	require.NoError(t, first.Close())
	second, err := Acquire(dir)
	require.NoError(t, err)
	require.NoError(t, second.Close())

	info, err := os.Stat(filepath.Join(dir, lockName))
	require.NoError(t, err)
	if runtime.GOOS != "windows" {
		require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	}

	// ErrHeld remains discoverable through the human-facing wrapper.
	require.ErrorIs(t, errHeldForTest(t, dir), ErrHeld)
}

func errHeldForTest(t *testing.T, dir string) error {
	t.Helper()
	lock, err := Acquire(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, lock.Close()) })
	_, err = Acquire(dir)
	return err
}
