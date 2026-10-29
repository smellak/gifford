//go:build unix

package label

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const stopEvent = `{"hook_event_name":"Stop"}`

func TestRecordHookRefusesSymlinkedDir(t *testing.T) {
	t.Parallel()

	data, outside := t.TempDir(), t.TempDir()
	require.NoError(t, os.Chmod(outside, 0o700))
	require.NoError(t, os.Symlink(outside, Dir(data)))

	require.ErrorIs(t, RecordHook(Dir(data), t0, strings.NewReader(stopEvent)), ErrUnsafeFile)
	entries, err := os.ReadDir(outside)
	require.NoError(t, err)
	require.Empty(t, entries, "nothing written outside labels/")
}

func TestRecordHookRefusesSymlinkedFile(t *testing.T) {
	t.Parallel()

	dir, outside := Dir(t.TempDir()), filepath.Join(t.TempDir(), "target")
	require.NoError(t, os.MkdirAll(dir, 0o700))
	require.NoError(t, os.WriteFile(outside, nil, 0o600))
	require.NoError(t, os.Symlink(outside, filepath.Join(dir, HooksFile)))

	require.ErrorIs(t, RecordHook(dir, t0, strings.NewReader(stopEvent)), ErrUnsafeFile)
	raw, err := os.ReadFile(outside)
	require.NoError(t, err)
	require.Empty(t, raw, "nothing written through the link")

	// A dangling link is not followed to create its target either.
	require.NoError(t, os.Remove(outside))
	require.Error(t, RecordHook(dir, t0, strings.NewReader(stopEvent)))
	_, err = os.Lstat(outside)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestRecordHookNeverBlocksOnFIFO(t *testing.T) {
	t.Parallel()

	dir := Dir(t.TempDir())
	require.NoError(t, os.MkdirAll(dir, 0o700))
	require.NoError(t, syscall.Mkfifo(filepath.Join(dir, HooksFile), 0o600))

	done := make(chan error, 1)
	go func() { done <- RecordHook(dir, t0, strings.NewReader(stopEvent)) }()
	select {
	case err := <-done:
		require.ErrorIs(t, err, ErrUnsafeFile)
	case <-time.After(5 * time.Second):
		t.Fatal("RecordHook blocked on a FIFO")
	}
}

func TestRecordHookRefusesNonDirLabels(t *testing.T) {
	t.Parallel()

	data := t.TempDir()
	require.NoError(t, os.WriteFile(Dir(data), nil, 0o600))
	require.Error(t, RecordHook(Dir(data), t0, strings.NewReader(stopEvent)))
}
