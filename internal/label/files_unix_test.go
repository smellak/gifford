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

func TestReadHooksRefusesUnsafeFiles(t *testing.T) {
	t.Parallel()

	fifoDir := Dir(t.TempDir())
	require.NoError(t, os.MkdirAll(fifoDir, 0o700))
	require.NoError(t, syscall.Mkfifo(filepath.Join(fifoDir, HooksFile), 0o600))
	done := make(chan error, 1)
	go func() { _, _, err := ReadHooks(fifoDir); done <- err }()
	select {
	case err := <-done:
		require.ErrorIs(t, err, ErrUnsafeFile)
	case <-time.After(5 * time.Second):
		t.Fatal("ReadHooks blocked on a FIFO")
	}

	linkDir, outside := Dir(t.TempDir()), filepath.Join(t.TempDir(), "target")
	require.NoError(t, os.MkdirAll(linkDir, 0o700))
	require.NoError(t, os.WriteFile(outside, []byte(`{"received_at":"2026-10-29T12:00:00Z","event":{"hook_event_name":"Stop"}}`+"\n"), 0o600))
	require.NoError(t, os.Symlink(outside, filepath.Join(linkDir, AnnotationsFile)))
	_, _, err := ReadAnnotations(linkDir)
	require.Error(t, err)
}

func TestReadRefusesSymlinkedLabelsDir(t *testing.T) {
	t.Parallel()

	data, outside := t.TempDir(), t.TempDir()
	require.NoError(t, os.Chmod(outside, 0o700))
	line := `{"received_at":"2026-10-29T12:00:00Z","event":{"hook_event_name":"Stop","session_id":"s1"}}` + "\n"
	require.NoError(t, os.WriteFile(filepath.Join(outside, HooksFile), []byte(line), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(outside, AnnotationsFile), nil, 0o600))
	require.NoError(t, os.Symlink(outside, Dir(data)))

	hooks, _, err := ReadHooks(Dir(data))
	require.ErrorIs(t, err, ErrUnsafeFile)
	require.Empty(t, hooks, "nothing read outside labels/")
	_, _, err = ReadAnnotations(Dir(data))
	require.ErrorIs(t, err, ErrUnsafeFile)

	open := Dir(t.TempDir())
	require.NoError(t, os.MkdirAll(open, 0o700))
	require.NoError(t, os.Chmod(open, 0o755))
	_, _, err = ReadHooks(open)
	require.ErrorIs(t, err, ErrUnsafeFile)

	// A missing labels/ is read as empty and not created.
	missing := Dir(t.TempDir())
	hooks, bad, err := ReadHooks(missing)
	require.NoError(t, err)
	require.Empty(t, hooks)
	require.Zero(t, bad)
	_, err = os.Lstat(missing)
	require.ErrorIs(t, err, os.ErrNotExist)
}
