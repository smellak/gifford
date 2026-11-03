package tmuxattach

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/tmuxlock/writerlog"
)

const testPaneWriterAttempt = "0123456789abcdef0123456789abcdef"

type paneWriterFileProvider struct {
	path  string
	calls int
	file  *os.File
	err   error
}

func (provider *paneWriterFileProvider) DuplicateLockFile(Target) (*os.File, error) {
	provider.calls++
	if provider.err != nil {
		return nil, provider.err
	}
	file, err := os.Open(provider.path)
	if err != nil {
		return nil, err
	}
	provider.file = file
	return file, nil
}

func TestPaneWriterLogDisabledPreservesNoIO(t *testing.T) {
	t.Parallel()
	client, err := writerlog.New("")
	require.NoError(t, err)
	provider := &paneWriterFileProvider{err: errors.New("must not open")}
	log, err := newPaneWriterLog(provider, client)
	require.NoError(t, err)

	require.NoError(t, log.Open(t.Context(), testTarget, testPaneWriterAttempt))
	interval, err := log.interval(testTarget)
	require.NoError(t, err)
	require.Nil(t, interval)
	require.NoError(t, log.Release(testTarget))
	require.Zero(t, provider.calls)
}

func TestPaneWriterLogConfiguredOpenFailsClosed(t *testing.T) {
	t.Parallel()
	lockPath := filepath.Join(t.TempDir(), "pane.lock")
	require.NoError(t, os.WriteFile(lockPath, nil, 0o600))
	client, err := writerlog.New(filepath.Join(t.TempDir(), "missing.sock"))
	require.NoError(t, err)
	provider := &paneWriterFileProvider{path: lockPath}
	log, err := newPaneWriterLog(provider, client)
	require.NoError(t, err)

	err = log.Open(t.Context(), testTarget, testPaneWriterAttempt)
	require.ErrorIs(t, err, ErrPaneWriterLogRequired)
	require.ErrorIs(t, err, writerlog.ErrUnavailable)
	require.Equal(t, 1, provider.calls)
	require.NotNil(t, provider.file)
	_, statErr := provider.file.Stat()
	require.ErrorIs(t, statErr, os.ErrClosed)
	_, err = log.interval(testTarget)
	require.ErrorIs(t, err, errPaneWriterLogNotOpen)
}

func TestPaneWriterLogRequiresProviderOnlyWhenEnabled(t *testing.T) {
	t.Parallel()
	disabled, err := writerlog.New("")
	require.NoError(t, err)
	_, err = newPaneWriterLog(nil, disabled)
	require.NoError(t, err)

	enabled, err := writerlog.New("/run/example/writer-log.sock")
	require.NoError(t, err)
	_, err = newPaneWriterLog(nil, enabled)
	require.ErrorIs(t, err, ErrInvalidOptions)
}
