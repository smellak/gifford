package writerlog

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

const testAttempt = "0123456789abcdef0123456789abcdef"

func TestDisabledClientKeepsLocalOrderingWithoutIO(t *testing.T) {
	t.Parallel()
	client, err := New("")
	require.NoError(t, err)
	require.False(t, client.Enabled())
	lockFile := newTestLockFile(t)
	interval, err := client.Open(t.Context(), lockFile, testAttempt)
	require.NoError(t, err)
	require.NoError(t, interval.KeyBegin("1"))
	require.NoError(t, interval.KeyEnd("1", OutcomeOK))
	require.NoError(t, interval.Release())
	require.ErrorIs(t, interval.Release(), ErrState)
}

func TestClientRejectsInvalidConfigurationAndArguments(t *testing.T) {
	t.Parallel()
	for _, socketPath := range []string{"relative.sock", "//double/root", "/tmp/../socket", "/tmp/bad\nname"} {
		_, err := New(socketPath)
		require.ErrorIs(t, err, ErrInvalid, socketPath)
	}
	client, err := New("")
	require.NoError(t, err)
	lockFile := newTestLockFile(t)
	_, err = client.Open(context.Background(), lockFile, "ABCDEF0123456789ABCDEF0123456789")
	require.ErrorIs(t, err, ErrInvalid)
	_, err = client.Open(context.Background(), nil, testAttempt)
	require.ErrorIs(t, err, ErrInvalid)
}

func TestDecodeACKRequiresExactCanonicalFields(t *testing.T) {
	t.Parallel()
	err := decodeACK([]byte(`{"ok":true,"seq":7,"v":1}`))
	require.NoError(t, err)

	tests := []string{
		`{"extra":null,"ok":true,"seq":1,"v":1}`,
		`{"ok":true,"seq":1,"v":1} `,
		`{"ok":true,"seq":1.0,"v":1}`,
		`{"ok":true,"ok":true,"seq":1,"v":1}`,
		`{"ok":true,"seq":0,"v":1}`,
		`{"ok":true,"seq":1,"v":true}`,
	}
	for _, raw := range tests {
		err := decodeACK([]byte(raw))
		require.ErrorIs(t, err, ErrProtocol, raw)
	}
}

func TestDecodeACKAcceptsOnlyClosedNegativeACK(t *testing.T) {
	t.Parallel()
	err := decodeACK([]byte(`{"ok":false,"reason":"lock-mismatch","v":1}`))
	require.ErrorIs(t, err, ErrRejected)
	require.NotErrorIs(t, err, ErrUncertain)

	err = decodeACK([]byte(`{"ok":false,"reason":"bad value","v":1}`))
	require.ErrorIs(t, err, ErrProtocol)
}

func newTestLockFile(t *testing.T) *os.File {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "panel-*.lock")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, file.Close()) })
	return file
}
