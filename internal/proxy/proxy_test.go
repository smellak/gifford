package proxy

import (
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestExit checks the shell status and the description of each outcome.
func TestExit(t *testing.T) {
	t.Parallel()

	require.Equal(t, 0, Exit{Code: 0}.Status())
	require.Equal(t, 7, Exit{Code: 7}.Status())
	require.Equal(t, 143, Exit{Code: -1, Signal: syscall.SIGTERM}.Status())
	require.Equal(t, "harness exited with status 0", Exit{Code: 0}.String())
	require.Equal(t, "harness exited with status 7", Exit{Code: 7}.String())
	require.Equal(t,
		"harness killed by signal 15 (terminated), status 143",
		Exit{Code: -1, Signal: syscall.SIGTERM}.String())
}
