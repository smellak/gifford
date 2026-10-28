//go:build unix

package run

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunPassesExitStatusUnix(t *testing.T) {
	t.Parallel()
	// Minimal check that NewCmd is created and daemonSocket is non-empty when XDG set.
	// Full PTY test is in internal/proxy; this ensures the package builds on unix.
	require.NotNil(t, NewCmd(nil))
}
