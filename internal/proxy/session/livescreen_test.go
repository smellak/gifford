package session

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/profiles"
	"github.com/smellak/gifford/internal/vt"
)

// museLiveFrame is a Muse Code frame whose empty input box sits on the
// cursor's row, with extra rows above it.
func museLiveFrame(extra ...string) vt.Snapshot {
	lines := append(append([]string{"agent output"}, extra...), "────────", "❯ ", "────────", "Build · modelo")
	return vt.Snapshot{
		Cols: 80, Rows: len(lines), Lines: lines,
		CursorRow: len(lines) - 3, CursorCol: 2, CursorVisible: true,
	}
}

func liveProfile(t *testing.T, name, version string) profiles.Profile {
	t.Helper()
	bank, err := profiles.Load()
	require.NoError(t, err)
	p, err := bank.Profile(name, version)
	require.NoError(t, err)
	return p
}

// LiveScreen masks the box of a frame that passes, withholds a frame past
// the report's bounds, and withholds every frame of an unmeasured profile.
func TestLiveScreenReasons(t *testing.T) {
	t.Parallel()

	muse := liveProfile(t, "muse-code", "1.4.13-R6996.1")
	rows, reason := LiveScreen(muse, museLiveFrame())
	require.Empty(t, reason)
	require.Equal(t, []string{"agent output", "────────", maskedInputRow, "────────", "Build · modelo"}, rows)

	rows, reason = LiveScreen(muse, museLiveFrame(strings.Repeat("x", maxObservedBytes)))
	require.Equal(t, LiveScreenOutOfBounds, reason)
	require.Nil(t, rows)

	rows, reason = LiveScreen(muse, museLiveFrame("\xff"))
	require.Equal(t, LiveScreenOutOfBounds, reason)
	require.Nil(t, rows)

	generic := liveProfile(t, "generic", "")
	require.False(t, generic.Measured)
	rows, reason = LiveScreen(generic, museLiveFrame())
	require.Equal(t, LiveScreenUnmeasured, reason)
	require.Nil(t, rows)
}
