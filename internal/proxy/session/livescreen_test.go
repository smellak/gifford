package session

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/profiles"
	"github.com/smellak/gifford/internal/proxy/draft"
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

type openCodeStatusFrame struct {
	cols       int
	statusGap  int
	statusText string
	nextStatus bool
}

// openCodeStatusScreen draws an empty input box near the bottom of a terminal.
// Status text is positioned separately so vt.Snapshot materializes the gap
// between the box and a right-aligned block instead of trimming it away.
func openCodeStatusScreen(t *testing.T, frame openCodeStatusFrame) vt.Snapshot {
	t.Helper()

	emulator, err := vt.New(frame.cols, 50)
	require.NoError(t, err)
	defer func() { require.NoError(t, emulator.Close()) }()

	var output bytes.Buffer
	output.WriteString("\x1b[2J")
	fmt.Fprint(&output, "\x1b[44;1H  ┃")
	fmt.Fprint(&output, "\x1b[45;1H  ┃  ")
	fmt.Fprint(&output, "\x1b[46;1H  ┃")
	if frame.statusText != "" {
		fmt.Fprintf(&output, "\x1b[46;%dH%s", frame.statusGap+4, frame.statusText)
	}
	fmt.Fprint(&output, "\x1b[47;1H  ┃  synthetic · test-model")
	if frame.nextStatus {
		fmt.Fprint(&output, "\x1b[47;161Hready")
	}
	fmt.Fprint(&output, "\x1b[48;1H  ╹▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀")
	fmt.Fprint(&output, "\x1b[45;6H\x1b[?25h")

	_, err = emulator.Write(output.Bytes())
	require.NoError(t, err)
	snapshot := emulator.Snapshot()
	require.Equal(t, 44, snapshot.CursorRow)
	require.Equal(t, 5, snapshot.CursorCol)
	require.True(t, snapshot.CursorVisible)
	return snapshot
}

func TestOpenCodeRightAlignedStatusBound(t *testing.T) {
	t.Parallel()

	profile := liveProfile(t, "opencode", "1.18.29")
	for _, tc := range []struct {
		name  string
		frame openCodeStatusFrame
		valid bool
	}{
		{
			name:  "bare row below the input",
			frame: openCodeStatusFrame{cols: 80},
			valid: true,
		},
		{
			name: "right-aligned status after 157 spaces",
			frame: openCodeStatusFrame{
				cols: 200, statusGap: 157, statusText: "synthetic-workspace:", nextStatus: true,
			},
			valid: true,
		},
		{
			name: "second draft line",
			frame: openCodeStatusFrame{
				cols: 80, statusGap: 2, statusText: "texto",
			},
		},
		{
			name: "status after only 39 spaces",
			frame: openCodeStatusFrame{
				cols: 80, statusGap: 39, statusText: "synthetic-status",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			snapshot := openCodeStatusScreen(t, tc.frame)
			if tc.frame.statusText != "" {
				require.Equal(t,
					"  ┃"+strings.Repeat(" ", tc.frame.statusGap)+tc.frame.statusText,
					snapshot.Lines[45],
				)
			}

			text, ok := draft.CursorInput(profile, snapshot)
			require.Empty(t, text)
			require.Equal(t, tc.valid, ok)

			rows, reason := LiveScreen(profile, snapshot)
			if tc.valid {
				require.Empty(t, reason)
				require.NotNil(t, rows)
				return
			}
			require.Equal(t, LiveScreenNoInputEvidence, reason)
			require.Nil(t, rows)
		})
	}
}
