package detect

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Bench over the 12 sanitized OpenCode 1.18.29 captures from the bank
// (testdata/opencode-canon-*.ansi, 120x40, users/paths/hosts neutralized).
// Truth comes with the captures; the acceptance criteria: idle
// Certain and free, typed box occupied, working Busy, dialogs never
// writable.
func TestOpenCodeBenchCaptures(t *testing.T) {
	cases := []struct {
		file          string
		certain       bool
		busy, boxBusy bool
	}{
		{"opencode-canon-idle-01.ansi", true, false, false},
		{"opencode-canon-idle-02.ansi", true, false, false},
		{"opencode-canon-idle-03.ansi", true, false, false},
		{"opencode-canon-text-01.ansi", true, false, true},
		{"opencode-canon-text-02.ansi", true, false, true},
		{"opencode-canon-text-03.ansi", true, false, true},
		{"opencode-canon-working-01.ansi", true, true, false},
		{"opencode-canon-working-02.ansi", true, true, false},
		{"opencode-canon-working-03.ansi", true, true, false},
		{"opencode-canon-dialog-01.ansi", false, false, false},
		{"opencode-canon-dialog-02.ansi", false, false, false},
		{"opencode-canon-dialog-03.ansi", false, false, false},
	}
	for _, tc := range cases {
		raw, err := os.ReadFile("testdata/" + tc.file)
		require.NoError(t, err)
		st := Classify(HarnessOpenCode, DefaultTable(), raw, 120, 40, 100, opencodeTree())
		require.Equal(t, tc.certain, st.Certain, "%s certain (detail %s)", tc.file, st.Detail)
		require.Equal(t, tc.busy, st.Busy, "%s busy", tc.file)
		require.Equal(t, tc.boxBusy, st.BoxBusy, "%s box (detail %s)", tc.file, st.Detail)
		require.True(t, st.IsCLI, "%s runs the CLI", tc.file)
		// Only idle screens may allow a write: certain, free box,
		// and not working.
		writable := st.Certain && !st.BoxBusy && !st.Busy
		require.Equal(t, strings.HasPrefix(tc.file, "opencode-canon-idle-"), writable,
			"%s write gate (detail %s)", tc.file, st.Detail)
	}
}

func TestOpenCodeGrayHintRemainsMeasuredTruecolor(t *testing.T) {
	t.Parallel()
	style := DefaultTable()[HarnessOpenCode].GrayHint
	require.Equal(t, "\x1b[38;2;128;128;128m", style.Sequence)
	require.False(t, style.IndexedCells, "the VT drops this truecolor evidence; raw bytes remain authoritative")
}

// TestOpenCodeLiveFooterForms models the public shape measured on a live
// 120x40 OpenCode pane: an empty three-row box, the "Build auto" model row,
// and the ctrl+p footer. Typed input must remain occupied; working must remain
// busy even though the composer itself is empty.
func TestOpenCodeLiveFooterForms(t *testing.T) {
	cases := []struct {
		name          string
		box           string
		working       bool
		wantBusy      bool
		wantBoxBusy   bool
		wantWriteGate bool
	}{
		{name: "idle empty", wantWriteGate: true},
		{name: "typed", box: "public synthetic draft", wantBoxBusy: true},
		{name: "working", working: true, wantBusy: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state := Classify(
				HarnessOpenCode,
				DefaultTable(),
				openCodeLiveFooterCapture(tc.box, tc.working),
				120,
				40,
				100,
				opencodeTree(),
			)
			require.True(t, state.Certain, "detail %s", state.Detail)
			require.Equal(t, tc.wantBusy, state.Busy)
			require.Equal(t, tc.wantBoxBusy, state.BoxBusy, "detail %s", state.Detail)
			require.Equal(t, tc.wantWriteGate, state.Certain && !state.Busy && !state.BoxBusy)
		})
	}
}

// TestOpenCodeLiveFooterChromeMutants proves each new chrome marker is doing
// work. Removing either one must close the write gate on the idle frame.
func TestOpenCodeLiveFooterChromeMutants(t *testing.T) {
	for _, removed := range []string{"Build auto ·", "ctrl+p commands"} {
		t.Run(removed, func(t *testing.T) {
			table := DefaultTable()
			profile := table[HarnessOpenCode]
			chrome := make([]string, 0, len(profile.Chrome)-1)
			for _, marker := range profile.Chrome {
				if marker != removed {
					chrome = append(chrome, marker)
				}
			}
			require.Len(t, chrome, len(profile.Chrome)-1, "mutant marker must exist in the profile")
			profile.Chrome = chrome
			table[HarnessOpenCode] = profile

			state := Classify(
				HarnessOpenCode,
				table,
				openCodeLiveFooterCapture("", false),
				120,
				40,
				100,
				opencodeTree(),
			)
			require.False(t, state.Certain && !state.Busy && !state.BoxBusy,
				"removing %q must close the write gate", removed)
		})
	}
}

func openCodeLiveFooterCapture(box string, working bool) []byte {
	const boxLead = "  ┃"

	lines := make([]string, 40)
	lines[30] = "  synthetic transcript"
	if working {
		lines[31] = "  working  esc interrupt"
	}
	lines[33] = boxLead
	lines[34] = boxLead
	if box != "" {
		lines[34] += "  " + box
	}
	lines[35] = boxLead
	lines[36] = "  ┃  Build auto · model provider"
	lines[37] = "  ╹" + strings.Repeat("▀", 116)
	lines[38] = "  example  12.4K (1%) · $0.01  ctrl+p commands"
	return []byte(strings.Join(lines, "\n"))
}

// opencodeTree returns a pane tree with the OpenCode CLI in the
// foreground: its group owns the pane tty. Trees do not come in the
// frames, so the harness half is synthetic while the screen half is
// measured.
func opencodeTree() []Process {
	return []Process{
		{PID: 100, PPID: 50, Pgrp: 100, State: 'S', TPGID: 200, Argv: []string{"/bin/bash"}},
		{PID: 200, PPID: 100, Pgrp: 200, State: 'S', TPGID: 200, Argv: []string{"opencode"}},
	}
}
