package detect

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Bench over the 12 sanitized OpenCode 1.18.29 captures from the bank
// (testdata/opencode-canon-*.ansi, 120x40, users/paths/hosts neutralized).
// Truth comes with the captures; the criteria are the mapper's: idle
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
