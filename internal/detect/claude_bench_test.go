package detect

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Bench over the 4 sanitized Claude bench frames from the bank
// (testdata/claude-*.ansi, 120x40, users/paths/hosts neutralized).
// Truth comes with the captures; the criteria are the mapper's: idle
// Certain and free, pending box occupied with the exact notice,
// working Busy, permission dialog never writable.
func TestClaudeBenchCaptures(t *testing.T) {
	cases := []struct {
		file          string
		certain       bool
		busy, boxBusy bool
	}{
		{"claude-idle-empty.ansi", true, false, false},
		{"claude-idle-pending-unsent.ansi", true, false, true},
		{"claude-working.ansi", true, true, false},
		{"claude-permission-dialog.ansi", false, false, false},
	}
	for _, tc := range cases {
		raw, err := os.ReadFile("testdata/" + tc.file)
		require.NoError(t, err)
		st := Classify(HarnessClaude, DefaultTable(), raw, 120, 40, 100, claudeTree())
		require.Equal(t, tc.certain, st.Certain, "%s certain (detail %s)", tc.file, st.Detail)
		require.Equal(t, tc.busy, st.Busy, "%s busy", tc.file)
		require.Equal(t, tc.boxBusy, st.BoxBusy, "%s box (detail %s)", tc.file, st.Detail)
		require.True(t, st.IsCLI, "%s runs the CLI", tc.file)
		// Only idle screens may allow a write: certain, free box,
		// and not working.
		writable := st.Certain && !st.BoxBusy && !st.Busy
		require.Equal(t, strings.HasPrefix(tc.file, "claude-idle-empty"), writable,
			"%s write gate (detail %s)", tc.file, st.Detail)
	}
}

// The pending frame must hold exactly the allowlisted notice: the
// writer's Enter gate depends on it.
func TestClaudePendingNoticeVerifiesExact(t *testing.T) {
	raw, err := os.ReadFile("testdata/claude-idle-pending-unsent.ansi")
	require.NoError(t, err)
	certain, exact := VerifyNotice(HarnessClaude, DefaultTable(),
		raw, 120, 40, "[GIFFORD] 1 pending message")
	require.True(t, certain)
	require.True(t, exact)
}

// claudeTree returns a pane tree with the Claude CLI in the foreground:
// its group owns the pane tty. Trees do not come in the frames, so the
// harness half is synthetic while the screen half is measured.
func claudeTree() []Process {
	return []Process{
		{PID: 100, PPID: 50, Pgrp: 100, State: 'S', TPGID: 200, Argv: []string{"/bin/bash"}},
		{PID: 200, PPID: 100, Pgrp: 200, State: 'S', TPGID: 200, Argv: []string{"claude"}},
	}
}
