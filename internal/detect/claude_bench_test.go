package detect

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Bench over the 4 sanitized Claude bench frames from the bank
// (testdata/claude-*.ansi, 120x40, users/paths/hosts neutralized)
// plus the 12 canonical bank frames (testdata/claude-canon-*.ansi,
// idle/working/text/dialog x3, same geometry and sanitizer).
// Truth comes with the captures; the acceptance criteria: idle
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
		{"claude-canon-idle-01.ansi", true, false, false},
		{"claude-canon-idle-02.ansi", true, false, false},
		{"claude-canon-idle-03.ansi", true, false, false},
		{"claude-canon-working-01.ansi", true, true, false},
		{"claude-canon-working-02.ansi", true, true, false},
		{"claude-canon-working-03.ansi", true, true, false},
		{"claude-canon-text-01.ansi", true, false, true},
		{"claude-canon-text-02.ansi", true, false, true},
		{"claude-canon-text-03.ansi", true, false, true},
		{"claude-canon-dialog-01.ansi", false, false, false},
		{"claude-canon-dialog-02.ansi", false, false, false},
		{"claude-canon-dialog-03.ansi", false, false, false},
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
		wantWritable := tc.file == "claude-idle-empty.ansi" ||
			strings.HasPrefix(tc.file, "claude-canon-idle-")
		require.Equal(t, wantWritable, writable,
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

// The bank's Claude Code paints a dim prompt suggestion in the empty
// composer after every response; the pixels below are public-safe
// synthetics in that measured shape (faint suggestion between rules,
// idle status line), never the bank capture. The suggestion must read
// as a free box, while the same sentence typed bright stays occupied.
func TestClaudeDimSuggestionGhost(t *testing.T) {
	const suggestion = "Summarize the changes in plain language"
	ghost := claudeGhostFrame("\x1b[2m" + suggestion + "\x1b[0m")
	typed := claudeGhostFrame(suggestion)
	free := Classify(HarnessClaude, DefaultTable(), ghost, 120, 40, 100, claudeTree())
	require.True(t, free.Certain, "ghost certain (detail %s)", free.Detail)
	require.False(t, free.BoxBusy, "ghost free (detail %s)", free.Detail)
	require.False(t, free.Busy, "ghost idle")
	require.True(t, free.IsCLI, "ghost runs the CLI")
	occupied := Classify(HarnessClaude, DefaultTable(), typed, 120, 40, 100, claudeTree())
	require.True(t, occupied.Certain, "typed certain (detail %s)", occupied.Detail)
	require.True(t, occupied.BoxBusy, "typed occupied (detail %s)", occupied.Detail)
}

// claudeGhostFrame builds one synthetic idle screen holding composer
// text between rules under the idle status line.
func claudeGhostFrame(composer string) []byte {
	lines := make([]string, 40)
	lines[1] = "  Claude Code"
	lines[3] = strings.Repeat("\u2500", 120)
	lines[4] = "❯ " + composer
	lines[5] = strings.Repeat("\u2500", 120)
	lines[6] = "  ⏸ manual mode on · ? for shortcuts"
	return []byte(strings.Join(lines, "\n"))
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
