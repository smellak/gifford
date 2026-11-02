package detect

import (
	_ "embed"
	"testing"

	"github.com/stretchr/testify/require"
)

// Real bench frames (120x40, sanitized) from the bank's Claude run.
// What the tests read is exactly what capture-pane -p -e produced,
// neutralized of users, paths and hosts.

//go:embed testdata/claude-idle-empty.ansi
var claudeIdleEmpty []byte

//go:embed testdata/claude-idle-pending-unsent.ansi
var claudePending []byte

//go:embed testdata/claude-working.ansi
var claudeWorking []byte

//go:embed testdata/claude-permission-dialog.ansi
var claudeDialog []byte

// claudeTree returns a pane tree with the Claude CLI in the foreground:
// its group owns the pane tty. Trees do not come in the frames, so the
// harness half is synthetic while the screen half is measured.
func claudeTree() []Process {
	return []Process{
		{PID: 100, PPID: 50, Pgrp: 100, State: 'S', TPGID: 200, Argv: []string{"/bin/bash"}},
		{PID: 200, PPID: 100, Pgrp: 200, State: 'S', TPGID: 200, Argv: []string{"claude"}},
	}
}

func TestClaudeIdleEmpty(t *testing.T) {
	st := Classify(HarnessClaude, DefaultTable(), claudeIdleEmpty, 120, 40, 100, claudeTree())
	require.True(t, st.Certain, "detail %s", st.Detail)
	require.False(t, st.Busy, "detail %s", st.Detail)
	require.False(t, st.BoxBusy, "detail %s", st.Detail)
	require.True(t, st.IsCLI, "detail %s", st.Detail)
}

func TestClaudePendingUnsent(t *testing.T) {
	st := Classify(HarnessClaude, DefaultTable(), claudePending, 120, 40, 100, claudeTree())
	require.True(t, st.Certain, "detail %s", st.Detail)
	require.False(t, st.Busy, "detail %s", st.Detail)
	require.True(t, st.BoxBusy, "detail %s", st.Detail)
	require.True(t, st.IsCLI, "detail %s", st.Detail)
}

func TestClaudeNoticeVerifiesExact(t *testing.T) {
	certain, exact := VerifyNotice(HarnessClaude, DefaultTable(),
		claudePending, 120, 40, "[GIFFORD] 1 pending message")
	require.True(t, certain)
	require.True(t, exact)
}

func TestClaudeWorking(t *testing.T) {
	st := Classify(HarnessClaude, DefaultTable(), claudeWorking, 120, 40, 100, claudeTree())
	require.True(t, st.Certain, "detail %s", st.Detail)
	require.True(t, st.Busy, "detail %s", st.Detail)
	require.False(t, st.BoxBusy, "detail %s", st.Detail)
}

func TestClaudePermissionDialogBlocks(t *testing.T) {
	// The dialog has no verifiable composer: its numbered options are
	// transcript, not the box, and the scan stops there. Uncertain means
	// no write and no Enter; the pending human approval already holds
	// the delivery above the detector.
	st := Classify(HarnessClaude, DefaultTable(), claudeDialog, 120, 40, 100, claudeTree())
	require.False(t, st.Certain, "detail %s", st.Detail)
	require.False(t, st.Busy, "detail %s", st.Detail)
	require.True(t, st.IsCLI, "detail %s", st.Detail)
	certain, exact := VerifyNotice(HarnessClaude, DefaultTable(),
		claudeDialog, 120, 40, "[GIFFORD] 1 pending message")
	require.False(t, certain)
	require.False(t, exact)
}
