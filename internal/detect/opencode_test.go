package detect

import (
	_ "embed"
	"testing"

	"github.com/stretchr/testify/require"
)

// Real bench frames (120x40, sanitized) from the bank's OpenCode run.
// What the tests read is exactly what capture-pane -p -e produced,
// neutralized of users, paths and hosts.

//go:embed testdata/opencode-idle-empty.ansi
var opencodeIdleEmpty []byte

//go:embed testdata/opencode-idle-pending-unsent.ansi
var opencodePending []byte

//go:embed testdata/opencode-working.ansi
var opencodeWorking []byte

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

func TestOpenCodeIdleEmpty(t *testing.T) {
	st := Classify(HarnessOpenCode, DefaultTable(), opencodeIdleEmpty, 120, 40, 100, opencodeTree())
	require.True(t, st.Certain, "detail %s", st.Detail)
	require.False(t, st.Busy, "detail %s", st.Detail)
	require.False(t, st.BoxBusy, "detail %s", st.Detail)
	require.True(t, st.IsCLI, "detail %s", st.Detail)
}

func TestOpenCodePendingUnsent(t *testing.T) {
	st := Classify(HarnessOpenCode, DefaultTable(), opencodePending, 120, 40, 100, opencodeTree())
	require.True(t, st.Certain, "detail %s", st.Detail)
	require.False(t, st.Busy, "detail %s", st.Detail)
	require.True(t, st.BoxBusy, "detail %s", st.Detail)
	require.True(t, st.IsCLI, "detail %s", st.Detail)
}

func TestOpenCodeNoticeVerifiesExact(t *testing.T) {
	certain, exact := VerifyNotice(HarnessOpenCode, DefaultTable(),
		opencodePending, 120, 40, "[GIFFORD] 1 pending message")
	require.True(t, certain)
	require.True(t, exact)
}

func TestOpenCodeWorking(t *testing.T) {
	st := Classify(HarnessOpenCode, DefaultTable(), opencodeWorking, 120, 40, 100, opencodeTree())
	require.True(t, st.Certain, "detail %s", st.Detail)
	require.True(t, st.Busy, "detail %s", st.Detail)
	require.False(t, st.BoxBusy, "detail %s", st.Detail)
}
