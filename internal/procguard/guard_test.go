package procguard

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// restTree is the synthetic census shape: node launcher, musl binary,
// capyverse shim, with or without codex-code-mode-host.
func restTree(host bool) []Entry {
	tree := []Entry{
		{PID: 100, StartTime: 11, Comm: "node", Cmdline: "node /usr/bin/codex"},
		{PID: 200, StartTime: 12, Comm: "codex", Cmdline: "codex --strict-config"},
		{PID: 300, StartTime: 13, Comm: "capyverse-shim", Cmdline: "shim"},
	}
	if host {
		tree = append(tree, Entry{PID: 400, StartTime: 14, Comm: "codex-code-mode-host"})
	}
	return tree
}

func TestGuardRestAndWork(t *testing.T) {
	t.Parallel()
	for _, host := range []bool{false, true} {
		var g Guard
		g.Rebaseline("pane", restTree(host))
		require.Equal(t, Free, g.Check("pane", restTree(host)))

		working := append(append([]Entry{}, restTree(host)...),
			Entry{PID: 500, StartTime: 15, Comm: "sleep", Cmdline: "/bin/sleep 30"})
		require.Equal(t, Busy, g.Check("pane", working))
	}
}

func TestGuardAuxiliaryAbsorbedAtNextRest(t *testing.T) {
	t.Parallel()
	var g Guard
	g.Rebaseline("pane", restTree(false))

	aux := append(append([]Entry{}, restTree(false)...),
		Entry{PID: 600, StartTime: 16, Comm: "helper", Cmdline: "helper --daemon"})
	require.Equal(t, Busy, g.Check("pane", aux))

	g.Rebaseline("pane", aux)
	require.Equal(t, Free, g.Check("pane", aux))
}

func TestGuardPidReuseIsNew(t *testing.T) {
	t.Parallel()
	var g Guard
	g.Rebaseline("pane", restTree(false))

	reused := append([]Entry{}, restTree(false)...)
	reused[1] = Entry{PID: 200, StartTime: 99, Comm: "codex", Cmdline: "codex --strict-config"}
	require.Equal(t, Busy, g.Check("pane", reused))
}

func TestGuardGoneMemberIsUncertain(t *testing.T) {
	t.Parallel()
	var g Guard
	g.Rebaseline("pane", restTree(true))
	require.Equal(t, Uncertain, g.Check("pane", restTree(false)))
}

func TestGuardUnknownPanelIsUncertain(t *testing.T) {
	t.Parallel()
	var g Guard
	require.Equal(t, Uncertain, g.Check("pane", restTree(false)))
}

func TestParseStartTimeSkipsParensInComm(t *testing.T) {
	t.Parallel()
	got, err := parseStartTime("123 (weird (name)) S 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 4242 20")
	require.NoError(t, err)
	require.Equal(t, uint64(4242), got)
}
