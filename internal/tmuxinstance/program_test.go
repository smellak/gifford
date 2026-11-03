package tmuxinstance

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProgramOf(t *testing.T) {
	t.Parallel()
	const (
		secret = "s3cr3t"
		agent  = "agent"
		bun    = "bun"
	)
	tests := []struct {
		name         string
		cmdline      string
		complete     bool
		argv0, argv1 string
	}{
		{name: "a native CLI", cmdline: "/usr/local/bin/codex\x00--model\x00" + secret + "\x00", complete: true, argv0: codexComm},
		{
			name: "a node script", cmdline: "node\x00/usr/lib/node_modules/agent/cli.js\x00--key=" + secret + "\x00",
			complete: true, argv0: nodeComm, argv1: cliJS,
		},
		{name: "a python script", cmdline: "python3.12\x00/opt/agent/bin/agent\x00", complete: true, argv0: "python3.12", argv1: agent},
		{name: "bun", cmdline: "/home/dev/.bun/bin/bun\x00run\x00" + secret + "\x00", complete: true, argv0: bun, argv1: "run"},
		{
			name: "an option after the interpreter", cmdline: "node\x00--api-key=" + secret + "\x00cli.js\x00",
			complete: true, argv0: nodeComm,
		},
		{name: "python's -m", cmdline: "python3\x00-m\x00agent\x00", complete: true, argv0: "python3"},
		{name: "a rewritten title", cmdline: "claude\x00\x00\x00\x00", complete: true, argv0: claudeComm},
		{name: "an interpreter alone", cmdline: "node\x00", complete: true, argv0: nodeComm},
		{name: "no command line", cmdline: "", complete: true},
		{name: "a script the bound cut", cmdline: "node\x00/usr/lib/node_modules/agent/cl", argv0: nodeComm},
		{name: "a program the bound cut", cmdline: "/usr/local/bin/cod"},
		{name: "nodejs is no interpreter here", cmdline: "nodejs\x00cli.js\x00", complete: true, argv0: "nodejs"},
	}
	for _, tt := range tests {
		argv0, argv1 := programOf([]byte(tt.cmdline), tt.complete)
		require.Equal(t, tt.argv0, argv0, tt.name)
		require.Equal(t, tt.argv1, argv1, tt.name)
		require.NotContains(t, argv0+argv1, secret, tt.name)
	}
}

func TestValidProgram(t *testing.T) {
	t.Parallel()
	for _, ok := range [][2]string{{"codex", ""}, {claudeComm, ""}, {nodeComm, cliJS}, {"bun", "run"}, {"python3.12", "agent"}, {"python", "agent"}} {
		require.True(t, validProgram(ok[0], ok[1]), ok)
	}
	for _, bad := range [][2]string{
		{"", ""},
		{codexComm, cliJS},
		{nodeComm, ""},
		{nodeComm, "--inspect"},
		{nodeComm, "-"},
		{"/usr/bin/codex", ""},
		{nodeComm, "lib/cli.js"},
		{".", ""},
		{"..", ""},
		{"my cli", ""},
		{strings.Repeat("a", 129), ""},
	} {
		require.False(t, validProgram(bad[0], bad[1]), bad)
	}
}
