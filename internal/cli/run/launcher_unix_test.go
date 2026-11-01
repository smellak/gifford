//go:build unix

package run

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/creack/pty"
	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/proxy"
)

// TestNodeLauncher reads the interpreter line of the command: a script run
// by node, through env or by path, is a launcher; a script of another
// interpreter, a binary, an empty file or no file at all is not.
func TestNodeLauncher(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	for name, c := range map[string]struct {
		body string
		want bool
	}{
		"env node":              {"#!/usr/bin/env node\nconsole.log(1)\n", true},
		"env with a flag":       {"#!/usr/bin/env -S node --no-warnings\n", true},
		"env with a variable":   {"#!/usr/bin/env NODE_OPTIONS=--trace-warnings node\n", true},
		"a path to node":        {"#!/usr/local/bin/node\n", true},
		"a space after #!":      {"#! /opt/node/bin/node\n", true},
		"no newline":            {"#!/usr/bin/env node", true},
		"sh starting node":      {"#!/bin/sh\nexec node launcher.js \"$@\"\n", false},
		"another interpreter":   {"#!/usr/bin/env python3\n", false},
		"a name starting node":  {"#!/usr/bin/env nodemon\n", false},
		"env and nothing else":  {"#!/usr/bin/env\n", false},
		"a binary":              {"\x7fELF\x02\x01\x01\x00", false},
		"an empty file":         {"", false},
		"node later in a line":  {"// #!/usr/bin/env node\n", false},
		"node after the bounds": {"#!/bin/" + string(make([]byte, maxInterpreterLine)) + "node\n", false},
	} {
		path := filepath.Join(dir, fmt.Sprintf("cmd-%x", name))
		writeTestExecutable(t, path, []byte(c.body))
		require.Equal(t, c.want, nodeLauncher(path), name)
	}
	require.False(t, nodeLauncher(filepath.Join(dir, "missing")), "no such command")
	require.False(t, nodeLauncher(dir), "a directory")
}

// TestRunWarnsForANodeLauncher runs a node script, by path and by name,
// through NewCmd: stderr carries the launcher notice once, then exactly what
// it carries for any command. A script of another interpreter and a binary
// get no notice. A stand-in node on PATH runs the script, so no Node.js is
// needed. It is serial: it sets process environment.
func TestRunWarnsForANodeLauncher(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	t.Setenv("GORACE", "atexit_sleep_ms=0")
	bin := t.TempDir()
	writeTestExecutable(t, filepath.Join(bin, "node"), []byte("#!/bin/sh\nexit 0\n"))
	writeTestExecutable(t, filepath.Join(bin, "node-launcher"), []byte("#!/usr/bin/env node\n"))
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	dir := t.TempDir()
	launcher := filepath.Join(dir, "launcher")
	writeTestExecutable(t, launcher, []byte("#!/usr/bin/env node\nrequire('child_process')\n"))
	script := filepath.Join(dir, "script")
	writeTestExecutable(t, script, []byte("#!/bin/sh\nexit 0\n"))

	outer, slave, err := pty.Open()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = outer.Close()
		_ = slave.Close()
	})
	require.NoError(t, pty.Setsize(outer, &pty.Winsize{Rows: 24, Cols: 80}))
	run := func(command ...string) string {
		t.Helper()
		cmd := NewCmd(nil)
		cmd.SilenceErrors = true
		cmd.SilenceUsage = true
		cmd.SetIn(slave)
		cmd.SetOut(slave)
		var stderr syncBuffer
		cmd.SetErr(&stderr)
		cmd.SetArgs(append([]string{"--"}, command...))
		require.NoError(t, cmd.Execute())
		return stderr.String()
	}
	plain := proxy.DaemonNotice + "\n"
	require.Equal(t, nodeLauncherNotice(launcher)+"\n"+plain, run(launcher))
	require.Equal(t, nodeLauncherNotice("node-launcher")+"\n"+plain, run("node-launcher"), "found on PATH as exec finds it")
	require.Equal(t, plain, run(script), "another interpreter")
	require.Equal(t, plain, run(cliHelperArgv("0")...), "a binary")
}
