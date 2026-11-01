package run

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// maxInterpreterLine bounds what nodeLauncher reads of the command: the
// kernel reads no more of a script's interpreter line.
const maxInterpreterLine = 256

// nodeLauncher reports whether the command, found as exec finds it, is a
// script whose interpreter line runs node: #!/usr/bin/env node, or a path
// to node. It reads that line and nothing else: it neither resolves nor
// runs any other program.
func nodeLauncher(command string) bool {
	path, err := exec.LookPath(command)
	if err != nil {
		return false
	}
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer func() { _ = f.Close() }()
	head := make([]byte, maxInterpreterLine)
	n, _ := io.ReadFull(f, head)
	line, _, _ := bytes.Cut(head[:n], []byte("\n"))
	rest, ok := bytes.CutPrefix(line, []byte("#!"))
	if !ok {
		return false
	}
	fields := strings.Fields(string(rest))
	if len(fields) == 0 {
		return false
	}
	interpreter := fields[0]
	if filepath.Base(interpreter) == "env" {
		// env's own options and assignments come before the program.
		interpreter = ""
		for _, field := range fields[1:] {
			if !strings.HasPrefix(field, "-") && !strings.Contains(field, "=") {
				interpreter = field
				break
			}
		}
	}
	return filepath.Base(interpreter) == "node"
}

// nodeLauncherNotice is the one stderr line saying the command is a
// Node.js launcher, and why that can keep deliveries back.
func nodeLauncherNotice(command string) string {
	return fmt.Sprintf("gifford run: %s is a Node.js launcher script: if it starts the harness as a child process instead of replacing itself, "+
		"the process guard defers every automatic delivery, because it binds exactly one harness process; "+
		"see \"Node.js launchers\" in README.md", command)
}
