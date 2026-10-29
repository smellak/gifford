//go:build tmux

package capturetest

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// testServer is one private tmux server with its socket.
type testServer struct {
	sock string
	t    *testing.T
}

// newServer starts a private server: its own socket under a temp
// dir, no config, TMUX cleared, killed in cleanup. Never the default
// server.
func newServer(t *testing.T) *testServer {
	t.Helper()
	dir := t.TempDir()
	sock := filepath.Join(dir, "tmux.sock")
	s := &testServer{sock: sock, t: t}
	s.run("new-session", "-d", "-s", "test", "-x", "200", "-y", "50")
	t.Cleanup(func() { s.run("kill-server") })
	return s
}

// run executes one tmux call with TMUX cleared and fails the test on
// error, returning trimmed stdout.
func (s *testServer) run(args ...string) string {
	s.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "tmux", append([]string{"-S", s.sock}, args...)...)
	cmd.Env = withoutTMUX()
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		s.t.Fatalf("tmux %s: %v: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return strings.TrimSpace(stdout.String())
}

// try runs one tmux call and returns its output and error without
// failing, for polling loops.
func (s *testServer) try(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "tmux", append([]string{"-S", s.sock}, args...)...)
	cmd.Env = withoutTMUX()
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return strings.TrimSpace(stdout.String()), err
}

// withoutTMUX clears the session environment so nested calls never
// attach to an outer server.
func withoutTMUX() []string {
	out := os.Environ()[:0:0]
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "TMUX=") {
			out = append(out, kv)
		}
	}
	return out
}

// newPane starts the helper as a new window's command and returns its
// pane id. No send-keys: the program is the pane command.
func (s *testServer) newPane(helperArgs ...string) string {
	s.t.Helper()
	exe, err := os.Executable()
	if err != nil {
		s.t.Fatal(err)
	}
	cmd := exe + " helper " + strings.Join(helperArgs, " ")
	return s.run("new-window", "-d", "-P", "-F", "#{pane_id}", "-t", "test:", cmd)
}

// panePipe reports #{pane_pipe} for one pane: 1 while a pipe is open.
func (s *testServer) panePipe(pane string) string {
	out, err := s.try("display-message", "-p", "-t", pane, "#{pane_pipe}")
	if err != nil {
		return "?"
	}
	return out
}

// waitPipe polls until the pane pipe flag matches, failing on timeout.
func (s *testServer) waitPipe(pane, want string, timeout time.Duration) {
	s.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if got := s.panePipe(pane); got == want {
			return
		}
		if !time.Now().Before(deadline) {
			s.t.Fatalf("pane %s pipe = %s, want %s", pane, s.panePipe(pane), want)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// self returns this test binary path for pipe commands.
func self(t *testing.T) string {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return exe
}

// counterLine formats one deterministic counter line: green index,
// reset, newline. Tests recompute exact bytes from it.
func counterLine(i int) string {
	return fmt.Sprintf("\x1b[32m%08d\x1b[0m\n", i)
}

// rawLine is the pipe image of one counter line: tmux normalizes the
// bare newline to CR LF as the bytes cross pipe-pane.
func rawLine(i int) string {
	return strings.Replace(counterLine(i), "\n", "\r\n", 1)
}

// runHelperMode runs the pane programs: deterministic ANSI output,
// then idle so the pane stays alive. Raw mode keeps bytes exact.
func runHelperMode(argv []string) {
	if len(argv) == 0 {
		fmt.Fprintln(os.Stderr, "helper: missing mode")
		os.Exit(2)
	}
	switch argv[0] {
	case "counter":
		n := mustAtoi(argv, 1)
		for i := 0; i < n; i++ {
			fmt.Fprint(os.Stdout, counterLine(i))
		}
		sleepForever()
	case "gated":
		// gated prints counter phases, each gated by a file: the
		// pipe only forwards output produced after it opens, so
		// the test attaches first and creates each gate after.
		phases := mustPhases(argv[1:])
		base := 0
		for _, phase := range phases {
			waitGate(phase.gate)
			for i := 0; i < phase.n; i++ {
				fmt.Fprint(os.Stdout, counterLine(base+i))
			}
			base += phase.n
		}
		sleepForever()
	case "junction":
		// junction prints n1 lines at once (the open snapshot
		// sees them), waits for the gate, then prints n2: the
		// snapshot's last line and the first chunk continue the
		// count with no gap and no overlap.
		if len(argv) != 4 {
			fmt.Fprintln(os.Stderr, "helper: junction needs n1, gate and n2")
			os.Exit(2)
		}
		n1 := mustAtoi(argv, 1)
		for i := 0; i < n1; i++ {
			fmt.Fprint(os.Stdout, counterLine(i))
		}
		waitGate(argv[2])
		n2 := mustAtoi(argv, 3)
		for i := 0; i < n2; i++ {
			fmt.Fprint(os.Stdout, counterLine(n1+i))
		}
		sleepForever()
	case "paced":
		// paced prints one counter line per interval, so output
		// keeps flowing while the test rearranges pipes.
		n := mustAtoi(argv, 1)
		ms := mustAtoi(argv, 2)
		for i := 0; i < n; i++ {
			fmt.Fprint(os.Stdout, counterLine(i))
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}
		sleepForever()
	case "gburst":
		// gated burst: waits for the gate, then writes total
		// pattern bytes at once, one short line per row: a
		// newline-free stream would build a single frame past
		// the frame cap and the ingest refuses it.
		if len(argv) != 3 {
			fmt.Fprintln(os.Stderr, "helper: gburst needs gate and total")
			os.Exit(2)
		}
		waitGate(argv[1])
		total := mustAtoi(argv, 2)
		line := bytes.Repeat([]byte("0123456789abcdef"), 4)
		line = append(line, '\n')
		buf := bytes.Repeat(line, 1024)
		for left := total; left > 0; {
			n := len(buf)
			if n > left {
				n = left
			}
			if _, err := os.Stdout.Write(buf[:n]); err != nil {
				os.Exit(1)
			}
			left -= n
		}
		sleepForever()
	case "stallfeed":
		// burst then slow counter: the burst overflows a blocked
		// writer while the counter keeps the pane alive after.
		if len(argv) != 4 {
			fmt.Fprintln(os.Stderr, "helper: stallfeed needs gate, total and base")
			os.Exit(2)
		}
		waitGate(argv[1])
		total := mustAtoi(argv, 2)
		base := mustAtoi(argv, 3)
		line := bytes.Repeat([]byte("0123456789abcdef"), 4)
		line = append(line, '\n')
		buf := bytes.Repeat(line, 1024)
		for left := total; left > 0; {
			n := len(buf)
			if n > left {
				n = left
			}
			if _, err := os.Stdout.Write(buf[:n]); err != nil {
				os.Exit(1)
			}
			left -= n
		}
		for i := base; ; i++ {
			fmt.Fprint(os.Stdout, counterLine(i))
			time.Sleep(50 * time.Millisecond)
		}
	case "finite":
		// waits for the gate, prints n lines and exits: with
		// remain-on-exit the pane stays dead and retained in its
		// slot.
		if len(argv) != 3 {
			fmt.Fprintln(os.Stderr, "helper: finite needs gate and count")
			os.Exit(2)
		}
		waitGate(argv[1])
		n := mustAtoi(argv, 2)
		for i := 0; i < n; i++ {
			fmt.Fprint(os.Stdout, counterLine(i))
		}
	case "exit0":
		// exits at once: with remain-on-exit the pane stays idle
		// and the pipe sees EOF.
	case "idle":
		sleepForever()
	default:
		fmt.Fprintf(os.Stderr, "helper: unknown mode %q\n", argv[0])
		os.Exit(2)
	}
	// Finite helpers must never fall through into the test runner.
	os.Exit(0)
}

// gatePhase is one gated counter phase.
type gatePhase struct {
	gate string
	n    int
}

// mustPhases parses gate/count pairs.
func mustPhases(argv []string) []gatePhase {
	if len(argv) == 0 || len(argv)%2 != 0 {
		fmt.Fprintln(os.Stderr, "helper: gated needs gate/count pairs")
		os.Exit(2)
	}
	var out []gatePhase
	for i := 0; i < len(argv); i += 2 {
		out = append(out, gatePhase{gate: argv[i], n: mustAtoi(argv, i+1)})
	}
	return out
}

// waitGate spins until the gate file exists.
func waitGate(gate string) {
	deadline := time.Now().Add(120 * time.Second)
	for {
		if _, err := os.Stat(gate); err == nil {
			return
		}
		if !time.Now().Before(deadline) {
			fmt.Fprintln(os.Stderr, "helper: gate timeout")
			os.Exit(1)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// mustAtoi reads a helper integer argument.
func mustAtoi(argv []string, i int) int {
	if i >= len(argv) {
		fmt.Fprintln(os.Stderr, "helper: missing argument")
		os.Exit(2)
	}
	n, err := strconv.Atoi(argv[i])
	if err != nil {
		fmt.Fprintf(os.Stderr, "helper: %v\n", err)
		os.Exit(2)
	}
	return n
}

// sleepForever parks the helper with the pane alive.
func sleepForever() {
	select {}
}

// waitFileSize polls until path reaches at least n bytes.
func waitFileSize(t *testing.T, path string, n int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		fi, err := os.Stat(path)
		if err == nil && fi.Size() >= n {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("%s did not reach %d bytes", path, n)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// waitFileGrows polls until path exists and is non-empty.
func waitFileGrows(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	waitFileSize(t, path, 1, timeout)
}
