//go:build unix

package proxy

import (
	"errors"
	"flag"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"golang.org/x/term"
)

// helperTimeout bounds every transcript wait: no test sleeps, every wait
// fails after 10 s.
const helperTimeout = 10 * time.Second

// TestHelperProcess is the fake harness every proxy test drives. It runs as
// the test binary again:
//
//	os.Args[0] -test.run=^TestHelperProcess$ -- <mode> [arg]
//
// Mode travels as an argument because children inherit the environment.
// With no argument it is a normal test run and skips. Every mode makes its
// PTY raw, writes its marker and exits with a nonzero status: a -race
// binary exiting 0 sleeps 1 s.
func TestHelperProcess(t *testing.T) {
	if flag.NArg() == 0 {
		t.Skip("not a helper process")
	}
	mode := flag.Arg(0)
	arg := flag.Arg(1)

	if _, err := unix.IoctlGetWinsize(int(os.Stdin.Fd()), unix.TIOCGWINSZ); err != nil {
		t.Fatalf("helper %s: own size: %v", mode, err)
	}
	// Raw until exit; the harness never restores.
	_, err := term.MakeRaw(int(os.Stdin.Fd()))
	require.NoError(t, err, "helper %s: raw", mode)

	switch mode {
	case "echo":
		helperEcho(t, arg)
	case "signals":
		helperSignals()
	case "hup":
		helperHup()
	case "stubborn":
		helperStubborn()
	case "deaf":
		helperDeaf()
	case "kill":
		helperWrite("<ready>")
		_ = unix.Kill(os.Getpid(), unix.SIGKILL)
		os.Exit(127)
	case "flood":
		helperFlood(t, arg)
	case "orphan":
		helperOrphan(t)
	case "sleep":
		grandchildReady()
		helperReadUntilError()
		os.Exit(94)
	case "jam":
		helperJam(t)
	case "hold":
		// Arm SIGTERM before the fd3 byte: startGrandchild returns
		// on that byte promising signals are set up.
		notify := make(chan os.Signal, 4)
		signal.Notify(notify, unix.SIGTERM)
		grandchildReady()
		<-notify
		signal.Stop(notify)
		os.Exit(96)
	case "spew":
		helperSpew(t)
	case "env":
		helperEnv(flag.Args()[1:])
	case "produce":
		grandchildReady()
		chunk := []byte(strings.Repeat("x", 64*1024))
		for {
			if err := writeFull(os.Stdout, chunk); err != nil {
				os.Exit(95)
			}
		}
	default:
		t.Fatalf("helper: unknown mode %q", mode)
	}
}

// helperArgv is the command line that re-runs this test binary as a helper.
func helperArgv(mode string, args ...string) []string {
	return append([]string{os.Args[0], "-test.run=^TestHelperProcess$", "--", mode}, args...)
}

// helperCmd builds the command that re-runs this test binary as a helper.
func helperCmd(t *testing.T, mode string, args ...string) *exec.Cmd {
	t.Helper()
	argv := helperArgv(mode, args...)
	return exec.CommandContext(t.Context(), argv[0], argv[1:]...)
}

// helperWrite writes one marker to the helper's PTY.
func helperWrite(s string) {
	_ = writeFull(os.Stdout, []byte(s))
}

// writeFull writes all of b, retrying EINTR.
func writeFull(f *os.File, b []byte) error {
	for len(b) > 0 {
		n, err := f.Write(b)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		b = b[n:]
	}
	return nil
}

// helperSize reports the helper's own PTY size as ROWSxCOLS.
func helperSize(t *testing.T) string {
	t.Helper()
	ws, err := unix.IoctlGetWinsize(int(os.Stdin.Fd()), unix.TIOCGWINSZ)
	require.NoError(t, err)
	return strconv.Itoa(int(ws.Row)) + "x" + strconv.Itoa(int(ws.Col))
}

// helperEcho writes <ready ROWSxCOLS>, echoes every chunk verbatim,
// reports <size ROWSxCOLS> per SIGWINCH and writes <bye N> after N bytes,
// then exits 7. The SIGWINCH writer shares a mutex with the echo loop.
func helperEcho(t *testing.T, arg string) {
	n, err := strconv.Atoi(arg)
	require.NoError(t, err)

	var mu sync.Mutex
	locked := func(b []byte) {
		mu.Lock()
		defer mu.Unlock()
		_ = writeFull(os.Stdout, b)
	}

	// The handler installs before <ready>: a SIGWINCH landing in
	// between used to hit the default disposition and get lost,
	// flaking TestRealSIGWINCH under load.
	winch := make(chan os.Signal, 8)
	signal.Notify(winch, unix.SIGWINCH)
	go func() {
		for range winch {
			ws, err := unix.IoctlGetWinsize(int(os.Stdin.Fd()), unix.TIOCGWINSZ)
			if err != nil {
				return
			}
			size := "<size " + strconv.Itoa(int(ws.Row)) + "x" + strconv.Itoa(int(ws.Col)) + ">"
			mu.Lock()
			werr := writeFull(os.Stdout, []byte(size))
			mu.Unlock()
			if werr != nil {
				return
			}
		}
	}()
	locked([]byte("<ready " + helperSize(t) + ">"))

	counted := 0
	buf := make([]byte, 4096)
	for counted < n {
		nr, rerr := os.Stdin.Read(buf)
		if nr > 0 {
			locked(buf[:nr])
			counted += nr
		}
		if rerr != nil {
			break
		}
	}
	locked([]byte("<bye " + arg + ">"))
	signal.Stop(winch)
	os.Exit(7)
}

// helperEnv writes <env N>, the number of variables in its environment,
// then <NAME=value> or <NAME unset> for each name, and exits 14.
func helperEnv(names []string) {
	var b strings.Builder
	b.WriteString("<env " + strconv.Itoa(len(os.Environ())) + ">")
	for _, name := range names {
		if value, ok := os.LookupEnv(name); ok {
			b.WriteString("<" + name + "=" + value + ">")
		} else {
			b.WriteString("<" + name + " unset>")
		}
	}
	helperWrite(b.String())
	os.Exit(14)
}

// helperSignals reports <sig N> for SIGUSR1 and SIGTERM and exits 3 after
// SIGTERM.
func helperSignals() {
	notify := make(chan os.Signal, 8)
	signal.Notify(notify, unix.SIGUSR1, unix.SIGTERM)
	helperWrite("<ready>")
	for sig := range notify {
		helperWrite("<sig " + strconv.Itoa(int(sig.(unix.Signal))) + ">")
		if sig == unix.SIGTERM {
			signal.Stop(notify)
			os.Exit(3)
		}
	}
}

// helperHup catches SIGHUP and exits 129 with it. Notify stays armed
// through the exit: a second SIGHUP (the kernel sends one when ptmx
// closes) then queues harmlessly instead of killing the helper in the
// Stop-to-Exit window, which used to flake CancelHangsUp under churn.
func helperHup() {
	notify := make(chan os.Signal, 4)
	signal.Notify(notify, unix.SIGHUP)
	helperWrite("<ready>")
	<-notify
	os.Exit(129)
}

// helperStubborn ignores SIGHUP and reads stdin until an error, then
// exits 8: it only learns of the hang-up by touching its terminal.
func helperStubborn() {
	signal.Ignore(unix.SIGHUP)
	helperWrite("<ready>")
	helperReadUntilError()
	os.Exit(8)
}

// helperDeaf ignores SIGHUP, never touches its terminal again and exits 13
// on SIGUSR1.
func helperDeaf() {
	signal.Ignore(unix.SIGHUP)
	notify := make(chan os.Signal, 4)
	signal.Notify(notify, unix.SIGUSR1)
	helperWrite("<ready>")
	<-notify
	signal.Stop(notify)
	os.Exit(13)
}

// helperReadUntilError drains stdin until the read fails.
func helperReadUntilError() {
	buf := make([]byte, 4096)
	for {
		if _, err := os.Stdin.Read(buf); err != nil {
			return
		}
	}
}

// helperFlood writes n bytes of 0123456789abcdef in one go and exits 5.
func helperFlood(t *testing.T, arg string) {
	n, err := strconv.Atoi(arg)
	require.NoError(t, err)
	pattern := []byte("0123456789abcdef")
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = pattern[i%len(pattern)]
	}
	_ = writeFull(os.Stdout, buf)
	os.Exit(5)
}

// startGrandchild starts the test binary again on the same terminal with a
// pipe as fd 3 and waits for its one ready byte. Only then does the parent
// go on. The test SIGKILLs the returned PID at the end.
func startGrandchild(t *testing.T, mode string) int {
	t.Helper()
	pr, pw, err := os.Pipe()
	require.NoError(t, err)
	cmd := helperCmd(t, mode)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.ExtraFiles = []*os.File{pw}
	require.NoError(t, cmd.Start())
	_ = pw.Close()
	one := make([]byte, 1)
	_, err = io.ReadFull(pr, one)
	_ = pr.Close()
	require.NoError(t, err, "grandchild %s ready", mode)
	pid := cmd.Process.Pid
	require.NoError(t, cmd.Process.Release())
	return pid
}

// grandchildReady is the grandchild preamble: ignore SIGHUP, and only then
// tell the parent through fd 3 that signals are set up.
func grandchildReady() {
	signal.Ignore(unix.SIGHUP)
	syncPipe := os.NewFile(uintptr(3), "sync")
	if syncPipe == nil {
		os.Exit(120)
	}
	_, _ = syncPipe.Write([]byte{0})
	_ = syncPipe.Close()
}

// helperOrphan starts a sleep grandchild, reports its PID and exits 6 while
// the grandchild holds and reads the child's end.
func helperOrphan(t *testing.T) {
	pid := startGrandchild(t, "sleep")
	helperWrite("<orphan " + strconv.Itoa(pid) + ">")
	os.Exit(6)
}

// helperJam starts a hold grandchild, reports its PID and exits 11 on
// SIGUSR1. The handler installs before <ready> so a SIGUSR1 in
// between cannot hit the default disposition.
func helperJam(t *testing.T) {
	pid := startGrandchild(t, "hold")
	notify := make(chan os.Signal, 4)
	signal.Notify(notify, unix.SIGUSR1)
	helperWrite("<ready " + strconv.Itoa(pid) + ">")
	<-notify
	signal.Stop(notify)
	os.Exit(11)
}

// helperSpew starts a produce grandchild, reports its PID, writes <bye> on
// SIGUSR1 and exits 12. The handler installs before <ready>, as in
// helperJam.
func helperSpew(t *testing.T) {
	pid := startGrandchild(t, "produce")
	notify := make(chan os.Signal, 4)
	signal.Notify(notify, unix.SIGUSR1)
	helperWrite("<ready " + strconv.Itoa(pid) + ">")
	<-notify
	signal.Stop(notify)
	helperWrite("<bye>")
	os.Exit(12)
}

// transcript is a mutex-guarded recording of what a test read from its PTY.
type transcript struct {
	mu    sync.Mutex
	cond  *sync.Cond
	buf   []byte
	ended bool
}

// record copies what it reads into a mutex-guarded buffer. The recorder
// reads the test's blocking outer end and ends when the other end closes
// after Run; a test closing the end it reads uses a canceler/fdReader and
// cancels first. Every mutation broadcasts while holding the lock, so a
// waiter cannot miss the last chunk.
func record(r io.Reader) *transcript {
	tr := &transcript{}
	tr.cond = sync.NewCond(&tr.mu)
	go func() {
		defer func() {
			tr.mu.Lock()
			tr.ended = true
			tr.cond.Broadcast()
			tr.mu.Unlock()
		}()
		buf := make([]byte, 32*1024)
		for {
			n, err := r.Read(buf)
			if n > 0 {
				tr.mu.Lock()
				tr.buf = append(tr.buf, buf[:n]...)
				tr.cond.Broadcast()
				tr.mu.Unlock()
			}
			if err != nil {
				return
			}
		}
	}()
	return tr
}

// bytes returns a copy of what arrived so far.
func (tr *transcript) bytes() []byte {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return append([]byte(nil), tr.buf...)
}

// waitFor waits until the transcript equals want, failing at once if it
// stops being a prefix or the reader ends, and after 10 s.
func (tr *transcript) waitFor(t *testing.T, want string) {
	t.Helper()
	deadline := time.Now().Add(helperTimeout)
	stop := time.AfterFunc(helperTimeout, tr.cond.Broadcast)
	defer stop.Stop()
	tr.mu.Lock()
	defer tr.mu.Unlock()
	for {
		got := string(tr.buf)
		if got == want {
			return
		}
		if !strings.HasPrefix(want, got) {
			t.Fatalf("transcript diverged:\n got %q\nwant %q", got, want)
		}
		if tr.ended {
			t.Fatalf("transcript ended at %q, want %q", got, want)
		}
		if time.Now().After(deadline) {
			t.Fatalf("transcript timed out at %q, want %q", got, want)
		}
		tr.cond.Wait()
	}
}

// waitEnded waits until the recorder's reader fails, or 10 s.
func (tr *transcript) waitEnded(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(helperTimeout)
	stop := time.AfterFunc(helperTimeout, tr.cond.Broadcast)
	defer stop.Stop()
	tr.mu.Lock()
	defer tr.mu.Unlock()
	for !tr.ended {
		if time.Now().After(deadline) {
			t.Fatal("recorder did not end")
		}
		tr.cond.Wait()
	}
}

// waitMarkerPID waits until the transcript contains <marker PID> and returns
// the PID, failing after 10 s or if the reader ends first.
func waitMarkerPID(t *testing.T, tr *transcript, marker string) int {
	t.Helper()
	prefix := "<" + marker + " "
	deadline := time.Now().Add(helperTimeout)
	stop := time.AfterFunc(helperTimeout, tr.cond.Broadcast)
	defer stop.Stop()
	tr.mu.Lock()
	defer tr.mu.Unlock()
	for {
		buf := string(tr.buf)
		if i := strings.Index(buf, prefix); i >= 0 {
			rest := buf[i+len(prefix):]
			j := 0
			for j < len(rest) && rest[j] >= '0' && rest[j] <= '9' {
				j++
			}
			if j > 0 && j < len(rest) && rest[j] == '>' {
				pid, err := strconv.Atoi(rest[:j])
				require.NoError(t, err)
				return pid
			}
		}
		if tr.ended {
			t.Fatalf("recorder ended before <%s PID> in %q", marker, buf)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for <%s PID> in %q", marker, buf)
		}
		tr.cond.Wait()
	}
}

// waitPID waits for <ready PID> and returns the PID.
func waitPID(t *testing.T, tr *transcript) int {
	t.Helper()
	return waitMarkerPID(t, tr, "ready")
}

// slowReader reads r at most perTick bytes per gate tick, dropping x bytes so a
// flooding producer cannot fill the transcript. The gate turns while the test
// needs output to flow; after the markers arrive the test stops the sender
// and closes the gate, so a blocked Read ends with the reader's own error
// once the descriptors close.
type slowReader struct {
	r       io.Reader
	perTick int
	gate    chan struct{}
	err     error
}

// newSlowReader returns a reader of r yielding at most perTick bytes per tick.
func newSlowReader(r io.Reader, perTick int) *slowReader {
	return &slowReader{r: r, perTick: perTick, gate: make(chan struct{})}
}

// turnGate keeps s.gate turning until stop closes, then reports on done.
func (s *slowReader) turnGate(stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	for {
		select {
		case s.gate <- struct{}{}:
		case <-stop:
			return
		}
	}
}

// Read waits for one gate tick, then returns up to perTick bytes without x,
// keeping a read error for the call after data.
func (s *slowReader) Read(p []byte) (int, error) {
	if s.err != nil {
		err := s.err
		s.err = nil
		return 0, err
	}
	<-s.gate
	n := len(p)
	if n > s.perTick {
		n = s.perTick
	}
	buf := make([]byte, n)
	nr, rerr := s.r.Read(buf)
	kept := buf[:0]
	for _, b := range buf[:nr] {
		if b != 'x' {
			kept = append(kept, b)
		}
	}
	if m := copy(p, kept); m > 0 {
		if rerr != nil {
			s.err = rerr
		}
		return m, nil
	}
	return 0, rerr
}

// pumpParkedInWriteChild reports whether some goroutine is parked in
// writeChild's cancelable wait: its stack shows proxy.writeChild( and
// Select(. Only serial tests may use it: parallel tests share stacks.
func pumpParkedInWriteChild() bool {
	var buf [1 << 20]byte
	n := runtime.Stack(buf[:], true)
	for _, st := range strings.Split(string(buf[:n]), "\n\n") {
		if strings.Contains(st, "proxy.writeChild(") && strings.Contains(st, "Select(") {
			return true
		}
	}
	return false
}
