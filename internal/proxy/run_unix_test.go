//go:build unix

package proxy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/creack/pty"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"golang.org/x/term"
)

// openPair returns a test-owned PTY pair, both ends closed on cleanup.
func openPair(t *testing.T) (outer, inner *os.File) {
	t.Helper()
	outer, inner, err := pty.Open()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = outer.Close()
		_ = inner.Close()
	})
	return outer, inner
}

// TestFDReader checks the cancelable waits: a canceled read ends at once
// even with bytes pending, a fresh reader gets its bytes, an idle child
// end reports nothing pending, and the size round-trips.
func TestFDReader(t *testing.T) {
	t.Parallel()

	t.Run("cancel interrupts a read with bytes pending", func(t *testing.T) {
		t.Parallel()
		outer, inner := openPair(t)
		_, err := inner.Write([]byte("x"))
		require.NoError(t, err)
		c, err := newCanceler()
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, c.close()) })
		r, err := c.reader(outer)
		require.NoError(t, err)
		c.cancel()
		_, err = r.Read(make([]byte, 16))
		require.ErrorIs(t, err, errCanceled)
	})

	t.Run("fresh reader reads back", func(t *testing.T) {
		t.Parallel()
		outer, inner := openPair(t)
		_, err := inner.Write([]byte("x"))
		require.NoError(t, err)
		c, err := newCanceler()
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, c.close()) })
		r, err := c.reader(outer)
		require.NoError(t, err)
		got := make([]byte, 16)
		n, err := r.Read(got)
		require.NoError(t, err)
		require.Equal(t, []byte("x"), got[:n])
	})

	t.Run("idle child end has nothing pending", func(t *testing.T) {
		t.Parallel()
		outer, _ := openPair(t)
		c, err := newCanceler()
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, c.close()) })
		child, err := c.child(outer)
		require.NoError(t, err)
		_, err = child.readNow(make([]byte, 16))
		require.ErrorIs(t, err, errNothingPending)
	})

	t.Run("child read ends on cancel", func(t *testing.T) {
		t.Parallel()
		outer, _ := openPair(t)
		c, err := newCanceler()
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, c.close()) })
		child, err := c.child(outer)
		require.NoError(t, err)
		c.cancel()
		_, err = child.Read(make([]byte, 16))
		require.ErrorIs(t, err, errCanceled)
	})

	t.Run("write to a full child ends on cancel", func(t *testing.T) {
		t.Parallel()
		outer, _ := openPair(t)
		c, err := newCanceler()
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, c.close()) })
		child, err := c.child(outer)
		require.NoError(t, err)
		c.cancel()
		err = writeChild(child, make([]byte, 1<<20))
		require.ErrorIs(t, err, errCanceled)
	})

	t.Run("size round-trips", func(t *testing.T) {
		t.Parallel()
		outer, _ := openPair(t)
		c, err := newCanceler()
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, c.close()) })
		child, err := c.child(outer)
		require.NoError(t, err)
		require.NoError(t, child.setSize(24, 80, 0, 0))
		size, err := pty.GetsizeFull(outer)
		require.NoError(t, err)
		require.Equal(t, 24, int(size.Rows))
		require.Equal(t, 80, int(size.Cols))
	})
}

// rawInner puts the test slave end in raw mode, so bytes cross without
// echo or processing, and restores it on cleanup.
func rawInner(t *testing.T, inner *os.File) {
	t.Helper()
	old, err := term.MakeRaw(int(inner.Fd()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, term.Restore(int(inner.Fd()), old)) })
}

// TestDrainLimit checks the drain bound: a PTY filled until EAGAIN holds
// far less than drainLimit, and a drain fed forever stops at the limit.
func TestDrainLimit(t *testing.T) {
	t.Parallel()

	t.Run("filled pty stays far under the limit", func(t *testing.T) {
		t.Parallel()
		outer, inner := openPair(t)
		rawInner(t, inner)
		// Hoist the descriptor: every Fd() call reverts it to blocking
		// when Go marked it nonblocking, so fcntl last and never call
		// Fd() again afterwards.
		sfd := int(inner.Fd())
		require.NoError(t, unix.SetNonblock(sfd, true))
		chunk := make([]byte, 4096)
		held := 0
		for {
			n, err := unix.Write(sfd, chunk)
			if n > 0 {
				held += n
			}
			if err == unix.EAGAIN {
				break
			}
			require.NoError(t, err)
		}
		t.Logf("pty holds %d bytes until EAGAIN", held)
		require.Less(t, held, drainLimit/8)
		c, err := newCanceler()
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, c.close()) })
		child, err := c.child(outer)
		require.NoError(t, err)
		r, w, err := os.Pipe()
		require.NoError(t, err)
		t.Cleanup(func() { _ = r.Close() })
		counted := make(chan int, 1)
		go func() {
			total := 0
			buf := make([]byte, 32*1024)
			for {
				n, err := r.Read(buf)
				total += n
				if err != nil {
					break
				}
			}
			counted <- total
		}()
		require.NoError(t, drainOutput(child, w))
		require.NoError(t, w.Close())
		require.Equal(t, held, <-counted)
	})

	t.Run("endless drain stops at the limit", func(t *testing.T) {
		t.Parallel()
		outer, inner := openPair(t)
		rawInner(t, inner)
		_, err := inner.Write(make([]byte, 4096))
		require.NoError(t, err)
		c, err := newCanceler()
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, c.close()) })
		child, err := c.child(outer)
		require.NoError(t, err)
		require.NoError(t, drainOutput(child, inner))
		r, w, err := os.Pipe()
		require.NoError(t, err)
		t.Cleanup(func() { _ = r.Close() })
		counted := make(chan int, 1)
		go func() {
			total := 0
			buf := make([]byte, 32*1024)
			for {
				n, err := r.Read(buf)
				total += n
				if err != nil {
					break
				}
			}
			counted <- total
		}()
		require.NoError(t, drainOutput(child, w))
		require.NoError(t, w.Close())
		require.Positive(t, <-counted)
	})
}

// runOutcome is one Run's result as the test goroutine reports it.
type runOutcome struct {
	exit Exit
	err  error
}

// proxyTestRun is one Run under test: a fresh PTY pair sized rows x cols, a
// recorder on the blocking outer end and Run on a goroutine.
type proxyTestRun struct {
	inner  *os.File
	outer  *os.File
	before *term.State
	tr     *transcript
	resCh  chan runOutcome
}

// startProxyRun opens a PTY pair, sizes it before Run starts, records the
// outer end, snapshots inner's termios and starts Run on a goroutine. ctx
// and sigs pass through; a nil sigs subscribes Run to the handled signals.
func startProxyRun(ctx context.Context, t *testing.T, command []string, sigs chan os.Signal) *proxyTestRun {
	t.Helper()
	outer, inner, err := pty.Open()
	require.NoError(t, err)
	require.NoError(t, pty.Setsize(outer, &pty.Winsize{Rows: 24, Cols: 80}))
	before, err := term.GetState(int(inner.Fd()))
	require.NoError(t, err)
	run := &proxyTestRun{inner: inner, outer: outer, before: before, tr: record(outer), resCh: make(chan runOutcome, 1)}
	go func() {
		exit, err := Run(ctx, Options{Command: command, In: inner, Out: inner, Signals: sigs})
		run.resCh <- runOutcome{exit: exit, err: err}
	}()
	return run
}

// finish waits for Run, requires inner's termios unchanged, closes inner,
// waits for the recorder and closes outer.
func (r *proxyTestRun) finish(t *testing.T) runOutcome {
	t.Helper()
	var res runOutcome
	select {
	case res = <-r.resCh:
	case <-time.After(helperTimeout):
		t.Fatal("Run did not return")
	}
	after, err := term.GetState(int(r.inner.Fd()))
	require.NoError(t, err)
	require.Equal(t, r.before, after, "outer termios changed")
	require.NoError(t, r.inner.Close())
	r.tr.waitEnded(t)
	require.NoError(t, r.outer.Close())
	return res
}

// keyTable holds one key per entry: the exact bytes a terminal sends.
func keyTable() []string {
	return []string{
		"a", "Z", "7", " ", "~", // printable ASCII
		"\u00e9", "\u20ac", "\u6f22", "\U0001F642", "e\u0301", // 2-, 3-, 3-, 4-byte UTF-8; combining accent
		"\x03", "\x04", "\x1a", "\x1c", "\x11", "\x13", "\x15", "\x16", "\x17", // Ctrl-C D Z \ Q S U V W
		"\x00", "\x7f", "\x08", "\t", "\r", "\n", "\x1b", // Ctrl-Space, Backspace, Ctrl-H, Tab, Enter, Ctrl-J, Esc
		"\x1b[A", "\x1bOB", "\x1b[1;5C", "\x1b[15~", // Up, Down (SS3), Ctrl-Right, F5
		"\x1bx", "\x1b\r", "\x1b[13;2u", "\x1b[I", // Alt-x, Alt-Enter, Shift-Enter (CSI u), focus in
		"\x1b[<0;12;5M", "\x1b[<0;12;5m", "\x1b[<65;12;5M", // SGR mouse: press, release, wheel down
		"\x1b[M \x81\x95",                               // X10 mouse press at column 97, row 117: bytes above 0x7f
		"\x1b[200~", "pasted\rtext \u00fc", "\x1b[201~", // bracketed paste
		"\xc3", // a lone UTF-8 lead byte
	}
}

// expectedEcho builds the DoD transcript: <ready 24x80>, keys 0-499,
// <size 40x100>, keys 500-999, <bye 3080>.
func expectedEcho() string {
	keys := keyTable()
	var sb strings.Builder
	sb.WriteString("<ready 24x80>")
	for i := range 500 {
		sb.WriteString(keys[i%len(keys)])
	}
	sb.WriteString("<size 40x100>")
	for j := range 500 {
		sb.WriteString(keys[(j+500)%len(keys)])
	}
	sb.WriteString("<bye 3080>")
	return sb.String()
}

// drive types keys 0-999 through w with a resize in the middle, awaiting
// every echo; it is shared by the direct and proxy runs.
func drive(t *testing.T, tr *transcript, w io.Writer, resize func()) {
	t.Helper()
	keys := keyTable()
	var sb strings.Builder
	sb.WriteString("<ready 24x80>")
	tr.waitFor(t, sb.String())
	for i := range 500 {
		_, err := io.WriteString(w, keys[i%len(keys)])
		require.NoError(t, err)
		sb.WriteString(keys[i%len(keys)])
	}
	tr.waitFor(t, sb.String())
	resize()
	sb.WriteString("<size 40x100>")
	tr.waitFor(t, sb.String())
	for j := range 500 {
		i := j + 500
		_, err := io.WriteString(w, keys[i%len(keys)])
		require.NoError(t, err)
		sb.WriteString(keys[i%len(keys)])
	}
	sb.WriteString("<bye 3080>")
	tr.waitFor(t, sb.String())
}

// sigMark formats the <sig N> marker the signals helper prints.
func sigMark(sig unix.Signal) string {
	return "<sig " + strconv.Itoa(int(sig)) + ">"
}

// TestForwardSignals checks that handled signals reach the harness group:
// SIGUSR1 and SIGTERM arrive as <sig N> markers and the harness exits 3.
func TestForwardSignals(t *testing.T) {
	t.Parallel()
	sigs := make(chan os.Signal, 8)
	run := startProxyRun(t.Context(), t, helperArgv("signals"), sigs)
	run.tr.waitFor(t, "<ready>")
	sigs <- unix.SIGUSR1
	run.tr.waitFor(t, "<ready>"+sigMark(unix.SIGUSR1))
	sigs <- unix.SIGTERM
	run.tr.waitFor(t, "<ready>"+sigMark(unix.SIGUSR1)+sigMark(unix.SIGTERM))
	res := run.finish(t)
	require.Equal(t, Exit{Code: 3}, res.exit)
	require.NoError(t, res.err)
}

// TestKilledBySignal checks the exit of a harness that SIGKILLs itself:
// Code -1 with Signal SIGKILL, shell status 137.
func TestKilledBySignal(t *testing.T) {
	t.Parallel()
	sigs := make(chan os.Signal, 8)
	run := startProxyRun(t.Context(), t, helperArgv("kill"), sigs)
	run.tr.waitFor(t, "<ready>")
	res := run.finish(t)
	require.Equal(t, Exit{Code: -1, Signal: syscall.SIGKILL}, res.exit)
	require.Equal(t, 137, res.exit.Status())
	require.NoError(t, res.err)
}

// TestRealSIGWINCH resizes through a real signal to the test process. It is
// serial: it signals the test process itself.
func TestRealSIGWINCH(t *testing.T) {
	run := startProxyRun(t.Context(), t, helperArgv("echo", "1"), nil)
	run.tr.waitFor(t, "<ready 24x80>")
	require.NoError(t, pty.Setsize(run.outer, &pty.Winsize{Rows: 30, Cols: 90}))
	require.NoError(t, unix.Kill(os.Getpid(), unix.SIGWINCH))
	run.tr.waitFor(t, "<ready 24x80><size 30x90>")
	_, err := run.outer.Write([]byte("q"))
	require.NoError(t, err)
	run.tr.waitFor(t, "<ready 24x80><size 30x90>q<bye 1>")
	res := run.finish(t)
	require.Equal(t, Exit{Code: 7}, res.exit)
	require.NoError(t, res.err)
}

// TestOutputDrainedAtExit checks that every byte written before the harness
// exits arrives: 16, 4,096, 65,536 and 1,048,576 bytes of 0123456789abcdef.
func TestOutputDrainedAtExit(t *testing.T) {
	t.Parallel()
	for _, n := range []int{16, 4096, 65536, 1048576} {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			t.Parallel()
			sigs := make(chan os.Signal, 8)
			run := startProxyRun(t.Context(), t, helperArgv("flood", strconv.Itoa(n)), sigs)
			run.tr.waitFor(t, strings.Repeat("0123456789abcdef", n/16))
			res := run.finish(t)
			require.Equal(t, Exit{Code: 5}, res.exit)
			require.NoError(t, res.err)
		})
	}
}

// TestGrandchildHoldingTerminal checks that Run returns the harness exit
// while its grandchild still holds the child's end, and SIGKILLs it after.
// The aliveness witness is best-effort: teardown legitimately reaps the
// holder concurrently (close, EIO, exit, reap), so under load it may already
// be gone; the binding proof is the exit without hanging.
func TestGrandchildHoldingTerminal(t *testing.T) {
	t.Parallel()
	sigs := make(chan os.Signal, 8)
	run := startProxyRun(t.Context(), t, helperArgv("orphan"), sigs)
	pid := waitMarkerPID(t, run.tr, "orphan")
	if err := unix.Kill(pid, 0); err != nil {
		t.Logf("grandchild %d already reaped: %v", pid, err)
	}
	res := run.finish(t)
	require.Equal(t, Exit{Code: 6}, res.exit)
	require.NoError(t, res.err)
	_ = unix.Kill(pid, unix.SIGKILL)
}

// TestInputStuckAfterExit checks that a pump parked in a full child queue
// ends on hang-up: the harness exits 11, the terminal is restored and the
// hold grandchild survives. It is serial: it reads all goroutine stacks.
func TestInputStuckAfterExit(t *testing.T) {
	sigs := make(chan os.Signal, 8)
	run := startProxyRun(t.Context(), t, helperArgv("jam"), sigs)
	hold := waitPID(t, run.tr)
	fd := int(run.outer.Fd())
	require.NoError(t, unix.SetNonblock(fd, true))
	chunk := make([]byte, 4096)
	deadline := time.Now().Add(helperTimeout)
	for !pumpParkedInWriteChild() {
		_, err := unix.Write(fd, chunk)
		if err != nil {
			require.ErrorIs(t, err, unix.EAGAIN)
		}
		if time.Now().After(deadline) {
			t.Fatal("input pump never parked in writeChild")
		}
	}
	sigs <- unix.SIGUSR1
	res := run.finish(t)
	require.Equal(t, Exit{Code: 11}, res.exit)
	require.NoError(t, res.err)
	require.NoError(t, unix.Kill(hold, 0), "hold grandchild alive")
	_ = unix.Kill(hold, unix.SIGKILL)
}

// TestProducerAfterExit checks that a flooding producer does not stall the
// exit: on SIGUSR1 the harness writes <bye> and exits 12 with the gate
// turning, the terminal restored, and the producer reaped after.
func TestProducerAfterExit(t *testing.T) {
	t.Parallel()
	sigs := make(chan os.Signal, 8)
	outer, inner, err := pty.Open()
	require.NoError(t, err)
	require.NoError(t, pty.Setsize(outer, &pty.Winsize{Rows: 24, Cols: 80}))
	before, err := term.GetState(int(inner.Fd()))
	require.NoError(t, err)
	sr := newSlowReader(outer, 64)
	stop := make(chan struct{})
	gateDone := make(chan struct{})
	go sr.turnGate(stop, gateDone)
	resCh := make(chan runOutcome, 1)
	go func() {
		exit, rerr := Run(t.Context(), Options{Command: helperArgv("spew"), In: inner, Out: inner, Signals: sigs})
		resCh <- runOutcome{exit: exit, err: rerr}
	}()
	run := &proxyTestRun{inner: inner, outer: outer, before: before, tr: record(sr), resCh: resCh}
	produce := waitPID(t, run.tr)
	sigs <- unix.SIGUSR1
	run.tr.waitFor(t, "<ready "+strconv.Itoa(produce)+"><bye>")
	close(stop)
	<-gateDone
	close(sr.gate)
	res := run.finish(t)
	require.Equal(t, Exit{Code: 12}, res.exit)
	require.NoError(t, res.err)
	_ = unix.Kill(produce, unix.SIGKILL)
}

// TestEchoThroughProxyMatchesDirect is the 1.4 definition of done: 1,000
// keys, 3,080 bytes, a resize after key 499. The direct transcript must
// equal the expected bytes, and the proxy transcript must equal the direct
// one, byte for byte, with inner's termios unchanged.
func TestEchoThroughProxyMatchesDirect(t *testing.T) {
	t.Parallel()
	keys := keyTable()
	require.Len(t, keys, 42)
	require.Len(t, strings.Join(keys, ""), 131)
	expected := expectedEcho()
	require.Len(t, expected, 3116)
	sum := sha256.Sum256([]byte(expected))
	require.Equal(t, "c93043b4f58ce7dd108dfbfd164ed1c7024cefeed01dfaef54144613a92ae552", hex.EncodeToString(sum[:]))

	cmd := helperCmd(t, "echo", "3080")
	master, err := pty.StartWithSize(cmd, &pty.Winsize{Rows: 24, Cols: 80})
	require.NoError(t, err)
	direct := record(master)
	drive(t, direct, master, func() {
		require.NoError(t, pty.Setsize(master, &pty.Winsize{Rows: 40, Cols: 100}))
	})
	_ = cmd.Wait()
	require.Equal(t, 7, cmd.ProcessState.ExitCode())
	direct.waitEnded(t)
	require.Equal(t, expected, string(direct.bytes()))
	require.NoError(t, master.Close())

	sigs := make(chan os.Signal, 8)
	run := startProxyRun(t.Context(), t, helperArgv("echo", "3080"), sigs)
	drive(t, run.tr, run.outer, func() {
		require.NoError(t, pty.Setsize(run.outer, &pty.Winsize{Rows: 40, Cols: 100}))
		sigs <- unix.SIGWINCH
	})
	require.Equal(t, expected, string(run.tr.bytes()))
	res := run.finish(t)
	require.Equal(t, Exit{Code: 7}, res.exit)
	require.NoError(t, res.err)
}

// TestCancelHangsUp checks that canceling the context hangs up a hup
// harness: it exits 129 and Run reports context.Canceled.
func TestCancelHangsUp(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	sigs := make(chan os.Signal, 8)
	run := startProxyRun(ctx, t, helperArgv("hup"), sigs)
	run.tr.waitFor(t, "<ready>")
	cancel()
	res := run.finish(t)
	require.Equal(t, Exit{Code: 129}, res.exit)
	require.ErrorIs(t, res.err, context.Canceled)
}

// TestCancelStubbornHarness checks that canceling hangs up a harness that
// ignores SIGHUP but reads its terminal: it exits 8 once ptmx closes.
func TestCancelStubbornHarness(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	sigs := make(chan os.Signal, 8)
	run := startProxyRun(ctx, t, helperArgv("stubborn"), sigs)
	run.tr.waitFor(t, "<ready>")
	cancel()
	res := run.finish(t)
	require.Equal(t, Exit{Code: 8}, res.exit)
	require.ErrorIs(t, res.err, context.Canceled)
}

// errCountingContext counts Err calls on its embedded context.
type errCountingContext struct {
	context.Context
	calls *atomic.Int64
}

// Err counts the call, then reports the embedded context's error.
func (c errCountingContext) Err() error {
	c.calls.Add(1)
	return c.Context.Err()
}

// TestCancelObservedOnce checks that the context error is read exactly
// twice: once in setup, once on teardown.
func TestCancelObservedOnce(t *testing.T) {
	t.Parallel()
	var calls atomic.Int64
	ctx, cancel := context.WithCancel(t.Context())
	sigs := make(chan os.Signal, 8)
	run := startProxyRun(errCountingContext{Context: ctx, calls: &calls}, t, helperArgv("deaf"), sigs)
	run.tr.waitFor(t, "<ready>")
	require.Equal(t, int64(1), calls.Load())
	cancel()
	sigs <- unix.SIGUSR1
	res := run.finish(t)
	require.Equal(t, Exit{Code: 13}, res.exit)
	require.EqualError(t, res.err, "proxy: context canceled")
	require.Equal(t, int64(2), calls.Load())
}

// TestOuterTerminalClosed checks that closing the outer end hangs up a hup
// harness: it exits 129 with no error. Nothing reads outer while it closes:
// a recorder blocked in Read would hold the master description open and the
// close would never reach the pumps. The test reads through a cancelable
// fdReader instead, cancels it, then closes.
func TestOuterTerminalClosed(t *testing.T) {
	t.Parallel()
	sigs := make(chan os.Signal, 8)
	outer, inner, err := pty.Open()
	require.NoError(t, err)
	require.NoError(t, pty.Setsize(outer, &pty.Winsize{Rows: 24, Cols: 80}))
	c, err := newCanceler()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.close()) })
	r, err := c.reader(outer)
	require.NoError(t, err)
	resCh := make(chan runOutcome, 1)
	go func() {
		exit, rerr := Run(t.Context(), Options{Command: helperArgv("hup"), In: inner, Out: inner, Signals: sigs})
		resCh <- runOutcome{exit: exit, err: rerr}
	}()
	var got []byte
	buf := make([]byte, 64)
	for !strings.Contains(string(got), "<ready>") {
		n, rerr := r.Read(buf)
		require.NoError(t, rerr)
		got = append(got, buf[:n]...)
	}
	c.cancel()
	require.NoError(t, outer.Close())
	var res runOutcome
	select {
	case res = <-resCh:
	case <-time.After(helperTimeout):
		t.Fatal("Run did not return")
	}
	require.Equal(t, Exit{Code: 129}, res.exit)
	require.NoError(t, res.err)
	// No termios check: with outer closed the slave answers EIO to TCGETS,
	// and the DoD exempts hang-ups ("hang-ups aside").
	require.NoError(t, inner.Close())
}

// panicWriter panics with v on Write: the session's failing output.
type panicWriter struct {
	v any
}

// Write panics instead of writing.
func (w panicWriter) Write([]byte) (int, error) {
	panic(w.v)
}

// TestPanicRestoresTerminal checks that a panicking output pump still tears
// the session down through the production path: the hup harness exits, the
// helper re-raises the first panic as *goroutinePanic, its deferred restore
// leaves the outer termios unchanged, and the harness is reaped.
func TestPanicRestoresTerminal(t *testing.T) {
	t.Parallel()
	sigs := make(chan os.Signal, 8)
	outer, inner, err := pty.Open()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = outer.Close()
		_ = inner.Close()
	})
	require.NoError(t, pty.Setsize(outer, &pty.Winsize{Rows: 24, Cols: 80}))
	before, err := term.GetState(int(inner.Fd()))
	require.NoError(t, err)
	cmd := helperCmd(t, "hup")
	var got any
	func() {
		defer func() { got = recover() }()
		_, _ = runWithWriter(t.Context(), cmd, inner, panicWriter{v: "boom"}, sigs)
	}()
	gp, ok := got.(*goroutinePanic)
	require.True(t, ok, "expected a *goroutinePanic, got %#v", got)
	require.Equal(t, "boom", gp.value)
	require.NotNil(t, cmd.ProcessState, "harness reaped")
	require.Equal(t, 129, cmd.ProcessState.ExitCode())
	after, err := term.GetState(int(inner.Fd()))
	require.NoError(t, err)
	require.Equal(t, before, after, "outer termios changed")
}

// TestDaemonNotice checks the one line on a missing daemon: an empty or
// missing socket prints exactly DaemonNotice, a listening one prints
// nothing.
func TestDaemonNotice(t *testing.T) {
	t.Parallel()
	runKill := func(t *testing.T, socket string) (string, Exit) {
		t.Helper()
		outer, inner, err := pty.Open()
		require.NoError(t, err)
		require.NoError(t, pty.Setsize(outer, &pty.Winsize{Rows: 24, Cols: 80}))
		before, err := term.GetState(int(inner.Fd()))
		require.NoError(t, err)
		var notice strings.Builder
		resCh := make(chan runOutcome, 1)
		sigs := make(chan os.Signal, 8)
		go func() {
			exit, rerr := Run(t.Context(), Options{
				Command:      helperArgv("kill"),
				In:           inner,
				Out:          inner,
				Notice:       &notice,
				DaemonSocket: socket,
				Signals:      sigs,
			})
			resCh <- runOutcome{exit: exit, err: rerr}
		}()
		run := &proxyTestRun{inner: inner, outer: outer, before: before, tr: record(outer), resCh: resCh}
		run.tr.waitFor(t, "<ready>")
		res := run.finish(t)
		return notice.String(), res.exit
	}
	t.Run("empty socket", func(t *testing.T) {
		t.Parallel()
		notice, exit := runKill(t, "")
		require.Equal(t, DaemonNotice+"\n", notice)
		require.Equal(t, Exit{Code: -1, Signal: syscall.SIGKILL}, exit)
	})
	t.Run("missing socket", func(t *testing.T) {
		t.Parallel()
		notice, exit := runKill(t, filepath.Join(t.TempDir(), "daemon.sock"))
		require.Equal(t, DaemonNotice+"\n", notice)
		require.Equal(t, Exit{Code: -1, Signal: syscall.SIGKILL}, exit)
	})
	t.Run("listening socket", func(t *testing.T) {
		t.Parallel()
		path := filepath.Join(t.TempDir(), "daemon.sock")
		if len(path) > 100 {
			t.Skipf("socket path too long: %d bytes", len(path))
		}
		l, err := (&net.ListenConfig{}).Listen(t.Context(), "unix", path)
		require.NoError(t, err)
		t.Cleanup(func() { _ = l.Close() })
		notice, _ := runKill(t, path)
		require.Empty(t, notice)
	})
}

// trueCmd is a command that starts: setup reaches it only when nothing
// earlier failed.
var trueCmd = []string{"true"}

// TestDrainErrorKeptAsRunError checks that a drain failure is kept as the
// run error when nothing earlier was recorded, that an earlier error wins,
// and that ptmx still closes exactly once either way.
func TestDrainErrorKeptAsRunError(t *testing.T) {
	t.Parallel()
	setup := func(t *testing.T) (*teardown, *session) {
		t.Helper()
		outer, inner, err := pty.Open()
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = outer.Close()
			_ = inner.Close()
		})
		c, err := newCanceler()
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, c.close()) })
		child, err := c.child(outer)
		require.NoError(t, err)
		_, err = inner.Write([]byte("undrained"))
		require.NoError(t, err)
		// Barrier: the slave write may not be visible on the master yet,
		// and a drain racing it would see EAGAIN and report nothing.
		// Waiting for readability consumes no bytes.
		require.NoError(t, c.wait(child.fd, false))
		r, w, err := os.Pipe()
		require.NoError(t, err)
		require.NoError(t, r.Close())
		t.Cleanup(func() { _ = w.Close() })
		ptmx, _ := openPair(t)
		return &teardown{pumps: 1}, &session{child: child, out: w, ptmx: ptmx}
	}
	t.Run("drain failure is kept", func(t *testing.T) {
		t.Parallel()
		td, s := setup(t)
		td.countPump(s)
		require.True(t, td.closed)
		require.ErrorIs(t, td.runErr, unix.EPIPE)
		require.Error(t, s.ptmx.Close(), "ptmx must already be closed")
	})
	t.Run("earlier error wins", func(t *testing.T) {
		t.Parallel()
		td, s := setup(t)
		td.runErr = context.Canceled
		td.countPump(s)
		require.True(t, td.closed)
		require.ErrorIs(t, td.runErr, context.Canceled)
		require.Error(t, s.ptmx.Close(), "ptmx must already be closed")
	})
}

// TestRunRefuses checks the setup failures: no command, a non-terminal
// file, an unknown command and a canceled context, all without notice.
func TestRunRefuses(t *testing.T) {
	t.Parallel()
	t.Run("no command", func(t *testing.T) {
		t.Parallel()
		var notice strings.Builder
		exit, err := Run(t.Context(), Options{Notice: &notice})
		require.ErrorIs(t, err, ErrNoCommand)
		require.Equal(t, Exit{}, exit)
		require.Empty(t, notice.String())
	})
	t.Run("regular file as stdin", func(t *testing.T) {
		t.Parallel()
		_, slave := openPair(t)
		f, err := os.CreateTemp(t.TempDir(), "stdin")
		require.NoError(t, err)
		t.Cleanup(func() { _ = f.Close() })
		var notice strings.Builder
		_, err = Run(t.Context(), Options{Command: trueCmd, In: f, Out: slave, Notice: &notice})
		require.ErrorIs(t, err, ErrNotTerminal)
		require.Empty(t, notice.String())
	})
	t.Run("regular file as stdout", func(t *testing.T) {
		t.Parallel()
		_, slave := openPair(t)
		f, err := os.CreateTemp(t.TempDir(), "stdout")
		require.NoError(t, err)
		t.Cleanup(func() { _ = f.Close() })
		var notice strings.Builder
		_, err = Run(t.Context(), Options{Command: trueCmd, In: slave, Out: f, Notice: &notice})
		require.ErrorIs(t, err, ErrNotTerminal)
		require.Empty(t, notice.String())
	})
	t.Run("unknown command", func(t *testing.T) {
		t.Parallel()
		_, slave := openPair(t)
		path := filepath.Join(t.TempDir(), "daemon.sock")
		if len(path) > 100 {
			t.Skipf("socket path too long: %d bytes", len(path))
		}
		l, err := (&net.ListenConfig{}).Listen(t.Context(), "unix", path)
		require.NoError(t, err)
		t.Cleanup(func() { _ = l.Close() })
		var notice strings.Builder
		_, err = Run(t.Context(), Options{
			Command:      []string{"gifford-no-such-command"},
			In:           slave,
			Out:          slave,
			Notice:       &notice,
			DaemonSocket: path,
		})
		require.ErrorIs(t, err, exec.ErrNotFound)
		require.Empty(t, notice.String())
	})
	t.Run("canceled context", func(t *testing.T) {
		t.Parallel()
		_, slave := openPair(t)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		var notice strings.Builder
		_, err := Run(ctx, Options{Command: trueCmd, In: slave, Out: slave, Notice: &notice})
		require.ErrorIs(t, err, context.Canceled)
		require.Empty(t, notice.String())
	})
}
