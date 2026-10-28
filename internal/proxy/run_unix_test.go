//go:build unix

package proxy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"strings"
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
func startProxyRun(ctx context.Context, t *testing.T, command []string, sigs chan os.Signal, rows, cols uint16) *proxyTestRun {
	t.Helper()
	outer, inner, err := pty.Open()
	require.NoError(t, err)
	require.NoError(t, pty.Setsize(outer, &pty.Winsize{Rows: rows, Cols: cols}))
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
	run := startProxyRun(t.Context(), t, helperArgv("echo", "3080"), sigs, 24, 80)
	drive(t, run.tr, run.outer, func() {
		require.NoError(t, pty.Setsize(run.outer, &pty.Winsize{Rows: 40, Cols: 100}))
		sigs <- unix.SIGWINCH
	})
	require.Equal(t, expected, string(run.tr.bytes()))
	res := run.finish(t)
	require.Equal(t, Exit{Code: 7}, res.exit)
	require.NoError(t, res.err)
}
