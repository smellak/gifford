//go:build unix

package proxy

import (
	"os"
	"testing"

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
