//go:build unix

package proxy

import (
	"os"
	"testing"

	"github.com/creack/pty"
	"github.com/stretchr/testify/require"
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
