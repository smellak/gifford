//go:build unix

package session

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

// A lone ESC may remain unresolved indefinitely while the child continues
// writing. Deferring recording must not retain that whole stream in RAM.
func TestReviewUnresolvedInputCannotBufferOutputWithoutBound(t *testing.T) {
	o := newOrderRun(t, "review-pending-output-bound")
	o.closed(t)
	o.typed(t, "\x1b")
	require.True(t, publishedSnapshot(t, o.s).Pending)
	chunk := bytes.Repeat([]byte("x"), o.s.opts.ChunkMaxBytes)
	for range 128 {
		o.s.onOutput(chunk)
	}
	o.s.mu.Lock()
	first := o.s.buffered
	o.s.mu.Unlock()
	for range 128 {
		o.s.onOutput(chunk)
	}
	o.s.mu.Lock()
	second, pieces := o.s.buffered, len(o.s.pending)
	o.s.mu.Unlock()
	t.Logf("unresolved ESC retained %d bytes, then %d bytes in %d pieces", first, second, pieces)
	require.True(t, publishedSnapshot(t, o.s).Pending, "a memory bound must not resolve input by timeout")
	// This intentionally generous cap is a diagnostic, not a proposed
	// production constant: choose and test an explicit bound in the fix.
	require.LessOrEqual(t, second, 1<<20, "unresolved input must not accumulate an unbounded output stream")
}
