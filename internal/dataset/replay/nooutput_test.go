package replay

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/dataset"
)

// lineHello is a session's first output in these tests.
const lineHello = "hello\r\n"

// TestWindowsBeforeTheFirstOutput replays a session whose first output
// comes after its first anchor: the window before it is marked
// NoOutputYet, and every window keeps its number, its id and its anchor,
// so the first one with output is timed from that output.
func TestWindowsBeforeTheFirstOutput(t *testing.T) {
	t.Parallel()
	p := build(t, []step{{ms: 1500, output: lineHello}, {ms: 2500, output: "world\r\n"}, {ms: 6000, output: "$ "}})
	p.write(t, p.raw.Bytes(), p.events.Bytes())
	ws := windows(t, p.data, time.Second)
	require.Len(t, ws, 6)
	for i, w := range ws {
		k := int64(i + 1)
		anchor := t0.Add(time.Duration(k) * time.Second)
		require.Equal(t, windowID(anchor, sid, k), w.WindowID, "window %d keeps its id", k)
		require.Equal(t, anchor, w.Provenance.AnchorAt, "window %d keeps its anchor", k)
		require.Equal(t, k == 1, w.NoOutputYet, "window %d", k)
	}
	require.InDelta(t, 0.5, ws[1].Features.SecondsSinceLastOutput, 1e-9)
	require.Equal(t, dataset.OutputBytes{Last2s: 7, Last10s: 7}, ws[1].Features.OutputBytes)
}

// TestEmptyChunkIsNoOutput replays a session with an io.chunk of no
// bytes: the quiet time runs on from the output before it, and it counts
// no bytes. One alone before an anchor leaves the window marked
// NoOutputYet.
func TestEmptyChunkIsNoOutput(t *testing.T) {
	t.Parallel()
	p := build(t, []step{{ms: 100, output: lineHello}, {ms: 3500, empty: true}, {ms: 6000, output: "$ "}})
	p.write(t, p.raw.Bytes(), p.events.Bytes())
	ws := windows(t, p.data, time.Second)
	require.Len(t, ws, 6)
	w := ws[3]
	require.False(t, w.NoOutputYet)
	require.InDelta(t, 3.9, w.Features.SecondsSinceLastOutput, 1e-9, "timed from the last output with bytes")
	require.Equal(t, dataset.OutputBytes{Last10s: 7}, w.Features.OutputBytes)

	q := build(t, []step{{ms: 500, empty: true}, {ms: 2500, output: lineX}})
	q.write(t, q.raw.Bytes(), q.events.Bytes())
	ws = windows(t, q.data, time.Second)
	require.Len(t, ws, 2)
	require.True(t, ws[0].NoOutputYet, "an empty chunk is no first output")
	require.True(t, ws[1].NoOutputYet)
}

// TestCorruptEmptyChunkFailsClosed replays an io.chunk of no bytes whose
// digest, recording sequence or screen hash disagree with the recording:
// it is checked like any other, and the session yields no window.
func TestCorruptEmptyChunkFailsClosed(t *testing.T) {
	t.Parallel()
	for _, corrupt := range []string{"digest", "sequence", "hash"} {
		t.Run(corrupt, func(t *testing.T) {
			t.Parallel()
			p := build(t, []step{{ms: 100, output: lineHello}, {ms: 1500, empty: true, corrupt: corrupt}, {ms: 3000, output: "$ "}})
			p.write(t, p.raw.Bytes(), p.events.Bytes())
			_, err := Open(Options{DataDir: p.data, SessionID: sid})
			require.ErrorIs(t, err, ErrReplay)
		})
	}
}
