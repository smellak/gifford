//go:build linux

package agentlink

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

func detectorReport(generation int64, digest byte, busy bool) protocol.StateObservationReport {
	report := linkObservation(generation)
	report.Detector = &protocol.StateObservationDetector{
		Certain: true, IsCLI: true, Busy: busy,
		ScreenDigest: protocol.Digest("sha256:" + strings.Repeat(string(digest), 64)),
	}
	return report
}

// The link keeps the newest verdict of its connection. The quiet window
// runs on while the same verdict on the same screen and guard version
// comes in again, and restarts on any change; an older observation changes
// nothing, a report without a verdict clears it, and so does another
// connection's report.
func TestClientKeepsTheDetectorQuietWindow(t *testing.T) {
	t.Parallel()
	clk := newManualClock()
	c := &Client{opts: Options{Clock: clk}, changes: make(chan Snapshot, 1), generation: 4}
	advance := func(d time.Duration) {
		clk.mu.Lock()
		clk.now = clk.now.Add(d)
		clk.mu.Unlock()
	}
	start := clk.Now()

	c.takeDetector(4, detectorReport(1, 'a', false))
	require.NotNil(t, c.Snapshot().Detector)
	require.Equal(t, start, c.Snapshot().Detector.QuietSince)

	advance(2 * time.Second)
	c.takeDetector(4, detectorReport(2, 'a', false))
	got := c.Snapshot().Detector
	require.Equal(t, start, got.QuietSince, "the same screen keeps its window")
	require.Equal(t, start.Add(2*time.Second), got.ReceivedAt)
	require.Equal(t, int64(2), got.ObservationGeneration)

	c.takeDetector(4, detectorReport(1, 'b', true))
	require.Equal(t, int64(2), c.Snapshot().Detector.ObservationGeneration, "an older cut is ignored")

	advance(time.Second)
	c.takeDetector(4, detectorReport(3, 'b', false))
	require.Equal(t, start.Add(3*time.Second), c.Snapshot().Detector.QuietSince, "a new screen restarts it")

	advance(time.Second)
	c.takeDetector(4, detectorReport(4, 'b', true))
	require.Equal(t, start.Add(4*time.Second), c.Snapshot().Detector.QuietSince, "a new verdict restarts it")

	advance(time.Second)
	moved := detectorReport(5, 'b', true)
	moved.InputGuardVersion = 2
	c.takeDetector(4, moved)
	require.Equal(t, start.Add(5*time.Second), c.Snapshot().Detector.QuietSince, "a new guard version restarts it")

	c.takeDetector(3, detectorReport(6, 'b', true))
	require.Equal(t, int64(5), c.Snapshot().Detector.ObservationGeneration, "another connection's cut is ignored")

	c.takeDetector(4, linkObservation(7))
	require.Nil(t, c.Snapshot().Detector, "a cut without a verdict clears it")
}
