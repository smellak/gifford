//go:build unix

package session

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/creack/pty"
	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/detect"
	"github.com/smellak/gifford/internal/profiles"
)

// detectorRun is a session of profile on an outer terminal of the bench
// frames' 120x40 geometry, with one detector bench frame on its screen.
func detectorRun(t *testing.T, profile profiles.Profile, frame string) *orderRun {
	t.Helper()
	_, out := testPTY(t)
	require.NoError(t, pty.Setsize(out, &pty.Winsize{Cols: 120, Rows: 40}))
	o := newOrderRun(t, "det-"+frame, func(opts *Options) { opts.Profile, opts.Out = profile, out })
	raw, err := os.ReadFile(filepath.Join("..", "..", "detect", "testdata", frame))
	require.NoError(t, err)
	o.s.onOutput(append([]byte(clearScreen), detect.NormalizeCapture(raw)...))
	requireNothingInFlight(t, o.s)
	return o
}

// The Proxy reads its live screen with the detector as the tmux route reads
// a pane dump: the measured Claude bench frames give the same verdicts as
// Classify gives the dumps, and every verdict carries a digest of the
// visible text.
func TestObservationCarriesTheDetectorVerdict(t *testing.T) {
	t.Parallel()
	profile := measuredObservationProfile(t, "claude-code", "2.1.280")
	for frame, want := range map[string]struct{ certain, busy, boxBusy bool }{
		"claude-canon-idle-01.ansi":       {certain: true},
		"claude-idle-empty.ansi":          {certain: true},
		"claude-working.ansi":             {certain: true, busy: true},
		"claude-idle-pending-unsent.ansi": {certain: true, boxBusy: true},
		"claude-permission-dialog.ansi":   {},
	} {
		r := observe(t, detectorRun(t, profile, frame).s)
		require.NotNil(t, r.Detector, frame)
		d := *r.Detector
		require.Equal(t, want.certain, d.Certain, "%s certain (detail %s)", frame, d.Detail)
		require.Equal(t, want.busy, d.Busy, "%s busy", frame)
		require.Equal(t, want.boxBusy, d.BoxBusy, "%s box (detail %s)", frame, d.Detail)
		require.True(t, d.IsCLI, frame)
		require.True(t, d.ScreenDigest.Valid(), frame)
	}
}

// The same screen gives the same digest, another screen another one.
func TestDetectorDigestFollowsTheScreen(t *testing.T) {
	t.Parallel()
	profile := measuredObservationProfile(t, "claude-code", "2.1.280")
	o := detectorRun(t, profile, "claude-canon-idle-01.ansi")
	first := observe(t, o.s).Detector
	again := observe(t, o.s).Detector
	require.Equal(t, first.ScreenDigest, again.ScreenDigest)
	o.s.onOutput([]byte(clearScreen + "another screen"))
	requireNothingInFlight(t, o.s)
	require.NotEqual(t, first.ScreenDigest, observe(t, o.s).Detector.ScreenDigest)
}

// An unmeasured profile, or a build outside the measured range, gives no
// verdict: the Core keeps asking for approval.
func TestUnmeasuredProfileSendsNoDetectorVerdict(t *testing.T) {
	t.Parallel()
	bank, err := profiles.Load()
	require.NoError(t, err)
	for _, version := range []string{"2.1.318", "1.0"} {
		profile, err := bank.Profile("claude-code", version)
		require.NoError(t, err)
		require.False(t, profile.Measured, version)
		r := observe(t, detectorRun(t, profile, "claude-canon-idle-01.ansi").s)
		require.Nil(t, r.Detector, version)
	}
}
