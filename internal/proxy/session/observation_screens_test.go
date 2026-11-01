//go:build unix

package session

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/vt"
)

var update = flag.Bool("update", false, "rewrite the golden files")

// observedScreen is what one observation reports about the screen.
type observedScreen struct {
	Case   string                           `json:"case"`
	State  protocol.StateObservationScreen  `json:"screen_state"`
	Rows   []string                         `json:"rows"`
	Cols   int64                            `json:"cols"`
	Cursor *protocol.StateObservationCursor `json:"cursor"`
}

// screenCase is one synthetic screen of a measured profile: the guard is
// closed on screen, then the harness writes after, if any, with nothing in
// flight.
type screenCase struct {
	name, profile, version, screen, after string
}

const (
	museProfile, museVersion         = "muse-code", "1.4.13-R6996.1"
	openCodeProfile, openCodeVersion = "opencode", "1.18.29"
)

func screenCases() []screenCase {
	muse, openCode := museObservationScreen(false), openCodeObservationScreen()
	return []screenCase{
		{"muse/proven", museProfile, museVersion, muse, ""},
		{"muse/two-boxes", museProfile, museVersion, muse, museObservationScreen(true)},
		{"muse/box-not-empty", museProfile, museVersion, muse, "draft text"},
		{"muse/cursor-off-the-box", museProfile, museVersion, muse, "\x1b[1;1H"},
		{"muse/cursor-hidden", museProfile, museVersion, muse, "\x1b[?25l"},
		{"muse/no-box", museProfile, museVersion, muse, clearScreen + "agent output\r\nno box here"},
		{"opencode/proven", openCodeProfile, openCodeVersion, openCode, ""},
		{"opencode/box-not-empty", openCodeProfile, openCodeVersion, openCode, "draft text"},
		{"opencode/cursor-off-the-box", openCodeProfile, openCodeVersion, openCode, "\x1b[1;1H"},
		{"opencode/cursor-hidden", openCodeProfile, openCodeVersion, openCode, "\x1b[?25l"},
	}
}

// TestObservationScreens pins what an observation reports for each
// synthetic screen: the screen with its editable area masked, or nothing.
// The golden file was written before the frame checks moved out of the
// session, so it proves the session reports the same screens since.
func TestObservationScreens(t *testing.T) {
	t.Parallel()

	cases := screenCases()
	got := make([]observedScreen, 0, len(cases))
	for _, c := range cases {
		profile := measuredObservationProfile(t, c.profile, c.version)
		o := newOrderRun(t, "screens-"+c.profile, func(opts *Options) { opts.Profile = profile })
		closeObservationOn(t, o, c.screen)
		if c.after != "" {
			o.s.onOutput([]byte(c.after))
			requireNothingInFlight(t, o.s)
		}
		r := observe(t, o.s)
		got = append(got, observedScreen{Case: c.name, State: r.ScreenState, Rows: r.ScreenRows, Cols: r.Cols, Cursor: r.Cursor})
	}
	raw, err := json.MarshalIndent(got, "", "  ")
	require.NoError(t, err)
	raw = append(raw, '\n')
	golden := filepath.Join("testdata", "observation_screens.golden.json")
	if *update {
		require.NoError(t, os.MkdirAll("testdata", 0o755))
		require.NoError(t, os.WriteFile(golden, raw, 0o600))
	}
	want, err := os.ReadFile(golden)
	require.NoError(t, err)
	require.Equal(t, string(want), string(raw))
}

// TestLiveScreenIsTheSessionsGate proves that, with the session's own
// conditions met, an observation carries exactly what LiveScreen gives for
// the session's last screen, and that LiveScreen names a reason for every
// screen the session withholds. Two boxes pass the input evidence and fail
// on the frame itself.
func TestLiveScreenIsTheSessionsGate(t *testing.T) {
	t.Parallel()

	named := []string{
		LiveScreenUnmeasured, LiveScreenNoInputEvidence, LiveScreenInputNotEmpty,
		LiveScreenOutOfBounds, LiveScreenNoInputArea,
	}
	for _, c := range screenCases() {
		profile := measuredObservationProfile(t, c.profile, c.version)
		o := newOrderRun(t, "gate-"+c.profile, func(opts *Options) { opts.Profile = profile })
		closeObservationOn(t, o, c.screen)
		if c.after != "" {
			o.s.onOutput([]byte(c.after))
			requireNothingInFlight(t, o.s)
		}
		r := observe(t, o.s)
		rows, reason := LiveScreen(profile, screenOf(o.s))
		if r.ScreenState == protocol.StateObservationScreenAvailable {
			require.Empty(t, reason, c.name)
			require.Equal(t, rows, r.ScreenRows, c.name)
			continue
		}
		requireWithheld(t, r)
		require.Contains(t, named, reason, c.name)
		require.Nil(t, rows, c.name)
		if c.name == "muse/two-boxes" {
			require.Equal(t, LiveScreenNoInputArea, reason)
		}
	}
}

func snapshotANSI(snap vt.Snapshot) string {
	var out strings.Builder
	out.WriteString(clearScreen)
	for row, line := range snap.Lines {
		if line != "" {
			_, _ = fmt.Fprintf(&out, "\x1b[%d;1H%s", row+1, line)
		}
	}
	out.WriteString("\x1b[0m")
	if snap.CursorRow >= 0 && snap.CursorRow < snap.Rows && snap.CursorCol >= 0 && snap.CursorCol < snap.Cols {
		_, _ = fmt.Fprintf(&out, "\x1b[%d;%dH", snap.CursorRow+1, snap.CursorCol+1)
	}
	if snap.CursorVisible {
		out.WriteString("\x1b[?25h")
	} else {
		out.WriteString("\x1b[?25l")
	}
	return out.String()
}

func TestPermissionDialogObservationIsCanonicalAndKeyInvalidatesIt(t *testing.T) {
	t.Parallel()
	profile := measuredObservationProfile(t, "opencode", "1.18.29")
	o := newOrderRun(t, "observe-permission", func(opts *Options) {
		opts.Profile = profile
		opts.HarnessVersion = openCodeRecordedVersion
		opts.PermissionDialogs = true
	})
	o.s.mu.Lock()
	require.NoError(t, o.s.emu.Resize(120, 50))
	o.s.lastCols, o.s.lastRows = 120, 50
	o.s.permissionDialog.Resize()
	o.s.mu.Unlock()
	modal := openCodePermission(false)
	o.s.onOutput([]byte(snapshotANSI(blankSnapshot(120, 50))))
	o.s.onOutput([]byte(snapshotANSI(modal)))
	requireWithheld(t, observe(t, o.s), PermissionDialogMarker)

	empty := newOpenCodePostTurnSnapshot(t, 120, 5)
	closeObservationOn(t, o, snapshotANSI(empty))

	prior := blankSnapshot(120, 50)
	prior.Lines[0] = "CANARY must not survive canonicalisation"
	o.s.onOutput([]byte(snapshotANSI(prior)))
	o.s.onOutput([]byte(snapshotANSI(modal)))
	r := observe(t, o.s)
	require.Equal(t, protocol.StateObservationScreenAvailable, r.ScreenState)
	require.Equal(t, []string{PermissionDialogMarker}, r.ScreenRows)
	require.Equal(t, int64(len(PermissionDialogMarker)), r.Cols)
	require.Equal(t, &protocol.StateObservationCursor{Row: 0, Col: 0}, r.Cursor)
	require.NotContains(t, strings.Join(r.ScreenRows, "\n"), "CANARY")

	frames := sinkPayloads(o.sink, EventTypePermissionDialogFrame)
	require.GreaterOrEqual(t, len(frames), 6)
	require.Equal(t, uint64(len(frames)), frames[len(frames)-1].(PermissionDialogFrameEvidence).Index)
	logged, err := json.Marshal(o.sink.all())
	require.NoError(t, err)
	require.NotContains(t, string(logged), "CANARY", "new observation evidence and logs are content-free")

	o.typed(t, "x")
	requireWithheld(t, observe(t, o.s), PermissionDialogMarker, "CANARY")
}
