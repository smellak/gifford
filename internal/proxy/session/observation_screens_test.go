//go:build unix

package session

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
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

	var got []observedScreen
	for _, c := range screenCases() {
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
