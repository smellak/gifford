package replay

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/profiles"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/session"
	"github.com/smellak/gifford/internal/vt"
)

// openCodeBox draws a synthetic OpenCode screen, its measured input box
// empty with the cursor in it; boxText is what typing into it adds. Each
// ends in a reset, which releases the last rune the emulator holds back.
const boxText = "\x1b[Kdraft text\x1b[0m"

var openCodeBox = "\x1b[H\x1b[2J\x1b[16;1Hagent output" +
	"\x1b[17;3H┃\x1b[18;3H┃  \x1b[38;5;8;48;5;234mAsk anything\x1b[0m\x1b[19;3H┃" +
	"\x1b[20;3H┃  Build · modelo\x1b[21;3H╹" + strings.Repeat("▀", 75) +
	"\x1b[18;6H\x1b[0m"

// The guard states the session's input.draft_* events land on, and the
// row the gate puts in the input box's place.
const (
	guardOpen, guardShut = "open", "closed"
	maskedRow            = "[input]"
)

// liveSession is an OpenCode session at version: its box empty under a
// closed guard, then the guard open, closed with text in the box, the
// cursor off the box, and the box empty again.
func liveSession(t *testing.T, version string) (*pair, []step) {
	t.Helper()
	steps := []step{
		{ms: 100, output: openCodeBox},
		{ms: 1500, draft: guardOpen},
		{ms: 2500, draft: guardShut},
		{ms: 2600, output: boxText},
		{ms: 3500, output: openCodeBox + "\x1b[1;1H"},
		{ms: 4500, output: openCodeBox},
		{ms: 5000, other: protocol.EventTypeInputKeyChunk},
	}
	opened := session.PTYOpened{Cols: 80, Rows: 24, Harness: protocol.HarnessOpencode, HarnessVersion: version}
	p := buildOpened(t, opened, true, steps)
	p.write(t, p.raw.Bytes(), p.events.Bytes())
	return p, steps
}

func drain(t *testing.T, src dataset.RecordingSource) []dataset.Recording {
	t.Helper()
	var out []dataset.Recording
	for {
		r, err := src.Next(context.Background())
		if errors.Is(err, io.EOF) {
			return out
		}
		require.NoError(t, err)
		out = append(out, r)
	}
}

func loadProfiles(t *testing.T) profiles.Profiles {
	t.Helper()
	ps, err := profiles.Load()
	require.NoError(t, err)
	return ps
}

// screenAt is the screen the steps drew by the anchor.
func screenAt(t *testing.T, steps []step, anchorMS int64) vt.Snapshot {
	t.Helper()
	emu, err := vt.New(80, 24)
	require.NoError(t, err)
	defer func() { _ = emu.Close() }()
	for _, s := range steps {
		if s.output != "" && s.ms <= anchorMS {
			_, err := emu.Write([]byte(s.output))
			require.NoError(t, err)
		}
	}
	return emu.Snapshot()
}

// TestLiveForm: a window in live form carries what the session's own gate
// gives for its screen, the input box masked and the draft closed, or no
// screen and the gate's reason, or the guard's while it is not closed. The
// guard is closed until the first draft event, as a session's starts. The
// windows keep their ids, and a replay without Live is as before.
func TestLiveForm(t *testing.T) {
	t.Parallel()
	p, steps := liveSession(t, "1.18.29")
	ps := loadProfiles(t)
	profile, err := ps.Profile(string(protocol.HarnessOpencode), "1.18.29")
	require.NoError(t, err)
	require.True(t, profile.Measured)

	plain := windows(t, p.data, time.Second)
	src, err := Open(Options{DataDir: p.data, SessionID: sid, Every: time.Second, Live: &ps})
	require.NoError(t, err)
	live := drain(t, src)
	require.Len(t, live, 5)
	require.Len(t, plain, len(live))

	want := []string{"", LiveDraftNotClosed, session.LiveScreenInputNotEmpty, session.LiveScreenNoInputEvidence, ""}
	for i, w := range live {
		anchor := int64(i+1) * 1000
		require.Equal(t, plain[i].WindowID, w.WindowID)
		require.Empty(t, plain[i].LiveWithheld)
		require.Equal(t, screenAt(t, steps, anchor).Lines, plain[i].Screen.Rows, "without Live the screen is the replayed one")
		require.Equal(t, want[i], w.LiveWithheld, "window %d", i+1)
		if w.LiveWithheld == "" {
			require.Equal(t, dataset.DraftClosed, w.Draft)
		} else {
			require.Nil(t, w.Screen.Rows, "a withheld window has no screen")
		}
		if want[i] == LiveDraftNotClosed {
			continue
		}
		rows, reason := session.LiveScreen(profile, screenAt(t, steps, anchor))
		require.Equal(t, reason, w.LiveWithheld, "window %d", i+1)
		require.Equal(t, rows, w.Screen.Rows, "window %d", i+1)
	}
	require.Equal(t, maskedRow, live[0].Screen.Rows[16])
	require.Equal(t, maskedRow, live[0].Screen.Rows[17])
	require.Equal(t, maskedRow, live[0].Screen.Rows[18])
	require.Equal(t, maskedRow, live[0].Screen.Rows[19])
	require.Equal(t, "  ╹"+strings.Repeat("▀", 75), live[0].Screen.Rows[20])
	require.Contains(t, live[0].Screen.Rows, "agent output")
	require.Equal(t, dataset.DraftUnknown, plain[0].Draft)
	require.Equal(t, live[0].Screen.Rows, live[4].Screen.Rows)
}

// TestLiveFormProfile: a version the harness's profile does not cover
// falls back, as gifford run --agent does, to no measurement, so every
// screen the guard does not already withhold is withheld as unmeasured.
// OpenAllLive cuts every session in live form.
func TestLiveFormProfile(t *testing.T) {
	t.Parallel()
	ps := loadProfiles(t)
	for _, version := range []string{"1.18.30", session.UnknownHarnessVersion} {
		p, _ := liveSession(t, version)
		src, err := OpenAllLive(p.data, time.Second, ps, nil)
		require.NoError(t, err)
		live := drain(t, src)
		require.Len(t, live, 5)
		for i, w := range live {
			want := session.LiveScreenUnmeasured
			if i == 1 {
				want = LiveDraftNotClosed
			}
			require.Equal(t, want, w.LiveWithheld, "version %s, window %d", version, i+1)
		}
	}

	p, _ := liveSession(t, "1.18.29")
	src, err := OpenAllLive(p.data, time.Second, ps, nil)
	require.NoError(t, err)
	require.Empty(t, drain(t, src)[0].LiveWithheld)
}
