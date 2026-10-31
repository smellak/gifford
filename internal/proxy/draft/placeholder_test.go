package draft

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/profiles"
	"github.com/smellak/gifford/internal/proxy/input"
	"github.com/smellak/gifford/internal/vt"
)

// hint is the input placeholder of the placeholder profile.
const hint = "Ask the agent anything"

// SGR and cursor sequences the placeholder screens draw with.
const (
	dim           = "\x1b[2m"
	dimUnder      = "\x1b[2;4m"
	reset         = "\x1b[0m"
	toHint        = "\x1b[1;3H"
	toBoundedHint = "\x1b[1A\r\x1b[2C"
	syncBegin     = "\x1b[?2026h"
	syncEnd       = "\x1b[?2026l"
)

// placeholderBox is the empty input line of the placeholder profile: the
// prompt, then the hint dimmed, the cursor on its first column.
const (
	placeholderBox = testInputTop + "\r\n› " + dim + hint + reset + "\r\n" +
		testInputBottom + toBoundedHint
	plainEmptyBox = testInputTop + "\r\n› \r\n" + testInputBottom + toBoundedHint + reset
)

// placeholderYAML is a measured profile for one exact version that
// declares an input placeholder; withHint false leaves the placeholder
// out, for the same profile without one.
func placeholderYAML(withHint bool) string {
	y := `harness: synth
measured: true
version_range: "1.2.3"
submit: "\r"
insert_newline: "\n"
clear_line: "\u0015"
cancel: "\u0003"
bracketed_paste: false
extended_keyboard: none
input_line: "^› ?(.*)$"
input_bounds:
  above: ["^input-top$"]
  below: ["^input-bottom$"]
`
	if withHint {
		y += `input_placeholder: "` + hint + `"` + "\n"
	}
	return y
}

func placeholderProfile(t *testing.T, withHint bool) profiles.Profile {
	t.Helper()
	p, err := profiles.ParseFile("synth", []byte(placeholderYAML(withHint)))
	require.NoError(t, err)
	return p
}

// painted is the screen raw draws on a fresh emulator, cell styles
// included.
func painted(t *testing.T, raw string) vt.Snapshot {
	t.Helper()
	e, err := vt.New(40, 6)
	require.NoError(t, err)
	defer func() { _ = e.Close() }()
	_, err = e.Write([]byte(raw))
	require.NoError(t, err)
	return e.Snapshot()
}

// TestCursorInputReadsThePlaceholder reads the input line of a profile
// that declares an input placeholder: it is empty only as the whole
// placeholder, dimmed in no other style, the cursor on its first
// column, on a whole frame. Typed text, the placeholder's own included,
// is a draft. The prompt alone, mixed styles, other dimmed text, the
// cursor elsewhere, no style evidence or a cell the screen does not
// account for prove nothing. Without a placeholder the style counts for
// nothing.
func TestCursorInputReadsThePlaceholder(t *testing.T) {
	t.Parallel()
	withHint := placeholderProfile(t, true)
	noHint := placeholderProfile(t, false)
	noStyle := painted(t, placeholderBox)
	noStyle.CursorCells = nil
	unaccounted := painted(t, placeholderBox)
	unaccounted.CursorCells = append([]vt.CursorCell(nil), unaccounted.CursorCells...)
	unaccounted.CursorCells[4].Text = "x"
	pending := painted(t, placeholderBox)
	pending.Pending = true
	for _, tc := range []struct {
		name    string
		profile profiles.Profile
		snap    vt.Snapshot
		text    string
		ok      bool
	}{
		{name: "the placeholder, dimmed, the cursor on it", profile: withHint, snap: painted(t, placeholderBox), ok: true},
		{name: "a synchronized update that ended", profile: withHint, snap: painted(t, syncBegin+placeholderBox+syncEnd), ok: true},
		{name: "a draft", profile: withHint, snap: painted(t, "› hello"+reset), text: "hello", ok: true},
		{name: "the placeholder's text, typed", profile: withHint, snap: painted(t, "› "+hint+reset), text: hint, ok: true},
		{name: "the placeholder's text, typed, the cursor on it", profile: withHint, snap: painted(t, "› "+hint+reset+toHint), text: hint, ok: true},
		{name: "the prompt alone", profile: withHint, snap: painted(t, "› "+reset+toHint)},
		{name: "the placeholder partly dimmed", profile: withHint, snap: painted(t, "› "+dim+"Ask the"+reset+" agent anything"+toHint)},
		{name: "a draft with a dimmed suggestion", profile: withHint, snap: painted(t, "› hel"+dim+"lo there"+reset)},
		{name: "the placeholder dimmed in another style too", profile: withHint, snap: painted(t, "› "+dimUnder+hint+reset+toHint)},
		{name: "other dimmed text", profile: withHint, snap: painted(t, "› "+dim+"Ask the agent"+reset+toHint)},
		{name: "the placeholder with the cursor after it", profile: withHint, snap: painted(t, "› "+dim+hint+reset)},
		{name: "the placeholder inside an open synchronized update", profile: withHint, snap: painted(t, syncBegin+placeholderBox)},
		{name: "the placeholder with bytes held back", profile: withHint, snap: pending},
		{name: "no style evidence", profile: withHint, snap: noStyle},
		{name: "a cell the screen does not account for", profile: withHint, snap: unaccounted},
		{name: "no placeholder, a dimmed capture", profile: noHint, snap: painted(t, placeholderBox), text: hint, ok: true},
		{name: "no placeholder, the prompt alone", profile: noHint, snap: painted(t, plainEmptyBox), ok: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			text, ok := CursorInput(tc.profile, tc.snap)
			require.Equal(t, tc.ok, ok)
			require.Equal(t, tc.text, text)
		})
	}
}

// TestOpenSyncUpdateNeverSettles answers a submit with an empty input
// line inside a synchronized update that stays open: however long it
// is quiet, the guard waits. The update's end is output of its own,
// and once that is quiet the submit closes.
func TestOpenSyncUpdateNeverSettles(t *testing.T) {
	t.Parallel()
	clk := &testClock{now: time.Now()}
	g := NewGuard(clk, mustMeasured())
	g.Feed(txt("h"))
	g.Feed(ev(input.KindSubmit))
	open := screen("> ")
	open.SyncOpen = true
	g.Output(open)
	clk.advance(time.Hour)
	require.Empty(t, g.Poll())
	require.Empty(t, g.Observe(open))
	require.Equal(t, StateSubmitting, g.State())

	g.Output(screen("> "))
	clk.advance(reactionQuiet - time.Nanosecond)
	require.Empty(t, g.Poll())
	clk.advance(time.Nanosecond)
	require.Equal(t, []Outcome{{Kind: OutcomeSubmitted, Generation: 1}}, g.Poll())
}

// TestPlaceholderSettlesTheDraft answers a submit on a placeholder
// profile: the placeholder, dimmed, closes it; the prompt alone keeps
// it waiting for more; the placeholder's text left undimmed is a
// draft, so the draft is open again, and the submit waits on: the
// dimmed placeholder drawn after that text still closes it.
func TestPlaceholderSettlesTheDraft(t *testing.T) {
	t.Parallel()
	p := placeholderProfile(t, true)
	start := func() (*Guard, *testClock) {
		clk := &testClock{now: time.Now()}
		g := NewGuard(clk, p)
		g.Feed(txt("h"))
		g.Feed(ev(input.KindSubmit))
		return g, clk
	}
	reply := func(g *Guard, clk *testClock, raw string) []Outcome {
		g.Output(painted(t, raw))
		clk.advance(reactionQuiet)
		return g.Poll()
	}

	g, clk := start()
	require.Equal(t, []Outcome{{Kind: OutcomeSubmitted, Generation: 1}}, reply(g, clk, placeholderBox))

	g, clk = start()
	require.Empty(t, reply(g, clk, "› "+reset+toHint), "the prompt alone proves nothing")
	require.Equal(t, StateSubmitting, g.State())
	require.Equal(t, []Outcome{{Kind: OutcomeSubmitted, Generation: 1}}, reply(g, clk, placeholderBox))

	g, clk = start()
	require.Empty(t, reply(g, clk, "› "+hint+reset+toHint))
	require.Equal(t, StateOpen, g.State(), "the placeholder's text, undimmed, is a draft")
	require.Equal(t, []Outcome{{Kind: OutcomeSubmitted, Generation: 1}}, reply(g, clk, placeholderBox),
		"the submit waits on past the text")
	require.Equal(t, StateClosed, g.State())
}
