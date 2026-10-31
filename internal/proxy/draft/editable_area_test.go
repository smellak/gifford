package draft

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/profiles"
	"github.com/smellak/gifford/internal/proxy/input"
	"github.com/smellak/gifford/internal/vt"
)

const museAreaYAML = `harness: muse
measured: true
version_range: "1.4.13-R6996.1"
submit: "\r"
insert_newline: "\u001b\r"
clear_line: "\u0015"
cancel: "\u0003"
bracketed_paste: false
extended_keyboard: none
input_line: "^❯ ?(.*)$"
input_bounds:
  above: ["^─+$"]
  below: ["^─+$"]
`

const openCodeAreaYAML = `harness: opencode
measured: true
version_range: "1.18.29"
submit: "\r"
insert_newline: "\n"
clear_line: "\u0015"
cancel: "\u0003"
bracketed_paste: false
extended_keyboard: none
input_line: "^  ┃  (.*)$"
input_bounds:
  above:
    - "^  ┃$"
  below:
    - "^  ┃$"
    - "^  ┃  Build · modelo$"
    - "^  ╹▀+$"
`

func areaProfile(t *testing.T, name, raw string) profiles.Profile {
	t.Helper()
	profile, err := profiles.ParseFile(name, []byte(raw))
	require.NoError(t, err)
	return profile
}

func areaScreen(cursor int, lines ...string) vt.Snapshot {
	return vt.Snapshot{
		Cols: 80, Rows: len(lines), Lines: lines,
		CursorRow: cursor, CursorCol: 5, CursorVisible: true,
	}
}

// TestMuseEmptyAreaRequiresBothBorders models the stable Muse box by hand.
// Clearing only the cursor row of a multiline draft cannot reproduce that
// canonical empty shape, wherever the cursor sits.
func TestMuseEmptyAreaRequiresBothBorders(t *testing.T) {
	t.Parallel()
	profile := areaProfile(t, "muse", museAreaYAML)
	canonical := areaScreen(1, "────────", "❯ ", "────────", "Build · modelo")
	pending := canonical
	pending.Pending = true
	syncOpen := canonical
	syncOpen.SyncOpen = true
	for _, tc := range []struct {
		name  string
		snap  vt.Snapshot
		text  string
		valid bool
	}{
		{
			name:  "canonical empty box",
			snap:  canonical,
			valid: true,
		},
		{
			name: "uno newline dos with first row cleared",
			snap: areaScreen(1, "────────", "❯ ", "  dos", "────────", "Build · modelo"),
		},
		{
			name: "two rows with the first blank",
			snap: areaScreen(1, "────────", "❯ ", "  ", "────────", "Build · modelo"),
		},
		{
			name: "cursor on a blank continuation",
			snap: areaScreen(2, "────────", "❯ uno", "  ", "────────", "Build · modelo"),
		},
		{
			name: "typed rule on the first row cannot bound a blank continuation",
			snap: areaScreen(2, "────────", "❯ ────", "  ", "────────", "Build · modelo"),
		},
		{
			name: "typed rule on a continuation cannot bound a blank first row",
			snap: areaScreen(1, "────────", "❯ ", "  ────", "────────", "Build · modelo"),
		},
		{name: "top border is offscreen", snap: areaScreen(0, "❯ ", "────────", "Build · modelo")},
		{name: "bottom border is cropped", snap: areaScreen(1, "────────", "❯ ")},
		{name: "scroll replaced the top border", snap: areaScreen(1, "older output", "❯ ", "────────")},
		{name: "incomplete pending redraw", snap: pending},
		{name: "open synchronized redraw", snap: syncOpen},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			text, ok := CursorInput(profile, tc.snap)
			require.Equal(t, tc.valid, ok)
			require.Equal(t, tc.text, text)
		})
	}
}

// TestOpenCodeEmptyAreaRejectsBlankContentRows models the fixed OpenCode
// filler, agent and bottom-border rows. A blank content row is visually like
// an empty cursor row, but its position relative to both borders is not.
func TestOpenCodeEmptyAreaRejectsBlankContentRows(t *testing.T) {
	t.Parallel()
	profile := areaProfile(t, "opencode", openCodeAreaYAML)
	full := "  ┃  " + strings.Repeat("x", 75)
	for _, tc := range []struct {
		name  string
		snap  vt.Snapshot
		text  string
		valid bool
	}{
		{
			name:  "canonical empty box",
			snap:  areaScreen(1, "  ┃", "  ┃  ", "  ┃", "  ┃  Build · modelo", "  ╹▀▀▀"),
			valid: true,
		},
		{
			name: "uno newline with cursor on the second row",
			snap: areaScreen(2, "  ┃", "  ┃  uno", "  ┃  ", "  ┃", "  ┃  Build · modelo", "  ╹▀▀▀"),
		},
		{
			name: "newline in an empty box leaves two blank rows",
			snap: areaScreen(2, "  ┃", "  ┃  ", "  ┃  ", "  ┃", "  ┃  Build · modelo", "  ╹▀▀▀"),
		},
		{
			name: "newline x with cursor on the first row",
			snap: areaScreen(1, "  ┃", "  ┃  ", "  ┃  x", "  ┃", "  ┃  Build · modelo", "  ╹▀▀▀"),
		},
		{
			name: "line exactly fills the width and cursor wraps blank",
			snap: areaScreen(2, "  ┃", full, "  ┃  ", "  ┃", "  ┃  Build · modelo", "  ╹▀▀▀"),
		},
		{
			name: "typed bottom rule on the first row cannot bound a blank continuation",
			snap: areaScreen(2, "  ┃", "  ┃  ▀▀▀", "  ┃  ", "  ┃", "  ┃  Build · modelo", "  ╹▀▀▀"),
		},
		{
			name: "typed bottom rule on a continuation cannot bound a blank first row",
			snap: areaScreen(1, "  ┃", "  ┃  ", "  ┃  ▀▀▀", "  ┃", "  ┃  Build · modelo", "  ╹▀▀▀"),
		},
		{
			name:  "wrapped continuation text is still read",
			snap:  areaScreen(2, "  ┃", full, "  ┃  tail", "  ┃", "  ┃  Build · modelo", "  ╹▀▀▀"),
			text:  "tail",
			valid: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			text, ok := CursorInput(profile, tc.snap)
			require.Equal(t, tc.valid, ok)
			require.Equal(t, tc.text, text)
		})
	}
}

func TestNonemptyInputDoesNotRequireEmptyAreaBounds(t *testing.T) {
	t.Parallel()
	profile := areaProfile(t, "opencode", openCodeAreaYAML)
	profile.InputBounds = profiles.InputBounds{}
	text, ok := CursorInput(profile, areaScreen(1, "  ┃", "  ┃  ", "  ┃", "  ┃  Build · modelo", "  ╹▀▀▀"))
	require.False(t, ok, "an empty row without both bounds is no evidence")
	require.Empty(t, text)
	text, ok = CursorInput(profile, areaScreen(0, "  ┃  still a draft"))
	require.True(t, ok)
	require.Equal(t, "still a draft", text)
}

// TestAreaBoundsControlGuardSettlement proves the reader's result drives the
// guard: cancel may close only on the canonical empty box, while clear_line on
// the second row of a multiline draft leaves the draft OPEN.
func TestAreaBoundsControlGuardSettlement(t *testing.T) {
	t.Parallel()

	t.Run("cancel empties the complete box", func(t *testing.T) {
		t.Parallel()
		clock := &testClock{now: time.Now()}
		guard := NewGuard(clock, areaProfile(t, "muse", museAreaYAML))
		guard.Feed(txt("draft"))
		guard.Feed(ev(input.KindCancel))
		guard.Output(areaScreen(1, "────────", "❯ ", "────────", "Build · modelo"))
		clock.advance(reactionQuiet)
		require.Equal(t, []Outcome{{Kind: OutcomeCancelled, Generation: 1}}, guard.Poll())
		require.Equal(t, StateClosed, guard.State())
	})

	t.Run("clear line on the second row leaves other text", func(t *testing.T) {
		t.Parallel()
		clock := &testClock{now: time.Now()}
		guard := NewGuard(clock, areaProfile(t, "opencode", openCodeAreaYAML))
		guard.Feed(txt("uno"))
		guard.Feed(ev(input.KindInsertNewline))
		guard.Feed(txt("dos"))
		guard.Feed(ev(input.KindClearLine))
		guard.Output(areaScreen(2, "  ┃", "  ┃  uno", "  ┃  ", "  ┃", "  ┃  Build · modelo", "  ╹▀▀▀"))
		clock.advance(reactionQuiet)
		require.Empty(t, guard.Poll())
		require.Equal(t, StateOpen, guard.State())
		require.False(t, guard.InjectionAllowed())
	})

	t.Run("muse clear line on the first row leaves the second row", func(t *testing.T) {
		t.Parallel()
		clock := &testClock{now: time.Now()}
		guard := NewGuard(clock, areaProfile(t, "muse", museAreaYAML))
		guard.Feed(txt("uno"))
		guard.Feed(ev(input.KindInsertNewline))
		guard.Feed(txt("dos"))
		guard.Feed(key(input.KeyArrowUp))
		guard.Feed(ev(input.KindClearLine))
		guard.Output(areaScreen(1, "────────", "❯ ", "  dos", "────────", "Build · modelo"))
		clock.advance(reactionQuiet)
		require.Empty(t, guard.Poll())
		require.Equal(t, StateOpen, guard.State())
		require.False(t, guard.InjectionAllowed())
	})
}
