package draft

import (
	"bytes"
	"fmt"
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
input_line: "^ *┃(?:  (.*))?$"
input_bounds:
  above:
    - "^ *┃$"
    - not: "^ *┃.*$"
  below:
    - "^ *┃$"
    - "^ *┃  Build · modelo$"
    - "^ *╹▀+$"
`

const areaFirstLine = "uno"

func areaProfile(t *testing.T, name, raw string) profiles.Profile {
	t.Helper()
	profile, err := profiles.ParseFile(name, []byte(raw))
	require.NoError(t, err)
	return profile
}

func areaVTScreen(t *testing.T, cursor int, suffix string, lines ...string) vt.Snapshot {
	t.Helper()
	emulator, err := vt.New(80, len(lines))
	require.NoError(t, err)
	var output bytes.Buffer
	output.WriteString("\x1b[2J")
	for row, line := range lines {
		fmt.Fprintf(&output, "\x1b[%d;1H%s", row+1, line)
	}
	fmt.Fprintf(&output, "\x1b[%d;3H%s", cursor+1, suffix)
	_, err = emulator.Write(output.Bytes())
	require.NoError(t, err)
	snapshot := emulator.Snapshot()
	require.Equal(t, cursor, snapshot.CursorRow)
	require.NoError(t, emulator.Close())
	return snapshot
}

type openCodeFrame struct {
	cols, rows int
	topRow     int
	outside    string
	content    []string
	cursor     int
	omitTop    bool
	suffix     string
}

// openCodeVTScreen draws the measured box through the production terminal
// emulator. In particular, a drawn "  ┃  " content row reaches the reader as
// "  ┃" because Snapshot trims trailing cells, just as it does in a session.
func openCodeVTScreen(t *testing.T, frame openCodeFrame) vt.Snapshot {
	t.Helper()
	if frame.cols == 0 {
		frame.cols = 40
	}
	if frame.rows == 0 {
		frame.rows = 12
	}
	if frame.topRow == 0 {
		frame.topRow = 3
	}
	require.NotEmpty(t, frame.content)
	require.GreaterOrEqual(t, frame.cursor, 0)
	require.Less(t, frame.cursor, len(frame.content))

	emulator, err := vt.New(frame.cols, frame.rows)
	require.NoError(t, err)
	var output bytes.Buffer
	output.WriteString("\x1b[2J")
	put := func(row int, text string) {
		if row < 1 || row > frame.rows {
			return
		}
		fmt.Fprintf(&output, "\x1b[%d;1H%s", row, text)
	}
	if frame.outside != "" {
		put(frame.topRow-1, frame.outside)
	}
	if !frame.omitTop {
		put(frame.topRow, "  ┃")
	}
	firstContentRow := frame.topRow + 1
	for index, text := range frame.content {
		put(firstContentRow+index, "  ┃  "+text)
	}
	bottomRow := firstContentRow + len(frame.content)
	put(bottomRow, "  ┃")
	put(bottomRow+1, "  ┃  Build · modelo")
	put(bottomRow+2, "  ╹▀▀▀▀▀▀▀▀")
	fmt.Fprintf(&output, "\x1b[%d;6H", firstContentRow+frame.cursor)
	output.WriteString(frame.suffix)
	_, err = emulator.Write(output.Bytes())
	require.NoError(t, err)
	snapshot := emulator.Snapshot()
	require.Equal(t, firstContentRow+frame.cursor-1, snapshot.CursorRow)
	require.NoError(t, emulator.Close())
	return snapshot
}

// TestMuseEmptyAreaRequiresBothBorders models the stable Muse box by hand.
// Clearing only the cursor row of a multiline draft cannot reproduce that
// canonical empty shape, wherever the cursor sits.
func TestMuseEmptyAreaRequiresBothBorders(t *testing.T) {
	t.Parallel()
	profile := areaProfile(t, "muse", museAreaYAML)
	canonical := areaVTScreen(t, 1, "", "────────", "❯ ", "────────", "Build · modelo")
	pending := areaVTScreen(t, 1, "e", "────────", "❯ ", "────────", "Build · modelo")
	syncOpen := areaVTScreen(t, 1, "\x1b[?2026h", "────────", "❯ ", "────────", "Build · modelo")
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
			snap: areaVTScreen(t, 1, "", "────────", "❯ ", "  dos", "────────", "Build · modelo"),
		},
		{
			name: "two rows with the first blank",
			snap: areaVTScreen(t, 1, "", "────────", "❯ ", "  ", "────────", "Build · modelo"),
		},
		{
			name: "cursor on a blank continuation",
			snap: areaVTScreen(t, 2, "", "────────", "❯ uno", "  ", "────────", "Build · modelo"),
		},
		{
			name: "typed rule on the first row cannot bound a blank continuation",
			snap: areaVTScreen(t, 2, "", "────────", "❯ ────", "  ", "────────", "Build · modelo"),
		},
		{
			name: "typed rule on a continuation cannot bound a blank first row",
			snap: areaVTScreen(t, 1, "", "────────", "❯ ", "  ────", "────────", "Build · modelo"),
		},
		{name: "top border is offscreen", snap: areaVTScreen(t, 0, "", "❯ ", "────────", "Build · modelo")},
		{name: "bottom border is cropped", snap: areaVTScreen(t, 1, "", "────────", "❯ ")},
		{name: "scroll replaced the top border", snap: areaVTScreen(t, 1, "", "older output", "❯ ", "────────")},
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

// TestOpenCodeEmptyAreaRejectsBlankContentRows draws the fixed OpenCode box
// from terminal bytes. Its blank content rows and filler rows both snapshot as
// "  ┃"; the existing exterior row is what distinguishes one canonical empty
// row from a multiline draft.
func TestOpenCodeEmptyAreaRejectsBlankContentRows(t *testing.T) {
	t.Parallel()
	profile := areaProfile(t, "opencode", openCodeAreaYAML)
	canonical := openCodeVTScreen(t, openCodeFrame{content: []string{""}})
	require.Equal(t, "  ┃", canonical.Lines[canonical.CursorRow], "the emulator trims the drawn spaces")
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
			name:  "canonical empty box at 80x24",
			snap:  openCodeVTScreen(t, openCodeFrame{cols: 80, rows: 24, content: []string{""}}),
			valid: true,
		},
		{
			name:  "canonical empty box at 120x40",
			snap:  openCodeVTScreen(t, openCodeFrame{cols: 120, rows: 40, content: []string{""}}),
			valid: true,
		},
		{
			name: "uno newline with cursor on the second row",
			snap: openCodeVTScreen(t, openCodeFrame{content: []string{areaFirstLine, ""}, cursor: 1}),
		},
		{
			name: "one newline leaves two blank rows",
			snap: openCodeVTScreen(t, openCodeFrame{content: []string{"", ""}, cursor: 1}),
		},
		{
			name: "two newlines leave three blank rows",
			snap: openCodeVTScreen(t, openCodeFrame{content: []string{"", "", ""}, cursor: 2}),
		},
		{
			name: "three newlines leave four blank rows",
			snap: openCodeVTScreen(t, openCodeFrame{content: []string{"", "", "", ""}, cursor: 3}),
		},
		{
			name: "text above two blank rows",
			snap: openCodeVTScreen(t, openCodeFrame{content: []string{areaFirstLine, "", ""}, cursor: 2}),
		},
		{
			name: "newline x with cursor on the first row",
			snap: openCodeVTScreen(t, openCodeFrame{content: []string{"", "x"}}),
		},
		{
			name: "cursor in a blank middle row",
			snap: openCodeVTScreen(t, openCodeFrame{content: []string{areaFirstLine, "", "dos"}, cursor: 1}),
		},
		{
			name: "line exactly fills the width before a blank continuation",
			snap: openCodeVTScreen(t, openCodeFrame{content: []string{strings.Repeat("x", 35), ""}, cursor: 1}),
		},
		{
			name: "typed bottom rule on the first row cannot bound a blank continuation",
			snap: openCodeVTScreen(t, openCodeFrame{content: []string{"▀▀▀", ""}, cursor: 1}),
		},
		{
			name: "typed bottom rule on a continuation cannot bound a blank first row",
			snap: openCodeVTScreen(t, openCodeFrame{content: []string{"", "▀▀▀"}}),
		},
		{
			name: "another box row cannot be the exterior boundary",
			snap: openCodeVTScreen(t, openCodeFrame{outside: "  ┃", content: []string{""}}),
		},
		{
			name: "exterior boundary is offscreen",
			snap: openCodeVTScreen(t, openCodeFrame{topRow: 1, content: []string{""}}),
		},
		{
			name: "bottom border is cropped",
			snap: openCodeVTScreen(t, openCodeFrame{rows: 6, content: []string{""}}),
		},
		{
			name: "partial redraw is missing top filler",
			snap: openCodeVTScreen(t, openCodeFrame{content: []string{""}, omitTop: true}),
		},
		{
			name: "terminal parser holds an incomplete redraw",
			snap: openCodeVTScreen(t, openCodeFrame{content: []string{""}, suffix: "e"}),
		},
		{
			name: "synchronized redraw remains open",
			snap: openCodeVTScreen(t, openCodeFrame{content: []string{""}, suffix: "\x1b[?2026h"}),
		},
		{
			name:  "nonempty cursor row is still read",
			snap:  openCodeVTScreen(t, openCodeFrame{content: []string{"tail"}}),
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
	text, ok := CursorInput(profile, openCodeVTScreen(t, openCodeFrame{content: []string{""}}))
	require.False(t, ok, "an empty row without both bounds is no evidence")
	require.Empty(t, text)
	text, ok = CursorInput(profile, openCodeVTScreen(t, openCodeFrame{content: []string{"still a draft"}}))
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
		guard.Output(areaVTScreen(t, 1, "", "────────", "❯ ", "────────", "Build · modelo"))
		clock.advance(reactionQuiet)
		require.Equal(t, []Outcome{{Kind: OutcomeCancelled, Generation: 1}}, guard.Poll())
		require.Equal(t, StateClosed, guard.State())
	})

	t.Run("clear line on the second row leaves other text", func(t *testing.T) {
		t.Parallel()
		clock := &testClock{now: time.Now()}
		guard := NewGuard(clock, areaProfile(t, "opencode", openCodeAreaYAML))
		guard.Feed(txt(areaFirstLine))
		guard.Feed(ev(input.KindInsertNewline))
		guard.Feed(txt("dos"))
		guard.Feed(ev(input.KindClearLine))
		guard.Output(openCodeVTScreen(t, openCodeFrame{content: []string{areaFirstLine, ""}, cursor: 1}))
		clock.advance(reactionQuiet)
		require.Empty(t, guard.Poll())
		require.Equal(t, StateOpen, guard.State())
		require.False(t, guard.InjectionAllowed())
	})

	t.Run("clear line above two blank rows leaves the earlier text", func(t *testing.T) {
		t.Parallel()
		clock := &testClock{now: time.Now()}
		guard := NewGuard(clock, areaProfile(t, "opencode", openCodeAreaYAML))
		guard.Feed(txt(areaFirstLine))
		guard.Feed(ev(input.KindInsertNewline))
		guard.Feed(ev(input.KindInsertNewline))
		guard.Feed(ev(input.KindInsertNewline))
		guard.Feed(ev(input.KindClearLine))
		guard.Output(openCodeVTScreen(t, openCodeFrame{content: []string{areaFirstLine, "", ""}, cursor: 2}))
		clock.advance(reactionQuiet)
		require.Empty(t, guard.Poll())
		require.Equal(t, StateOpen, guard.State())
		require.False(t, guard.InjectionAllowed())
	})

	t.Run("muse clear line on the first row leaves the second row", func(t *testing.T) {
		t.Parallel()
		clock := &testClock{now: time.Now()}
		guard := NewGuard(clock, areaProfile(t, "muse", museAreaYAML))
		guard.Feed(txt(areaFirstLine))
		guard.Feed(ev(input.KindInsertNewline))
		guard.Feed(txt("dos"))
		guard.Feed(key(input.KeyArrowUp))
		guard.Feed(ev(input.KindClearLine))
		guard.Output(areaVTScreen(t, 1, "", "────────", "❯ ", "  dos", "────────", "Build · modelo"))
		clock.advance(reactionQuiet)
		require.Empty(t, guard.Poll())
		require.Equal(t, StateOpen, guard.State())
		require.False(t, guard.InjectionAllowed())
	})
}
