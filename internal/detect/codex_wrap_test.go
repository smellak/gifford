package detect

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// wrapNotice has the shape of a configured notice that a narrow Codex
// pane wrapped onto three rows: a long last word, as a tool name is.
const wrapNotice = ": [GIFFORD] 1 pending message: check list_pending_messages"

// codexWrapCols is the width of the measured narrow Codex panes.
const codexWrapCols = 33

// The notice's rows in the measured 33-column box.
const (
	wrapFirstRow  = ": [GIFFORD] 1 pending"
	wrapMiddleRow = "message: check"
	wrapLastRow   = "list_pending_messages"
)

// hyphenNotice wraps after a hyphen, where a break drops no space.
const hyphenNotice = ": [GIFFORD] 1 pending message: run gifford-inbox-check"

// codexNarrowCapture draws a narrow Codex pane as capture-pane prints it,
// laid out like a measured 33-column frame: transcript, two blank rows,
// the box with its bold accent prompt and two-cell continuation indent,
// one blank row and the footer. Transcript and footer stay short enough
// for the narrowest pane the tests draw.
func codexNarrowCapture(box ...string) []byte {
	lines := []string{
		"",
		"\x1b[2m• \x1b[0mChecked it.",
		"  Done.",
		" ",
		" ",
		"\x1b[1m\x1b[38;5;215m›\x1b[0m " + box[0],
	}
	for _, row := range box[1:] {
		lines = append(lines, "  "+row)
	}
	return []byte(strings.Join(append(lines, " ", "  model · ~"), "\n"))
}

// codexWrap lays text out the way the measured Codex box does: words
// fill each row of width cells, a break drops its space, a hyphenated
// word may break after any hyphen, and a word wider than the row breaks
// at the row edge.
func codexWrap(text string, width int) []string {
	var rows []string
	line := ""
	for _, word := range strings.Split(text, " ") {
		for i, piece := range hyphenPieces(word) {
			gap := ""
			if i == 0 && line != "" {
				gap = " "
			}
			if len(line)+len(gap)+len(piece) <= width {
				line += gap + piece
				continue
			}
			if line != "" {
				rows = append(rows, line)
			}
			for len(piece) > width {
				rows = append(rows, piece[:width])
				piece = piece[width:]
			}
			line = piece
		}
	}
	return append(rows, line)
}

func hyphenPieces(word string) []string {
	var pieces []string
	for {
		at := strings.Index(word, "-")
		if at < 0 || at == len(word)-1 {
			return append(pieces, word)
		}
		pieces = append(pieces, word[:at+1])
		word = word[at+1:]
	}
}

// The measured narrow Codex box wrapped the notice onto three rows, the
// verifier read no composer, and the delivery ended uncertain with the
// notice left typed. The whole box now reads as exactly the notice, and
// Classify sees the typed box the own-notice submit relies on.
func TestVerifyCodexNoticeWrappedOntoRows(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		notice string
		rows   []string
	}{
		{
			name:   "three rows, measured breaks",
			notice: wrapNotice,
			rows:   []string{wrapFirstRow, wrapMiddleRow, wrapLastRow},
		},
		{
			name:   "two rows",
			notice: ": [GIFFORD] 1 pending message: check your inbox",
			rows:   []string{wrapFirstRow, "message: check your inbox"},
		},
		{
			name:   "break after a hyphen keeps no space",
			notice: hyphenNotice,
			rows:   []string{wrapFirstRow, "message: run gifford-inbox-", "check"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			capture := codexNarrowCapture(tc.rows...)
			certain, exact := VerifyNotice(HarnessCodex, DefaultTable(), capture,
				codexWrapCols, 42, tc.notice)
			require.True(t, certain)
			require.True(t, exact, "the wrapped box holds exactly the notice")

			state := Classify(HarnessCodex, DefaultTable(), capture,
				codexWrapCols, 42, 0, nil)
			require.True(t, state.Certain, "detail: %s", state.Detail)
			require.True(t, state.BoxBusy, "detail: %s", state.Detail)
			require.Equal(t, detailTyped, state.Detail)
		})
	}
}

// Every width that wraps the notice, at every right margin Codex could
// keep, reads exact: the test is not tied to one set of breaks.
func TestVerifyCodexWrappedNoticeAtEveryWidth(t *testing.T) {
	t.Parallel()
	notices := []string{
		wrapNotice,
		hyphenNotice,
		colonNotice,
	}
	for _, notice := range notices {
		for cols := 16; cols <= 72; cols++ {
			for margin := range 6 {
				rows := codexWrap(notice, cols-2-margin)
				if len(rows) < 2 {
					continue
				}
				name := fmt.Sprintf("%q at %d cols, margin %d", notice, cols, margin)
				certain, exact := VerifyNotice(HarnessCodex, DefaultTable(),
					codexNarrowCapture(rows...), cols, 42, notice)
				require.True(t, certain, name)
				require.True(t, exact, name)
			}
		}
	}
}

func TestVerifyCodexWrappedBoxWithOtherTextIsNotExact(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		rows []string
	}{
		{name: "text after the notice", rows: []string{wrapFirstRow, wrapMiddleRow, wrapLastRow + " ok"}},
		{name: "text on its own row after the notice", rows: []string{wrapFirstRow, wrapMiddleRow, wrapLastRow, "ok"}},
		{name: "draft before the notice", rows: []string{"hi : [GIFFORD] 1 pending", wrapMiddleRow, wrapLastRow}},
		{name: "a row missing", rows: []string{wrapFirstRow, wrapLastRow}},
		{name: "partial notice", rows: []string{wrapFirstRow, wrapMiddleRow}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			certain, exact := VerifyNotice(HarnessCodex, DefaultTable(),
				codexNarrowCapture(tc.rows...), codexWrapCols, 42, wrapNotice)
			require.True(t, certain)
			require.False(t, exact)
		})
	}
}

// Rows that spell the notice but break where the width forced nothing
// hold line breaks, or are not the box's rows: never Enter.
func TestVerifyBreaksTheWidthDidNotForceAreUncertain(t *testing.T) {
	t.Parallel()
	capture := codexNarrowCapture(wrapFirstRow, wrapMiddleRow, wrapLastRow)
	certain, exact := VerifyNotice(HarnessCodex, DefaultTable(), capture, 120, 42, wrapNotice)
	require.False(t, certain)
	require.False(t, exact)

	state := Classify(HarnessCodex, DefaultTable(), capture, 120, 42, 0, nil)
	require.True(t, state.Certain, "detail: %s", state.Detail)
	require.True(t, state.BoxBusy, "the rows are still text in the box")
}

// The transcript keeps an earlier notice wrapped the same way, above the
// live box. The blank row between them ends the walk, so the old copy
// never stands in for the empty box.
func TestVerifyTranscriptCopyOfTheNoticeIsNotTheBox(t *testing.T) {
	t.Parallel()
	capture := []byte(strings.Join([]string{
		"",
		"\x1b[1;2m›\x1b[0m : [GIFFORD] 1 pending",
		"  message: check",
		"  list_pending_messages",
		"",
		"\x1b[2m• \x1b[0mRead the message.",
		" ",
		"\x1b[1m\x1b[38;5;215m›\x1b[0m ",
		" ",
		"  model default · ~/project",
	}, "\n"))
	certain, exact := VerifyNotice(HarnessCodex, DefaultTable(), capture,
		codexWrapCols, 42, wrapNotice)
	require.True(t, certain)
	require.False(t, exact)

	state := Classify(HarnessCodex, DefaultTable(), capture, codexWrapCols, 42, 0, nil)
	require.True(t, state.Certain, "detail: %s", state.Detail)
	require.False(t, state.BoxBusy, "detail: %s", state.Detail)
}

// A faint suggestion wraps like typed text in a narrow pane and still
// reads as an empty box across all its rows.
func TestCodexPlaceholderWrappedOntoRowsIsEmpty(t *testing.T) {
	t.Parallel()
	capture := codexNarrowCapture(
		"\x1b[2mCheck recently modified\x1b[0m",
		"\x1b[2mfunctions for\x1b[0m",
		"\x1b[2mcompatibility\x1b[0m",
	)
	state := Classify(HarnessCodex, DefaultTable(), capture, 26, 42, 0, nil)
	require.True(t, state.Certain, "detail: %s", state.Detail)
	require.False(t, state.BoxBusy, "detail: %s", state.Detail)
	require.Equal(t, detailEmpty, state.Detail)
}
