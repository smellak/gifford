package session

import (
	"slices"
	"unicode/utf8"

	"github.com/smellak/gifford/internal/profiles"
	"github.com/smellak/gifford/internal/proxy/draft"
	"github.com/smellak/gifford/internal/vt"
)

// The report's bounds. A screen past them is withheld, never cut.
const (
	maxObservedRows  = 10000
	maxObservedBytes = 512 << 10
	maxObservedCols  = 10000
	maskedInputRow   = "[input]"
)

// Why LiveScreen withholds a screen. The dataset counts the windows it
// drops by these reasons; the session only withholds.
const (
	// LiveScreenUnmeasured: the profile has no measured input line.
	LiveScreenUnmeasured = "profile_unmeasured"
	// LiveScreenNoInputEvidence: the input line cannot be read under the
	// cursor, which may be hidden, pending or off the screen.
	LiveScreenNoInputEvidence = "no_input_evidence"
	// LiveScreenInputNotEmpty: the input line under the cursor holds text.
	LiveScreenInputNotEmpty = "input_not_empty"
	// LiveScreenOutOfBounds: the frame is past the report's bounds or is not
	// valid UTF-8.
	LiveScreenOutOfBounds = "frame_out_of_bounds"
	// LiveScreenNoInputArea: the input line and its measured bounds do not
	// identify exactly one area, on the cursor's row.
	LiveScreenNoInputArea = "no_single_input_area_at_cursor"
)

// LiveScreen is the frame part of the live screen gate: snap's rows with
// p's measured editable area replaced by a fixed marker, or the reason the
// screen is withheld. The canonical editable area must read empty on this
// same screen, and the input line and every measured bound must identify
// exactly one area, on the cursor's row; an ambiguous or cropped frame
// stays withheld. It reads nothing but its arguments.
//
// The session calls it once its own checks pass: every human chunk
// written and folded, no key sequence held in part, the guard closed and
// the harness running. The dataset replay calls it once its recorded draft
// is closed, so a training window and a live observation of one screen
// carry the same rows.
func LiveScreen(p profiles.Profile, snap vt.Snapshot) ([]string, string) {
	if !p.Measured || p.InputLine == nil {
		return nil, LiveScreenUnmeasured
	}
	text, ok := draft.CursorInput(p, snap)
	switch {
	case !ok:
		return nil, LiveScreenNoInputEvidence
	case text != "":
		return nil, LiveScreenInputNotEmpty
	}
	if snap.Cols < 1 || snap.Cols > maxObservedCols || len(snap.Lines) > maxObservedRows ||
		snap.CursorRow < 0 || snap.CursorRow >= len(snap.Lines) ||
		snap.CursorCol < 0 || snap.CursorCol >= snap.Cols {
		return nil, LiveScreenOutOfBounds
	}
	total := 0
	for _, line := range snap.Lines {
		total += len(line)
		if total > maxObservedBytes || !utf8.ValidString(line) {
			return nil, LiveScreenOutOfBounds
		}
	}
	first, last, ok := editableArea(p, snap)
	if !ok {
		return nil, LiveScreenNoInputArea
	}
	rows := slices.Clone(snap.Lines)
	for row := first; row < last; row++ {
		rows[row] = maskedInputRow
	}
	total = 0
	for _, line := range rows {
		total += len(line)
		if total > maxObservedBytes {
			return nil, LiveScreenOutOfBounds
		}
	}
	return rows, ""
}

// editableArea locates the one canonical input box in the frame. Bounds
// are nearest-row first, so the rows between their two outermost entries are
// the area to mask; the outer entries themselves remain as screen context.
func editableArea(p profiles.Profile, snap vt.Snapshot) (int, int, bool) {
	if !p.InputBounds.Complete() {
		return 0, 0, false
	}
	candidate := -1
	for row, line := range snap.Lines {
		if len(p.InputLine.FindAllStringSubmatchIndex(line, 2)) != 1 ||
			!inputBoundsAt(p.InputBounds, snap.Lines, snap.Rows, row) {
			continue
		}
		if candidate != -1 {
			return 0, 0, false
		}
		candidate = row
	}
	if candidate != snap.CursorRow {
		return 0, 0, false
	}
	return candidate - len(p.InputBounds.Above) + 1,
		candidate + len(p.InputBounds.Below), true
}

func inputBoundsAt(bounds profiles.InputBounds, lines []string, rows, cursor int) bool {
	for offset, boundary := range bounds.Above {
		row := cursor - offset - 1
		if row < 0 || row >= rows || row >= len(lines) || !boundary.Matches(lines[row]) {
			return false
		}
	}
	for offset, boundary := range bounds.Below {
		row := cursor + offset + 1
		if row < 0 || row >= rows || row >= len(lines) || !boundary.Matches(lines[row]) {
			return false
		}
	}
	return true
}
