package session

import (
	"slices"
	"unicode/utf8"

	"github.com/smellak/gifford/internal/detect"
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
	return liveScreen(p, snap, true)
}

// LiveScreenForCapture evaluates a tmux capture-pane dump instead of a
// live PTY frame. A dump carries no cursor: the canvas ends where the
// capture stopped, so the input row is located structurally — the only
// row matching the input line with all bounds satisfied — instead of by
// cursor. The live PTY path keeps its real cursor and stays untouched.
// Callers normalize the dump before emulation with detect.NormalizeCapture
// and share no other copy of that step.
func LiveScreenForCapture(p profiles.Profile, snap vt.Snapshot) ([]string, string) {
	return liveScreen(p, snap, false)
}

func liveScreen(p profiles.Profile, snap vt.Snapshot, byCursor bool) ([]string, string) {
	if !p.Measured || p.InputLine == nil {
		return nil, LiveScreenUnmeasured
	}
	if byCursor {
		text, ok := draft.CursorInput(p, snap)
		switch {
		case !ok:
			return nil, LiveScreenNoInputEvidence
		case text != "":
			return nil, LiveScreenInputNotEmpty
		}
	}
	if snap.Cols < 1 || snap.Cols > maxObservedCols || len(snap.Lines) > maxObservedRows ||
		snap.CursorCol < 0 || snap.CursorCol >= snap.Cols {
		return nil, LiveScreenOutOfBounds
	}
	if byCursor && (snap.CursorRow < 0 || snap.CursorRow >= len(snap.Lines)) {
		return nil, LiveScreenOutOfBounds
	}
	total := 0
	for _, line := range snap.Lines {
		total += len(line)
		if total > maxObservedBytes || !utf8.ValidString(line) {
			return nil, LiveScreenOutOfBounds
		}
	}
	first, last, input, ok := editableArea(p, snap, snap.Lines, byCursor)
	if !ok && p.InputBounds.Box.Configured() {
		if lines, cropped := draft.CropInputBox(p, snap); cropped {
			first, last, input, ok = editableArea(p, snap, lines, byCursor)
		}
	}
	if !ok {
		return nil, LiveScreenNoInputArea
	}
	if !byCursor {
		// A dump carries no cursor to read the draft under, so the
		// input row itself must read empty; a verified placeholder
		// counts as empty, anything else withholds.
		if input < 0 || input >= len(snap.Lines) {
			return nil, LiveScreenInputNotEmpty
		}
		if text := inputText(p, snap.Lines[input]); text != "" && !capturePlaceholderEmpty(p, snap, input, text) {
			return nil, LiveScreenInputNotEmpty
		}
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
// With byCursor the box must sit on the cursor row, as a live harness
// parks its cursor in its input box; a cursorless dump locates the box
// structurally instead and reports its row for the emptiness check.
func editableArea(p profiles.Profile, snap vt.Snapshot, lines []string, byCursor bool) (first, last, input int, ok bool) {
	if !p.InputBounds.Complete() {
		return 0, 0, -1, false
	}
	candidate := -1
	for row, line := range lines {
		if len(p.InputLine.FindAllStringSubmatchIndex(line, 2)) != 1 ||
			!inputBoundsAt(p.InputBounds, lines, snap.Rows, row) {
			continue
		}
		if candidate != -1 {
			return 0, 0, -1, false
		}
		candidate = row
	}
	if byCursor && candidate != snap.CursorRow {
		return 0, 0, -1, false
	}
	if candidate < 0 {
		return 0, 0, -1, false
	}
	return candidate - len(p.InputBounds.Above) + 1,
		candidate + len(p.InputBounds.Below), candidate, true
}

// capturePlaceholderEmpty reports whether text on row is exactly the
// profile placeholder rendered entirely faint, mirroring the detect
// gate's verified-placeholder rule for cursorless dumps.
func capturePlaceholderEmpty(p profiles.Profile, snap vt.Snapshot, row int, text string) bool {
	if p.InputPlaceholder == "" || text != p.InputPlaceholder {
		return false
	}
	faint, complete := detect.SpanStyle(snap, row, text)
	return complete && faint
}

// inputText extracts the input line's captured text on an already
// located row. Anything but empty withholds the screen as typed input.
func inputText(p profiles.Profile, line string) string {
	m := p.InputLine.FindStringSubmatch(line)
	if len(m) != 2 {
		return ""
	}
	return m[1]
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
