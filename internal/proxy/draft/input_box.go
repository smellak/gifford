package draft

import (
	"slices"

	"github.com/smellak/gifford/internal/profiles"
	"github.com/smellak/gifford/internal/vt"
)

// SupportsInputLayout reports whether snap's pane width has a measured box
// layout. Profiles without structural box geometry keep their existing rules.
func SupportsInputLayout(p profiles.Profile, snap vt.Snapshot) bool {
	if !p.InputBounds.Box.Configured() {
		return true
	}
	_, ok := p.InputBounds.Box.Layout(snap.Cols)
	return ok
}

func inputBoxAligned(p profiles.Profile, snap vt.Snapshot) (aligned, known bool) {
	box := p.InputBounds.Box
	if !box.Configured() {
		return false, true
	}
	cells, ok := snap.CellsForRow(snap.CursorRow)
	if !ok {
		return false, false
	}
	for _, cell := range cells {
		if cell.Col < box.LeftColumn && (cell.Text != " " || cell.Width != 1) {
			return false, true
		}
		if cell.Col == box.LeftColumn {
			return cell.Text == "┃" && cell.Width == 1, true
		}
		if cell.Col > box.LeftColumn {
			return false, true
		}
	}
	return false, true
}

// CropInputBox returns screen lines cut at the measured right edge of the
// input box. It succeeds only when the expected bottom border is complete,
// unique and aligned with a contiguous run of box rows containing the cursor.
// Every cut is made at a display-cell boundary supplied by the VT snapshot.
func CropInputBox(p profiles.Profile, snap vt.Snapshot) ([]string, bool) {
	box := p.InputBounds.Box
	if !box.Configured() {
		return nil, false
	}
	layout, ok := box.Layout(snap.Cols)
	if !ok || snap.Rows < 1 || snap.Rows != len(snap.Lines) {
		return nil, false
	}
	right := snap.Cols - 1 - layout.RightMargin
	borderRow := snap.Rows - 1 - layout.BottomMargin
	if right <= box.LeftColumn || borderRow < 0 || borderRow >= snap.Rows ||
		!measuredBottomBorder(snap, borderRow, box.LeftColumn, right) {
		return nil, false
	}

	top := borderRow
	for row := borderRow - 1; row >= 0 && alignedBoxRow(snap, row, box.LeftColumn); row-- {
		top = row
	}
	if snap.CursorRow < top || snap.CursorRow >= borderRow {
		return nil, false
	}

	lines := slices.Clone(snap.Lines)
	for row := top; row <= borderRow; row++ {
		line, exact := snap.LinePrefix(row, right+1)
		if !exact {
			return nil, false
		}
		lines[row] = line
	}
	return lines, true
}

func measuredBottomBorder(snap vt.Snapshot, row, left, right int) bool {
	cells, ok := snap.CellsForRow(row)
	if !ok {
		return false
	}
	markers := 0
	for _, cell := range cells {
		if cell.Text == "╹" {
			markers++
		}
		switch {
		case cell.Col < left:
			if cell.Text != " " || cell.Width != 1 {
				return false
			}
		case cell.Col == left:
			if cell.Text != "╹" || cell.Width != 1 {
				return false
			}
		case cell.Col <= right:
			if cell.Text != "▀" || cell.Width != 1 {
				return false
			}
		case cell.Col == right+1:
			if cell.Text == "▀" {
				return false
			}
		}
	}
	return markers == 1
}

func alignedBoxRow(snap vt.Snapshot, row, left int) bool {
	cells, ok := snap.CellsForRow(row)
	if !ok {
		return false
	}
	for _, cell := range cells {
		if cell.Col < left && (cell.Text != " " || cell.Width != 1) {
			return false
		}
		if cell.Col == left {
			return cell.Text == "┃" && cell.Width == 1
		}
		if cell.Col > left {
			return false
		}
	}
	return false
}
