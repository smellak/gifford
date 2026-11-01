package vt

import (
	"strings"

	uv "github.com/charmbracelet/ultraviolet"
)

// CursorCell is one grapheme cell on the cursor row. OtherStyle reports any
// style evidence besides faint: another attribute, colour, underline or link.
type CursorCell struct {
	Text       string
	Col        int
	Width      int
	Faint      bool
	OtherStyle bool
}

// Snapshot is a reconstructed plain-text screen with optional cursor-row
// style evidence. Lines, Diff and Hash remain independent of cell styles.
type Snapshot struct {
	Cols, Rows    int
	Lines         []string
	CursorCells   []CursorCell
	CursorCol     int
	CursorRow     int
	CursorVisible bool
	AltScreen     bool
	Pending       bool
	SyncOpen      bool
	rowCells      [][]CursorCell
}

// Text joins Lines with a newline.
func (s Snapshot) Text() string { return strings.Join(s.Lines, "\n") }

// CursorCellsForBytes maps the byte range [start,end) in Lines[CursorRow] to
// whole cursor-row cells and its starting display column. It fails closed when
// the range cuts a grapheme cell or the optional cell evidence is incomplete.
func (s Snapshot) CursorCellsForBytes(start, end int) ([]CursorCell, int, bool) {
	if start < 0 || end < start || s.CursorRow < 0 || s.CursorRow >= len(s.Lines) ||
		len(s.CursorCells) == 0 || end > len(s.Lines[s.CursorRow]) {
		return nil, 0, false
	}
	if !validCellRow(s.CursorCells, s.Cols, s.Lines[s.CursorRow]) {
		return nil, 0, false
	}
	first, column, ok := cursorCellBoundary(s.CursorCells, start)
	if !ok {
		return nil, 0, false
	}
	last, _, ok := cursorCellBoundary(s.CursorCells, end)
	if !ok {
		return nil, 0, false
	}
	cells := append([]CursorCell(nil), s.CursorCells[first:last]...)
	return cells, column, true
}

// CellsForRow returns complete grapheme-cell evidence for row. It fails
// closed for hand-built snapshots and any row whose cells do not reconstruct
// the corresponding plain-text line and pane width.
func (s Snapshot) CellsForRow(row int) ([]CursorCell, bool) {
	if row < 0 || row >= len(s.Lines) || row >= len(s.rowCells) ||
		!validCellRow(s.rowCells[row], s.Cols, s.Lines[row]) {
		return nil, false
	}
	return append([]CursorCell(nil), s.rowCells[row]...), true
}

// LinePrefix returns the first columns display cells of row, trimming trailing
// spaces as Snapshot does. It fails rather than split a wide grapheme cell.
func (s Snapshot) LinePrefix(row, columns int) (string, bool) {
	if columns < 0 || columns > s.Cols {
		return "", false
	}
	cells, ok := s.CellsForRow(row)
	if !ok {
		return "", false
	}
	var text strings.Builder
	for _, cell := range cells {
		if cell.Col >= columns {
			break
		}
		if cell.Col+cell.Width > columns {
			return "", false
		}
		text.WriteString(cell.Text)
	}
	return strings.TrimRight(text.String(), " "), true
}

func validCellRow(cells []CursorCell, cols int, line string) bool {
	var text strings.Builder
	column := 0
	for _, cell := range cells {
		if cell.Text == "" || cell.Col != column || cell.Width < 1 {
			return false
		}
		text.WriteString(cell.Text)
		column += cell.Width
	}
	return column == cols && strings.TrimRight(text.String(), " ") == line
}

func cursorCellBoundary(cells []CursorCell, target int) (int, int, bool) {
	offset := 0
	column := 0
	for index, cell := range cells {
		if offset == target {
			return index, cell.Col, true
		}
		offset += len(cell.Text)
		column = cell.Col + cell.Width
		if offset > target {
			return 0, 0, false
		}
	}
	if offset == target {
		return len(cells), column, true
	}
	return 0, 0, false
}

// LineChange is one row whose text changed.
type LineChange struct {
	Row  int
	Text string
}

// ScreenDiff is what changed from one snapshot to the next.
type ScreenDiff struct {
	Resized     bool
	Changed     []LineChange
	CursorMoved bool
	AltChanged  bool
}

// Snapshot returns the current screen. It changes nothing.
func (e *Emulator) Snapshot() Snapshot {
	cols, rows := e.term.Width(), e.term.Height()
	lines := make([]string, rows)
	cursor := e.term.CursorPosition()
	rowCells := make([][]CursorCell, rows)
	for row := range rows {
		var line strings.Builder
		for col := 0; col < cols; {
			cell := e.term.CellAt(col, row)
			evidence, ok := snapshotCell(cell, col)
			if !ok {
				col++
				continue
			}
			line.WriteString(evidence.Text)
			rowCells[row] = append(rowCells[row], evidence)
			col += evidence.Width
		}
		lines[row] = strings.TrimRight(line.String(), " ")
	}
	var cursorCells []CursorCell
	if cursor.Y >= 0 && cursor.Y < len(rowCells) {
		cursorCells = append(cursorCells, rowCells[cursor.Y]...)
	}
	return Snapshot{
		Cols:          cols,
		Rows:          rows,
		Lines:         lines,
		CursorCells:   cursorCells,
		CursorCol:     cursor.X,
		CursorRow:     cursor.Y,
		CursorVisible: !e.term.CursorHidden(),
		AltScreen:     e.term.IsAltScreen(),
		Pending:       len(e.pending) != 0,
		SyncOpen:      e.syncOpen,
		rowCells:      rowCells,
	}
}

func snapshotCell(cell *uv.Cell, col int) (CursorCell, bool) {
	evidence := CursorCell{Text: " ", Col: col, Width: 1}
	if cell == nil {
		return evidence, true
	}
	if cell.Width == 0 {
		return CursorCell{}, false
	}
	if cell.Content != "" {
		evidence.Text = cell.Content
	}
	if cell.Width > 0 {
		evidence.Width = cell.Width
	}
	evidence.Faint = cell.Style.Attrs&uv.AttrFaint != 0
	evidence.OtherStyle = cell.Style.Attrs&^uv.AttrFaint != 0 ||
		cell.Style.Fg != nil || cell.Style.Bg != nil || cell.Style.UnderlineColor != nil ||
		cell.Style.Underline != uv.UnderlineNone || cell.Link.URL != "" || cell.Link.Params != ""
	return evidence, true
}

// Diff reports the changes between two snapshots.
func Diff(prev, next Snapshot) ScreenDiff {
	diff := ScreenDiff{
		Resized:     prev.Cols != next.Cols || prev.Rows != next.Rows,
		CursorMoved: prev.CursorCol != next.CursorCol || prev.CursorRow != next.CursorRow,
		AltChanged:  prev.AltScreen != next.AltScreen,
	}
	for row, text := range next.Lines {
		changed := diff.Resized || row >= len(prev.Lines) || prev.Lines[row] != text
		if changed {
			diff.Changed = append(diff.Changed, LineChange{Row: row, Text: text})
		}
	}
	return diff
}
