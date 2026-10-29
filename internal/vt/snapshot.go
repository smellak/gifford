package vt

import "strings"

// Snapshot is a reconstructed screen, without colours or attributes.
type Snapshot struct {
	Cols, Rows    int
	Lines         []string
	CursorCol     int
	CursorRow     int
	CursorVisible bool
	AltScreen     bool
	Pending       bool
}

// Text joins Lines with a newline.
func (s Snapshot) Text() string { return strings.Join(s.Lines, "\n") }

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
	for row := range rows {
		var line strings.Builder
		for col := 0; col < cols; {
			cell := e.term.CellAt(col, row)
			if cell == nil {
				line.WriteByte(' ')
				col++
				continue
			}
			if cell.Width == 0 {
				col++
				continue
			}
			if cell.Content == "" {
				line.WriteByte(' ')
			} else {
				line.WriteString(cell.Content)
			}
			width := cell.Width
			if width < 1 {
				width = 1
			}
			col += width
		}
		lines[row] = strings.TrimRight(line.String(), " ")
	}
	cursor := e.term.CursorPosition()
	return Snapshot{
		Cols:          cols,
		Rows:          rows,
		Lines:         lines,
		CursorCol:     cursor.X,
		CursorRow:     cursor.Y,
		CursorVisible: !e.term.CursorHidden(),
		AltScreen:     e.term.IsAltScreen(),
		Pending:       len(e.pending) != 0,
	}
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
