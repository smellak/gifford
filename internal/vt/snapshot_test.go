package vt

import (
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSyncOpenTracksSynchronizedOutputMode(t *testing.T) {
	t.Run("set reset and query", func(t *testing.T) {
		emulator, err := New(8, 1)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, emulator.Close()) })

		writeEmulator(t, emulator, []byte("\x1b[?2026h"))
		require.True(t, emulator.Snapshot().SyncOpen)
		writeEmulator(t, emulator, []byte("\x1b[?2026$p"))
		require.True(t, emulator.Snapshot().SyncOpen, "DECRQM must not change the mode")
		writeEmulator(t, emulator, []byte("\x1b[?2026l"))
		require.False(t, emulator.Snapshot().SyncOpen)
		writeEmulator(t, emulator, []byte("\x1b[?2026$p"))
		require.False(t, emulator.Snapshot().SyncOpen, "a closed-mode query must stay closed")
	})

	t.Run("split and multiple parameters", func(t *testing.T) {
		emulator, err := New(8, 1)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, emulator.Close()) })

		writeEmulator(t, emulator, []byte("\x1b[?2004;20"))
		require.False(t, emulator.Snapshot().SyncOpen)
		writeEmulator(t, emulator, []byte("26h"))
		require.True(t, emulator.Snapshot().SyncOpen)
		writeEmulator(t, emulator, []byte("\x1b[?2004;2026l"))
		require.False(t, emulator.Snapshot().SyncOpen)
		writeEmulator(t, emulator, []byte("\x1b[?2026h\x1b[?99999999999999999999;2026l"))
		require.False(t, emulator.Snapshot().SyncOpen, "an oversized earlier parameter must not hide 2026")
	})

	t.Run("eight bit CSI", func(t *testing.T) {
		emulator, err := New(8, 1)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, emulator.Close()) })

		writeEmulator(t, emulator, []byte{0x9b, '?', '2', '0', '2', '6', 'h'})
		require.True(t, emulator.Snapshot().SyncOpen)
		writeEmulator(t, emulator, []byte{0x9b, '?', '2', '0', '2', '6', 'l'})
		require.False(t, emulator.Snapshot().SyncOpen)
	})

	t.Run("RIS and missing close", func(t *testing.T) {
		emulator, err := New(8, 1)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, emulator.Close()) })

		writeEmulator(t, emulator, []byte("\x1b[?2026hok\x1b[?25h"))
		require.True(t, emulator.Snapshot().SyncOpen, "an unrelated sequence must not close the mode")
		require.NoError(t, emulator.Resize(9, 1))
		require.True(t, emulator.Snapshot().SyncOpen, "resize must not invent a close")
		writeEmulator(t, emulator, []byte("\x1bc"))
		require.False(t, emulator.Snapshot().SyncOpen)
	})
}

func TestSyncOpenDoesNotChangePendingDiffOrHash(t *testing.T) {
	plain := render(t, 8, 1, []byte("ok\x1b[?25h"))
	open := render(t, 8, 1, []byte("ok\x1b[?25h\x1b[?2026h"))
	require.False(t, plain.Pending)
	require.False(t, open.Pending)
	require.False(t, plain.SyncOpen)
	require.True(t, open.SyncOpen)
	require.Equal(t, plain.Lines, open.Lines)
	require.Equal(t, ScreenDiff{}, Diff(plain, open))
	require.Equal(t, Hash(plain), Hash(open))
}

func TestSyncOpenRequiresExactPrivateMode(t *testing.T) {
	for _, test := range []struct {
		name     string
		sequence string
	}{
		{name: "different number", sequence: "\x1b[?20260l"},
		{name: "oversized number", sequence: "\x1b[?202600000000000000000000000000l"},
		{name: "intermediate", sequence: "\x1b[?2026$l"},
		{name: "subparameter", sequence: "\x1b[?2026:1l"},
	} {
		t.Run(test.name, func(t *testing.T) {
			emulator, err := New(8, 1)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, emulator.Close()) })

			writeEmulator(t, emulator, []byte("\x1b[?2026h"))
			writeEmulator(t, emulator, []byte(test.sequence))
			require.True(t, emulator.Snapshot().SyncOpen)
			writeEmulator(t, emulator, []byte("\x1b[?2026l"))
			require.False(t, emulator.Snapshot().SyncOpen)
		})
	}
}

func TestCursorCellsCaptureFaintAndOtherStyles(t *testing.T) {
	input := "\x1b[2mf\x1b[22mn\x1b[1;2mb\x1b[0m" +
		"\x1b[2;31mc\x1b[0mp\x1b[4mu\x1b[0m" +
		"\x1b]8;;https://example.invalid\x1b\\l\x1b]8;;\x1b\\"
	snapshot := render(t, 12, 1, []byte(input))
	require.Equal(t, []string{"fnbcpul"}, snapshot.Lines)

	tests := []struct {
		col        int
		text       string
		faint      bool
		otherStyle bool
	}{
		{col: 0, text: "f", faint: true},
		{col: 1, text: "n"},
		{col: 2, text: "b", faint: true, otherStyle: true},
		{col: 3, text: "c", faint: true, otherStyle: true},
		{col: 4, text: "p"},
		{col: 5, text: "u", otherStyle: true},
		{col: 6, text: "l", otherStyle: true},
	}
	for _, test := range tests {
		cell := cursorCellAt(t, snapshot, test.col)
		require.Equal(t, test.text, cell.Text)
		require.Equal(t, test.faint, cell.Faint, "column %d", test.col)
		require.Equal(t, test.otherStyle, cell.OtherStyle, "column %d", test.col)
	}
	blank := cursorCellAt(t, snapshot, 7)
	require.Equal(t, " ", blank.Text)
	require.False(t, blank.Faint)
	require.False(t, blank.OtherStyle)
}

func TestCursorCellsPreserveGraphemesWidthsAndLineInvariant(t *testing.T) {
	snapshot := render(t, 12, 1, []byte("A漢🙂é"))
	require.Equal(t, []string{"A漢🙂é"}, snapshot.Lines)
	require.Equal(t, []CursorCell{
		{Text: "A", Col: 0, Width: 1},
		{Text: "漢", Col: 1, Width: 2},
		{Text: "🙂", Col: 3, Width: 2},
		{Text: "é", Col: 5, Width: 1},
	}, snapshot.CursorCells[:4])
	requireCursorCellInvariant(t, snapshot)
}

func TestCursorCellsForBytesUsesWholeCellBoundaries(t *testing.T) {
	snapshot := render(t, 12, 1, []byte("> 漢é!"))
	indices := regexp.MustCompile(`^> (漢é)(!)$`).FindStringSubmatchIndex(snapshot.Lines[0])
	require.Len(t, indices, 6)

	cells, column, ok := snapshot.CursorCellsForBytes(indices[2], indices[3])
	require.True(t, ok)
	require.Equal(t, 2, column)
	require.Equal(t, []CursorCell{
		{Text: "漢", Col: 2, Width: 2},
		{Text: "é", Col: 4, Width: 1},
	}, cells)

	_, _, ok = snapshot.CursorCellsForBytes(indices[2]+1, indices[3])
	require.False(t, ok, "a byte offset inside a wide grapheme is not a cell boundary")
	_, _, ok = snapshot.CursorCellsForBytes(indices[2], indices[2]+len("漢")-1)
	require.False(t, ok, "a range ending inside a wide grapheme is not exact")
	_, _, ok = snapshot.CursorCellsForBytes(-1, 0)
	require.False(t, ok)
	_, _, ok = snapshot.CursorCellsForBytes(0, len(snapshot.Lines[0])+1)
	require.False(t, ok)

	missing := snapshot
	missing.CursorCells = nil
	_, _, ok = missing.CursorCellsForBytes(indices[2], indices[3])
	require.False(t, ok, "missing optional evidence must fail closed")

	emptyAtEnd, column, ok := snapshot.CursorCellsForBytes(len(snapshot.Lines[0]), len(snapshot.Lines[0]))
	require.True(t, ok)
	require.Empty(t, emptyAtEnd)
	require.Equal(t, 6, column)
}

func TestCursorCellsUnavailableWhenCursorIsOutsideScreen(t *testing.T) {
	snapshot := Snapshot{Cols: 4, Rows: 1, Lines: []string{"text"}, CursorRow: 2}
	require.Nil(t, snapshot.CursorCells)
	_, _, ok := snapshot.CursorCellsForBytes(0, 1)
	require.False(t, ok)
}

func TestCursorCellStylesDoNotChangeHash(t *testing.T) {
	plain := render(t, 8, 1, []byte("same\x1b[?25h"))
	styled := render(t, 8, 1, []byte("\x1b[2msame\x1b[0m\x1b[?25h"))
	require.Equal(t, plain.Lines, styled.Lines)
	require.NotEqual(t, plain.CursorCells, styled.CursorCells)
	require.Equal(t, Hash(plain), Hash(styled))
}

func writeEmulator(t *testing.T, emulator *Emulator, data []byte) {
	t.Helper()
	n, err := emulator.Write(data)
	require.NoError(t, err)
	require.Len(t, data, n)
}

func cursorCellAt(t *testing.T, snapshot Snapshot, col int) CursorCell {
	t.Helper()
	for _, cell := range snapshot.CursorCells {
		if cell.Col == col {
			return cell
		}
	}
	t.Fatalf("no cursor cell starts at column %d", col)
	return CursorCell{}
}

func requireCursorCellInvariant(t *testing.T, snapshot Snapshot) {
	t.Helper()
	require.GreaterOrEqual(t, snapshot.CursorRow, 0)
	require.Less(t, snapshot.CursorRow, len(snapshot.Lines))
	var text strings.Builder
	column := 0
	for _, cell := range snapshot.CursorCells {
		require.Equal(t, column, cell.Col)
		require.NotEmpty(t, cell.Text)
		require.Positive(t, cell.Width)
		text.WriteString(cell.Text)
		column += cell.Width
	}
	require.Equal(t, snapshot.Cols, column)
	require.Equal(t, snapshot.Lines[snapshot.CursorRow], strings.TrimRight(text.String(), " "))
}
