package vt

import (
	"reflect"
	"strings"
	"testing"
)

func FuzzWrite(f *testing.F) {
	for _, fixture := range corpus {
		f.Add([]byte(fixture.input))
	}
	f.Fuzz(func(t *testing.T, input []byte) {
		emulator, err := New(80, 24)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = emulator.Close() }()
		if n, writeErr := emulator.Write(input); writeErr != nil || n != len(input) {
			t.Fatalf("Write() = %d, %v", n, writeErr)
		}
		snapshot := emulator.Snapshot()
		if len(snapshot.Lines) != snapshot.Rows {
			t.Fatalf("len(Lines) = %d, Rows = %d", len(snapshot.Lines), snapshot.Rows)
		}
		if snapshot.CursorRow >= 0 && snapshot.CursorRow < snapshot.Rows {
			var text strings.Builder
			lastEnd := 0
			for _, cell := range snapshot.CursorCells {
				if cell.Col < lastEnd || cell.Width < 1 || cell.Text == "" ||
					cell.Col+cell.Width > snapshot.Cols {
					t.Fatalf("invalid cursor cell after column %d: %+v", lastEnd, cell)
				}
				text.WriteString(cell.Text)
				lastEnd = cell.Col + cell.Width
			}
			got := strings.TrimRight(text.String(), " ")
			if got != snapshot.Lines[snapshot.CursorRow] {
				t.Fatalf("cursor cell text = %q, line = %q", got, snapshot.Lines[snapshot.CursorRow])
			}
		}
	})
}

func FuzzChunking(f *testing.F) {
	for _, fixture := range corpus {
		f.Add([]byte(fixture.input), len(fixture.input)/2)
	}
	for _, input := range []string{testCombining, "✔️x", "👩‍💻x", "👍🏽x"} {
		f.Add([]byte(input), len(input)/2)
	}
	f.Fuzz(func(t *testing.T, input []byte, split int) {
		if len(input) == 0 {
			split = 0
		} else {
			split %= len(input) + 1
			if split < 0 {
				split = -split
			}
		}
		whole := render(t, 80, 24, input)
		chunked := render(t, 80, 24, input[:split], input[split:])
		if !reflect.DeepEqual(whole, chunked) {
			t.Fatalf("snapshots differ at split %d", split)
		}
	})
}
