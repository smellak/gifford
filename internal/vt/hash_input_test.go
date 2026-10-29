package vt

import (
	"fmt"
	"testing"

	"github.com/smellak/gifford/internal/protocol"

	"github.com/stretchr/testify/require"
)

func TestHashStableAgainstTicks(t *testing.T) {
	t.Parallel()
	spinners := []rune("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏✢✳✶✻✽")
	var want protocol.Digest
	for second := range 60 {
		snapshot := Snapshot{Cols: 80, Rows: 10, Lines: make([]string, 10)}
		snapshot.Lines[0] = fmt.Sprintf("%c Working 12:00:%02d", spinners[second%len(spinners)], second)
		got := Hash(snapshot)
		if second == 0 {
			want = got
		} else {
			require.Equal(t, want, got)
		}
	}
}

func TestHashSensitive(t *testing.T) {
	t.Parallel()
	base := Snapshot{Cols: 10, Rows: 2, Lines: []string{testHello, ""}, CursorVisible: true}
	changes := []Snapshot{
		{Cols: 10, Rows: 2, Lines: []string{testHello, testNew}},
		{Cols: 10, Rows: 2, Lines: []string{"hullo", ""}},
		{Cols: 11, Rows: 2, Lines: []string{testHello, ""}},
		{Cols: 10, Rows: 2, Lines: []string{testHello, ""}, AltScreen: true},
	}
	for _, changed := range changes {
		require.NotEqual(t, Hash(base), Hash(changed))
	}
	moved := base
	moved.CursorCol = 7
	moved.CursorVisible = false
	moved.Pending = true
	require.Equal(t, Hash(base), Hash(moved))
}

func TestHashFormat(t *testing.T) {
	t.Parallel()
	snapshot := Snapshot{Cols: 3, Rows: 2, Lines: []string{"a1", "⠋b"}}
	digest := Hash(snapshot)
	require.True(t, digest.Valid())
	require.Equal(t, protocol.Digest("sha256:efdafc3da7306a87d96534aea8cb7efbef933573fd012b0867dee66bcbf9d1ed"), digest)
}

func TestInputPattern(t *testing.T) {
	t.Parallel()
	for _, expression := range []string{`^no capture$`, `^(two) (captures)$`, `(`} {
		_, err := NewInputPattern(expression)
		require.ErrorIs(t, err, ErrInvalidPattern)
	}
	_, err := NewInputPattern(`^\s*>\s?(.*)$`)
	require.NoError(t, err)
}

func TestInputLine(t *testing.T) {
	t.Parallel()
	pattern, err := NewInputPattern(`^\s*>\s?(.*)$`)
	require.NoError(t, err)
	tests := []struct {
		name     string
		snapshot Snapshot
		want     string
		ok       bool
	}{
		{name: "cursor wins", snapshot: Snapshot{Lines: []string{"> upper", "> cursor", "> lower"}, CursorRow: 1}, want: "cursor", ok: true},
		{name: "bottom up", snapshot: Snapshot{Lines: []string{"> upper", testNone, "> lower"}, CursorRow: 1}, want: "lower", ok: true},
		{name: "empty", snapshot: Snapshot{Lines: []string{"> "}}, ok: true},
		{name: "none", snapshot: Snapshot{Lines: []string{testNone}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, ok := InputLine(test.snapshot, pattern)
			require.Equal(t, test.want, got)
			require.Equal(t, test.ok, ok)
		})
	}
	boxed, err := NewInputPattern(`^│\s*>\s?(.*?)\s*│$`)
	require.NoError(t, err)
	got, ok := InputLine(Snapshot{Lines: []string{"│ > boxed  │"}}, boxed)
	require.True(t, ok)
	require.Equal(t, "boxed", got)
}

func TestInputLinePending(t *testing.T) {
	t.Parallel()
	pattern, err := NewInputPattern(`^\s*>\s?(.*)$`)
	require.NoError(t, err)
	for _, input := range []string{"> a", "> "} {
		emulator, newErr := New(10, 1)
		require.NoError(t, newErr)
		_, writeErr := emulator.Write([]byte(input))
		require.NoError(t, writeErr)
		got, ok := InputLine(emulator.Snapshot(), pattern)
		require.False(t, ok)
		require.Empty(t, got)
		require.NoError(t, emulator.Flush())
		got, ok = InputLine(emulator.Snapshot(), pattern)
		require.True(t, ok)
		require.Equal(t, input[2:], got)
		require.NoError(t, emulator.Close())
	}
	emulator, err := New(10, 1)
	require.NoError(t, err)
	_, err = emulator.Write([]byte("> a\x1b[?25h"))
	require.NoError(t, err)
	got, ok := InputLine(emulator.Snapshot(), pattern)
	require.True(t, ok)
	require.Equal(t, "a", got)
	require.NoError(t, emulator.Close())
}

func TestDiff(t *testing.T) {
	t.Parallel()
	prev := Snapshot{Cols: 5, Rows: 3, Lines: []string{testSame, "old", testSame}, CursorCol: 1}
	next := Snapshot{Cols: 5, Rows: 3, Lines: []string{testSame, testNew, testSame}, CursorCol: 2, AltScreen: true}
	require.Equal(t, ScreenDiff{
		Changed:     []LineChange{{Row: 1, Text: testNew}},
		CursorMoved: true,
		AltChanged:  true,
	}, Diff(prev, next))
	resized := Snapshot{Cols: 4, Rows: 2, Lines: []string{testSame, testNew}}
	require.Equal(t, []LineChange{{Row: 0, Text: testSame}, {Row: 1, Text: testNew}}, Diff(next, resized).Changed)
	require.True(t, Diff(next, resized).Resized)
}
