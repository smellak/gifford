package redact

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// nestedMarkLines builds three identical lines holding the longest of 64
// nested paths followed by 200 combining marks: every occurrence ends on
// the same byte, the O(E*L) case of the mark-runs follow-up.
func nestedMarkLines() (lines []string, paths []string, longest string) {
	const npaths = 64
	paths = make([]string, npaths)
	for i := range paths {
		paths[i] = strings.Repeat("/x", i) + "/a"
	}
	longest = paths[npaths-1]
	line := longest + strings.Repeat("\u0301", 200) + "x"
	return []string{line, line, line}, paths, longest
}

// TestMarkRunWorkCounter is the deterministic absorption regression: 64
// nested paths over three line units, each followed by 200 marks. Every
// span jumps over its run, so the walk examines one rune per span, and the
// table is built once per line. Counting only the final loop could not
// tell a per-span rebuild apart.
func TestMarkRunWorkCounter(t *testing.T) {
	t.Parallel()
	lines, paths, longest := nestedMarkLines()
	info := make([]lineSpans, len(lines))
	for i, line := range lines {
		info[i] = lineSpans{cpref: clusterPrefix(line), wpref: widthPrefix(line)}
	}
	parts := make([]span, 0, len(paths)*len(lines))
	for li := range lines {
		for _, p := range paths {
			parts = append(parts, span{line: li, start: len(longest) - len(p), end: len(longest)})
		}
	}
	ext, err := extendParts("test", lines, info, parts)
	require.NoError(t, err)
	require.Len(t, ext, len(parts))
	wantEnd := len(longest) + len(strings.Repeat("\u0301", 200))
	for _, sp := range ext {
		require.Equal(t, wantEnd, sp.end)
	}
	walked, builds := 0, 0
	for _, sp := range info {
		walked += sp.walked
		builds += sp.markBuilds
	}
	require.Equal(t, len(lines), builds)
	require.Equal(t, len(parts), walked)
}

// TestMarkRunGuards pins the results on both methods: nested paths
// followed by marks mask as today with the marks vanishing into the
// mask, a variation selector or U+20E3 inside the run fails closed with
// the zero result, and so does a ZWJ after it.
func TestMarkRunGuards(t *testing.T) {
	t.Parallel()
	r, err := New(Options{SensitivePaths: []string{"/a", "/x/a", "/x/x/a"}})
	require.NoError(t, err)
	longest := "/x/x/a"
	masked := strings.Repeat(placeholder, CellWidth(longest))
	wantCounts := Counts{Total: 1, ByKind: map[Kind]int64{KindSensitivePath: 1}}

	t.Run("marks vanish", func(t *testing.T) {
		t.Parallel()
		input := longest + strings.Repeat("\u0301", 200) + " ok"
		got, err := r.RedactText(input)
		require.NoError(t, err)
		require.Equal(t, masked+" ok", got.Text)
		require.Equal(t, PolicyVersion, got.PolicyVersion)
		require.Equal(t, wantCounts, got.Counts)
		again, err := r.RedactText(got.Text)
		require.NoError(t, err)
		require.Equal(t, got.Text, again.Text)
		require.Equal(t, Counts{}, again.Counts)

		rows := []string{input}
		screen, err := r.RedactScreen(Screen{Cols: CellWidth(input), Rows: rows})
		require.NoError(t, err)
		require.Equal(t, []string{masked + " ok"}, screen.Rows)
		require.Equal(t, PolicyVersion, screen.PolicyVersion)
		require.Equal(t, wantCounts, screen.Counts)
		rescreen, err := r.RedactScreen(Screen{Cols: CellWidth(screen.Rows[0]), Rows: screen.Rows})
		require.NoError(t, err)
		require.Equal(t, screen.Rows, rescreen.Rows)
		require.Equal(t, Counts{}, rescreen.Counts)
	})

	t.Run("cluster in run fails closed", func(t *testing.T) {
		t.Parallel()
		for _, mid := range []string{"\uFE0F", "\u20E3"} {
			input := longest + strings.Repeat("\u0301", 50) + mid + strings.Repeat("\u0301", 50) + " ok"
			text, err := r.RedactText(input)
			require.ErrorIs(t, err, ErrUnsupportedInput, "mid %q", mid)
			require.Equal(t, TextResult{}, text, "mid %q", mid)
			require.NotContains(t, err.Error(), longest, "mid %q", mid)
			screen, err := r.RedactScreen(Screen{Cols: CellWidth(input), Rows: []string{input}})
			require.ErrorIs(t, err, ErrUnsupportedInput, "mid %q", mid)
			require.Equal(t, ScreenResult{}, screen, "mid %q", mid)
		}
	})

	t.Run("joiner after run fails closed", func(t *testing.T) {
		t.Parallel()
		input := longest + strings.Repeat("\u0301", 200) + "\u200D ok"
		text, err := r.RedactText(input)
		require.ErrorIs(t, err, ErrUnsupportedInput)
		require.Equal(t, TextResult{}, text)
		require.NotContains(t, err.Error(), longest)
	})
}
