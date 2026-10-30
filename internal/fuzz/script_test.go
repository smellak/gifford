package fuzz

import (
	"slices"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

// TestScriptsRepeat draws a batch's scripts twice from the same seed:
// they are the same, step by step. Another seed draws other scripts.
func TestScriptsRepeat(t *testing.T) {
	t.Parallel()
	const seed = 20261030
	differ := 0
	for i := range 50 {
		sc := NewScript(seed, i)
		require.Equal(t, sc, NewScript(seed, i), "scenario %d", i)
		require.Equal(t, sc.String(), NewScript(seed, i).String())
		if sc.String() != NewScript(seed+1, i).String() {
			differ++
		}
	}
	require.Equal(t, 50, differ, "another seed, other scripts")
}

// TestScriptsHold checks what every script promises, over many: modes
// alternate, a draft fits its line, the human ends on an empty draft,
// every anchor falls on a step, and each notice is one printable line
// no human line can pass for. Across the scripts the human cuts key
// sequences in two: a paste start marker, an escape sequence, a lone
// escape before its key, and a rune.
func TestScriptsHold(t *testing.T) {
	t.Parallel()
	instants := map[Instant]int{}
	cutMarkers, cutEscapes, lonePieces, cutRunes := 0, 0, 0, 0
	for i := range 2000 {
		sc := NewScript(7, i)
		want := protocol.InjectModeTyped
		if i%2 == 1 {
			want = protocol.InjectModeBracketedPaste
		}
		require.Equal(t, want, sc.Mode)
		require.Contains(t, submitDelays, sc.SubmitDelay)
		require.GreaterOrEqual(t, sc.Budget, 3)
		require.NotEmpty(t, sc.Steps)

		var e editor
		pastes := 0
		for j, st := range sc.Steps {
			require.Equal(t, string(e.draft), st.Draft, "scenario %d step %d", i, j)
			first := st.Kind == StepPaste && (j == 0 || sc.Steps[j-1].Kind != StepPaste)
			if first {
				if !strings.HasPrefix(st.Keys, pasteStart) {
					cutMarkers++
				}
				pastes++
			}
			if st.Kind == StepEscape && !slices.Contains(escapes, st.Keys) {
				cutEscapes++
				if st.Keys == "\x1b" {
					lonePieces++
				}
			}
			if st.Kind == StepType && !utf8.ValidString(st.Keys) {
				cutRunes++
			}
			if st.Kind == StepPause {
				require.Empty(t, st.Keys)
			}
			e.feed([]byte(st.Keys))
			require.LessOrEqual(t, utf8.RuneCount(e.draft), maxDraft)
			if st.Kind == StepPaste && (j+1 == len(sc.Steps) || sc.Steps[j+1].Kind != StepPaste) {
				require.False(t, e.pasting, "scenario %d step %d ends its paste", i, j)
			}
		}
		require.Empty(t, e.draft, "scenario %d ends on an empty draft", i)
		require.False(t, e.pasting)
		require.Positive(t, pastes, "scenario %d pastes", i)
		for _, r := range Expected(sc.Keys()) {
			require.NotEqual(t, RecordEOF, r.Kind)
			require.False(t, strings.ContainsAny(r.Text, noticeOnly), "a human line never passes for a notice")
		}

		require.NotEmpty(t, sc.Notices)
		for _, n := range sc.Notices {
			require.True(t, strings.HasPrefix(n.Text, noticeTag))
			require.True(t, utf8.ValidString(n.Text))
			for _, r := range n.Text {
				require.True(t, unicode.IsPrint(r), "notice %q", n.Text)
			}
			require.Less(t, n.Plan.At.Step, len(sc.Steps))
			instants[n.Plan.At.Instant]++
		}
		require.NotEmpty(t, sc.Retries)
		for _, p := range sc.Retries {
			require.Less(t, p.At.Step, len(sc.Steps))
		}
	}
	for _, in := range []Instant{
		InstantRandom, InstantPause, InstantLineStart, InstantEnter, InstantAfterEnter, InstantPasteStart, InstantPaste, InstantCut,
	} {
		require.Positive(t, instants[in], "notices arrive at %s", in)
	}
	require.Positive(t, cutMarkers, "a paste cuts its start marker")
	require.Positive(t, cutEscapes, "an escape sequence comes in two pieces")
	require.Positive(t, lonePieces, "a lone escape comes before its key")
	require.Positive(t, cutRunes, "a rune comes in two pieces")
}
