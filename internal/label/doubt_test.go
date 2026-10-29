package label

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/redact"
)

const screenCanary = "CANARY-SCREEN-4kz9"

// recIn is rec in a named session, with the canary on its screen.
func recIn(n int, harness protocol.Harness, sessionID string, sec, quiet float64, burst int64) dataset.Recording {
	r := rec(n, harness, sec, quiet, burst, nil)
	r.Provenance.SessionID = protocol.OpaqueID(sessionID)
	r.Screen = redact.Screen{Rows: []string{"$ run", screenCanary}, Cols: 80}
	return r
}

// turn is one session's weak turn: silence until 5 s, a burst at 11 s
// (THINKING_STREAMING from 10.9 s), silence again from 14 s. Windows at
// 12 s and 13 s carry the weak label; the others sit on transitions or
// after the last one.
func turn(first int, harness protocol.Harness, sessionID string) []dataset.Recording {
	return []dataset.Recording{
		recIn(first, harness, sessionID, 10, 5, 0),
		recIn(first+1, harness, sessionID, 11, 0.1, 500),
		recIn(first+2, harness, sessionID, 12, 0.2, 100),
		recIn(first+3, harness, sessionID, 13, 0.2, 100),
		recIn(first+4, harness, sessionID, 20, 6, 0),
	}
}

func byWindow(ls []AlignedLabel) map[protocol.ULID]AlignedLabel {
	m := map[protocol.ULID]AlignedLabel{}
	for _, l := range ls {
		m[l.WindowID] = l
	}
	return m
}

func TestDoubtReasons(t *testing.T) {
	t.Parallel()

	c, x := protocol.HarnessClaudeCode, protocol.HarnessCodex
	idle := []HookRecord{hk(0, evStop), hk(30, evPrompt)}
	claude := turn(0, c, "sess-a")
	codex := turn(10, x, "sess-b")
	lone := recIn(20, c, "sess-c", 50, 0.1, 10) // after the last hook state, no weak

	recs := append(append(append([]dataset.Recording{}, claude...), codex...), lone)
	got, st := align(t, DefaultAlignConfig(), recs, idle, nil)
	m := byWindow(got)

	// Hook says idle, weak says thinking: a conflict, the hook label kept.
	for _, r := range claude[2:4] {
		l := m[r.WindowID]
		require.Equal(t, DoubtConflict, l.Doubt)
		require.Equal(t, dataset.LabelA, l.HookLabel)
		require.Equal(t, dataset.LabelB, l.WeakLabel)
		require.Equal(t, dataset.LabelA, l.Label)
		require.Equal(t, SourceHook, l.Source)
	}
	// Only the hook label: not doubtful.
	require.Empty(t, m[claude[4].WindowID].Doubt)
	require.Equal(t, SourceHook, m[claude[4].WindowID].Source)
	// A harness without hooks with a weak label only.
	for _, r := range codex[2:4] {
		l := m[r.WindowID]
		require.Equal(t, DoubtWeakOnly, l.Doubt)
		require.Equal(t, SourceWeak, l.Source)
		require.Empty(t, l.HookLabel)
	}
	// Nothing at all.
	require.Equal(t, DoubtUnlabeled, m[codex[4].WindowID].Doubt)
	require.Equal(t, DoubtUnlabeled, m[lone.WindowID].Doubt)
	require.Empty(t, m[lone.WindowID].Label)

	// Windows on a weak transition are discarded, not doubtful.
	for _, r := range []dataset.Recording{claude[0], claude[1], codex[0], codex[1]} {
		_, listed := m[r.WindowID]
		require.False(t, listed, "window on a transition is listed")
	}
	require.Equal(t, 4, st.Discarded)
	require.Equal(t, 4, st.DiscardedWeakOnly, "no hook transition near them")
	require.Equal(t, map[protocol.Harness]int{c: 2, x: 2}, st.DiscardedWeakOnlyByHarness)
	require.Equal(t, DoubtCounts{Conflict: 2, Unlabeled: 2, WeakOnly: 2}, st.Doubtful)
	require.Equal(t, map[protocol.Harness]DoubtCounts{
		c: {Conflict: 2, Unlabeled: 1},
		x: {Unlabeled: 1, WeakOnly: 2},
	}, st.DoubtfulByHarness)

	// Hook and weak agree: no doubt.
	thinking := []HookRecord{hk(0, evPrompt), hk(30, evStop)}
	agree, _ := align(t, DefaultAlignConfig(), turn(0, c, "sess-a"), thinking, nil)
	for _, l := range agree {
		require.Empty(t, l.Doubt, "window %s", l.WindowID)
	}
}

func TestDoubtfulQueue(t *testing.T) {
	t.Parallel()

	c, x := protocol.HarnessClaudeCode, protocol.HarnessCodex
	idle := []HookRecord{hk(0, evStop), hk(30, evPrompt)}
	claude := turn(0, c, "sess-z")
	codex := turn(10, x, "sess-y")
	late := recIn(30, c, "sess-z", 60, 0.1, 10)
	early := recIn(31, c, "sess-a", 70, 0.1, 10)
	nearHook := recIn(32, c, "sess-a", 29.9, 0.1, 10) // within 300 ms of the prompt at 30 s
	// Input order is scrambled on purpose.
	recs := []dataset.Recording{
		codex[3], late, claude[3], nearHook, codex[2], early, claude[2], codex[4],
		claude[0], claude[1], claude[4], codex[0], codex[1],
	}

	got, _ := align(t, DefaultAlignConfig(), recs, idle, nil)
	q := DoubtfulQueue(got)
	require.Equal(t, []protocol.ULID{
		claude[2].WindowID, claude[3].WindowID, // conflicts, by time
		early.WindowID, codex[4].WindowID, late.WindowID, // unlabeled: sess-a, sess-y, sess-z
		codex[2].WindowID, codex[3].WindowID, // weak only, by time
	}, q)
	require.NotContains(t, q, nearHook.WindowID, "a window in the margin is never asked")

	// A manual annotation takes a window out of the queue.
	anns := []Annotation{{WindowID: claude[2].WindowID, Label: dataset.LabelB, Annotator: "a", LabeledAt: at(1)}}
	got, st := align(t, DefaultAlignConfig(), recs, idle, anns)
	q = DoubtfulQueue(got)
	require.NotContains(t, q, claude[2].WindowID)
	require.Len(t, q, 6)
	require.Equal(t, 1, st.Doubtful.Conflict)
	require.Equal(t, SourceManual, byWindow(got)[claude[2].WindowID].Source)
}

// TestDoubtSavesNoScreen: nothing saved holds the screen.
func TestDoubtSavesNoScreen(t *testing.T) {
	t.Parallel()

	dir := Dir(t.TempDir())
	recs := turn(0, protocol.HarnessClaudeCode, "sess-a")
	got, _ := align(t, DefaultAlignConfig(), recs, []HookRecord{hk(0, evStop), hk(30, evPrompt)}, nil)
	require.NoError(t, SaveAligned(dir, got))
	require.NoError(t, SaveAnnotations(dir, []Annotation{{WindowID: recs[2].WindowID, Label: dataset.LabelB, Annotator: "a", LabeledAt: at(1)}}))
	for _, name := range []string{AlignedFile, AnnotationsFile} {
		raw, err := os.ReadFile(filepath.Join(dir, name))
		require.NoError(t, err)
		require.NotContains(t, string(raw), screenCanary, name)
		require.NotContains(t, string(raw), "CANARY", name)
	}
}

// TestMarginBeatsManual: a manual annotation near a hook or a weak
// transition is discarded like any other window: not in the JSONL, not
// in the queue, counted only as Discarded.
func TestMarginBeatsManual(t *testing.T) {
	t.Parallel()

	c := protocol.HarnessClaudeCode
	claude := turn(0, c, "sess-a")                    // weak transitions at 10-10.9 s
	nearHook := recIn(40, c, "sess-a", 29.9, 0.1, 10) // 100 ms from the prompt at 30 s
	recs := append(append([]dataset.Recording{}, claude...), nearHook)
	anns := make([]Annotation, 0, 3)
	for _, r := range []dataset.Recording{claude[0], claude[1], nearHook} {
		anns = append(anns, Annotation{WindowID: r.WindowID, Label: dataset.LabelE, Annotator: "a", LabeledAt: at(1)})
	}
	got, st := align(t, DefaultAlignConfig(), recs, []HookRecord{hk(0, evStop), hk(30, evPrompt)}, anns)

	m := byWindow(got)
	for _, r := range []dataset.Recording{claude[0], claude[1], nearHook} {
		_, listed := m[r.WindowID]
		require.False(t, listed, "annotated window in the margin is listed")
		require.NotContains(t, DoubtfulQueue(got), r.WindowID)
	}
	require.Equal(t, 3, st.Discarded)
	require.Equal(t, 2, st.DiscardedWeakOnly, "the one near the prompt hook is not weak only")
	require.Equal(t, map[protocol.Harness]int{c: 2}, st.DiscardedWeakOnlyByHarness)
	require.Zero(t, st.Manual)
	require.Equal(t, len(recs)-3, st.Hook+st.Weak+st.Unlabeled)

	dir := Dir(t.TempDir())
	require.NoError(t, SaveAligned(dir, got))
	raw, err := os.ReadFile(filepath.Join(dir, AlignedFile))
	require.NoError(t, err)
	for _, r := range []dataset.Recording{claude[0], claude[1], nearHook} {
		require.NotContains(t, string(raw), string(r.WindowID))
	}
}
