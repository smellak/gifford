package label

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/protocol"
)

// tl is a timeline line of the fixture session at sec.
func tl(sec float64, label string) TimelineRecord {
	return TimelineRecord{Session: "sess-1", At: at(sec), Label: label}
}

func alignTimeline(t *testing.T, recs []dataset.Recording, hooks []HookRecord, anns []Annotation, timeline []TimelineRecord) ([]AlignedLabel, AlignStats) {
	t.Helper()
	cfg := DefaultAlignConfig()
	if len(hooks) > 0 {
		cfg.RootSessionSources = []WaitSource{{Harness: protocol.HarnessClaudeCode, HarnessVersion: testHarnessVersion}}
		hooks = append([]HookRecord{hk(-1, evRoot)}, hooks...)
	}
	got, st, err := AlignWithTimeline(context.Background(), cfg, &sliceSource{recs: recs}, hooks, anns, nil, timeline)
	require.NoError(t, err)
	return got, st
}

func TestAlignScriptTimeline(t *testing.T) {
	t.Parallel()

	c := protocol.HarnessCodex
	timeline := []TimelineRecord{
		tl(0, "IDLE_AT_PROMPT"), tl(10, TimelineNone), tl(12, "WAITING_APPROVAL"), tl(20, TimelineNone),
	}
	other := rec(6, c, 5, 0.1, 10, nil)
	other.Provenance.SessionID = "sess-2"
	recs := []dataset.Recording{
		rec(1, c, 5, 0.1, 10, nil),    // A
		rec(2, c, 9.75, 0.1, 10, nil), // within the margin of NONE at 10
		rec(3, c, 11, 0.1, 10, nil),   // after NONE: no state
		rec(4, c, 15, 0.1, 10, nil),   // D
		rec(5, c, 25, 0.1, 10, nil),   // after the last line: no end, no state
		other,                         // another session: none of these lines
	}
	got, st := alignTimeline(t, recs, nil, nil, timeline)
	byID := map[protocol.ULID]AlignedLabel{}
	for _, l := range got {
		byID[l.WindowID] = l
	}
	require.Equal(t, AlignedLabel{WindowID: recs[0].WindowID, Label: dataset.LabelA, Source: SourceScript, ScriptLabel: dataset.LabelA}, withScript(got)[0])
	require.NotContains(t, byID, recs[1].WindowID, "a timeline line has the margin of a hook transition")
	require.Equal(t, DoubtUnlabeled, byID[recs[2].WindowID].Doubt)
	require.Equal(t, dataset.LabelD, byID[recs[3].WindowID].Label)
	require.Equal(t, SourceScript, byID[recs[3].WindowID].Source)
	require.Empty(t, byID[recs[3].WindowID].Doubt)
	require.Equal(t, DoubtUnlabeled, byID[recs[4].WindowID].Doubt)
	require.Equal(t, DoubtUnlabeled, byID[other.WindowID].Doubt)
	require.Equal(t, 6, st.Windows)
	require.Equal(t, 2, st.Script)
	require.Equal(t, 1, st.Discarded)
	require.Zero(t, st.DiscardedWeakOnly)
	require.Equal(t, 3, st.Unlabeled)
}

func TestAlignScriptAgainstOtherSources(t *testing.T) {
	t.Parallel()

	// The hooks say A from 0 to 10.
	hooks := []HookRecord{hk(0, evStop), hk(10, evPrompt)}
	agree := []TimelineRecord{tl(-0.5, "IDLE_AT_PROMPT"), tl(9, TimelineNone)}
	disagree := []TimelineRecord{tl(-0.5, "WAITING_APPROVAL"), tl(9, TimelineNone)}

	got, st := alignTimeline(t, []dataset.Recording{claudeAt(5)}, hooks, nil, agree)
	require.Equal(t, []AlignedLabel{{
		WindowID: claudeAt(5).WindowID, Label: dataset.LabelA, Source: SourceScript, ScriptLabel: dataset.LabelA, HookLabel: dataset.LabelA,
	}}, withScript(got))
	require.Equal(t, 1, st.Script)
	require.Zero(t, st.Hook)

	// No priority between the two: no label, and a human decides.
	got, st = alignTimeline(t, []dataset.Recording{claudeAt(5)}, hooks, nil, disagree)
	require.Equal(t, []AlignedLabel{{
		WindowID: claudeAt(5).WindowID, ScriptLabel: dataset.LabelD, HookLabel: dataset.LabelA, Doubt: DoubtConflict,
	}}, withScript(got))
	require.Equal(t, 1, st.Doubtful.Conflict)
	require.Equal(t, 1, st.Unlabeled)

	// A manual annotation wins over both.
	ann := []Annotation{{WindowID: claudeAt(5).WindowID, Label: dataset.LabelE, Annotator: "a", LabeledAt: at(100)}}
	got, _ = alignTimeline(t, []dataset.Recording{claudeAt(5)}, hooks, ann, disagree)
	require.Len(t, got, 1)
	require.Equal(t, dataset.LabelE, got[0].Label)
	require.Equal(t, SourceManual, got[0].Source)
	require.Empty(t, got[0].Doubt)

	// Over weak activity the script wins, with no doubt: as in
	// TestAlignWeakRules, a burst after silence makes window 3 weak B,
	// up to the tool window 5 starts; the script says A.
	cx := protocol.HarnessCodex
	recs := []dataset.Recording{
		rec(1, cx, 10, 5, 0, nil), rec(2, cx, 11, 0.1, 500, nil), rec(3, cx, 12, 0.2, 100, nil),
		rec(4, cx, 13, 0.2, 100, nil), rec(5, cx, 15, 0.1, 50, []string{"git"}),
	}
	labelOf := func(ls []AlignedLabel, id protocol.ULID) AlignedLabel {
		t.Helper()
		for _, l := range ls {
			if l.WindowID == id {
				return l
			}
		}
		t.Fatalf("window %s not aligned", id)
		return AlignedLabel{}
	}
	weakOnly, _ := alignTimeline(t, recs, nil, nil, nil)
	require.Equal(t, SourceWeak, labelOf(weakOnly, recs[2].WindowID).Source)
	require.Equal(t, dataset.LabelB, labelOf(weakOnly, recs[2].WindowID).Label)
	got, _ = alignTimeline(t, recs, nil, nil, []TimelineRecord{tl(0, "IDLE_AT_PROMPT"), tl(30, TimelineNone)})
	l := labelOf(got, recs[2].WindowID)
	require.Equal(t, dataset.LabelA, l.Label)
	require.Equal(t, SourceScript, l.Source)
	require.Equal(t, dataset.LabelB, l.WeakLabel)
	require.Empty(t, l.Doubt)
}

// withScript is public with the script label kept.
func withScript(ls []AlignedLabel) []AlignedLabel {
	out := public(ls)
	for i := range out {
		out[i].ScriptLabel = ls[i].ScriptLabel
	}
	return out
}

func TestReadTimeline(t *testing.T) {
	t.Parallel()

	dir := Dir(t.TempDir())
	got, err := ReadTimeline(dir)
	require.NoError(t, err)
	require.Empty(t, got, "no file, no lines")

	good := []TimelineRecord{tl(0, "IDLE_AT_PROMPT"), tl(1.5, TimelineNone), tl(2, "UNKNOWN")}
	var lines []byte
	for _, r := range good {
		b, err := json.Marshal(r)
		require.NoError(t, err)
		lines = append(append(lines, b...), '\n')
	}
	require.NoError(t, appendLine(dir, TimelineFile, lines))
	got, err = ReadTimeline(dir)
	require.NoError(t, err)
	require.Len(t, got, 3)
	require.True(t, got[1].At.Equal(at(1.5)))
	require.Equal(t, TimelineNone, got[1].Label)

	for name, line := range map[string]string{
		"background work":  `{"session":"sess-1","at":"2026-10-29T12:00:00Z","label":"BACKGROUND_WORK"}`,
		"a letter":         `{"session":"sess-1","at":"2026-10-29T12:00:00Z","label":"A"}`,
		"no label":         `{"session":"sess-1","at":"2026-10-29T12:00:00Z"}`,
		"no instant":       `{"session":"sess-1","label":"NONE"}`,
		"a bad session":    `{"session":"sess 1","at":"2026-10-29T12:00:00Z","label":"NONE"}`,
		"an unknown field": `{"session":"sess-1","at":"2026-10-29T12:00:00Z","label":"NONE","x":1}`,
		"torn":             `{"session":"sess-1","at":`,
	} {
		d := Dir(t.TempDir())
		require.NoError(t, appendLine(d, TimelineFile, append(append([]byte{}, lines...), line+"\n"...)), name)
		_, err := ReadTimeline(d)
		require.ErrorContains(t, err, fmt.Sprintf("%s: 1 unreadable lines", TimelineFile), name)
	}

	// Open to others, it is refused like every label file.
	open := Dir(t.TempDir())
	require.NoError(t, appendLine(open, TimelineFile, lines))
	require.NoError(t, os.Chmod(filepath.Join(open, TimelineFile), 0o644))
	_, err = ReadTimeline(open)
	require.ErrorIs(t, err, ErrUnsafeFile)
}
