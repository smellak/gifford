package label

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	nativeTestSession  = "native-session"
	nativeTestVersion  = "1.2.3"
	nativeTestTurn     = "turn-1"
	nativeTestTool     = "tool-1"
	nativeTestApproval = "approval-1"
	nativeTestQuestion = "question-1"
)

func nativeTestRecord(sequence int64, second int, eventType protocol.EventType) NativeRecord {
	return NativeRecord{
		EventID:    protocol.ULID(fmt.Sprintf("01JB000000000000000000%04d", sequence+100)),
		Type:       eventType,
		ReceivedAt: t0.Add(time.Duration(second) * time.Second),
		Sequence:   sequence,
	}
}

func nativeTestCapability(sequence int64, second int, before, after bool) NativeRecord {
	record := nativeTestRecord(sequence, second, protocol.EventTypeAdapterCapabilitiesChanged)
	beforeCapabilities := protocol.Capabilities{
		StructuredEvents: before, ToolEvents: before, ApprovalEvents: before,
	}
	afterCapabilities := protocol.Capabilities{
		StructuredEvents: after, ToolEvents: after, ApprovalEvents: after,
	}
	record.CapabilityBefore = &beforeCapabilities
	record.CapabilityAfter = &afterCapabilities
	return record
}

func nativeTestRun(events ...NativeRecord) NativeRun {
	return NativeRun{
		SessionID: nativeTestSession, CaptureSessionID: nativeTestSession,
		Harness:        protocol.HarnessCodex,
		HarnessVersion: nativeTestVersion,
		RunDigest:      protocol.Digest("sha256:" + strings.Repeat("a", 64)),
		StartedAt:      t0.Add(-time.Second), EndedAt: t0.Add(time.Hour),
		Events: events,
	}
}

func TestFoldNativeTimelineProjectsOnlyBThroughF(t *testing.T) {
	t.Parallel()
	events := []NativeRecord{
		nativeTestCapability(0, 0, false, true),
		nativeTestRecord(1, 1, protocol.EventTypePromptSubmitted),
		nativeTestRecord(2, 2, protocol.EventTypeToolStarted),
		nativeTestRecord(3, 3, protocol.EventTypeApprovalRequested),
		nativeTestRecord(4, 4, protocol.EventTypeApprovalResolved),
		nativeTestRecord(5, 5, protocol.EventTypeQuestionRequested),
		nativeTestRecord(6, 6, protocol.EventTypeQuestionResolved),
		nativeTestRecord(7, 7, protocol.EventTypeToolFailed),
		nativeTestRecord(8, 8, protocol.EventTypeTurnStreaming),
		nativeTestRecord(9, 9, protocol.EventTypeTurnCompleted),
		nativeTestRecord(10, 10, protocol.EventTypePromptSubmitted),
		nativeTestRecord(11, 11, protocol.EventTypeTurnCompleted),
		nativeTestRecord(12, 12, protocol.EventTypeSessionEnded),
	}
	for i := 1; i < len(events); i++ {
		events[i].TurnRef = nativeTestTurn
	}
	events[2].ToolRef, events[7].ToolRef = nativeTestTool, nativeTestTool
	events[3].RequestRef, events[4].RequestRef = nativeTestApproval, nativeTestApproval
	events[5].RequestRef, events[6].RequestRef = nativeTestQuestion, nativeTestQuestion
	events[9].Result = protocol.NativeEventResultFailed
	events[10].TurnRef, events[11].TurnRef = "turn-2", "turn-2"
	events[11].Result = protocol.NativeEventResultOK

	timeline := foldNativeTimeline(time.Hour, nativeTestRun(events...))
	labels := make([]dataset.Label, 0, len(timeline.spans))
	for _, span := range timeline.spans {
		labels = append(labels, span.label)
		require.NotEqual(t, dataset.LabelA, span.label)
		require.NotEqual(t, dataset.LabelG, span.label)
	}
	require.Equal(t, []dataset.Label{
		dataset.LabelB, dataset.LabelC, dataset.LabelD, dataset.LabelC,
		dataset.LabelE, dataset.LabelC, dataset.LabelB, dataset.LabelF, dataset.LabelB,
	}, labels)
	require.Equal(t, protocol.EventTypeToolStarted, timeline.spans[3].basis.Type,
		"C resumes from its durable opener after D closes")
	require.Equal(t, protocol.EventTypeTurnCompleted, timeline.spans[7].basis.Type)
	require.Equal(t, protocol.NativeEventResultFailed, events[9].Result)
	require.Zero(t, timeline.unclosed)
}

func TestFoldNativeTimelineConflictsAndNeverTurnsToolFailureIntoF(t *testing.T) {
	t.Parallel()
	events := []NativeRecord{
		nativeTestCapability(0, 0, false, true),
		nativeTestRecord(1, 1, protocol.EventTypeTurnStarted),
		nativeTestRecord(2, 2, protocol.EventTypeToolStarted),
		nativeTestRecord(3, 3, protocol.EventTypeApprovalRequested),
		nativeTestRecord(4, 4, protocol.EventTypeQuestionRequested),
		nativeTestRecord(5, 5, protocol.EventTypeApprovalResolved),
		nativeTestRecord(6, 6, protocol.EventTypeQuestionResolved),
		nativeTestRecord(7, 7, protocol.EventTypeToolFailed),
		nativeTestRecord(8, 8, protocol.EventTypeTurnCompleted),
	}
	for i := 1; i < len(events); i++ {
		events[i].TurnRef = nativeTestTurn
	}
	events[2].ToolRef, events[7].ToolRef = nativeTestTool, nativeTestTool
	events[3].RequestRef, events[5].RequestRef = nativeTestApproval, nativeTestApproval
	events[4].RequestRef, events[6].RequestRef = nativeTestQuestion, nativeTestQuestion
	events[8].Result = protocol.NativeEventResultOK

	timeline := foldNativeTimeline(time.Hour, nativeTestRun(events...))
	require.True(t, slices.ContainsFunc(timeline.spans, func(span nativeSpan) bool { return span.conflict }))
	require.False(t, slices.ContainsFunc(timeline.spans, func(span nativeSpan) bool {
		return span.label == dataset.LabelF
	}), "tool.failed closes C but only a failed turn can open F")
	require.True(t, slices.ContainsFunc(timeline.spans, func(span nativeSpan) bool {
		return span.label == dataset.LabelE
	}), "E remains after the simultaneous D request resolves")
}

func TestFoldNativeTimelineCutsOverlappingOperationBasisHandoff(t *testing.T) {
	t.Parallel()
	events := []NativeRecord{
		nativeTestCapability(0, 0, false, true),
		nativeTestRecord(1, 1, protocol.EventTypeTurnStarted),
		nativeTestRecord(2, 2, protocol.EventTypeToolStarted),
		nativeTestRecord(3, 3, protocol.EventTypeToolStarted),
		nativeTestRecord(4, 4, protocol.EventTypeToolCompleted),
		nativeTestRecord(5, 5, protocol.EventTypeToolCompleted),
		nativeTestRecord(6, 6, protocol.EventTypeTurnCompleted),
	}
	for i := 1; i < len(events); i++ {
		events[i].TurnRef = nativeTestTurn
	}
	events[2].ToolRef, events[4].ToolRef = "tool-first", "tool-first"
	events[3].ToolRef, events[5].ToolRef = "tool-second", "tool-second"
	events[6].Result = protocol.NativeEventResultOK

	timeline := foldNativeTimeline(time.Hour, nativeTestRun(events...))
	var tools []nativeSpan
	for _, span := range timeline.spans {
		if span.label == dataset.LabelC {
			tools = append(tools, span)
		}
	}
	require.Len(t, tools, 2)
	require.Equal(t, int64(2), tools[0].basis.Sequence)
	require.Equal(t, int64(4), tools[0].end.Sequence)
	require.Equal(t, int64(3), tools[1].basis.Sequence)
	require.Equal(t, int64(4), tools[1].start.Sequence)
	require.Equal(t, int64(5), tools[1].end.Sequence)
}

func TestFoldNativeTimelineRequiresContinuousEpochAndReactivation(t *testing.T) {
	t.Parallel()
	events := []NativeRecord{
		nativeTestCapability(0, 0, false, true),
		nativeTestRecord(1, 1, protocol.EventTypeTurnStarted),
		// Sequence 2 is missing: the open B span is abandoned, not closed.
		nativeTestRecord(3, 3, protocol.EventTypeToolStarted),
		nativeTestCapability(4, 4, true, false),
		nativeTestCapability(5, 5, false, true),
		nativeTestRecord(6, 6, protocol.EventTypeTurnStarted),
		nativeTestRecord(7, 7, protocol.EventTypeTurnCompleted),
	}
	events[1].TurnRef = "turn-old"
	events[2].TurnRef, events[2].ToolRef = "turn-old", "tool-old"
	events[5].TurnRef, events[6].TurnRef = "turn-new", "turn-new"
	events[6].Result = protocol.NativeEventResultOK

	timeline := foldNativeTimeline(time.Hour, nativeTestRun(events...))
	require.Equal(t, 1, timeline.unclosed)
	require.Len(t, timeline.spans, 1)
	require.Equal(t, dataset.LabelB, timeline.spans[0].label)
	require.Equal(t, int64(6), timeline.spans[0].start.Sequence)
	require.NotEqual(t, timeline.spans[0].run, protocol.Digest(""))
	require.True(t, timeline.spans[0].epoch.Valid())
}

func TestAlignNativeAlwaysUsesFullWindowSpanAndMargin(t *testing.T) {
	t.Parallel()
	events := []NativeRecord{
		nativeTestCapability(0, 0, false, true),
		nativeTestRecord(1, 1, protocol.EventTypePromptSubmitted),
		nativeTestRecord(2, 5, protocol.EventTypeTurnCompleted),
	}
	events[1].TurnRef, events[2].TurnRef = nativeTestTurn, nativeTestTurn
	events[2].Result = protocol.NativeEventResultOK
	run := nativeTestRun(events...)
	run.CaptureSessionID = "run-123-1793520000000000000"
	run.EndedAt = t0.Add(6 * time.Second)
	ids := []protocol.ULID{"01JB0000000000000000000201", "01JB0000000000000000000202"}
	recording := func(id protocol.ULID, from, to time.Time) dataset.Recording {
		return dataset.Recording{
			WindowID: id,
			Features: dataset.AnalyzeInput{Harness: protocol.HarnessCodex},
			Provenance: dataset.Provenance{
				SessionID: run.CaptureSessionID, CaptureSessionID: run.CaptureSessionID,
				HarnessVersion: nativeTestVersion, WindowStart: from, WindowEnd: to, AnchorAt: to,
			},
		}
	}
	src := &sliceSource{recs: []dataset.Recording{
		recording(ids[0], t0.Add(2*time.Second), t0.Add(4*time.Second)),
		// Its anchor is inside B, but its complete visual span crosses the opener.
		recording(ids[1], t0.Add(900*time.Millisecond), t0.Add(3*time.Second)),
	}}
	cfg := DefaultAlignConfig()
	cfg.EventWindow = EventWindowAnchor
	cfg.NativeSources = []WaitSource{{Harness: protocol.HarnessCodex, HarnessVersion: nativeTestVersion}}

	got, stats, err := AlignNative(context.Background(), cfg, src, []NativeRun{run})
	require.NoError(t, err)
	require.NotNil(t, got[ids[0]].Evidence)
	require.Equal(t, dataset.LabelB, got[ids[0]].Evidence.Label)
	require.Nil(t, got[ids[1]].Evidence)
	require.True(t, got[ids[1]].NearTransition)
	require.Equal(t, 1, stats.Candidates)
}

func TestMergeNativeDoesNotArbitrateConflicts(t *testing.T) {
	t.Parallel()
	ids := []protocol.ULID{
		"01JB0000000000000000000301", "01JB0000000000000000000302",
		"01JB0000000000000000000303", "01JB0000000000000000000304",
		"01JB0000000000000000000305", "01JB0000000000000000000306",
		"01JB0000000000000000000307",
	}
	base := []AlignedLabel{
		{WindowID: ids[0], Doubt: DoubtUnlabeled},
		{WindowID: ids[1], Label: dataset.LabelB, Source: SourceManual},
		{WindowID: ids[2], Label: dataset.LabelB, Source: SourceManual},
		{WindowID: ids[3], Label: dataset.LabelB, Source: SourceWeak, Doubt: DoubtWeakOnly},
		{WindowID: ids[4], Label: dataset.LabelB, Source: SourceHook, Doubt: DoubtConflict},
		{WindowID: ids[5], Label: dataset.LabelB, Source: SourceManual},
		{WindowID: ids[6], Label: dataset.LabelB, Source: SourceManual},
	}
	evidence := func(id protocol.ULID, label dataset.Label) *dataset.NativeLabelEvidence {
		return &dataset.NativeLabelEvidence{WindowID: id, Label: label}
	}
	native := map[protocol.ULID]NativeWindowAlignment{
		ids[0]: {Evidence: evidence(ids[0], dataset.LabelC)},
		ids[1]: {Evidence: evidence(ids[1], dataset.LabelC)},
		ids[2]: {Evidence: evidence(ids[2], dataset.LabelB)},
		ids[3]: {Evidence: evidence(ids[3], dataset.LabelB)},
		ids[4]: {Evidence: evidence(ids[4], dataset.LabelB)},
		ids[5]: {NearTransition: true},
		ids[6]: {Conflict: true},
	}

	got, selected, stats := MergeNative(base, native)
	require.Len(t, got, 6)
	require.Equal(t, SourceNative, got[0].Source)
	require.Equal(t, DoubtConflict, got[1].Doubt, "a manual disagreement is not arbitrated")
	require.Equal(t, SourceManual, got[2].Source, "agreeing independent truth keeps its provenance")
	require.Equal(t, SourceNative, got[3].Source)
	require.Equal(t, DoubtConflict, got[4].Doubt, "native cannot resolve an existing conflict")
	require.Equal(t, DoubtConflict, got[5].Doubt, "D/E native ambiguity makes the window NONE")
	require.Equal(t, map[protocol.ULID]dataset.NativeLabelEvidence{
		ids[0]: *evidence(ids[0], dataset.LabelC),
		ids[3]: *evidence(ids[3], dataset.LabelB),
	}, selected)
	require.Equal(t, NativeMergeStats{Discarded: 1, Conflicts: 2, Selected: 2}, stats)
}
