package label

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/protocol"
)

// NativeAlignStats are content-free counters from the durable projection.
type NativeAlignStats struct {
	Candidates int
	Conflicts  int
	Unclosed   int
}

// NativeMergeStats count native decisions applied to the ordinary alignment.
type NativeMergeStats struct {
	Discarded int
	Conflicts int
	Selected  int
}

// NativeWindowAlignment is the native source's result for one replay window.
type NativeWindowAlignment struct {
	Evidence       *dataset.NativeLabelEvidence
	NearTransition bool
	Conflict       bool
}

type nativeOpen struct{ record NativeRecord }

type nativeProjectionState struct {
	turn        protocol.OpaqueID
	base        *NativeRecord
	failure     *NativeRecord
	tools       map[protocol.OpaqueID]nativeOpen
	approvals   map[protocol.OpaqueID]nativeOpen
	questions   map[protocol.OpaqueID]nativeOpen
	backgrounds map[protocol.OpaqueID]nativeOpen
}

type nativeVisible struct {
	label    dataset.Label
	conflict bool
	basis    NativeRecord
}

type nativeSpan struct {
	from, to time.Time
	label    dataset.Label
	conflict bool
	run      protocol.Digest
	epoch    protocol.Digest
	start    NativeRecord
	basis    NativeRecord
	end      NativeRecord
}

type nativeTimeline struct {
	spans      []nativeSpan
	boundaries []time.Time
	unclosed   int

	visible nativeVisible
	start   NativeRecord
	epoch   protocol.Digest
}

func newNativeProjectionState() nativeProjectionState {
	return nativeProjectionState{
		tools: map[protocol.OpaqueID]nativeOpen{}, approvals: map[protocol.OpaqueID]nativeOpen{},
		questions: map[protocol.OpaqueID]nativeOpen{}, backgrounds: map[protocol.OpaqueID]nativeOpen{},
	}
}

// AlignNative projects durable event epochs onto the complete span of replay
// windows. It never uses cfg.EventWindow: native evidence always covers
// WindowStart through WindowEnd plus cfg.Margin.
func AlignNative(
	ctx context.Context,
	cfg AlignConfig,
	src dataset.RecordingSource,
	runs []NativeRun,
) (map[protocol.ULID]NativeWindowAlignment, NativeAlignStats, error) {
	if err := cfg.Validate(); err != nil {
		return nil, NativeAlignStats{}, err
	}
	if src == nil {
		return nil, NativeAlignStats{}, errors.New("native labels: no recording source")
	}
	spanConfig := cfg
	spanConfig.EventWindow = EventWindowSpan
	windows, err := readWindows(ctx, spanConfig, src)
	if err != nil {
		return nil, NativeAlignStats{}, err
	}
	type key struct {
		session protocol.OpaqueID
		harness protocol.Harness
		version string
	}
	timelines := map[key]nativeTimeline{}
	runByKey := map[key]NativeRun{}
	stats := NativeAlignStats{}
	for _, run := range runs {
		if !run.SessionID.Valid() || !run.RunDigest.Valid() || run.StartedAt.IsZero() ||
			run.EndedAt.IsZero() || !run.EndedAt.After(run.StartedAt) ||
			!cfg.NativeSourceEnabled(run.Harness, run.HarnessVersion) {
			continue
		}
		k := key{run.SessionID, run.Harness, run.HarnessVersion}
		if _, exists := timelines[k]; exists {
			return nil, NativeAlignStats{}, errors.New("native labels: ambiguous run lineage")
		}
		timeline := foldNativeTimeline(cfg.MaxInterval, run)
		timelines[k], runByKey[k] = timeline, run
		stats.Unclosed += timeline.unclosed
	}
	out := map[protocol.ULID]NativeWindowAlignment{}
	for _, window := range windows {
		k := key{window.capture, window.harness, window.harnessVersion}
		run, ok := runByKey[k]
		if !ok || window.session != run.SessionID || window.from.IsZero() || window.to.IsZero() ||
			window.from.Before(run.StartedAt) || window.to.After(run.EndedAt) {
			continue
		}
		timeline := timelines[k]
		result := NativeWindowAlignment{
			NearTransition: nativeNear(timeline.boundaries, window.from, window.to, cfg.Margin),
		}
		span, found := nativeSpanIn(timeline.spans, window.from, window.to, cfg.Margin)
		if found && span.conflict {
			result.Conflict = true
			stats.Conflicts++
		} else if found && span.label != "" {
			evidence := dataset.NativeLabelEvidence{
				WindowID: window.id, Label: span.label, RunDigest: span.run,
				EpochDigest: span.epoch, Start: nativeBoundary(span.start),
				Basis: nativeBoundary(span.basis), End: nativeBoundary(span.end),
			}
			result.Evidence = &evidence
			stats.Candidates++
		}
		if result.Evidence != nil || result.NearTransition || result.Conflict {
			out[window.id] = result
		}
	}
	return out, stats, nil
}

// MergeNative combines an optional native source with the existing sources.
// Native never resolves an existing conflict. Any disagreement with an
// independent real label is a conflict; an agreeing real source keeps its own
// provenance. Native replaces weak-only or absent evidence.
func MergeNative(
	base []AlignedLabel,
	native map[protocol.ULID]NativeWindowAlignment,
) ([]AlignedLabel, map[protocol.ULID]dataset.NativeLabelEvidence, NativeMergeStats) {
	out := make([]AlignedLabel, 0, len(base))
	evidence := map[protocol.ULID]dataset.NativeLabelEvidence{}
	var stats NativeMergeStats
	for _, aligned := range base {
		result, ok := native[aligned.WindowID]
		if ok && result.NearTransition {
			stats.Discarded++
			continue
		}
		if ok && result.Conflict {
			aligned.Label, aligned.Source, aligned.Doubt = "", "", DoubtConflict
			aligned.NativeLabel = ""
			stats.Conflicts++
			out = append(out, aligned)
			continue
		}
		if !ok || result.Evidence == nil {
			out = append(out, aligned)
			continue
		}
		item := *result.Evidence
		aligned.NativeLabel = item.Label
		switch {
		case aligned.Doubt == DoubtConflict:
			// A third source cannot arbitrate a conflict already present.
		case aligned.Source == SourceManual || aligned.Source == SourceScript || aligned.Source == SourceHook:
			if aligned.Label != item.Label {
				aligned.Label, aligned.Source, aligned.Doubt = "", "", DoubtConflict
				stats.Conflicts++
			}
		case aligned.Source == SourceWeak:
			if aligned.Label != "" && aligned.Label != item.Label {
				aligned.Label, aligned.Source, aligned.Doubt = "", "", DoubtConflict
				stats.Conflicts++
			} else {
				aligned.Label, aligned.Source, aligned.Doubt = item.Label, SourceNative, ""
			}
		default:
			aligned.Label, aligned.Source, aligned.Doubt = item.Label, SourceNative, ""
		}
		if aligned.Source == SourceNative && aligned.Doubt == "" {
			evidence[aligned.WindowID] = item
			stats.Selected++
		}
		out = append(out, aligned)
	}
	return out, evidence, stats
}

func foldNativeTimeline(maxInterval time.Duration, run NativeRun) nativeTimeline {
	events := slices.Clone(run.Events)
	slices.SortStableFunc(events, func(a, b NativeRecord) int {
		if a.Sequence < b.Sequence {
			return -1
		}
		if a.Sequence > b.Sequence {
			return 1
		}
		return 0
	})
	state := newNativeProjectionState()
	var timeline nativeTimeline
	trusted, seen := false, false
	var lastSequence int64
	var lastAt time.Time
	for _, event := range events {
		discontinuous := !seen && event.Sequence != 0 || seen && event.Sequence != lastSequence+1
		regressed := seen && event.ReceivedAt.Before(lastAt)
		seen, lastSequence = true, event.Sequence
		if event.ReceivedAt.After(lastAt) {
			lastAt = event.ReceivedAt
		}
		if discontinuous || regressed {
			timeline.abandon()
			state = newNativeProjectionState()
			trusted = false
		}
		if event.Type == protocol.EventTypeAdapterCapabilitiesChanged {
			if event.CapabilityBefore == nil || event.CapabilityAfter == nil {
				timeline.abandon()
				trusted = false
				continue
			}
			direction := nativeCapabilityDirection(*event.CapabilityBefore, *event.CapabilityAfter)
			switch direction {
			case 1:
				timeline.abandon()
				state = newNativeProjectionState()
				timeline.epoch = nativeEpochDigest(run.RunDigest, event)
				trusted = !regressed
			case -1:
				if trusted && !discontinuous && !regressed {
					timeline.change(run.RunDigest, maxInterval, event, nativeVisible{})
				} else {
					timeline.abandon()
				}
				state = newNativeProjectionState()
				trusted = false
			default:
				if !nativeAllCapabilities(*event.CapabilityAfter) {
					timeline.abandon()
					state = newNativeProjectionState()
					trusted = false
				}
			}
			continue
		}
		if !trusted {
			continue
		}
		if !applyNativeRecord(&state, event) {
			timeline.abandon()
			state = newNativeProjectionState()
			trusted = false
			continue
		}
		timeline.change(run.RunDigest, maxInterval, event, state.visible())
		if event.Type == protocol.EventTypeSessionEnded {
			trusted = false
		}
	}
	timeline.abandon()
	return timeline
}

func applyNativeRecord(state *nativeProjectionState, event NativeRecord) bool {
	bindTurn := func(allowNew bool) bool {
		if !event.TurnRef.Valid() {
			return false
		}
		if state.turn != "" && state.turn != event.TurnRef {
			if !allowNew || state.hasOpenOperations() || state.base != nil {
				return false
			}
		}
		state.turn = event.TurnRef
		return true
	}
	open := func(states map[protocol.OpaqueID]nativeOpen, ref protocol.OpaqueID) bool {
		if !bindTurn(false) || !ref.Valid() {
			return false
		}
		if _, exists := states[ref]; exists {
			return false
		}
		states[ref] = nativeOpen{record: event}
		return true
	}
	closeRef := func(states map[protocol.OpaqueID]nativeOpen, ref protocol.OpaqueID) bool {
		entry, exists := states[ref]
		if !exists || !event.TurnRef.Valid() || entry.record.TurnRef != event.TurnRef ||
			state.turn != event.TurnRef {
			return false
		}
		delete(states, ref)
		return true
	}
	switch event.Type {
	case protocol.EventTypeSessionStarted:
		*state = newNativeProjectionState()
	case protocol.EventTypeSessionEnded:
		*state = newNativeProjectionState()
	case protocol.EventTypePromptSubmitted:
		if state.hasOpenOperations() || event.TurnRef != "" && !event.TurnRef.Valid() {
			return false
		}
		state.turn, state.failure = event.TurnRef, nil
		state.base = cloneNativeRecord(event)
	case protocol.EventTypeTurnStarted, protocol.EventTypeTurnStreaming:
		if !bindTurn(true) {
			return false
		}
		state.failure = nil
		state.base = cloneNativeRecord(event)
	case protocol.EventTypeToolStarted:
		if !open(state.tools, event.ToolRef) {
			return false
		}
		state.base, state.failure = nil, nil
	case protocol.EventTypeToolCompleted, protocol.EventTypeToolFailed:
		if !closeRef(state.tools, event.ToolRef) {
			return false
		}
	case protocol.EventTypeApprovalRequested:
		if !open(state.approvals, event.RequestRef) {
			return false
		}
		state.base, state.failure = nil, nil
	case protocol.EventTypeApprovalResolved:
		if !closeRef(state.approvals, event.RequestRef) {
			return false
		}
	case protocol.EventTypeQuestionRequested:
		if !open(state.questions, event.RequestRef) {
			return false
		}
		state.base, state.failure = nil, nil
	case protocol.EventTypeQuestionResolved:
		if !closeRef(state.questions, event.RequestRef) {
			return false
		}
	case protocol.EventTypeBackgroundTaskStarted:
		ref := nativeBackgroundRef(event)
		if !open(state.backgrounds, ref) {
			return false
		}
		state.base, state.failure = nil, nil
	case protocol.EventTypeBackgroundTaskCompleted:
		if !closeRef(state.backgrounds, nativeBackgroundRef(event)) {
			return false
		}
	case protocol.EventTypeTurnCompleted:
		if !event.TurnRef.Valid() || state.turn != event.TurnRef ||
			event.Result == "" || !event.Result.Valid() ||
			!state.operationsBelongTo(event.TurnRef) {
			return false
		}
		clear(state.tools)
		clear(state.approvals)
		clear(state.questions)
		clear(state.backgrounds)
		state.base = nil
		if event.Result == protocol.NativeEventResultFailed {
			state.failure = cloneNativeRecord(event)
		} else {
			state.failure = nil
		}
	case protocol.EventTypeNotificationReceived,
		protocol.EventTypeSubagentStarted, protocol.EventTypeSubagentCompleted,
		protocol.EventTypeContextCompactionStarted, protocol.EventTypeContextCompactionCompleted:
		// Informational events do not move a label boundary.
	default:
		return false
	}
	return true
}

func (state *nativeProjectionState) visible() nativeVisible {
	if state.failure != nil {
		return nativeVisible{label: dataset.LabelF, basis: *state.failure}
	}
	if len(state.approvals) > 0 && len(state.questions) > 0 {
		return nativeVisible{conflict: true}
	}
	if first, ok := firstNativeOpen(state.approvals); ok {
		return nativeVisible{label: dataset.LabelD, basis: first.record}
	}
	if first, ok := firstNativeOpen(state.questions); ok {
		return nativeVisible{label: dataset.LabelE, basis: first.record}
	}
	if first, ok := firstNativeOpen(state.tools); ok {
		return nativeVisible{label: dataset.LabelC, basis: first.record}
	}
	if state.base != nil {
		return nativeVisible{label: dataset.LabelB, basis: *state.base}
	}
	return nativeVisible{}
}

func (state *nativeProjectionState) hasOpenOperations() bool {
	return len(state.tools)+len(state.approvals)+len(state.questions)+len(state.backgrounds) != 0
}

func (state *nativeProjectionState) operationsBelongTo(turn protocol.OpaqueID) bool {
	for _, states := range []map[protocol.OpaqueID]nativeOpen{
		state.tools, state.approvals, state.questions, state.backgrounds,
	} {
		for _, open := range states {
			if open.record.TurnRef != turn {
				return false
			}
		}
	}
	return true
}

func firstNativeOpen(states map[protocol.OpaqueID]nativeOpen) (nativeOpen, bool) {
	var first nativeOpen
	found := false
	for _, open := range states {
		if !found || open.record.Sequence < first.record.Sequence {
			first, found = open, true
		}
	}
	return first, found
}

func nativeBackgroundRef(event NativeRecord) protocol.OpaqueID {
	if event.ToolRef.Valid() {
		return event.ToolRef
	}
	return event.TurnRef
}

func cloneNativeRecord(event NativeRecord) *NativeRecord {
	cloned := event
	return &cloned
}

func sameNativeVisible(a, b nativeVisible) bool {
	return a.label == b.label && a.conflict == b.conflict
}

func (timeline *nativeTimeline) change(
	run protocol.Digest,
	maxInterval time.Duration,
	event NativeRecord,
	next nativeVisible,
) {
	if sameNativeVisible(timeline.visible, next) {
		return
	}
	if timeline.visible.label != "" || timeline.visible.conflict {
		to := event.ReceivedAt
		if limit := timeline.start.ReceivedAt.Add(maxInterval); to.After(limit) {
			to = limit
		}
		if to.After(timeline.start.ReceivedAt) {
			timeline.spans = append(timeline.spans, nativeSpan{
				from: timeline.start.ReceivedAt, to: to,
				label: timeline.visible.label, conflict: timeline.visible.conflict,
				run: run, epoch: timeline.epoch, start: timeline.start,
				basis: timeline.visible.basis, end: event,
			})
		}
	}
	timeline.boundaries = append(timeline.boundaries, event.ReceivedAt)
	timeline.visible = next
	if next.label != "" || next.conflict {
		timeline.start = event
	} else {
		timeline.start = NativeRecord{}
	}
}

func (timeline *nativeTimeline) abandon() {
	if timeline.visible.label != "" || timeline.visible.conflict {
		timeline.unclosed++
	}
	timeline.visible = nativeVisible{}
	timeline.start = NativeRecord{}
	timeline.epoch = ""
}

func nativeSpanIn(spans []nativeSpan, from, to time.Time, margin time.Duration) (nativeSpan, bool) {
	lo, hi := from.Add(-margin), to.Add(margin)
	for _, span := range spans {
		if !span.from.After(lo) && !span.to.Before(hi) {
			return span, true
		}
	}
	return nativeSpan{}, false
}

func nativeNear(boundaries []time.Time, from, to time.Time, margin time.Duration) bool {
	lo, hi := from.Add(-margin), to.Add(margin)
	for _, boundary := range boundaries {
		if boundary.After(lo) && boundary.Before(hi) {
			return true
		}
	}
	return false
}

func nativeBoundary(event NativeRecord) dataset.NativeLabelBoundary {
	return dataset.NativeLabelBoundary{
		EventID: event.EventID, EventType: event.Type,
		Sequence: event.Sequence, At: event.ReceivedAt, Result: event.Result,
	}
}
