package analyst

import (
	"slices"
	"time"

	"github.com/smellak/gifford/internal/protocol"
)

// NativeAvailability is the verified structured-event side of a capability
// transition. A one-shot hook ending normally produces no transition.
type NativeAvailability string

const (
	// NativeAvailable starts a freshly authenticated native-event epoch.
	NativeAvailable NativeAvailability = "available"
	// NativeUnavailable records a failed native-event epoch.
	NativeUnavailable NativeAvailability = "unavailable"
	// NativeUnchanged preserves sequencing for a capability transition that
	// does not change the structured lifecycle dimensions.
	NativeUnchanged NativeAvailability = "unchanged"
)

// Valid reports whether a capability transition has a known lifecycle effect.
func (a NativeAvailability) Valid() bool {
	return a == NativeAvailable || a == NativeUnavailable || a == NativeUnchanged
}

// NativeOperationTTL bounds how long each open native operation remains
// authoritative. Unrelated later events cannot renew an older operation. It
// is intentionally independent of the longer PTY analysis lifetime; expiry
// withdraws the native veto and records uncertainty, never idle.
const NativeOperationTTL = 5 * time.Second

type nativeOpen struct {
	eventID protocol.ULID
	turn    protocol.OpaqueID
	seq     int64
	at      time.Time
}

type nativeOperationKey struct {
	turn protocol.OpaqueID
	ref  protocol.OpaqueID
}

// NativeHistory is the incremental, content-free prefix of one run's durable
// native event stream. Core builds it only from immutable event rows through a
// prepared high-water; direct Analyst callers may instead retain the prefix
// before a bounded window. Its zero value is ready for use, and Reduce never
// mutates the retained prefix.
type NativeHistory struct {
	approvals  map[nativeOperationKey]nativeOpen
	questions  map[nativeOperationKey]nativeOpen
	tools      map[nativeOperationKey]nativeOpen
	background map[nativeOperationKey]nativeOpen

	initialized     bool
	seen            bool
	expected        int64
	unknown         bool
	unknownEvidence protocol.ULID
}

// NativeState is the restrictive result of reducing one session's durable
// structured evidence. Blocks is false when PTY analysis remains authoritative.
type NativeState struct {
	Phase      protocol.AgentPhase
	Evidence   []protocol.ULID
	Blocks     bool
	Confidence float64
	Stale      bool
	ExpiresAt  time.Time
}

// ReduceNative folds a complete durable event slice. The bounded run path uses
// NativeHistory.Reduce so a durable prefix can precede the reported window.
func ReduceNative(events []NativeEvent, now time.Time) NativeState {
	return (*NativeHistory)(nil).Reduce(events, now)
}

// Fold appends validated durable events to the retained prefix in sequence
// order. A gap is represented by the absent durable sequence itself; callers
// must not manufacture continuity for a dropped event.
func (h *NativeHistory) Fold(events []NativeEvent) {
	if h == nil || len(events) == 0 {
		return
	}
	ordered := append([]NativeEvent(nil), events...)
	slices.SortStableFunc(ordered, compareNativeSequence)
	for _, event := range ordered {
		h.apply(event)
	}
}

// Advance appends one validated event that follows the prefix's current
// sequence order. Durable row scans use it to remain memory-bounded.
func (h *NativeHistory) Advance(event NativeEvent) {
	if h == nil {
		return
	}
	h.apply(event)
}

// Clone returns an independent history suitable for a concurrent analysis or
// timer-only reevaluation.
func (h *NativeHistory) Clone() *NativeHistory {
	if h == nil {
		return &NativeHistory{}
	}
	out := *h
	out.approvals = cloneNativeOpen(h.approvals)
	out.questions = cloneNativeOpen(h.questions)
	out.tools = cloneNativeOpen(h.tools)
	out.background = cloneNativeOpen(h.background)
	return &out
}

// Reduce folds a current bounded window, if any, over this durable prefix and
// returns its state at now. It never mutates the prefix, so operation deadlines
// can be reevaluated at more than one observation time.
func (h *NativeHistory) Reduce(events []NativeEvent, now time.Time) NativeState {
	work := h.Clone()
	work.Fold(events)
	return work.state(now)
}

func compareNativeSequence(a, b NativeEvent) int {
	switch {
	case a.Sequence < b.Sequence:
		return -1
	case a.Sequence > b.Sequence:
		return 1
	default:
		return 0
	}
}

func (h *NativeHistory) initialize() {
	if h.initialized {
		return
	}
	h.approvals = make(map[nativeOperationKey]nativeOpen)
	h.questions = make(map[nativeOperationKey]nativeOpen)
	h.tools = make(map[nativeOperationKey]nativeOpen)
	h.background = make(map[nativeOperationKey]nativeOpen)
	h.expected = 1
	h.initialized = true
}

func (h *NativeHistory) apply(event NativeEvent) {
	h.initialize()
	if !h.seen && event.Sequence == 0 {
		h.expected = 0
	}
	h.seen = true
	discontinuous := event.Sequence != h.expected
	if event.Sequence >= h.expected {
		h.expected = event.Sequence + 1
	}
	if event.Type == protocol.EventTypeAdapterCapabilitiesChanged {
		switch event.Availability {
		case NativeAvailable:
			h.reset()
			if discontinuous {
				h.markUnknown(event.EventID)
			}
		case NativeUnavailable:
			h.reset()
			h.markUnknown(event.EventID)
		case NativeUnchanged:
			if discontinuous {
				h.markUnknown(event.EventID)
			}
		}
		return
	}
	if discontinuous {
		h.markUnknown(event.EventID)
	}
	if h.unknown {
		return
	}
	switch event.Type {
	case protocol.EventTypeApprovalRequested:
		h.open(h.approvals, event.RequestRef, event)
	case protocol.EventTypeApprovalResolved:
		closeNativeRef(h.approvals, event.RequestRef, event.TurnRef)
	case protocol.EventTypeQuestionRequested:
		h.open(h.questions, event.RequestRef, event)
	case protocol.EventTypeQuestionResolved:
		closeNativeRef(h.questions, event.RequestRef, event.TurnRef)
	case protocol.EventTypeToolStarted:
		h.open(h.tools, event.ToolRef, event)
	case protocol.EventTypeToolCompleted, protocol.EventTypeToolFailed:
		closeNativeRef(h.tools, event.ToolRef, event.TurnRef)
	case protocol.EventTypeBackgroundTaskStarted:
		h.open(h.background, backgroundRef(event), event)
	case protocol.EventTypeBackgroundTaskCompleted:
		closeNativeRef(h.background, backgroundRef(event), event.TurnRef)
	case protocol.EventTypeTurnCompleted:
		if event.Result == protocol.NativeEventResultCancelled {
			h.cancelTurn(event.TurnRef)
		}
	default:
		// Other native lifecycle events do not open or close a blocking
		// operation in the v0 reducer.
	}
}

func (h *NativeHistory) reset() {
	clear(h.approvals)
	clear(h.questions)
	clear(h.tools)
	clear(h.background)
	h.unknown = false
	h.unknownEvidence = ""
}

func (h *NativeHistory) markUnknown(eventID protocol.ULID) {
	h.unknown = true
	if h.unknownEvidence == "" {
		h.unknownEvidence = eventID
	}
}

func (h *NativeHistory) open(
	states map[nativeOperationKey]nativeOpen,
	ref protocol.OpaqueID,
	event NativeEvent,
) {
	if ref == "" {
		h.markUnknown(event.EventID)
		return
	}
	states[nativeOperationKey{turn: event.TurnRef, ref: ref}] = nativeOpen{
		eventID: event.EventID, turn: event.TurnRef, seq: event.Sequence, at: event.At,
	}
}

func closeNativeRef(
	states map[nativeOperationKey]nativeOpen,
	ref protocol.OpaqueID,
	turn protocol.OpaqueID,
) {
	if ref == "" {
		return
	}
	for key, state := range states {
		if key.ref != ref || (turn != "" && state.turn != "" && state.turn != turn) {
			continue
		}
		delete(states, key)
	}
}

func (h *NativeHistory) cancelTurn(turn protocol.OpaqueID) {
	if turn == "" {
		return
	}
	for _, states := range []map[nativeOperationKey]nativeOpen{
		h.approvals, h.questions, h.tools, h.background,
	} {
		for ref, state := range states {
			if state.turn == turn {
				delete(states, ref)
			}
		}
	}
}

func (h *NativeHistory) state(now time.Time) NativeState {
	if !h.seen {
		return NativeState{}
	}
	if h.unknown {
		return NativeState{
			Phase: protocol.AgentPhaseUnknown, Evidence: evidence(h.unknownEvidence),
			Stale: true,
		}
	}
	var expired nativeOpen
	expiredFound := false
	for _, states := range []map[nativeOperationKey]nativeOpen{
		h.approvals, h.questions, h.tools, h.background,
	} {
		for key, state := range states {
			if now.Before(state.at.Add(NativeOperationTTL)) {
				continue
			}
			delete(states, key)
			if !expiredFound || state.seq < expired.seq {
				expired, expiredFound = state, true
			}
		}
	}
	if state, ok := firstOpen(h.approvals); ok {
		return nativeBlocking(
			protocol.AgentPhaseWaitingApproval, state.eventID,
			state.at.Add(NativeOperationTTL),
		)
	}
	if state, ok := firstOpen(h.questions); ok {
		return nativeBlocking(
			protocol.AgentPhaseAskingQuestion, state.eventID,
			state.at.Add(NativeOperationTTL),
		)
	}
	if state, ok := firstOpen(h.tools); ok {
		return nativeBlocking(
			protocol.AgentPhaseRunningTool, state.eventID,
			state.at.Add(NativeOperationTTL),
		)
	}
	if state, ok := firstOpen(h.background); ok {
		return nativeBlocking(
			protocol.AgentPhaseBackgroundWork, state.eventID,
			state.at.Add(NativeOperationTTL),
		)
	}
	if expiredFound {
		return NativeState{
			Phase: protocol.AgentPhaseUnknown, Evidence: evidence(expired.eventID),
			Stale: true,
		}
	}
	return NativeState{}
}

func cloneNativeOpen(in map[nativeOperationKey]nativeOpen) map[nativeOperationKey]nativeOpen {
	if in == nil {
		return nil
	}
	out := make(map[nativeOperationKey]nativeOpen, len(in))
	for key, state := range in {
		out[key] = state
	}
	return out
}

func backgroundRef(event NativeEvent) protocol.OpaqueID {
	if event.ToolRef != "" {
		return event.ToolRef
	}
	return event.TurnRef
}

func firstOpen(states map[nativeOperationKey]nativeOpen) (nativeOpen, bool) {
	var first nativeOpen
	found := false
	for _, state := range states {
		if !found || state.seq < first.seq {
			first, found = state, true
		}
	}
	return first, found
}

func nativeBlocking(
	phase protocol.AgentPhase,
	eventID protocol.ULID,
	expiresAt time.Time,
) NativeState {
	return NativeState{
		Phase: phase, Evidence: evidence(eventID), Blocks: true, Confidence: 1,
		ExpiresAt: expiresAt,
	}
}

func evidence(eventID protocol.ULID) []protocol.ULID {
	if eventID == "" {
		return []protocol.ULID{}
	}
	return []protocol.ULID{eventID}
}
