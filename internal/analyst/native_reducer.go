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

// ReduceNative folds durable events by sequence and operation reference.
func ReduceNative(events []NativeEvent, now time.Time) NativeState {
	if len(events) == 0 {
		return NativeState{}
	}
	ordered := append([]NativeEvent(nil), events...)
	slices.SortStableFunc(ordered, func(a, b NativeEvent) int {
		switch {
		case a.Sequence < b.Sequence:
			return -1
		case a.Sequence > b.Sequence:
			return 1
		default:
			return 0
		}
	})

	approvals := make(map[nativeOperationKey]nativeOpen)
	questions := make(map[nativeOperationKey]nativeOpen)
	tools := make(map[nativeOperationKey]nativeOpen)
	background := make(map[nativeOperationKey]nativeOpen)
	unknown := false
	var unknownEvidence protocol.ULID
	expected := int64(1)
	if ordered[0].Sequence == 0 {
		expected = 0
	}

	reset := func() {
		clear(approvals)
		clear(questions)
		clear(tools)
		clear(background)
		unknown = false
		unknownEvidence = ""
	}
	markUnknown := func(eventID protocol.ULID) {
		unknown = true
		if unknownEvidence == "" {
			unknownEvidence = eventID
		}
	}
	open := func(states map[nativeOperationKey]nativeOpen, ref protocol.OpaqueID, event NativeEvent) {
		if ref == "" {
			markUnknown(event.EventID)
			return
		}
		states[nativeOperationKey{turn: event.TurnRef, ref: ref}] = nativeOpen{
			eventID: event.EventID, turn: event.TurnRef, seq: event.Sequence, at: event.At,
		}
	}
	closeRef := func(
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
	cancelTurn := func(turn protocol.OpaqueID) {
		if turn == "" {
			return
		}
		for _, states := range []map[nativeOperationKey]nativeOpen{
			approvals, questions, tools, background,
		} {
			for ref, state := range states {
				if state.turn == turn {
					delete(states, ref)
				}
			}
		}
	}

	for _, event := range ordered {
		discontinuous := event.Sequence != expected
		if event.Sequence >= expected {
			expected = event.Sequence + 1
		}
		if event.Type == protocol.EventTypeAdapterCapabilitiesChanged {
			switch event.Availability {
			case NativeAvailable:
				reset()
				if discontinuous {
					markUnknown(event.EventID)
				}
			case NativeUnavailable:
				reset()
				markUnknown(event.EventID)
			case NativeUnchanged:
				if discontinuous {
					markUnknown(event.EventID)
				}
			}
			continue
		}
		if discontinuous {
			markUnknown(event.EventID)
		}
		if unknown {
			continue
		}
		switch event.Type {
		case protocol.EventTypeApprovalRequested:
			open(approvals, event.RequestRef, event)
		case protocol.EventTypeApprovalResolved:
			closeRef(approvals, event.RequestRef, event.TurnRef)
		case protocol.EventTypeQuestionRequested:
			open(questions, event.RequestRef, event)
		case protocol.EventTypeQuestionResolved:
			closeRef(questions, event.RequestRef, event.TurnRef)
		case protocol.EventTypeToolStarted:
			open(tools, event.ToolRef, event)
		case protocol.EventTypeToolCompleted, protocol.EventTypeToolFailed:
			closeRef(tools, event.ToolRef, event.TurnRef)
		case protocol.EventTypeBackgroundTaskStarted:
			open(background, backgroundRef(event), event)
		case protocol.EventTypeBackgroundTaskCompleted:
			closeRef(background, backgroundRef(event), event.TurnRef)
		case protocol.EventTypeTurnCompleted:
			if event.Result == protocol.NativeEventResultCancelled {
				cancelTurn(event.TurnRef)
			}
		default:
			// Other native lifecycle events do not open or close a blocking
			// operation in the v0 reducer.
		}
	}

	if unknown {
		return NativeState{
			Phase: protocol.AgentPhaseUnknown, Evidence: evidence(unknownEvidence),
			Stale: true,
		}
	}
	var expired nativeOpen
	expiredFound := false
	for _, states := range []map[nativeOperationKey]nativeOpen{
		approvals, questions, tools, background,
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
	if state, ok := firstOpen(approvals); ok {
		return nativeBlocking(
			protocol.AgentPhaseWaitingApproval, state.eventID,
			state.at.Add(NativeOperationTTL),
		)
	}
	if state, ok := firstOpen(questions); ok {
		return nativeBlocking(
			protocol.AgentPhaseAskingQuestion, state.eventID,
			state.at.Add(NativeOperationTTL),
		)
	}
	if state, ok := firstOpen(tools); ok {
		return nativeBlocking(
			protocol.AgentPhaseRunningTool, state.eventID,
			state.at.Add(NativeOperationTTL),
		)
	}
	if state, ok := firstOpen(background); ok {
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
