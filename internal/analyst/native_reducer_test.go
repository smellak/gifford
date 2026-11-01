package analyst

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

const (
	testRequestRef protocol.OpaqueID = "request-1"
	testToolRef    protocol.OpaqueID = "tool-1"
	testTurnRef    protocol.OpaqueID = "turn-1"
)

func nativeTestEvent(sequence int64, typ protocol.EventType, at time.Time) NativeEvent {
	return NativeEvent{
		EventID: protocol.ULID(fmt.Sprintf("01ARZ3NDEKTSV4RRFFQ69G%04d", sequence+1)),
		Type:    typ, At: at, Sequence: sequence,
	}
}

func TestNativeReducerKeepsReferencedOperationsOpen(t *testing.T) {
	t.Parallel()
	now := t0
	t.Run("approval survives ordinary turn completion", func(t *testing.T) {
		t.Parallel()
		approval := nativeTestEvent(1, protocol.EventTypeApprovalRequested, now)
		approval.RequestRef, approval.TurnRef = testRequestRef, testTurnRef
		completed := nativeTestEvent(2, protocol.EventTypeTurnCompleted, now)
		completed.TurnRef, completed.Result = testTurnRef, protocol.NativeEventResultOK
		got := ReduceNative([]NativeEvent{approval, completed}, now)
		require.Equal(t, protocol.AgentPhaseWaitingApproval, got.Phase)
		require.Equal(t, []protocol.ULID{approval.EventID}, got.Evidence)
	})
	t.Run("question survives ordinary turn completion", func(t *testing.T) {
		t.Parallel()
		question := nativeTestEvent(1, protocol.EventTypeQuestionRequested, now)
		question.RequestRef, question.TurnRef = testRequestRef, testTurnRef
		completed := nativeTestEvent(2, protocol.EventTypeTurnCompleted, now)
		completed.TurnRef = testTurnRef
		got := ReduceNative([]NativeEvent{question, completed}, now)
		require.Equal(t, protocol.AgentPhaseAskingQuestion, got.Phase)
	})
	t.Run("turn cancellation closes its operations", func(t *testing.T) {
		t.Parallel()
		approval := nativeTestEvent(1, protocol.EventTypeApprovalRequested, now)
		approval.RequestRef, approval.TurnRef = testRequestRef, testTurnRef
		cancelled := nativeTestEvent(2, protocol.EventTypeTurnCompleted, now)
		cancelled.TurnRef, cancelled.Result = testTurnRef, protocol.NativeEventResultCancelled
		require.False(t, ReduceNative([]NativeEvent{approval, cancelled}, now).Blocks)
	})
}

func TestNativeReducerTracksParallelToolsByReference(t *testing.T) {
	t.Parallel()
	first := nativeTestEvent(1, protocol.EventTypeToolStarted, t0)
	first.ToolRef, first.TurnRef = testToolRef, testTurnRef
	second := nativeTestEvent(2, protocol.EventTypeToolStarted, t0)
	second.ToolRef, second.TurnRef = "tool-2", testTurnRef
	closed := nativeTestEvent(3, protocol.EventTypeToolCompleted, t0)
	closed.ToolRef, closed.TurnRef = first.ToolRef, first.TurnRef

	got := ReduceNative([]NativeEvent{first, second, closed}, t0)
	require.Equal(t, protocol.AgentPhaseRunningTool, got.Phase)
	require.Equal(t, []protocol.ULID{second.EventID}, got.Evidence,
		"closing one tool leaves the other open")

	unknownClose := nativeTestEvent(3, protocol.EventTypeToolCompleted, t0)
	unknownClose.ToolRef, unknownClose.TurnRef = "tool-unknown", testTurnRef
	got = ReduceNative([]NativeEvent{first, second, unknownClose}, t0)
	require.Equal(t, protocol.AgentPhaseRunningTool, got.Phase)
	require.Equal(t, []protocol.ULID{first.EventID}, got.Evidence,
		"an unknown close releases no operation")
}

func TestNativeReducerRequiresMatchingTurnWhenPresent(t *testing.T) {
	t.Parallel()
	first := nativeTestEvent(1, protocol.EventTypeToolStarted, t0)
	first.ToolRef, first.TurnRef = testToolRef, testTurnRef
	second := nativeTestEvent(2, protocol.EventTypeToolStarted, t0)
	second.ToolRef, second.TurnRef = testToolRef, "turn-2"
	wrongTurn := nativeTestEvent(3, protocol.EventTypeToolCompleted, t0)
	wrongTurn.ToolRef, wrongTurn.TurnRef = testToolRef, "turn-3"

	got := ReduceNative([]NativeEvent{first, second, wrongTurn}, t0)
	require.Equal(t, protocol.AgentPhaseRunningTool, got.Phase)
	require.Equal(t, []protocol.ULID{first.EventID}, got.Evidence,
		"a matching reference from another turn closes neither operation")

	closeFirst := nativeTestEvent(4, protocol.EventTypeToolCompleted, t0)
	closeFirst.ToolRef, closeFirst.TurnRef = testToolRef, testTurnRef
	got = ReduceNative([]NativeEvent{first, second, wrongTurn, closeFirst}, t0)
	require.Equal(t, protocol.AgentPhaseRunningTool, got.Phase)
	require.Equal(t, []protocol.ULID{second.EventID}, got.Evidence)
}

func TestNativeReducerPermissionWinsOverEarlyToolStart(t *testing.T) {
	t.Parallel()
	tool := nativeTestEvent(1, protocol.EventTypeToolStarted, t0)
	tool.ToolRef, tool.TurnRef = testToolRef, testTurnRef
	approval := nativeTestEvent(2, protocol.EventTypeApprovalRequested, t0)
	approval.RequestRef, approval.TurnRef = testRequestRef, testTurnRef
	got := ReduceNative([]NativeEvent{tool, approval}, t0)
	require.Equal(t, protocol.AgentPhaseWaitingApproval, got.Phase)
	require.Equal(t, []protocol.ULID{approval.EventID}, got.Evidence)
	require.Equal(t, t0.Add(NativeOperationTTL), got.ExpiresAt)
}

func TestNativeReducerLatePermissionDoesNotActAsOperationClock(t *testing.T) {
	t.Parallel()
	tool := nativeTestEvent(1, protocol.EventTypeToolStarted, t0)
	tool.ToolRef, tool.TurnRef = testToolRef, testTurnRef
	approval := nativeTestEvent(
		2, protocol.EventTypeApprovalRequested, t0.Add(NativeOperationTTL+time.Second),
	)
	approval.RequestRef, approval.TurnRef = testRequestRef, testTurnRef

	got := ReduceNative([]NativeEvent{tool, approval}, approval.At)
	require.Equal(t, protocol.AgentPhaseWaitingApproval, got.Phase)
	require.Equal(t, []protocol.ULID{approval.EventID}, got.Evidence)
	require.Equal(t, approval.At.Add(NativeOperationTTL), got.ExpiresAt)

	got = ReduceNative(
		[]NativeEvent{tool, approval}, approval.At.Add(NativeOperationTTL),
	)
	require.Equal(t, protocol.AgentPhaseUnknown, got.Phase,
		"without newer durable evidence, the open epoch expires to uncertainty")
	require.False(t, got.Blocks, "expired native evidence withdraws its veto")
}

func TestNativeReducerGapAndExpiryWithdrawAuthority(t *testing.T) {
	t.Parallel()
	tool := nativeTestEvent(1, protocol.EventTypeToolStarted, t0)
	tool.ToolRef = testToolRef
	gap := nativeTestEvent(3, protocol.EventTypeToolCompleted, t0)
	gap.ToolRef = tool.ToolRef
	got := ReduceNative([]NativeEvent{tool, gap}, t0)
	require.False(t, got.Blocks)
	require.Equal(t, protocol.AgentPhaseUnknown, got.Phase)
	require.True(t, got.Stale)

	expired := nativeTestEvent(1, protocol.EventTypeApprovalRequested,
		t0.Add(-NativeOperationTTL-time.Millisecond))
	expired.RequestRef = testRequestRef
	got = ReduceNative([]NativeEvent{expired}, t0)
	require.False(t, got.Blocks)
	require.Equal(t, protocol.AgentPhaseUnknown, got.Phase)
	require.Equal(t, []protocol.ULID{expired.EventID}, got.Evidence)

	secondExpired := nativeTestEvent(2, protocol.EventTypeApprovalRequested, expired.At)
	secondExpired.RequestRef = "request-2"
	for range 10 {
		got = ReduceNative([]NativeEvent{secondExpired, expired}, t0)
		require.Equal(t, []protocol.ULID{expired.EventID}, got.Evidence,
			"expiry evidence is stable across map iteration order")
	}
}

func TestNativeReducerExpiredEpochIsNotRevivedByGenericEvent(t *testing.T) {
	t.Parallel()
	available := nativeTestEvent(0, protocol.EventTypeAdapterCapabilitiesChanged, t0)
	available.Availability = NativeAvailable
	approval := nativeTestEvent(1, protocol.EventTypeApprovalRequested, t0)
	approval.RequestRef, approval.TurnRef = "request-old", "turn-old"

	expiredAt := t0.Add(NativeOperationTTL)
	expired := ReduceNative([]NativeEvent{available, approval}, expiredAt)
	require.Equal(t, protocol.AgentPhaseUnknown, expired.Phase)
	require.False(t, expired.Blocks)
	require.Equal(t, []protocol.ULID{approval.EventID}, expired.Evidence)

	notification := nativeTestEvent(
		2, protocol.EventTypeNotificationReceived, expiredAt.Add(time.Second),
	)
	stillExpired := ReduceNative(
		[]NativeEvent{available, approval, notification}, notification.At,
	)
	require.Equal(t, protocol.AgentPhaseUnknown, stillExpired.Phase)
	require.False(t, stillExpired.Blocks,
		"an unrelated receipt cannot renew an expired operation")
	require.Equal(t, []protocol.ULID{approval.EventID}, stillExpired.Evidence)

	fresh := nativeTestEvent(3, protocol.EventTypeApprovalRequested, notification.At)
	fresh.RequestRef, fresh.TurnRef = "request-new", "turn-new"
	rebound := ReduceNative(
		[]NativeEvent{available, approval, notification, fresh}, fresh.At,
	)
	require.Equal(t, protocol.AgentPhaseWaitingApproval, rebound.Phase)
	require.True(t, rebound.Blocks, "fresh verified operation evidence can regain authority")
	require.Equal(t, []protocol.ULID{fresh.EventID}, rebound.Evidence,
		"fresh authority cannot resurrect the invalidated approval")
	require.Equal(t, fresh.At.Add(NativeOperationTTL), rebound.ExpiresAt)
}

func TestNativeReducerAvailabilityNeedsFreshBinding(t *testing.T) {
	t.Parallel()
	tool := nativeTestEvent(1, protocol.EventTypeToolStarted, t0)
	tool.ToolRef = testToolRef
	down := nativeTestEvent(2, protocol.EventTypeAdapterCapabilitiesChanged, t0)
	down.Availability = NativeUnavailable
	got := ReduceNative([]NativeEvent{tool, down}, t0)
	require.Equal(t, protocol.AgentPhaseUnknown, got.Phase)
	require.False(t, got.Blocks, "degradation withdraws native authority")

	up := nativeTestEvent(3, protocol.EventTypeAdapterCapabilitiesChanged, t0)
	up.Availability = NativeAvailable
	got = ReduceNative([]NativeEvent{tool, down, up}, t0)
	require.False(t, got.Blocks, "a new verified binding starts a fresh native epoch")

	gappedUp := nativeTestEvent(4, protocol.EventTypeAdapterCapabilitiesChanged, t0)
	gappedUp.Sequence = 5
	gappedUp.Availability = NativeAvailable
	got = ReduceNative([]NativeEvent{tool, down, gappedUp}, t0)
	require.Equal(t, protocol.AgentPhaseUnknown, got.Phase,
		"a fresh binding cannot erase its own sequence gap")
	require.False(t, got.Blocks)

	neutral := nativeTestEvent(3, protocol.EventTypeAdapterCapabilitiesChanged, t0)
	neutral.Sequence = 2
	neutral.Availability = NativeUnchanged
	got = ReduceNative([]NativeEvent{tool, neutral}, t0)
	require.Equal(t, protocol.AgentPhaseRunningTool, got.Phase,
		"an unrelated capability transition preserves the lifecycle epoch")
}

func TestNativeReducerContinuesAcrossDurablePrefix(t *testing.T) {
	t.Parallel()
	const total = 312
	events := make([]NativeEvent, 0, total)
	for sequence := int64(1); sequence <= total; sequence++ {
		event := nativeTestEvent(sequence, protocol.EventTypeNotificationReceived, t0)
		events = append(events, event)
	}
	tool := nativeTestEvent(total, protocol.EventTypeToolStarted, t0)
	tool.ToolRef, tool.TurnRef = "tool-window", "turn-window"
	events[len(events)-1] = tool

	prefix := &NativeHistory{}
	prefix.Fold(events[:total-256])
	got := prefix.Reduce(events[total-256:], t0.Add(time.Second))
	require.Equal(t, protocol.AgentPhaseRunningTool, got.Phase)
	require.True(t, got.Blocks, "the run's own window cut is continuous durable history")
	require.Equal(t, []protocol.ULID{tool.EventID}, got.Evidence)
	require.Equal(t, t0.Add(NativeOperationTTL), got.ExpiresAt)
}

func TestNativeReducerPrefixUncertaintySurvivesWindowCut(t *testing.T) {
	t.Parallel()
	prefixEvents := make([]NativeEvent, 0, 56)
	for sequence := int64(1); sequence <= 56; sequence++ {
		if sequence == 2 {
			continue
		}
		prefixEvents = append(prefixEvents, nativeTestEvent(
			sequence, protocol.EventTypeNotificationReceived, t0,
		))
	}
	prefix := &NativeHistory{}
	prefix.Fold(prefixEvents)

	window := make([]NativeEvent, 0, 256)
	for sequence := int64(57); sequence <= 312; sequence++ {
		window = append(window, nativeTestEvent(
			sequence, protocol.EventTypeNotificationReceived, t0,
		))
	}
	got := prefix.Reduce(window, t0.Add(time.Second))
	require.Equal(t, protocol.AgentPhaseUnknown, got.Phase)
	require.Equal(t, []protocol.ULID{prefixEvents[1].EventID}, got.Evidence)

	available := nativeTestEvent(200, protocol.EventTypeAdapterCapabilitiesChanged, t0)
	available.Availability = NativeAvailable
	window[200-57] = available
	tool := nativeTestEvent(312, protocol.EventTypeToolStarted, t0)
	tool.ToolRef = "tool-rebound"
	window[len(window)-1] = tool
	got = prefix.Reduce(window, t0.Add(time.Second))
	require.Equal(t, protocol.AgentPhaseRunningTool, got.Phase,
		"only a new continuous verified binding clears prefix uncertainty")
	require.Equal(t, []protocol.ULID{tool.EventID}, got.Evidence)
}
