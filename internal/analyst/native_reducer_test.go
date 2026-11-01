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
