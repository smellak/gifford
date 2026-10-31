package channel

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

const testRootMessageID = "01ARZ3NDEKTSV4RRFFQ69G5FAV"

func testLoopRow(id protocol.LoopID, thread protocol.ThreadID, generation int64) protocol.Loop {
	started := testRecordTime
	return protocol.Loop{
		LoopID:         id,
		ThreadID:       thread,
		LoopGeneration: generation,
		OwnerNode:      testNodeA,
		Origin:         protocol.LoopOriginImplicit,
		Participants:   []protocol.AgentID{testAgent1, testAgent2},
		Mode:           protocol.LoopModeAuto,
		EffectiveMode:  protocol.LoopModeAuto,
		BudgetProfile:  protocol.BudgetProfileDefault,
		RootMessageID:  testRootMessageID,
		Budget:         expectedBudgets()[protocol.BudgetProfileDefault],
		ControlVersion: 1,
		StartedAt:      started,
		ExpiresAt:      started.Add(10 * time.Minute),
		CreatedBy:      testNodeA,
		UpdatedAt:      started,
	}
}

func TestLoopRepositoryCreateReadAndCAS(t *testing.T) {
	t.Parallel()

	store := testChannelStore(t)
	ctx := t.Context()
	first := testLoopRow(testLoopID, testThreadID, 1)
	require.NoError(t, createLoopCAS(ctx, store.DB(), first, 0))

	byID, found, err := selectLoopByID(ctx, store.DB(), first.LoopID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, first, byID)
	active, found, err := selectActiveLoopByThread(ctx, store.DB(), first.ThreadID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, first, active)
	latest, found, err := selectLatestLoopByThread(ctx, store.DB(), first.ThreadID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, first, latest)

	err = createLoopCAS(ctx, store.DB(), first, 0)
	require.ErrorIs(t, err, ErrVersionConflict)
	conflict, ok := VersionConflict(err)
	require.True(t, ok)
	require.Equal(t, first.ControlVersion, conflict.CurrentVersion)
	require.Equal(t, first.LoopGeneration, conflict.CurrentGeneration)
	require.Equal(t, first.LoopID, conflict.CurrentLoopID)

	next := first
	next.Mode = protocol.LoopModeStep
	next.EffectiveMode = protocol.LoopModeStep
	next.Counters.TurnCount = 1
	next.Counters.MessageCount = 2
	next.Counters.StepTokens = 1
	next.LastSenderAgent = testAgent2
	next.ControlVersion = 2
	next.LastProgressAt = testRecordTime.Add(time.Second)
	next.UpdatedAt = testRecordTime.Add(time.Second)
	require.NoError(t, updateLoopCAS(ctx, store.DB(), next, 1))

	got, found, err := selectLoopByID(ctx, store.DB(), first.LoopID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, next, got)

	stale := next
	stale.ControlVersion = 2
	err = updateLoopCAS(ctx, store.DB(), stale, 1)
	require.ErrorIs(t, err, ErrVersionConflict)
	conflict, ok = VersionConflict(err)
	require.True(t, ok)
	require.Equal(t, int64(2), conflict.CurrentVersion)

	changedIdentity := next
	changedIdentity.OwnerNode = testNodeB
	changedIdentity.ControlVersion = 3
	err = updateLoopCAS(ctx, store.DB(), changedIdentity, 2)
	require.ErrorIs(t, err, ErrInvalidLoop)

	decreased := next
	decreased.Counters.MessageCount = 1
	decreased.ControlVersion = 3
	err = updateLoopCAS(ctx, store.DB(), decreased, 2)
	require.ErrorIs(t, err, ErrInvalidLoop)
}

func TestLoopRepositoryGenerations(t *testing.T) {
	t.Parallel()

	store := testChannelStore(t)
	ctx := t.Context()
	first := testLoopRow(testLoopID, testThreadID, 1)
	require.NoError(t, createLoopCAS(ctx, store.DB(), first, 0))

	first.Mode = protocol.LoopModeStopped
	first.EffectiveMode = protocol.LoopModeStopped
	first.StateReason = protocol.LoopStateReasonManual
	first.StoppedAt = testRecordTime.Add(time.Second)
	first.UpdatedAt = first.StoppedAt
	first.ControlVersion = 2
	require.NoError(t, updateLoopCAS(ctx, store.DB(), first, 1))
	reopened := first
	reopened.Mode = protocol.LoopModeAuto
	reopened.EffectiveMode = protocol.LoopModeAuto
	reopened.StoppedAt = time.Time{}
	reopened.ControlVersion = 3
	reopened.UpdatedAt = testRecordTime.Add(2 * time.Second)
	err := updateLoopCAS(ctx, store.DB(), reopened, 2)
	require.ErrorIs(t, err, ErrInvalidLoop, "a terminal generation cannot be resurrected")
	_, found, err := selectActiveLoopByThread(ctx, store.DB(), first.ThreadID)
	require.NoError(t, err)
	require.False(t, found)

	second := testLoopRow("loop_01JB0000000000000000000004", first.ThreadID, 2)
	second.RootMessageID = "01ARZ3NDEKTSV4RRFFQ69G5FAW"
	require.NoError(t, createLoopCAS(ctx, store.DB(), second, 1))

	third := testLoopRow("loop_01JB0000000000000000000005", first.ThreadID, 3)
	third.RootMessageID = "01ARZ3NDEKTSV4RRFFQ69G5FAX"
	err = createLoopCAS(ctx, store.DB(), third, 2)
	require.ErrorIs(t, err, ErrVersionConflict, "an active generation cannot be replaced")
	conflict, ok := VersionConflict(err)
	require.True(t, ok)
	require.Equal(t, second.LoopID, conflict.CurrentLoopID)
	require.Equal(t, int64(2), conflict.CurrentGeneration)

	second.Mode = protocol.LoopModeCompleted
	second.EffectiveMode = protocol.LoopModeCompleted
	second.UpdatedAt = testRecordTime.Add(2 * time.Second)
	second.ControlVersion = 2
	require.NoError(t, updateLoopCAS(ctx, store.DB(), second, 1))

	err = createLoopCAS(ctx, store.DB(), third, 1)
	require.ErrorIs(t, err, ErrVersionConflict, "stale generation CAS loses")
	conflict, ok = VersionConflict(err)
	require.True(t, ok)
	require.Equal(t, int64(2), conflict.CurrentGeneration)
	require.NoError(t, createLoopCAS(ctx, store.DB(), third, 2))

	latest, found, err := selectLatestLoopByThread(ctx, store.DB(), first.ThreadID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, third, latest)
}

func TestLoopRepositoryPreservesPatternState(t *testing.T) {
	t.Parallel()

	store := testChannelStore(t)
	ctx := t.Context()
	loop := testLoopRow(testLoopID, testThreadID, 1)
	require.NoError(t, insertLoop(ctx, store.DB(), loop))
	const pattern = `{"recent":[]}`
	_, err := store.DB().ExecContext(ctx,
		"UPDATE communication_loops SET pattern_state_json = ? WHERE loop_id = ?",
		pattern, loop.LoopID)
	require.NoError(t, err)

	loop.ControlVersion = 2
	loop.UpdatedAt = testRecordTime.Add(time.Second)
	require.NoError(t, updateLoopCAS(ctx, store.DB(), loop, 1))
	var got sql.NullString
	require.NoError(t, store.DB().QueryRowContext(ctx,
		"SELECT pattern_state_json FROM communication_loops WHERE loop_id = ?", loop.LoopID).Scan(&got))
	require.True(t, got.Valid)
	require.JSONEq(t, pattern, got.String)

	_, err = store.DB().ExecContext(ctx,
		"UPDATE communication_loops SET pattern_state_json = 'not-json' WHERE loop_id = ?",
		loop.LoopID)
	require.NoError(t, err)
	_, _, err = selectLoopByID(ctx, store.DB(), loop.LoopID)
	require.ErrorIs(t, err, ErrInvalidLoop)
}

func TestLoopRepositoryRejectsInvalidMutation(t *testing.T) {
	t.Parallel()

	store := testChannelStore(t)
	loop := testLoopRow(testLoopID, testThreadID, 1)
	loop.LocalOverride = protocol.ControlModePaused
	loop.EffectiveMode = protocol.LoopModePaused
	err := insertLoop(t.Context(), store.DB(), loop)
	require.ErrorIs(t, err, ErrInvalidLoop)

	loop = testLoopRow(testLoopID, testThreadID, 1)
	loop.LoopGeneration = 2
	err = createLoopCAS(t.Context(), store.DB(), loop, 0)
	require.ErrorIs(t, err, ErrInvalidLoop)

	missing := testLoopRow(testLoopID, testThreadID, 1)
	missing.ControlVersion = 2
	err = updateLoopCAS(t.Context(), store.DB(), missing, 1)
	require.ErrorIs(t, err, ErrVersionConflict)
}
