package channel

import (
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/eventstore"
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

func insertLoopRootMessage(
	t *testing.T,
	store *eventstore.Store,
	id protocol.ULID,
	thread protocol.ThreadID,
	sequence any,
) {
	t.Helper()
	stamp := eventstore.FormatTime(testRecordTime)
	_, err := store.DB().ExecContext(t.Context(), `INSERT INTO messages
		(message_id, client_msg_id, origin_node, from_agent, destination_node,
		 thread_ref, thread_id, priority, state, created_at, expires_at, updated_at,
		 thread_sequence, hop_count, route_digest)
		VALUES (?, ?, ?, ?, ?, ?, ?, 'NORMAL', 'ADMITTED', ?, ?, ?, ?, 0, ?)`,
		id, "client-loop-root-"+string(id), testNodeA, testAgent1, testNodeA,
		"thr_01ARZ3NDEKTSV4RRFFQ69G5FAV", thread, stamp,
		eventstore.FormatTime(testRecordTime.Add(time.Hour)), stamp, sequence, testGateRoute)
	require.NoError(t, err)
	_, err = store.DB().ExecContext(t.Context(), `INSERT INTO message_recipients
		(message_id, agent_id, ordinal, delivery_mode, requires_ack, created_at)
		VALUES (?, ?, 0, 'WHEN_SAFE', 1, ?)`, id, testAgent2, stamp)
	require.NoError(t, err)
}

func testReplyLoopPlan(
	t *testing.T,
	store *eventstore.Store,
	root protocol.ULID,
) replyLoopPlan {
	t.Helper()
	plan, err := testResolveReplyLoop(t, store, root)
	require.NoError(t, err)
	return plan
}

func testResolveReplyLoop(
	t *testing.T,
	store *eventstore.Store,
	root protocol.ULID,
) (replyLoopPlan, error) {
	t.Helper()
	tx, err := store.DB().BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
	require.NoError(t, err)
	parent, err := selectReplyParent(t.Context(), tx, root)
	require.NoError(t, err)
	plan, resolveErr := resolveReplyLoop(t.Context(), tx, parent)
	require.NoError(t, tx.Rollback())
	return plan, resolveErr
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
	const legacyRootMessageID = "01ARZ3NDEKTSV4RRFFQ69G5FAR"
	const priorRootMessageID = "01ARZ3NDEKTSV4RRFFQ69G5FAT"
	insertLoopRootMessage(t, store, legacyRootMessageID, first.ThreadID, nil)
	insertLoopRootMessage(t, store, priorRootMessageID, first.ThreadID, 6)
	insertLoopRootMessage(t, store, first.RootMessageID, first.ThreadID, 7)

	first.Mode = protocol.LoopModeStopped
	first.EffectiveMode = protocol.LoopModeStopped
	first.StateReason = protocol.LoopStateReasonManual
	first.StoppedAt = testRecordTime.Add(time.Second)
	first.UpdatedAt = first.StoppedAt
	first.ControlVersion = 2
	require.NoError(t, updateLoopCAS(ctx, store.DB(), first, 1))
	closedAt, known, err := selectLoopClosureSequence(ctx, store.DB(), first.LoopID)
	require.NoError(t, err)
	require.True(t, known)
	require.Equal(t, int64(7), closedAt)
	_, err = store.DB().ExecContext(ctx,
		"UPDATE messages SET thread_sequence = NULL WHERE message_id = ?", first.RootMessageID)
	require.NoError(t, err, "an exact legacy root retains its durable loop binding")
	require.NoError(t, updateLoopCAS(ctx, store.DB(), first, 2), "an exact terminal no-op is idempotent")
	for name, mutate := range map[string]func(*protocol.Loop){
		"counter": func(loop *protocol.Loop) { loop.Counters.MessageCount++ },
		"budget":  func(loop *protocol.Loop) { loop.Budget.MaxTurns-- },
		"tokens":  func(loop *protocol.Loop) { loop.Counters.StepTokens++ },
	} {
		changed := first
		mutate(&changed)
		changed.ControlVersion++
		changed.UpdatedAt = changed.UpdatedAt.Add(time.Second)
		err := updateLoopCAS(ctx, store.DB(), changed, first.ControlVersion)
		require.ErrorIs(t, err, ErrInvalidLoop, "terminal %s", name)
	}
	reopened := first
	reopened.Mode = protocol.LoopModeAuto
	reopened.EffectiveMode = protocol.LoopModeAuto
	reopened.StoppedAt = time.Time{}
	reopened.ControlVersion = 3
	reopened.UpdatedAt = testRecordTime.Add(2 * time.Second)
	err = updateLoopCAS(ctx, store.DB(), reopened, 2)
	require.ErrorIs(t, err, ErrInvalidLoop, "a terminal generation cannot be resurrected")
	_, found, err := selectActiveLoopByThread(ctx, store.DB(), first.ThreadID)
	require.NoError(t, err)
	require.False(t, found)

	const nextRootMessageID = "01ARZ3NDEKTSV4RRFFQ69G5FAW"
	insertLoopRootMessage(t, store, nextRootMessageID, first.ThreadID, 8)

	oldPlan := testReplyLoopPlan(t, store, first.RootMessageID)
	require.False(t, oldPlan.opening)
	require.NotNil(t, oldPlan.loop)
	require.Equal(t, first.LoopID, oldPlan.loop.LoopID, "a delayed old-root reply stays terminal")
	priorPlan := testReplyLoopPlan(t, store, priorRootMessageID)
	require.False(t, priorPlan.opening)
	require.NotNil(t, priorPlan.loop)
	require.Equal(t, first.LoopID, priorPlan.loop.LoopID,
		"another root admitted before closure stays in the first generation")
	newPlan := testReplyLoopPlan(t, store, nextRootMessageID)
	require.True(t, newPlan.opening)
	require.Nil(t, newPlan.loop)
	require.Equal(t, int64(1), newPlan.expectedGeneration)

	second := testLoopRow("loop_01JB0000000000000000000004", first.ThreadID, 2)
	second.RootMessageID = nextRootMessageID
	require.NoError(t, createLoopCAS(ctx, store.DB(), second, 1))
	const secondRangeRootMessageID = "01ARZ3NDEKTSV4RRFFQ69G5FAX"
	insertLoopRootMessage(t, store, secondRangeRootMessageID, first.ThreadID, 9)
	for name, root := range map[string]protocol.ULID{
		"original root":       first.RootMessageID,
		"root before closure": priorRootMessageID,
	} {
		plan := testReplyLoopPlan(t, store, root)
		require.NotNil(t, plan.loop, name)
		require.Equal(t, first.LoopID, plan.loop.LoopID, name)
		require.Equal(t, protocol.LoopModeStopped, plan.loop.Mode, name)
	}
	activePlan := testReplyLoopPlan(t, store, nextRootMessageID)
	require.NotNil(t, activePlan.loop)
	require.Equal(t, second.LoopID, activePlan.loop.LoopID)
	require.Equal(t, protocol.LoopModeAuto, activePlan.loop.Mode)
	activeRangePlan := testReplyLoopPlan(t, store, secondRangeRootMessageID)
	require.NotNil(t, activeRangePlan.loop)
	require.Equal(t, second.LoopID, activeRangePlan.loop.LoopID)
	legacyPlan, legacyErr := testResolveReplyLoop(t, store, legacyRootMessageID)
	require.ErrorIs(t, legacyErr, ErrInvalidQuery)
	require.Nil(t, legacyPlan.loop)
	require.False(t, legacyPlan.opening,
		"an unbound legacy root cannot enter a generation after a closed boundary")

	third := testLoopRow("loop_01JB0000000000000000000005", first.ThreadID, 3)
	const thirdRootMessageID = "01ARZ3NDEKTSV4RRFFQ69G5FAY"
	third.RootMessageID = thirdRootMessageID
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
	closedAt, known, err = selectLoopClosureSequence(ctx, store.DB(), second.LoopID)
	require.NoError(t, err)
	require.True(t, known)
	require.Equal(t, int64(9), closedAt)

	err = createLoopCAS(ctx, store.DB(), third, 1)
	require.ErrorIs(t, err, ErrVersionConflict, "stale generation CAS loses")
	conflict, ok = VersionConflict(err)
	require.True(t, ok)
	require.Equal(t, int64(2), conflict.CurrentGeneration)
	insertLoopRootMessage(t, store, thirdRootMessageID, first.ThreadID, 10)
	thirdPlan := testReplyLoopPlan(t, store, thirdRootMessageID)
	require.True(t, thirdPlan.opening)
	require.Nil(t, thirdPlan.loop)
	require.Equal(t, int64(2), thirdPlan.expectedGeneration)
	require.NoError(t, createLoopCAS(ctx, store.DB(), third, 2))

	for name, test := range map[string]struct {
		root protocol.ULID
		loop protocol.Loop
	}{
		"first generation root":   {root: first.RootMessageID, loop: first},
		"first generation range":  {root: priorRootMessageID, loop: first},
		"second generation root":  {root: nextRootMessageID, loop: second},
		"second generation range": {root: secondRangeRootMessageID, loop: second},
		"active successor root":   {root: thirdRootMessageID, loop: third},
	} {
		plan := testReplyLoopPlan(t, store, test.root)
		require.False(t, plan.opening, name)
		require.NotNil(t, plan.loop, name)
		require.Equal(t, test.loop.LoopID, plan.loop.LoopID, name)
		require.Equal(t, test.loop.Mode, plan.loop.Mode, name)
	}

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

func TestControllerLoopReadsAndFilters(t *testing.T) {
	t.Parallel()

	controller, _, _ := testController(t)
	active := testLoopRow(testLoopID, testThreadID, 1)
	closed := testLoopRow("loop_01JB0000000000000000000004", "thread-2", 1)
	closed.Mode = protocol.LoopModeStopped
	closed.EffectiveMode = protocol.LoopModeStopped
	closed.StateReason = protocol.LoopStateReasonOwnerStop
	closed.StoppedAt = testRecordTime
	require.NoError(t, insertLoop(t.Context(), controller.db, active))
	require.NoError(t, insertLoop(t.Context(), controller.db, closed))

	got, err := controller.Loop(t.Context(), active.LoopID)
	require.NoError(t, err)
	require.Equal(t, active, got)
	_, err = controller.Loop(t.Context(), "loop_01JB0000000000000000000005")
	require.ErrorIs(t, err, ErrNotFound)

	all, err := controller.Loops(t.Context(), protocol.ListLoopsParams{})
	require.NoError(t, err)
	require.Equal(t, []protocol.Loop{active, closed}, all.Loops)
	listed, err := controller.Loops(t.Context(), protocol.ListLoopsParams{Active: true})
	require.NoError(t, err)
	require.Equal(t, []protocol.Loop{active}, listed.Loops)
	listed, err = controller.Loops(t.Context(), protocol.ListLoopsParams{ThreadID: closed.ThreadID})
	require.NoError(t, err)
	require.Equal(t, []protocol.Loop{closed}, listed.Loops)
	listed, err = controller.Loops(t.Context(), protocol.ListLoopsParams{AgentID: testAgent3})
	require.NoError(t, err)
	require.Empty(t, listed.Loops)
}

func TestControllerLoopBudgetModeAndStop(t *testing.T) {
	t.Parallel()

	controller, clk, sink := testController(t)
	actor := Actor{Type: protocol.ActorTypeHuman, ID: testOperator}
	loop := testLoopRow(testLoopID, testThreadID, 1)
	loop.Mode = protocol.LoopModePaused
	loop.EffectiveMode = protocol.LoopModePaused
	loop.StateReason = protocol.LoopStateReasonBudgetExhausted
	loop.Counters.MessageCount = loop.Budget.MaxMessages
	require.NoError(t, insertLoop(t.Context(), controller.db, loop))

	tx, err := controller.db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	approval, created, err := controller.engine.ensureLoopBudgetApproval(t.Context(), tx, loop, clk.Now())
	require.NoError(t, err)
	require.True(t, created)
	require.NoError(t, tx.Commit())

	addTurns, addMessages, addDuration := int64(5), int64(2), int64(time.Minute/time.Millisecond)
	extended, err := controller.ExtendLoopBudget(t.Context(), protocol.ExtendLoopBudgetParams{
		LoopID: loop.LoopID, AddTurns: &addTurns, AddMessages: &addMessages,
		AddDurationMS: &addDuration, Reason: "operator extended budget", ExpectedVersion: 1,
	}, actor)
	require.NoError(t, err)
	require.Equal(t, protocol.LoopModeAuto, extended.Mode)
	require.Empty(t, extended.StateReason)
	require.Equal(t, protocol.BudgetProfileCustom, extended.BudgetProfile)
	require.Equal(t, loop.Budget.MaxTurns+addTurns, extended.Budget.MaxTurns)
	require.Equal(t, loop.Budget.MaxMessages+addMessages, extended.Budget.MaxMessages)
	require.Equal(t, loop.Budget.MaxDurationMS+addDuration, extended.Budget.MaxDurationMS)
	require.Equal(t, loop.ExpiresAt.Add(time.Minute), extended.ExpiresAt)
	require.Equal(t, int64(2), extended.ControlVersion)
	resolved, err := controller.Approval(t.Context(), approval.ApprovalID)
	require.NoError(t, err)
	require.Equal(t, protocol.ApprovalStateSuperseded, resolved.State)
	require.Equal(t, testOperator, resolved.ResolvedBy)
	require.Equal(t, []protocol.EventType{
		protocol.EventTypeLoopBudgetExtended, protocol.EventTypeLoopResumed,
	}, []protocol.EventType{sink.events[0].Type, sink.events[1].Type})

	_, err = controller.SetLoopMode(t.Context(), protocol.SetLoopModeParams{
		LoopID: loop.LoopID, Mode: protocol.LoopModeStep, ExpectedVersion: 1,
	}, actor)
	conflict, ok := VersionConflict(err)
	require.True(t, ok)
	require.Equal(t, int64(2), conflict.CurrentVersion)

	paused, err := controller.SetLoopMode(t.Context(), protocol.SetLoopModeParams{
		LoopID: loop.LoopID, Mode: protocol.LoopModePaused, Reason: "operator pause", ExpectedVersion: 2,
	}, actor)
	require.NoError(t, err)
	require.Equal(t, protocol.LoopStateReasonManual, paused.StateReason)

	one := int64(1)
	stillPaused, err := controller.ExtendLoopBudget(t.Context(), protocol.ExtendLoopBudgetParams{
		LoopID: loop.LoopID, AddMessages: &one, ExpectedVersion: 3,
	}, actor)
	require.NoError(t, err)
	require.Equal(t, protocol.LoopModePaused, stillPaused.Mode)
	require.Equal(t, protocol.LoopStateReasonManual, stillPaused.StateReason)

	tx, err = controller.db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	modeApproval, created, err := controller.engine.ensureLoopBudgetApproval(t.Context(), tx, stillPaused, clk.Now())
	require.NoError(t, err)
	require.True(t, created)
	require.NoError(t, tx.Commit())

	stepped, err := controller.SetLoopMode(t.Context(), protocol.SetLoopModeParams{
		LoopID: loop.LoopID, Mode: protocol.LoopModeStep, ExpectedVersion: 4,
	}, actor)
	require.NoError(t, err)
	require.Equal(t, protocol.LoopModeStep, stepped.Mode)
	require.Empty(t, stepped.StateReason)
	modeResolved, err := controller.Approval(t.Context(), modeApproval.ApprovalID)
	require.NoError(t, err)
	require.Equal(t, protocol.ApprovalStateSuperseded, modeResolved.State)

	tx, err = controller.db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	stepApproval, created, err := controller.engine.ensurePendingApproval(t.Context(), tx, pendingApproval{
		source: protocol.ApprovalSourceLoopStep, objectType: protocol.ApprovalObjectTypeMessage,
		objectID: testRootMessageID, agentID: testAgent2, loopID: loop.LoopID,
		options: []protocol.ApprovalOption{"steps:1", "stop"},
	}, clk.Now())
	require.NoError(t, err)
	require.True(t, created)
	require.NoError(t, tx.Commit())

	stopped, err := controller.StopLoop(t.Context(), protocol.StopLoopParams{
		LoopID: loop.LoopID, Reason: "operator stop", ExpectedVersion: 5,
	}, actor)
	require.NoError(t, err)
	require.Equal(t, protocol.LoopModeStopped, stopped.Mode)
	require.Equal(t, protocol.LoopStateReasonOwnerStop, stopped.StateReason)
	require.Equal(t, clk.Now(), stopped.StoppedAt)
	require.Equal(t, int64(6), stopped.ControlVersion)
	stepResolved, err := controller.Approval(t.Context(), stepApproval.ApprovalID)
	require.NoError(t, err)
	require.Equal(t, protocol.ApprovalStateSuperseded, stepResolved.State)
	require.Equal(t, protocol.EventTypeLoopStopped, sink.events[len(sink.events)-1].Type)
	var eventValue struct {
		Mode protocol.LoopMode `json:"mode"`
	}
	require.NoError(t, json.Unmarshal(sink.events[len(sink.events)-1].Payload.NewValue, &eventValue))
	require.Equal(t, protocol.LoopModeStopped, eventValue.Mode)
}
