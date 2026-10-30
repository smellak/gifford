package channel

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

func testPTYMessage() Message {
	m := testGateMessage()
	// Distinct nodes put both the sender and receiver node components in the
	// version vector exercised by revalidation.
	m.ToNode = testNodeB
	return m
}

func testInsertPTYDelivery(t *testing.T, db *sql.DB, m Message, generation int64) {
	t.Helper()
	stamp := eventstore.FormatTime(testEngineTime)
	_, err := db.ExecContext(t.Context(), `INSERT INTO delivery_queue
		(message_id, agent_id, delivery_generation, from_agent, thread_ref,
		 state, priority, created_at, expires_at, updated_at, hop_count, route_digest)
		VALUES (?, ?, ?, ?, ?, 'QUEUED', 2, ?, ?, ?, ?, ?)`,
		m.ID, m.To[0].Agent, generation, m.From, m.ThreadID, stamp,
		eventstore.FormatTime(testEngineTime.Add(time.Hour)), stamp, m.HopCount, testGateRoute)
	require.NoError(t, err)
}

func testPTYDecisionRow(
	t *testing.T,
	db *sql.DB,
	id protocol.ChannelDecisionID,
) (protocol.DecisionStage, protocol.DecisionOutcome, protocol.DecisionReason) {
	t.Helper()
	var stage protocol.DecisionStage
	var outcome protocol.DecisionOutcome
	var reason protocol.DecisionReason
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT stage, outcome, reason
		FROM channel_control_decisions WHERE decision_id = ?`, id).
		Scan(&stage, &outcome, &reason))
	return stage, outcome, reason
}

func testAuthorizePTY(
	t *testing.T,
	store *eventstore.Store,
	engine *Engine,
	m Message,
	generation int64,
) Decision {
	t.Helper()
	testInsertPTYDelivery(t, store.DB(), m, generation)
	tx := testBeginGateTx(t, store)
	decided, err := engine.AuthorizePTYDelivery(t.Context(), tx, m, generation)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	return decided
}

func TestAuthorizePTYDeliveryRecordsTypedDecision(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		mode       protocol.LinkMode
		want       protocol.DecisionOutcome
		wantReason protocol.DecisionReason
	}{
		{name: "allow", want: Allow, wantReason: protocol.DecisionReasonHoldReleased},
		{name: "hold", mode: protocol.LinkModeHold, want: Hold, wantReason: protocol.DecisionReasonLinkHold},
		// A queued object is held rather than deleted when its link becomes DENY.
		{name: "deny control holds queued object", mode: protocol.LinkModeDeny, want: Hold, wantReason: protocol.DecisionReasonLinkDenied},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			store := testChannelStore(t)
			if test.mode != "" {
				testSeedControl(t, store.DB(), testLinkRoot(
					testControlID(500+index), testAgent1, testAgent2,
					protocol.PlaneMessage, test.mode))
			}
			testSetEpoch(t, store.DB(), 4)
			engine, _ := testGateEngine(t)
			m := testPTYMessage()
			decided := testAuthorizePTY(t, store, engine, m, 2)

			require.Equal(t, protocol.DecisionStageDelivery, decided.Stage())
			require.Equal(t, test.want, decided.Outcome())
			require.Equal(t, test.wantReason, decided.Reason())
			require.Equal(t, int64(2), decided.d.delivery.generation)
			require.Equal(t, int64(4), decided.d.delivery.epoch)
			objectType, objectID := decided.Object()
			require.Equal(t, protocol.DecisionObjectTypeMessage, objectType)
			require.Equal(t, protocol.OpaqueID(m.ID), objectID)
			require.Equal(t, m.To[0].Agent, decided.To())
			stage, outcome, reason := testPTYDecisionRow(t, store.DB(), decided.ID())
			require.Equal(t, protocol.DecisionStageDelivery, stage)
			require.Equal(t, test.want, outcome)
			require.Equal(t, test.wantReason, reason)
			require.Equal(t, int64(1), testDecisionCount(t, store.DB(), m.ID))
		})
	}
}

func TestRevalidateStableDecisionTwiceWithoutConsumption(t *testing.T) {
	t.Parallel()
	store := testChannelStore(t)
	engine, _ := testGateEngine(t)
	m := testPTYMessage()
	decided := testAuthorizePTY(t, store, engine, m, 1)
	before := testDecisionCount(t, store.DB(), m.ID)

	for range 2 {
		tx := testBeginGateTx(t, store)
		require.NoError(t, engine.Revalidate(t.Context(), tx, decided, m, 1))
		require.NoError(t, tx.Commit())
	}

	require.Equal(t, before, testDecisionCount(t, store.DB(), m.ID))
	var attempts int64
	require.NoError(t, store.DB().QueryRowContext(t.Context(), `SELECT attempts
		FROM delivery_queue WHERE message_id = ? AND agent_id = ? AND delivery_generation = 1`,
		m.ID, m.To[0].Agent).Scan(&attempts))
	require.Zero(t, attempts)
}

func requirePTYRevalidationFailure(
	t *testing.T,
	store *eventstore.Store,
	engine *Engine,
	decided Decision,
	m Message,
	generation int64,
) string {
	t.Helper()
	before := testDecisionCount(t, store.DB(), m.ID)
	tx := testBeginGateTx(t, store)
	err := engine.Revalidate(t.Context(), tx, decided, m, generation)
	var changed *RevalidationError
	require.ErrorAs(t, err, &changed)
	require.ErrorIs(t, err, ErrControlChanged)
	require.Equal(t, protocol.DecisionReasonControlChanged, changed.Reason())
	require.NotEmpty(t, changed.Cause())
	require.NoError(t, tx.Commit())
	stage, outcome, reason := testPTYDecisionRow(t, store.DB(), changed.DecisionID())
	require.Equal(t, protocol.DecisionStageRevalidation, stage)
	require.Equal(t, Hold, outcome)
	require.Equal(t, protocol.DecisionReasonControlChanged, reason)
	require.Equal(t, before+1, testDecisionCount(t, store.DB(), m.ID))
	return changed.Cause()
}

func TestRevalidateRejectsInvalidAuthorityAndBinding(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		change func(t *testing.T, store *eventstore.Store, engine *Engine, m Message, d Decision) (Decision, Message, int64)
	}{
		{
			name: "empty decision",
			change: func(_ *testing.T, _ *eventstore.Store, _ *Engine, m Message, _ Decision) (Decision, Message, int64) {
				return Decision{}, m, 1
			},
		},
		{
			name: "wrong stage",
			change: func(t *testing.T, _ *eventstore.Store, _ *Engine, m Message, _ Decision) (Decision, Message, int64) {
				return testDecision(t), m, 1
			},
		},
		{
			name: "other object",
			change: func(_ *testing.T, _ *eventstore.Store, _ *Engine, m Message, d Decision) (Decision, Message, int64) {
				m.ID = "01ARZ3NDEKTSV4RRFFQ69G5FAA"
				return d, m, 1
			},
		},
		{
			name: "other recipient",
			change: func(_ *testing.T, _ *eventstore.Store, _ *Engine, m Message, d Decision) (Decision, Message, int64) {
				m.To[0].Agent = testAgent3
				return d, m, 1
			},
		},
		{
			name: "other generation",
			change: func(_ *testing.T, _ *eventstore.Store, _ *Engine, m Message, d Decision) (Decision, Message, int64) {
				return d, m, 2
			},
		},
		{
			name: "policy no longer allows",
			change: func(_ *testing.T, _ *eventstore.Store, _ *Engine, m Message, d Decision) (Decision, Message, int64) {
				m.To[0].Policy = PolicyDenied
				return d, m, 1
			},
		},
		{
			name: "queue became terminal",
			change: func(t *testing.T, store *eventstore.Store, _ *Engine, m Message, d Decision) (Decision, Message, int64) {
				_, err := store.DB().ExecContext(t.Context(), `UPDATE delivery_queue SET state = 'DELIVERED'
					WHERE message_id = ? AND agent_id = ? AND delivery_generation = 1`, m.ID, m.To[0].Agent)
				require.NoError(t, err)
				return d, m, 1
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			store := testChannelStore(t)
			engine, _ := testGateEngine(t)
			m := testPTYMessage()
			decided := testAuthorizePTY(t, store, engine, m, 1)
			decided, m, generation := test.change(t, store, engine, m, decided)
			requirePTYRevalidationFailure(t, store, engine, decided, m, generation)
		})
	}
}

func TestRevalidateRejectsNonAllowDecision(t *testing.T) {
	t.Parallel()
	store := testChannelStore(t)
	testSeedControl(t, store.DB(), testLinkRoot(
		testControlID(550), testAgent1, testAgent2, protocol.PlaneMessage, protocol.LinkModeHold))
	engine, _ := testGateEngine(t)
	m := testPTYMessage()
	decided := testAuthorizePTY(t, store, engine, m, 1)
	require.Equal(t, Hold, decided.Outcome())
	requirePTYRevalidationFailure(t, store, engine, decided, m, 1)
}

func TestRevalidateRejectsEveryCoveredVersionComponent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		root func() protocol.ChannelControl
	}{
		{"global", func() protocol.ChannelControl { return testEngineRoot(testControlID(560)) }},
		{"sender node", func() protocol.ChannelControl {
			return testNodeRoot(testControlID(561), testNodeA, protocol.ChannelModeActive)
		}},
		{"receiver node", func() protocol.ChannelControl {
			return testNodeRoot(testControlID(562), testNodeB, protocol.ChannelModeActive)
		}},
		{"sender agent", func() protocol.ChannelControl {
			return testAgentRoot(testControlID(563), testAgent1, true)
		}},
		{"receiver agent", func() protocol.ChannelControl {
			return testAgentRoot(testControlID(564), testAgent2, true)
		}},
		{"link", func() protocol.ChannelControl {
			return testLinkRoot(testControlID(565), testAgent1, testAgent2,
				protocol.PlaneMessage, protocol.LinkModeAllow)
		}},
		{"thread", func() protocol.ChannelControl {
			return testThreadRoot(testControlID(566), testThreadID, protocol.LinkModeAllow)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			store := testChannelStore(t)
			engine, _ := testGateEngine(t)
			m := testPTYMessage()
			decided := testAuthorizePTY(t, store, engine, m, 1)
			testSeedControl(t, store.DB(), test.root())
			requirePTYRevalidationFailure(t, store, engine, decided, m, 1)
		})
	}
}

func TestRevalidateRejectsEpochChangeWithoutVectorChange(t *testing.T) {
	t.Parallel()
	store := testChannelStore(t)
	engine, _ := testGateEngine(t)
	m := testPTYMessage()
	decided := testAuthorizePTY(t, store, engine, m, 1)
	testSetEpoch(t, store.DB(), 1)
	cause := requirePTYRevalidationFailure(t, store, engine, decided, m, 1)
	require.Equal(t, "control epoch changed", cause)
}

func TestRevalidateRecordsControlChangeWhenControlsBecomeUnreadable(t *testing.T) {
	t.Parallel()
	store := testChannelStore(t)
	root := testLinkRoot(
		testControlID(570), testAgent1, testAgent2, protocol.PlaneMessage, protocol.LinkModeAllow)
	testSeedControl(t, store.DB(), root)
	engine, _ := testGateEngine(t)
	m := testPTYMessage()
	decided := testAuthorizePTY(t, store, engine, m, 1)
	_, err := store.DB().ExecContext(t.Context(), `UPDATE channel_controls
		SET restore_json = '{' WHERE control_id = ?`, root.ControlID)
	require.NoError(t, err)
	require.NotEmpty(t, requirePTYRevalidationFailure(t, store, engine, decided, m, 1))
}
