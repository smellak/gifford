package channel

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

func testQueuedDeliveryPreview(t *testing.T) (*eventstore.Store, *Engine, Message) {
	t.Helper()
	store := testChannelStore(t)
	engine, _ := testGateEngine(t)
	m := testPTYMessage()
	admitGateMessage(t, store, engine, m)
	testInsertPTYDelivery(t, store.DB(), m, 1)
	tx := testBeginGateTx(t, store)
	require.NoError(t, engine.InitializeDelivery(t.Context(), tx, m.ID, m.To[0].Agent, 1))
	require.NoError(t, tx.Commit())
	return store, engine, m
}

func testReadOnlyPreview(
	t *testing.T,
	store *eventstore.Store,
	engine *Engine,
	m Message,
	recipient protocol.AgentID,
	generation int64,
) bool {
	t.Helper()
	tx, err := store.DB().BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
	require.NoError(t, err)
	var before, after int64
	require.NoError(t, tx.QueryRowContext(t.Context(), "SELECT total_changes()").Scan(&before))
	allowed, err := engine.PreviewQueuedDelivery(
		t.Context(), tx, m.ID, recipient, generation,
	)
	require.NoError(t, err)
	require.NoError(t, tx.QueryRowContext(t.Context(), "SELECT total_changes()").Scan(&after))
	require.Equal(t, before, after, "preview must not write through its transaction")
	require.NoError(t, tx.Commit())
	return allowed
}

func testSeedPreviewStepLoop(t *testing.T, store *eventstore.Store, m Message) {
	t.Helper()
	stamp := eventstore.FormatTime(testEngineTime)
	_, err := store.DB().ExecContext(t.Context(), `INSERT INTO communication_loops (
		loop_id, thread_id, loop_generation, owner_node, origin, participants_json,
		mode, budget_profile, root_message_id, max_turns, max_messages,
		max_duration_ms, max_messages_per_minute, max_hops,
		max_consecutive_same_pair, max_retries, max_artifact_count,
		max_artifact_bytes, step_tokens, control_version, started_at, expires_at,
		created_by, updated_at
	) VALUES (?, ?, 1, ?, 'human', ?, 'STEP', 'default', ?, 12, 24,
		300000, 12, 24, 3, 3, 3, 104857600, 1, 1, ?, ?, ?, ?)`,
		testLoopID, m.ThreadID, m.ToNode, `["agent-1","agent-2"]`, m.ID,
		stamp, eventstore.FormatTime(testEngineTime.Add(5*time.Minute)), testOperator, stamp)
	require.NoError(t, err)
	_, err = store.DB().ExecContext(t.Context(), `UPDATE messages
		SET parent_message_id = ?, loop_id = ?, loop_generation = 1, turn_index = 1
		WHERE message_id = ?`,
		protocol.ULID(testOtherMessageID), testLoopID, m.ID)
	require.NoError(t, err)
	_, err = store.DB().ExecContext(t.Context(), `UPDATE delivery_queue
		SET loop_id = ?, loop_generation = 1, turn_index = 1
		WHERE message_id = ? AND agent_id = ? AND delivery_generation = 1`,
		testLoopID, m.ID, m.To[0].Agent)
	require.NoError(t, err)
}

func testPreviewStepTokens(t *testing.T, store *eventstore.Store) int64 {
	t.Helper()
	var tokens int64
	require.NoError(t, store.DB().QueryRowContext(t.Context(),
		"SELECT step_tokens FROM communication_loops WHERE loop_id = ?", testLoopID).
		Scan(&tokens))
	return tokens
}

func TestPreviewQueuedDeliveryUsesFreshControlsWithoutWrites(t *testing.T) {
	tests := []struct {
		name     string
		restrict func(*testing.T, *eventstore.Store)
	}{
		{
			name: "global off",
			restrict: func(t *testing.T, store *eventstore.Store) {
				root := testEngineRoot(testControlID(600))
				root.Mode = protocol.ControlMode(protocol.ChannelModeOff)
				testSeedControl(t, store.DB(), root)
			},
		},
		{
			name: "link hold",
			restrict: func(t *testing.T, store *eventstore.Store) {
				testSeedControl(t, store.DB(), testLinkRoot(
					testControlID(601), testAgent1, testAgent2,
					protocol.PlaneMessage, protocol.LinkModeHold,
				))
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, engine, m := testQueuedDeliveryPreview(t)
			engine.entropy = errorReader{}

			beforeTriple := testReadTriple(t, store.DB(), "delivery_queue", m.ID)
			require.Equal(t, protocol.ControlStateAllowed, beforeTriple.state)
			require.Equal(t, protocol.DecisionReasonAllowed, beforeTriple.reason)
			beforeDecisions := testDecisionCount(t, store.DB(), m.ID)
			require.True(t, testReadOnlyPreview(
				t, store, engine, m, m.To[0].Agent, 1,
			), "the initially allowed queued row should pass")

			test.restrict(t, store)
			testSetEpoch(t, store.DB(), 1)
			require.Equal(t, beforeTriple, testReadTriple(t, store.DB(), "delivery_queue", m.ID),
				"the queued control triple remains stale until the real gate runs")
			require.False(t, testReadOnlyPreview(
				t, store, engine, m, m.To[0].Agent, 1,
			), "a fresh transactional view must see the new restriction")

			require.Equal(t, beforeTriple, testReadTriple(t, store.DB(), "delivery_queue", m.ID))
			require.Equal(t, beforeDecisions, testDecisionCount(t, store.DB(), m.ID))
		})
	}
}

func TestPreviewQueuedDeliveryStepFailsClosedWithoutConsumption(t *testing.T) {
	store, engine, m := testQueuedDeliveryPreview(t)
	testSeedPreviewStepLoop(t, store, m)
	beforeDecisions := testDecisionCount(t, store.DB(), m.ID)
	beforeTokens := testPreviewStepTokens(t, store)

	require.False(t, testReadOnlyPreview(t, store, engine, m, m.To[0].Agent, 1),
		"aggregate STEP tokens do not authorize a delivery before per-delivery grants exist")
	require.Equal(t, beforeDecisions, testDecisionCount(t, store.DB(), m.ID))
	require.Equal(t, beforeTokens, testPreviewStepTokens(t, store),
		"preview must not reserve or consume a STEP token")
}

func TestPreviewQueuedDeliveryFailsClosedOutsideQueuedGeneration(t *testing.T) {
	store, engine, m := testQueuedDeliveryPreview(t)
	engine.entropy = errorReader{}

	require.False(t, testReadOnlyPreview(t, store, engine, m, m.To[0].Agent, 2))
	require.False(t, testReadOnlyPreview(t, store, engine, m, testAgent3, 1))
	_, err := store.DB().ExecContext(t.Context(), `UPDATE delivery_queue SET state = 'CLAIMED'
		WHERE message_id = ? AND agent_id = ? AND delivery_generation = 1`,
		m.ID, m.To[0].Agent)
	require.NoError(t, err)
	require.False(t, testReadOnlyPreview(t, store, engine, m, m.To[0].Agent, 1))
}

func TestPreviewQueuedDeliveryRejectsInvalidIdentity(t *testing.T) {
	store, engine, m := testQueuedDeliveryPreview(t)
	_, err := engine.PreviewQueuedDelivery(
		t.Context(), nil, m.ID, m.To[0].Agent, 1,
	)
	require.ErrorIs(t, err, ErrInvalidQuery)
	tx := testBeginGateTx(t, store)
	_, err = engine.PreviewQueuedDelivery(t.Context(), tx, "bad", m.To[0].Agent, 1)
	require.ErrorIs(t, err, ErrInvalidQuery)
	_, err = engine.PreviewQueuedDelivery(t.Context(), tx, m.ID, "!!", 1)
	require.ErrorIs(t, err, ErrInvalidQuery)
	_, err = engine.PreviewQueuedDelivery(t.Context(), tx, m.ID, m.To[0].Agent, 0)
	require.ErrorIs(t, err, ErrInvalidQuery)
	require.NoError(t, tx.Rollback())
}
