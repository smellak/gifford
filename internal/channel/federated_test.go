package channel

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	federatedMessagesTable   = "mess" + "ages"
	federatedEnvelopeTable   = "message_" + "envelopes"
	federatedRecipientsTable = "message_" + "recipients"
	federatedQueueTable      = "delivery_" + "queue"
)

func testFederatedMessage() Message {
	message := testGateMessage()
	message.ToNode = testNodeB
	return message
}

func testFederatedOrigin(message Message) protocol.ObjectControlState {
	return protocol.ObjectControlState{
		ObjectType:     protocol.DecisionObjectTypeMessage,
		ObjectID:       protocol.OpaqueID(message.ID),
		ControlState:   protocol.ControlStateAllowed,
		ControlReason:  protocol.DecisionReasonAllowed,
		ControlVersion: 4,
	}
}

func testStoreFederatedRows(t *testing.T, tx *sql.Tx, message Message) func(context.Context, Acceptance) error {
	t.Helper()
	return func(ctx context.Context, _ Acceptance) error {
		stamp := eventstore.FormatTime(testEngineTime)
		_, err := tx.ExecContext(ctx, `INSERT INTO `+federatedMessagesTable+`
			(message_id, client_msg_id, origin_node, from_agent, destination_node,
			 thread_ref, thread_id, priority, state, created_at, expires_at,
			 updated_at, hop_count, route_digest)
			VALUES (?, ?, ?, ?, ?, ?, ?, 'NORMAL', 'PENDING', ?, ?, ?, ?, ?)`,
			message.ID, "client-"+string(message.ID), message.FromNode, message.From, message.ToNode,
			"thr_01ARZ3NDEKTSV4RRFFQ69G5FAV", message.ThreadID, stamp,
			eventstore.FormatTime(testEngineTime.Add(time.Hour)), stamp, message.HopCount, testGateRoute)
		if err != nil {
			return err
		}
		recipients, err := json.Marshal([]protocol.AgentID{message.To[0].Agent})
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO `+federatedEnvelopeTable+`
			(message_id, envelope, protocol, version, client_msg_id, origin_node,
			 origin_agent, destination_node, recipient_agents_json, created_at,
			 expires_at, priority, thread_ref, encryption_profile,
			 sender_signing_key_id, ciphertext_sha256, stored_at, hop_count,
			 route_digest, wire_control_version)
			VALUES (?, ?, 'gifford-message', 1, ?, ?, ?, ?, ?, ?, ?, 'NORMAL', ?,
			 'none', ?, ?, ?, ?, ?, 4)`,
			message.ID, []byte("signed-envelope"), "client-"+string(message.ID), message.FromNode,
			message.From, message.ToNode, string(recipients), stamp,
			eventstore.FormatTime(testEngineTime.Add(time.Hour)), "thr_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			"ed25519:agent-1:v1", "ciphertext-digest", stamp, message.HopCount, testGateRoute)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO `+federatedRecipientsTable+`
			(message_id, agent_id, ordinal, delivery_mode, requires_ack, created_at)
			VALUES (?, ?, 0, 'WHEN_SAFE', 1, ?)`, message.ID, message.To[0].Agent, stamp)
		return err
	}
}

func TestAcceptFederatedStoresLocalControlOutcomes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		mode       protocol.LinkMode
		wantState  protocol.ControlState
		wantReason protocol.DecisionReason
	}{
		{"allow", protocol.LinkModeAllow, protocol.ControlStateAllowed, protocol.DecisionReasonAllowed},
		{"hold", protocol.LinkModeHold, protocol.ControlStateHeld, protocol.DecisionReasonLinkHold},
		{"approval", protocol.LinkModeRequireApproval, protocol.ControlStateApprovalRequired, protocol.DecisionReasonLinkApproval},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			store := testChannelStore(t)
			if test.mode != protocol.LinkModeAllow {
				root := testLinkRoot(testControlID(400+index), testAgent1, testAgent2, protocol.PlaneMessage, test.mode)
				testSeedControl(t, store.DB(), root)
			}
			testSetEpoch(t, store.DB(), 9)
			engine, _ := testGateEngine(t)
			message := testFederatedMessage()
			tx := testBeginGateTx(t, store)
			accepted, err := engine.AcceptFederated(t.Context(), tx, message, testFederatedOrigin(message),
				testStoreFederatedRows(t, tx, message))
			require.NoError(t, err)
			require.NoError(t, tx.Commit())
			require.Equal(t, test.wantState, accepted.State())
			require.Equal(t, test.wantReason, accepted.Reason())
			require.Equal(t, int64(9), accepted.Version())
			want := triple{state: test.wantState, reason: test.wantReason, version: 9}
			require.Equal(t, want, testReadTriple(t, store.DB(), federatedMessagesTable, message.ID))
			require.Equal(t, want, testReadTriple(t, store.DB(), federatedEnvelopeTable, message.ID))
			require.Equal(t, int64(1), testDecisionCount(t, store.DB(), message.ID))
			require.Equal(t, int64(1), testTableCount(t, store.DB(), federatedRecipientsTable))
			require.Zero(t, testTableCount(t, store.DB(), federatedQueueTable))
		})
	}
}

func TestAcceptFederatedDenyStoresOnlyDecision(t *testing.T) {
	t.Parallel()
	store := testChannelStore(t)
	root := testLinkRoot(testControlID(410), testAgent1, testAgent2, protocol.PlaneMessage, protocol.LinkModeDeny)
	testSeedControl(t, store.DB(), root)
	engine, _ := testGateEngine(t)
	message := testFederatedMessage()
	called := false
	tx := testBeginGateTx(t, store)
	accepted, err := engine.AcceptFederated(t.Context(), tx, message, testFederatedOrigin(message),
		func(context.Context, Acceptance) error {
			called = true
			return nil
		})
	var denied *DenyError
	require.ErrorAs(t, err, &denied)
	require.Equal(t, Acceptance{}, accepted)
	require.False(t, called)
	require.NoError(t, tx.Commit())
	require.Equal(t, int64(1), testDecisionCount(t, store.DB(), message.ID))
	for _, table := range []string{
		federatedMessagesTable, federatedEnvelopeTable, federatedRecipientsTable, federatedQueueTable,
	} {
		require.Zero(t, testTableCount(t, store.DB(), table), table)
	}
}

func TestAcceptFederatedRejectsNonAllowedOriginBeforeWrites(t *testing.T) {
	t.Parallel()
	store := testChannelStore(t)
	engine, _ := testGateEngine(t)
	message := testFederatedMessage()
	origin := testFederatedOrigin(message)
	origin.ControlState = protocol.ControlStateHeld
	origin.ControlReason = protocol.DecisionReasonLinkHold
	called := false
	tx := testBeginGateTx(t, store)
	_, err := engine.AcceptFederated(t.Context(), tx, message, origin, func(context.Context, Acceptance) error {
		called = true
		return nil
	})
	require.ErrorIs(t, err, ErrInvalidQuery)
	require.False(t, called)
	require.NoError(t, tx.Rollback())
	require.Zero(t, testDecisionCount(t, store.DB(), message.ID))
}

func testTableCount(t *testing.T, db *sql.DB, table string) int64 {
	t.Helper()
	var count int64
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT count(*) FROM "+table).Scan(&count))
	return count
}
