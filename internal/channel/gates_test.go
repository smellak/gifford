package channel

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	testGateMessageID = "01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testGateClientID  = "client-gate-1"
	testGateRoute     = "sha256:route"
	testGateDecision  = "chd_01ARZ3NDEKTSV4RRFFQ69G5FAV"
)

func testGateMessage() Message {
	return Message{
		ID:       testGateMessageID,
		From:     testAgent1,
		FromNode: testNodeA,
		To: []Recipient{{
			Agent:       testAgent2,
			Deliverable: true,
			Policy:      PolicyAllowed,
		}},
		ToNode:   testNodeA,
		ThreadID: testThreadID,
	}
}

func testGateEngine(t *testing.T) (*Engine, *advancingClock) {
	t.Helper()
	clock := &advancingClock{next: testEngineTime, step: time.Second}
	engine, err := New(Options{Clock: clock})
	require.NoError(t, err)
	return engine, clock
}

func testBeginGateTx(t *testing.T, store *eventstore.Store) *sql.Tx {
	t.Helper()
	tx, err := store.DB().BeginTx(t.Context(), nil)
	require.NoError(t, err)
	return tx
}

func testStoreMessage(t *testing.T, tx *sql.Tx, m Message) func(context.Context, protocol.AdmissionResult) error {
	t.Helper()
	return func(ctx context.Context, result protocol.AdmissionResult) error {
		raw, err := json.Marshal(result)
		if err != nil {
			return err
		}
		stamp := eventstore.FormatTime(testEngineTime)
		_, err = tx.ExecContext(ctx, `INSERT INTO messages
			(message_id, client_msg_id, origin_node, from_agent, destination_node,
			 thread_ref, thread_id, priority, state, created_at, expires_at,
			 updated_at, hop_count, route_digest, admission_result_json)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			m.ID, testGateClientID, m.FromNode, m.From, m.ToNode,
			"thr_01ARZ3NDEKTSV4RRFFQ69G5FAV", m.ThreadID, "NORMAL", "ADMITTED",
			stamp, eventstore.FormatTime(testEngineTime.Add(time.Hour)), stamp,
			m.HopCount, testGateRoute, string(raw))
		return err
	}
}

func testStoreEnvelope(t *testing.T, tx *sql.Tx, m Message, wireVersion int64) func(context.Context) error {
	t.Helper()
	return func(ctx context.Context) error {
		stamp := eventstore.FormatTime(testEngineTime)
		recipients, err := json.Marshal([]protocol.AgentID{m.To[0].Agent})
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO message_envelopes
			(message_id, envelope, protocol, version, client_msg_id, origin_node,
			 origin_agent, destination_node, recipient_agents_json, created_at,
			 expires_at, priority, thread_ref, encryption_profile,
			 sender_signing_key_id, ciphertext_sha256, stored_at, hop_count,
			 route_digest, wire_control_version)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			m.ID, []byte("signed-envelope"), "gifford-message", 1, testGateClientID,
			m.FromNode, m.From, m.ToNode, string(recipients), stamp,
			eventstore.FormatTime(testEngineTime.Add(time.Hour)), "NORMAL",
			"thr_01ARZ3NDEKTSV4RRFFQ69G5FAV", "none", "ed25519:agent-1:v1",
			"sha256:ciphertext", stamp, m.HopCount, testGateRoute, wireVersion)
		return err
	}
}

func testSeedControl(t *testing.T, db *sql.DB, root protocol.ChannelControl) {
	t.Helper()
	params, err := controlParams(root)
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), testQueryText(t, "controls_insert.sql"), params...)
	require.NoError(t, err)
}

func testSetEpoch(t *testing.T, db *sql.DB, epoch int64) {
	t.Helper()
	_, err := db.ExecContext(t.Context(), "UPDATE channel_epoch SET epoch = ? WHERE id = 1", epoch)
	require.NoError(t, err)
}

func testReadTriple(t *testing.T, db *sql.DB, table string, id protocol.ULID) triple {
	t.Helper()
	var got triple
	err := db.QueryRowContext(t.Context(), "SELECT control_state, control_reason, control_version FROM "+table+" WHERE message_id = ?", id).
		Scan(&got.state, &got.reason, &got.version)
	require.NoError(t, err)
	return got
}

func testDecisionCount(t *testing.T, db *sql.DB, id protocol.ULID) int64 {
	t.Helper()
	var count int64
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT count(*) FROM channel_control_decisions WHERE object_id = ?", id).Scan(&count))
	return count
}

func TestAdmitAllow(t *testing.T) {
	store := testChannelStore(t)
	engine, clock := testGateEngine(t)
	m := testGateMessage()
	tx := testBeginGateTx(t, store)
	decision, result, err := engine.Admit(t.Context(), tx, m, testStoreMessage(t, tx, m))
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	require.Equal(t, Allow, decision.Outcome())
	require.Equal(t, protocol.AdmissionStatusAccepted, result.Result)
	require.Equal(t, decision.ID(), result.DecisionID)
	require.Equal(t, m.ID, result.MessageID)
	require.Equal(t, int64(0), result.ControlVersion)
	require.Equal(t, 1, clock.readCount())
	require.Equal(t, testEngineTime, decision.DecidedAt())
	require.Equal(t, triple{protocol.ControlStateAllowed, protocol.DecisionReasonAllowed, 0},
		testReadTriple(t, store.DB(), "messages", m.ID))
	require.Equal(t, int64(1), testDecisionCount(t, store.DB(), m.ID))
	require.Equal(t, testEngineTime.UnixMilli(), decisionULIDMillis(t, decision.ID()))
}

func TestAdmitHoldAndDeny(t *testing.T) {
	t.Run("hold", func(t *testing.T) {
		store := testChannelStore(t)
		root := testLinkRoot(testControlID(201), testAgent1, testAgent2, protocol.PlaneMessage, protocol.LinkModeHold)
		testSeedControl(t, store.DB(), root)
		testSetEpoch(t, store.DB(), 7)
		engine, _ := testGateEngine(t)
		m := testGateMessage()
		tx := testBeginGateTx(t, store)
		decision, result, err := engine.Admit(t.Context(), tx, m, testStoreMessage(t, tx, m))
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
		require.Equal(t, Hold, decision.Outcome())
		require.Equal(t, protocol.AdmissionStatusAcceptedHeld, result.Result)
		require.Equal(t, protocol.DecisionReasonLinkHold, result.ControlReason)
		require.Equal(t, int64(7), result.ControlVersion)
		require.Equal(t, triple{protocol.ControlStateHeld, protocol.DecisionReasonLinkHold, 7},
			testReadTriple(t, store.DB(), "messages", m.ID))
	})

	t.Run("deny", func(t *testing.T) {
		store := testChannelStore(t)
		root := testLinkRoot(testControlID(202), testAgent1, testAgent2, protocol.PlaneMessage, protocol.LinkModeDeny)
		testSeedControl(t, store.DB(), root)
		engine, _ := testGateEngine(t)
		m := testGateMessage()
		called := false
		tx := testBeginGateTx(t, store)
		decision, result, err := engine.Admit(t.Context(), tx, m, func(context.Context, protocol.AdmissionResult) error {
			called = true
			return nil
		})
		var denied *DenyError
		require.ErrorAs(t, err, &denied)
		require.ErrorIs(t, err, ErrDenied)
		require.Equal(t, protocol.ErrorCodeLinkDenied, denied.Code())
		require.True(t, decision.IsZero())
		require.Equal(t, protocol.AdmissionResult{}, result)
		require.False(t, called)
		require.NoError(t, tx.Commit())
		require.Equal(t, int64(1), testDecisionCount(t, store.DB(), m.ID))
		var count int64
		require.NoError(t, store.DB().QueryRowContext(t.Context(), "SELECT count(*) FROM messages").Scan(&count))
		require.Zero(t, count)
	})
}

func TestGateUnavailableAndApproval(t *testing.T) {
	t.Run("bad controls hold", func(t *testing.T) {
		store := testChannelStore(t)
		root := testLinkRoot(testControlID(203), testAgent1, testAgent2, protocol.PlaneMessage, protocol.LinkModeHold)
		testSeedControl(t, store.DB(), root)
		_, err := store.DB().ExecContext(t.Context(), "UPDATE channel_controls SET restore_json = '{' WHERE control_id = ?", root.ControlID)
		require.NoError(t, err)
		testSetEpoch(t, store.DB(), 9)
		engine, _ := testGateEngine(t)
		m := testGateMessage()
		tx := testBeginGateTx(t, store)
		_, result, err := engine.Admit(t.Context(), tx, m, testStoreMessage(t, tx, m))
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
		require.Equal(t, protocol.AdmissionStatusAcceptedHeld, result.Result)
		require.Equal(t, protocol.DecisionReasonControlUnavailable, result.ControlReason)
		require.Equal(t, int64(9), testReadTriple(t, store.DB(), "messages", m.ID).version)
	})

	for _, name := range []string{"missing epoch", "invalid epoch"} {
		t.Run(name, func(t *testing.T) {
			store := testChannelStore(t)
			if name == "missing epoch" {
				_, err := store.DB().ExecContext(t.Context(), "DELETE FROM channel_epoch")
				require.NoError(t, err)
			} else {
				_, err := store.DB().ExecContext(t.Context(), "PRAGMA ignore_check_constraints = ON")
				require.NoError(t, err)
				_, err = store.DB().ExecContext(t.Context(), "UPDATE channel_epoch SET epoch = 'x'")
				require.NoError(t, err)
			}
			engine, _ := testGateEngine(t)
			m := testGateMessage()
			called := false
			tx := testBeginGateTx(t, store)
			_, _, err := engine.Admit(t.Context(), tx, m, func(context.Context, protocol.AdmissionResult) error {
				called = true
				return nil
			})
			require.ErrorIs(t, err, ErrUnavailable)
			require.False(t, called)
			require.NoError(t, tx.Rollback())
			require.Zero(t, testDecisionCount(t, store.DB(), m.ID))
		})
	}

	t.Run("approval unavailable", func(t *testing.T) {
		store := testChannelStore(t)
		root := testLinkRoot(testControlID(204), testAgent1, testAgent2, protocol.PlaneMessage, protocol.LinkModeRequireApproval)
		testSeedControl(t, store.DB(), root)
		engine, _ := testGateEngine(t)
		m := testGateMessage()
		tx := testBeginGateTx(t, store)
		_, _, err := engine.Admit(t.Context(), tx, m, testStoreMessage(t, tx, m))
		require.ErrorIs(t, err, ErrApprovalUnavailable)
		require.NoError(t, tx.Rollback())
		require.Zero(t, testDecisionCount(t, store.DB(), m.ID))
	})
}

func TestGateValidationReplyAndEntropy(t *testing.T) {
	store := testChannelStore(t)
	engine, clock := testGateEngine(t)
	m := testGateMessage()
	m.To[0].Policy = PolicyUnset
	tx := testBeginGateTx(t, store)
	_, _, err := engine.Admit(t.Context(), tx, m, testStoreMessage(t, tx, m))
	require.ErrorIs(t, err, ErrInvalidQuery)
	require.Zero(t, clock.readCount())
	require.NoError(t, tx.Rollback())

	m = testGateMessage()
	m.ParentID = "01ARZ3NDEKTSV4RRFFQ69G5FAA"
	tx = testBeginGateTx(t, store)
	_, _, err = engine.Admit(t.Context(), tx, m, testStoreMessage(t, tx, m))
	require.ErrorIs(t, err, ErrUnsupported)
	require.Zero(t, clock.readCount())
	require.NoError(t, tx.Rollback())

	for _, reader := range []io.Reader{errorReader{}, strings.NewReader("abc")} {
		engine, _ = testGateEngine(t)
		engine.entropy = &lockedReader{reader: reader}
		m = testGateMessage()
		tx = testBeginGateTx(t, store)
		_, _, err = engine.Admit(t.Context(), tx, m, testStoreMessage(t, tx, m))
		require.Error(t, err)
		require.NotErrorIs(t, err, ErrDenied)
		require.NoError(t, tx.Rollback())
		require.Zero(t, testDecisionCount(t, store.DB(), m.ID))
	}
}

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) { return 0, errors.New("entropy unavailable") }

type lockedReader struct {
	mu     sync.Mutex
	reader io.Reader
}

func (r *lockedReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.reader.Read(p)
}

func TestAcceptAndNeverRelax(t *testing.T) {
	t.Run("allow", func(t *testing.T) {
		store := testChannelStore(t)
		engine, _ := testGateEngine(t)
		m := testGateMessage()
		admitGateMessage(t, store, engine, m)

		tx := testBeginGateTx(t, store)
		accepted, err := engine.Accept(t.Context(), tx, m, testStoreEnvelope(t, tx, m, 7))
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
		require.Equal(t, protocol.ControlStateAllowed, accepted.State())
		require.Equal(t, protocol.DecisionReasonAllowed, accepted.Reason())
		require.NotEmpty(t, accepted.EvaluationID())
		require.Equal(t, testReadTriple(t, store.DB(), "messages", m.ID),
			testReadTriple(t, store.DB(), "message_envelopes", m.ID))
		require.Equal(t, int64(2), testDecisionCount(t, store.DB(), m.ID))
		var wire int64
		require.NoError(t, store.DB().QueryRowContext(t.Context(), "SELECT wire_control_version FROM message_envelopes").Scan(&wire))
		require.Equal(t, int64(7), wire)
	})

	t.Run("sender hold persists", func(t *testing.T) {
		store := testChannelStore(t)
		root := testLinkRoot(testControlID(205), testAgent1, testAgent2, protocol.PlaneMessage, protocol.LinkModeHold)
		testSeedControl(t, store.DB(), root)
		testSetEpoch(t, store.DB(), 4)
		engine, _ := testGateEngine(t)
		m := testGateMessage()
		admitGateMessage(t, store, engine, m)
		_, err := store.DB().ExecContext(t.Context(), testQueryText(t, "controls_delete_cas.sql"),
			root.ScopeType, root.ScopeKey, root.Plane, root.Version)
		require.NoError(t, err)
		testSetEpoch(t, store.DB(), 5)

		tx := testBeginGateTx(t, store)
		accepted, err := engine.Accept(t.Context(), tx, m, testStoreEnvelope(t, tx, m, 4))
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
		require.Equal(t, protocol.ControlStateHeld, accepted.State())
		require.Equal(t, protocol.DecisionReasonLinkHold, accepted.Reason())
		require.Equal(t, int64(5), accepted.Version())
		require.Equal(t, triple{protocol.ControlStateHeld, protocol.DecisionReasonLinkHold, 5},
			testReadTriple(t, store.DB(), "messages", m.ID))
	})

	t.Run("receiver deny", func(t *testing.T) {
		store := testChannelStore(t)
		engine, _ := testGateEngine(t)
		m := testGateMessage()
		admitGateMessage(t, store, engine, m)
		root := testAgentRoot(testControlID(206), testAgent2, true)
		root.TxEnabled = nil
		root.RxEnabled = testBool(false)
		testSeedControl(t, store.DB(), root)
		testSetEpoch(t, store.DB(), 2)

		called := false
		tx := testBeginGateTx(t, store)
		accepted, err := engine.Accept(t.Context(), tx, m, func(context.Context) error {
			called = true
			return nil
		})
		var denied *DenyError
		require.ErrorAs(t, err, &denied)
		require.Equal(t, protocol.ErrorCodeAgentRXDisabled, denied.Code())
		require.Equal(t, Acceptance{}, accepted)
		require.False(t, called)
		require.NoError(t, tx.Commit())
		require.Equal(t, triple{protocol.ControlStateDenied, protocol.DecisionReasonAgentRXDisabled, 2},
			testReadTriple(t, store.DB(), "messages", m.ID))
		var count int64
		require.NoError(t, store.DB().QueryRowContext(t.Context(), "SELECT count(*) FROM message_envelopes").Scan(&count))
		require.Zero(t, count)
	})
}

func admitGateMessage(t *testing.T, store *eventstore.Store, engine *Engine, m Message) {
	t.Helper()
	tx := testBeginGateTx(t, store)
	_, _, err := engine.Admit(t.Context(), tx, m, testStoreMessage(t, tx, m))
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
}

func TestAcceptAntecedentEpochAndOnce(t *testing.T) {
	t.Run("needs admission", func(t *testing.T) {
		store := testChannelStore(t)
		engine, _ := testGateEngine(t)
		m := testGateMessage()
		tx := testBeginGateTx(t, store)
		_, err := engine.Accept(t.Context(), tx, m, testStoreEnvelope(t, tx, m, 0))
		require.ErrorIs(t, err, ErrInvalidQuery)
		require.NoError(t, tx.Rollback())
	})

	t.Run("row without decisions", func(t *testing.T) {
		store := testChannelStore(t)
		engine, _ := testGateEngine(t)
		m := testGateMessage()
		tx := testBeginGateTx(t, store)
		require.NoError(t, testStoreMessage(t, tx, m)(t.Context(), protocol.AdmissionResult{
			Result: protocol.AdmissionStatusAccepted, DecisionID: testGateDecision,
			MessageID: m.ID, ThreadID: m.ThreadID,
		}))
		require.NoError(t, tx.Commit())
		tx = testBeginGateTx(t, store)
		_, err := engine.Accept(t.Context(), tx, m, testStoreEnvelope(t, tx, m, 0))
		require.ErrorIs(t, err, ErrInvalidControl)
		require.NoError(t, tx.Rollback())
	})

	t.Run("epoch regression", func(t *testing.T) {
		store := testChannelStore(t)
		testSetEpoch(t, store.DB(), 9)
		engine, _ := testGateEngine(t)
		m := testGateMessage()
		admitGateMessage(t, store, engine, m)
		testSetEpoch(t, store.DB(), 3)
		tx := testBeginGateTx(t, store)
		_, err := engine.Accept(t.Context(), tx, m, testStoreEnvelope(t, tx, m, 9))
		require.ErrorIs(t, err, ErrInvalidControl)
		require.NoError(t, tx.Rollback())
		require.Equal(t, int64(9), testReadTriple(t, store.DB(), "messages", m.ID).version)
		require.Equal(t, int64(1), testDecisionCount(t, store.DB(), m.ID))
	})

	t.Run("accepted once", func(t *testing.T) {
		store := testChannelStore(t)
		engine, _ := testGateEngine(t)
		m := testGateMessage()
		admitGateMessage(t, store, engine, m)
		tx := testBeginGateTx(t, store)
		_, err := engine.Accept(t.Context(), tx, m, testStoreEnvelope(t, tx, m, 0))
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
		before := testDecisionCount(t, store.DB(), m.ID)
		tx = testBeginGateTx(t, store)
		_, err = engine.Accept(t.Context(), tx, m, testStoreEnvelope(t, tx, m, 0))
		require.ErrorIs(t, err, ErrInvalidQuery)
		require.NoError(t, tx.Rollback())
		require.Equal(t, before, testDecisionCount(t, store.DB(), m.ID))
	})
}

func TestGateStoreRollbackContract(t *testing.T) {
	store := testChannelStore(t)
	engine, _ := testGateEngine(t)
	m := testGateMessage()
	tx := testBeginGateTx(t, store)
	decision, result, err := engine.Admit(t.Context(), tx, m, func(ctx context.Context, result protocol.AdmissionResult) error {
		if err := testStoreMessage(t, tx, m)(ctx, result); err != nil {
			return err
		}
		return errors.New("caller failed")
	})
	require.ErrorIs(t, err, ErrStore)
	require.True(t, decision.IsZero())
	require.Equal(t, protocol.AdmissionResult{}, result)
	require.NoError(t, tx.Rollback())
	require.Zero(t, testDecisionCount(t, store.DB(), m.ID))

	tx = testBeginGateTx(t, store)
	_, _, err = engine.Admit(t.Context(), tx, m, func(context.Context, protocol.AdmissionResult) error {
		return &DenyError{decisionID: testGateDecision, reason: protocol.DecisionReasonLinkDenied}
	})
	require.ErrorIs(t, err, ErrStore)
	var denied *DenyError
	require.NotErrorAs(t, err, &denied)
	require.NotErrorIs(t, err, ErrDenied)
	require.NoError(t, tx.Rollback())
}

func TestAdmitOnceAndPolicyDenied(t *testing.T) {
	t.Run("policy denied", func(t *testing.T) {
		store := testChannelStore(t)
		engine, _ := testGateEngine(t)
		m := testGateMessage()
		m.To[0].Policy = PolicyDenied
		tx := testBeginGateTx(t, store)
		_, _, err := engine.Admit(t.Context(), tx, m, testStoreMessage(t, tx, m))
		var denied *DenyError
		require.ErrorAs(t, err, &denied)
		require.Equal(t, protocol.ErrorCodePolicyDenied, denied.Code())
		require.NoError(t, tx.Commit())
	})

	t.Run("denied id is not admitted again", func(t *testing.T) {
		store := testChannelStore(t)
		root := testLinkRoot(testControlID(207), testAgent1, testAgent2, protocol.PlaneMessage, protocol.LinkModeDeny)
		testSeedControl(t, store.DB(), root)
		engine, _ := testGateEngine(t)
		m := testGateMessage()
		tx := testBeginGateTx(t, store)
		_, _, err := engine.Admit(t.Context(), tx, m, testStoreMessage(t, tx, m))
		require.ErrorIs(t, err, ErrDenied)
		require.NoError(t, tx.Commit())
		_, err = store.DB().ExecContext(t.Context(), testQueryText(t, "controls_delete_cas.sql"),
			root.ScopeType, root.ScopeKey, root.Plane, root.Version)
		require.NoError(t, err)
		tx = testBeginGateTx(t, store)
		_, _, err = engine.Admit(t.Context(), tx, m, testStoreMessage(t, tx, m))
		require.ErrorIs(t, err, ErrInvalidQuery)
		require.NoError(t, tx.Rollback())
		require.Equal(t, int64(1), testDecisionCount(t, store.DB(), m.ID))
	})
}

func TestDenyErrorCodes(t *testing.T) {
	reasons := []protocol.DecisionReason{
		protocol.DecisionReasonPolicyDenied,
		protocol.DecisionReasonChannelOff,
		protocol.DecisionReasonChannelDraining,
		protocol.DecisionReasonNodeOff,
		protocol.DecisionReasonNodeDraining,
		protocol.DecisionReasonAgentTXDisabled,
		protocol.DecisionReasonAgentRXDisabled,
		protocol.DecisionReasonLinkDenied,
		protocol.DecisionReasonLoopStopped,
		protocol.DecisionReasonLoopDraining,
		protocol.DecisionReasonLoopBudgetExhausted,
	}
	for _, reason := range reasons {
		t.Run(string(reason), func(t *testing.T) {
			err := &DenyError{reason: reason}
			require.True(t, err.Code().Valid())
			require.Equal(t, string(reason), string(err.Code()))
		})
	}
}

func decisionULIDMillis(t *testing.T, id protocol.ChannelDecisionID) int64 {
	t.Helper()
	const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	n := newBigInt(t, string(id)[4:], alphabet)
	raw := n.FillBytes(make([]byte, 16))
	var millis int64
	for i := range 6 {
		millis = millis<<8 | int64(raw[i])
	}
	return millis
}

func newBigInt(t *testing.T, encoded, alphabet string) *big.Int {
	t.Helper()
	n := new(big.Int)
	base := big.NewInt(32)
	for _, char := range encoded {
		index := strings.IndexRune(alphabet, char)
		require.NotEqual(t, -1, index)
		n.Mul(n, base)
		n.Add(n, big.NewInt(int64(index)))
	}
	return n
}

func TestGateCancelledContext(t *testing.T) {
	store := testChannelStore(t)
	engine, _ := testGateEngine(t)
	m := testGateMessage()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	tx := testBeginGateTx(t, store)
	_, _, err := engine.Admit(ctx, tx, m, testStoreMessage(t, tx, m))
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrDenied)
	require.NoError(t, tx.Rollback())
}

func TestGateErrorsHaveContext(t *testing.T) {
	err := (&DenyError{decisionID: testGateDecision, reason: protocol.DecisionReasonLinkDenied}).Error()
	require.Equal(t, "denied: LINK_DENIED", err)
	require.Contains(t, fmt.Sprint(ErrApprovalUnavailable), "approval")
}
