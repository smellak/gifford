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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	testGateMessageID = "01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testGateRoute     = "sha256:route"
	testGateDecision  = "chd_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testStoreError    = "store error"
	testForeignDeny   = "foreign deny"
	testDamagedRecord = "damaged record"
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
			m.ID, "client-"+string(m.ID), m.FromNode, m.From, m.ToNode,
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
			m.ID, []byte("signed-envelope"), "gifford-message", 1, "client-"+string(m.ID),
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

func TestGateUsesTransactionViewOnly(t *testing.T) {
	t.Run("rollback publishes nothing", func(t *testing.T) {
		store := testChannelStore(t)
		engine, _ := testGateEngine(t)
		m := testGateMessage()
		tx := testBeginGateTx(t, store)
		_, _, err := engine.Admit(t.Context(), tx, m, testStoreMessage(t, tx, m))
		require.NoError(t, err)
		require.NoError(t, tx.Rollback())
		require.Zero(t, testDecisionCount(t, store.DB(), m.ID))

		x, err := engine.Explain(Query{
			Stage: protocol.DecisionStageAdmission, Plane: protocol.PlaneMessage,
			From: m.From, FromNode: m.FromNode, To: m.To[0].Agent, ToNode: m.ToNode,
			Kind: KindRoot, ThreadID: m.ThreadID,
		})
		require.NoError(t, err)
		require.Equal(t, protocol.DecisionReasonControlUnavailable, x.Reason)
	})

	t.Run("table overrides shared cache", func(t *testing.T) {
		store := testChannelStore(t)
		cached := testLinkRoot(testControlID(301), testAgent1, testAgent2, protocol.PlaneMessage, protocol.LinkModeHold)
		engine, err := New(Options{Clock: testClock{now: testEngineTime}})
		require.NoError(t, err)
		require.NoError(t, engine.load([]protocol.ChannelControl{cached}))
		m := testGateMessage()
		tx := testBeginGateTx(t, store)
		decision, _, err := engine.Admit(t.Context(), tx, m, testStoreMessage(t, tx, m))
		require.NoError(t, err)
		require.Equal(t, Allow, decision.Outcome())
		require.NoError(t, tx.Commit())

		q := Query{
			Stage: protocol.DecisionStageAdmission, Plane: protocol.PlaneMessage,
			From: m.From, FromNode: m.FromNode, To: m.To[0].Agent, ToNode: m.ToNode,
			Kind: KindRoot, ThreadID: m.ThreadID,
		}
		x, err := engine.Explain(q)
		require.NoError(t, err)
		require.Equal(t, protocol.DecisionReasonLinkHold, x.Reason)
	})
}

func TestGateRecipientPrecedence(t *testing.T) {
	for _, order := range []string{"approval first", "deny first"} {
		t.Run(order, func(t *testing.T) {
			store := testChannelStore(t)
			m := testGateMessage()
			m.To = append(m.To, Recipient{Agent: "agent-3", Deliverable: true, Policy: PolicyAllowed})
			modes := []protocol.LinkMode{protocol.LinkModeRequireApproval, protocol.LinkModeDeny}
			if order == "deny first" {
				modes[0], modes[1] = modes[1], modes[0]
			}
			for i, recipient := range m.To {
				root := testLinkRoot(testControlID(310+i), testAgent1, string(recipient.Agent), protocol.PlaneMessage, modes[i])
				testSeedControl(t, store.DB(), root)
			}
			engine, clock := testGateEngine(t)
			tx := testBeginGateTx(t, store)
			_, _, err := engine.Admit(t.Context(), tx, m, testStoreMessage(t, tx, m))
			var denied *DenyError
			require.ErrorAs(t, err, &denied)
			require.Equal(t, protocol.DecisionReasonLinkDenied, denied.Reason())
			require.Equal(t, 1, clock.readCount())
			require.NoError(t, tx.Commit())
			require.Equal(t, int64(2), testDecisionCount(t, store.DB(), m.ID))

			rows, err := store.DB().QueryContext(t.Context(),
				"SELECT outcome FROM channel_control_decisions WHERE object_id = ? ORDER BY rowid", m.ID)
			require.NoError(t, err)
			defer func() { _ = rows.Close() }()
			var outcomes []protocol.DecisionOutcome
			for rows.Next() {
				var outcome protocol.DecisionOutcome
				require.NoError(t, rows.Scan(&outcome))
				outcomes = append(outcomes, outcome)
			}
			require.NoError(t, rows.Err())
			require.ElementsMatch(t, []protocol.DecisionOutcome{Approval, Deny}, outcomes)
		})
	}

	t.Run("approval beside allow fails before writes", func(t *testing.T) {
		store := testChannelStore(t)
		m := testGateMessage()
		m.To = append(m.To, Recipient{Agent: "agent-3", Deliverable: true, Policy: PolicyAllowed})
		root := testLinkRoot(testControlID(320), testAgent1, testAgent2, protocol.PlaneMessage, protocol.LinkModeRequireApproval)
		testSeedControl(t, store.DB(), root)
		engine, _ := testGateEngine(t)
		tx := testBeginGateTx(t, store)
		_, _, err := engine.Admit(t.Context(), tx, m, testStoreMessage(t, tx, m))
		require.ErrorIs(t, err, ErrApprovalUnavailable)
		require.NoError(t, tx.Rollback())
		require.Zero(t, testDecisionCount(t, store.DB(), m.ID))
	})
}

func TestAcceptPreservesApprovalAndHardensHold(t *testing.T) {
	t.Run("stored approval", func(t *testing.T) {
		store := testChannelStore(t)
		engine, _ := testGateEngine(t)
		m := testGateMessage()
		admitGateMessage(t, store, engine, m)
		testSetEpoch(t, store.DB(), 5)
		_, err := store.DB().ExecContext(t.Context(), testQueryText(t, "messages_set_control.sql"),
			protocol.ControlStateApprovalRequired, protocol.DecisionReasonLinkApproval, 4, m.ID)
		require.NoError(t, err)
		tx := testBeginGateTx(t, store)
		accepted, err := engine.Accept(t.Context(), tx, m, testStoreEnvelope(t, tx, m, 4))
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
		require.Equal(t, protocol.ControlStateApprovalRequired, accepted.State())
		require.Equal(t, protocol.DecisionReasonLinkApproval, accepted.Reason())
		require.Equal(t, int64(5), accepted.Version())
	})

	t.Run("receiver hold", func(t *testing.T) {
		store := testChannelStore(t)
		engine, _ := testGateEngine(t)
		m := testGateMessage()
		admitGateMessage(t, store, engine, m)
		root := testLinkRoot(testControlID(330), testAgent1, testAgent2, protocol.PlaneMessage, protocol.LinkModeHold)
		testSeedControl(t, store.DB(), root)
		tx := testBeginGateTx(t, store)
		accepted, err := engine.Accept(t.Context(), tx, m, testStoreEnvelope(t, tx, m, 0))
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
		require.Equal(t, protocol.ControlStateHeld, accepted.State())
		require.Equal(t, protocol.DecisionReasonLinkHold, accepted.Reason())
	})
}

func TestAcceptRollbackContract(t *testing.T) {
	for _, failure := range []string{testStoreError, testForeignDeny, "missing envelope", "changed triple"} {
		t.Run(failure, func(t *testing.T) {
			store := testChannelStore(t)
			engine, _ := testGateEngine(t)
			m := testGateMessage()
			admitGateMessage(t, store, engine, m)
			before := testReadTriple(t, store.DB(), "messages", m.ID)
			tx := testBeginGateTx(t, store)
			var callback func(context.Context) error
			switch failure {
			case testStoreError:
				callback = func(ctx context.Context) error {
					if err := testStoreEnvelope(t, tx, m, 0)(ctx); err != nil {
						return err
					}
					return errors.New("store failed")
				}
			case testForeignDeny:
				callback = func(ctx context.Context) error {
					if err := testStoreEnvelope(t, tx, m, 0)(ctx); err != nil {
						return err
					}
					return &DenyError{decisionID: testGateDecision, reason: protocol.DecisionReasonLinkDenied}
				}
			case "missing envelope":
				callback = func(context.Context) error { return nil }
			case "changed triple":
				callback = func(ctx context.Context) error {
					if err := testStoreEnvelope(t, tx, m, 0)(ctx); err != nil {
						return err
					}
					_, err := tx.ExecContext(ctx, testQueryText(t, "messages_set_control.sql"),
						protocol.ControlStateHeld, protocol.DecisionReasonLinkHold, 0, m.ID)
					return err
				}
			}
			accepted, err := engine.Accept(t.Context(), tx, m, callback)
			require.Error(t, err)
			require.Equal(t, Acceptance{}, accepted)
			if failure == testStoreError || failure == testForeignDeny {
				require.ErrorIs(t, err, ErrStore)
			}
			var denied *DenyError
			require.NotErrorAs(t, err, &denied)
			require.NoError(t, tx.Rollback())
			require.Equal(t, before, testReadTriple(t, store.DB(), "messages", m.ID))
			require.Equal(t, int64(1), testDecisionCount(t, store.DB(), m.ID))
			var count int64
			require.NoError(t, store.DB().QueryRowContext(t.Context(),
				"SELECT count(*) FROM message_envelopes").Scan(&count))
			require.Zero(t, count)
		})
	}
}

func TestAcceptBindsAndValidatesAntecedent(t *testing.T) {
	for _, failure := range []string{"sender", "recipient", testDamagedRecord} {
		t.Run(failure, func(t *testing.T) {
			store := testChannelStore(t)
			engine, _ := testGateEngine(t)
			m := testGateMessage()
			admitGateMessage(t, store, engine, m)
			switch failure {
			case "sender":
				m.From = "agent-x"
			case "recipient":
				m.To[0].Agent = "agent-x"
			case testDamagedRecord:
				_, err := store.DB().ExecContext(t.Context(),
					"UPDATE channel_control_decisions SET stage = 'broken' WHERE object_id = ?", m.ID)
				require.NoError(t, err)
			}
			tx := testBeginGateTx(t, store)
			_, err := engine.Accept(t.Context(), tx, m, testStoreEnvelope(t, tx, m, 0))
			if failure == testDamagedRecord {
				require.ErrorIs(t, err, ErrInvalidControl)
			} else {
				require.ErrorIs(t, err, ErrInvalidQuery)
			}
			require.NoError(t, tx.Rollback())
		})
	}
}

func TestGateCoordinated(t *testing.T) {
	t.Run("admission before writer", func(t *testing.T) {
		store := testChannelStore(t)
		root := testLinkRoot(testControlID(340), testAgent1, testAgent2, protocol.PlaneMessage, protocol.LinkModeAllow)
		testSeedControl(t, store.DB(), root)
		engine, _ := testGateEngine(t)
		first := testGateMessage()
		tx := testBeginGateTx(t, store)
		_, _, err := engine.Admit(t.Context(), tx, first, testStoreMessage(t, tx, first))
		require.NoError(t, err)

		root.Version = 2
		root.Mode = protocol.ControlMode(protocol.LinkModeDeny)
		params := updateParams(t, root, 1)
		updateQuery := testQueryText(t, "controls_update_cas.sql")
		done := make(chan error, 1)
		go func() {
			writer, err := store.DB().BeginTx(t.Context(), nil)
			if err == nil {
				var result sql.Result
				result, err = writer.ExecContext(t.Context(), updateQuery, params...)
				if err == nil {
					err = requireOneRow(result, "update test link")
				}
				if err == nil {
					err = writer.Commit()
				} else {
					_ = writer.Rollback()
				}
			}
			done <- err
		}()
		select {
		case err := <-done:
			require.Failf(t, "writer did not wait", "error: %v", err)
		case <-time.After(100 * time.Millisecond):
		}
		require.NoError(t, tx.Commit())
		require.NoError(t, <-done)

		second := testGateMessage()
		second.ID = testGateULID(2)
		tx = testBeginGateTx(t, store)
		_, _, err = engine.Admit(t.Context(), tx, second, testStoreMessage(t, tx, second))
		require.ErrorIs(t, err, ErrDenied)
		require.NoError(t, tx.Commit())
		require.Equal(t, []decisionVersion{{Allow, 1}, {Deny, 2}}, readDecisionVersions(t, store.DB()))
	})

	t.Run("writer before admission", func(t *testing.T) {
		store := testChannelStore(t)
		root := testLinkRoot(testControlID(341), testAgent1, testAgent2, protocol.PlaneMessage, protocol.LinkModeDeny)
		testSeedControl(t, store.DB(), root)
		root.Version = 2
		root.Mode = protocol.ControlMode(protocol.LinkModeAllow)
		params := updateParams(t, root, 1)
		writer := testBeginGateTx(t, store)
		result, err := writer.ExecContext(t.Context(), testQueryText(t, "controls_update_cas.sql"), params...)
		require.NoError(t, err)
		require.NoError(t, requireOneRow(result, "update test link"))

		engine, _ := testGateEngine(t)
		m := testGateMessage()
		done := make(chan error, 1)
		go func() {
			tx, err := store.DB().BeginTx(t.Context(), nil)
			if err == nil {
				_, _, err = engine.Admit(t.Context(), tx, m, testStoreMessage(t, tx, m))
				if err == nil {
					err = tx.Commit()
				} else {
					_ = tx.Rollback()
				}
			}
			done <- err
		}()
		select {
		case err := <-done:
			require.Failf(t, "admission did not wait", "error: %v", err)
		case <-time.After(100 * time.Millisecond):
		}
		require.NoError(t, writer.Commit())
		require.NoError(t, <-done)
		require.Equal(t, []decisionVersion{{Allow, 2}}, readDecisionVersions(t, store.DB()))
	})
}

type decisionVersion struct {
	outcome protocol.DecisionOutcome
	version int64
}

func readDecisionVersions(t *testing.T, db *sql.DB) []decisionVersion {
	t.Helper()
	rows, err := db.QueryContext(t.Context(),
		"SELECT outcome, control_versions_json FROM channel_control_decisions ORDER BY rowid")
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	var found []decisionVersion
	for rows.Next() {
		var outcome protocol.DecisionOutcome
		var raw string
		require.NoError(t, rows.Scan(&outcome, &raw))
		var versions protocol.ControlVersions
		require.NoError(t, json.Unmarshal([]byte(raw), &versions))
		found = append(found, decisionVersion{outcome: outcome, version: versions[testLinkVersionKey]})
	}
	require.NoError(t, rows.Err())
	return found
}

func testGateULID(n int) protocol.ULID {
	const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	var suffix [4]byte
	for i := len(suffix) - 1; i >= 0; i-- {
		suffix[i] = alphabet[n%32]
		n /= 32
	}
	return protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G" + string(suffix[:]))
}

func TestGateSerializable(t *testing.T) {
	store := testChannelStore(t)
	root := testLinkRoot(testControlID(350), testAgent1, testAgent2, protocol.PlaneMessage, protocol.LinkModeAllow)
	testSeedControl(t, store.DB(), root)
	engine, _ := testGateEngine(t)
	updateQuery := testQueryText(t, "controls_update_cas.sql")
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	const admissions = 200
	var next atomic.Int64
	errs := make(chan error, admissions+1)
	stopWriter := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		current := root
		for version := int64(2); version <= 20_000; version++ {
			select {
			case <-stopWriter:
				return
			case <-ctx.Done():
				errs <- ctx.Err()
				return
			default:
			}
			nextRoot := current
			nextRoot.Version = version
			if version%2 == 0 {
				nextRoot.Mode = protocol.ControlMode(protocol.LinkModeDeny)
			} else {
				nextRoot.Mode = protocol.ControlMode(protocol.LinkModeAllow)
			}
			params, err := gateUpdateParams(nextRoot, current.Version)
			if err != nil {
				errs <- err
				return
			}
			tx, err := store.DB().BeginTx(ctx, nil)
			if err == nil {
				var result sql.Result
				result, err = tx.ExecContext(ctx, updateQuery, params...)
				if err == nil {
					err = requireOneRow(result, "update test link")
				}
				if err == nil {
					err = tx.Commit()
				} else {
					_ = tx.Rollback()
				}
			}
			if err != nil {
				errs <- err
				return
			}
			current = nextRoot
			time.Sleep(time.Millisecond)
		}
	}()

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1))
				if i > admissions {
					return
				}
				m := testGateMessage()
				m.ID = testGateULID(i)
				tx, err := store.DB().BeginTx(ctx, nil)
				if err == nil {
					_, _, err = engine.Admit(ctx, tx, m, testStoreMessage(t, tx, m))
					if err == nil || errors.Is(err, ErrDenied) {
						err = tx.Commit()
					} else {
						_ = tx.Rollback()
					}
				}
				if err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(stopWriter)
	<-writerDone
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	decisions := readDecisionVersions(t, store.DB())
	require.Len(t, decisions, admissions)
	var allowCount int64
	lastVersion := int64(0)
	for _, decision := range decisions {
		require.GreaterOrEqual(t, decision.version, lastVersion)
		lastVersion = decision.version
		if decision.version%2 == 0 {
			require.Equal(t, Deny, decision.outcome)
		} else {
			require.Equal(t, Allow, decision.outcome)
			allowCount++
		}
	}
	var messages int64
	require.NoError(t, store.DB().QueryRowContext(t.Context(), "SELECT count(*) FROM messages").Scan(&messages))
	require.Equal(t, allowCount, messages)
}

func gateUpdateParams(root protocol.ChannelControl, expected int64) ([]any, error) {
	params, err := controlParams(root)
	if err != nil {
		return nil, err
	}
	return []any{
		params[4], params[5], params[6], params[7], params[8], params[11],
		params[12], params[13], params[14], params[15], params[16],
		root.Version, params[19], params[1], params[2], params[3], expected,
	}, nil
}
