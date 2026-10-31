package channel

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

const (
	approvalTestMessage    = protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	testApprovalDeliverNow = "deliver_now"
	testApprovalReject     = "reject"
	testApprovalUnoffered  = "stop"
)

func TestOrchestratorApprovalLifecycle(t *testing.T) {
	t.Parallel()

	controller, clk, _ := testController(t)
	tx, err := controller.db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	request, created, err := controller.EnsureOrchestratorApproval(
		t.Context(), tx, approvalTestMessage, testAgent2, "automatic delivery deferred",
	)
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, protocol.ApprovalSourceOrchestrator, request.Source)
	require.Equal(t, protocol.ApprovalObjectTypeMessage, request.ObjectType)
	require.Equal(t, string(approvalTestMessage), request.ObjectID)
	require.Equal(t, protocol.AgentID(testAgent2), request.AgentID)
	require.Equal(t, []protocol.ApprovalOption{testApprovalDeliverNow, testApprovalReject}, request.Options)

	again, created, err := controller.EnsureOrchestratorApproval(
		t.Context(), tx, approvalTestMessage, testAgent2, "different note",
	)
	require.NoError(t, err)
	require.False(t, created)
	require.Equal(t, request.ApprovalID, again.ApprovalID)
	require.NoError(t, tx.Commit())

	got, err := controller.Approval(t.Context(), request.ApprovalID)
	require.NoError(t, err)
	require.Equal(t, request, got)
	list, err := controller.Approvals(t.Context(), protocol.ListApprovalsParams{
		State: protocol.ApprovalStatePending, AgentID: testAgent2,
	})
	require.NoError(t, err)
	require.Equal(t, []protocol.ApprovalRequest{request}, list.Approvals)

	clk.now = clk.now.Add(time.Minute)
	tx, err = controller.db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	resolved, err := controller.ResolveOrchestratorApproval(t.Context(), tx,
		protocol.ResolveApprovalParams{
			ApprovalID: request.ApprovalID, Option: testApprovalDeliverNow, Reason: "operator requested delivery",
		}, Actor{Type: protocol.ActorTypeHuman, ID: testOperator})
	require.NoError(t, err)
	require.Equal(t, protocol.ApprovalStateApproved, resolved.State)
	require.Equal(t, protocol.ApprovalOption(testApprovalDeliverNow), resolved.Resolution.Option)
	require.Equal(t, testOperator, resolved.ResolvedBy)
	require.Equal(t, clk.now, resolved.ResolvedAt)
	require.NoError(t, tx.Commit())

	tx, err = controller.db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	replayed, err := controller.ResolveOrchestratorApproval(t.Context(), tx,
		protocol.ResolveApprovalParams{
			ApprovalID: request.ApprovalID, Option: testApprovalDeliverNow, Reason: "operator requested delivery",
		}, Actor{Type: protocol.ActorTypeHuman, ID: testOperator})
	require.NoError(t, err)
	require.Equal(t, resolved, replayed)
	require.NoError(t, tx.Commit())

	tx, err = controller.db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	_, err = controller.ResolveOrchestratorApproval(t.Context(), tx,
		protocol.ResolveApprovalParams{ApprovalID: request.ApprovalID, Option: testApprovalReject},
		Actor{Type: protocol.ActorTypeHuman, ID: testOperator})
	require.ErrorIs(t, err, ErrInvalidQuery)
	require.NoError(t, tx.Rollback())
}

func TestResolveOrchestratorApprovalRejectsUnofferedAndUnverifiedActor(t *testing.T) {
	t.Parallel()

	controller, _, _ := testController(t)
	tx, err := controller.db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	request, _, err := controller.EnsureOrchestratorApproval(
		t.Context(), tx, approvalTestMessage, testAgent2, "deferred",
	)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	tests := []struct {
		name   string
		option protocol.ApprovalOption
		actor  Actor
	}{
		{name: "unoffered option", option: testApprovalUnoffered, actor: Actor{Type: protocol.ActorTypeHuman, ID: testOperator}},
		{name: "system actor", option: testApprovalReject, actor: Actor{Type: protocol.ActorTypeSystem, ID: "system"}},
		{name: "empty actor", option: testApprovalReject, actor: Actor{Type: protocol.ActorTypeHuman}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			tx, err := controller.db.BeginTx(t.Context(), nil)
			require.NoError(t, err)
			_, err = controller.ResolveOrchestratorApproval(t.Context(), tx,
				protocol.ResolveApprovalParams{ApprovalID: request.ApprovalID, Option: test.option}, test.actor)
			require.ErrorIs(t, err, ErrInvalidQuery)
			require.NoError(t, tx.Rollback())
		})
	}
}

func TestOrchestratorApprovalsAreRecipientScopedAndRecoveryIsFresh(t *testing.T) {
	t.Parallel()

	controller, clk, _ := testController(t)
	tx, err := controller.db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	agentA := protocol.AgentID(testAgent2)
	agentB := protocol.AgentID("agent-3")
	firstA, _, err := controller.EnsureOrchestratorApproval(
		t.Context(), tx, approvalTestMessage, agentA, "automatic A")
	require.NoError(t, err)
	firstB, _, err := controller.EnsureOrchestratorApproval(
		t.Context(), tx, approvalTestMessage, agentB, "automatic B")
	require.NoError(t, err)
	require.NotEqual(t, firstA.ApprovalID, firstB.ApprovalID)
	require.NoError(t, tx.Commit())

	clk.now = clk.now.Add(time.Minute)
	tx, err = controller.db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	recoveryA, err := controller.ReplaceOrchestratorApproval(
		t.Context(), tx, approvalTestMessage, agentA, "uncertain delivery A")
	require.NoError(t, err)
	require.NotEqual(t, firstA.ApprovalID, recoveryA.ApprovalID)
	require.NoError(t, tx.Commit())

	superseded, err := controller.Approval(t.Context(), firstA.ApprovalID)
	require.NoError(t, err)
	require.Equal(t, protocol.ApprovalStateSuperseded, superseded.State)
	require.Equal(t, "orchestrator", superseded.ResolvedBy)
	require.Equal(t, clk.now, superseded.ResolvedAt)
	require.Nil(t, superseded.Resolution)

	untouched, err := controller.Approval(t.Context(), firstB.ApprovalID)
	require.NoError(t, err)
	require.Equal(t, protocol.ApprovalStatePending, untouched.State)
}

func TestSupersedeOrchestratorApprovalClosesOnlyExactPendingRequest(t *testing.T) {
	t.Parallel()

	controller, clk, sink := testController(t)
	tx, err := controller.db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	agentA := protocol.AgentID(testAgent2)
	agentB := protocol.AgentID("agent-3")
	target, _, err := controller.EnsureOrchestratorApproval(
		t.Context(), tx, approvalTestMessage, agentA, "automatic A")
	require.NoError(t, err)
	otherRecipient, _, err := controller.EnsureOrchestratorApproval(
		t.Context(), tx, approvalTestMessage, agentB, "automatic B")
	require.NoError(t, err)
	otherMessageID := protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G5FAW")
	otherObject, _, err := controller.EnsureOrchestratorApproval(
		t.Context(), tx, otherMessageID, agentA, "automatic other message")
	require.NoError(t, err)

	const triageApprovalID = protocol.ApprovalID("apr_01ARZ3NDEKTSV4RRFFQ69G5FAY")
	insert, err := queryText("approvals_insert.sql")
	require.NoError(t, err)
	_, err = tx.ExecContext(t.Context(), insert,
		triageApprovalID, protocol.ApprovalSourceTriage, protocol.ApprovalObjectTypeMessage,
		approvalTestMessage, agentA, nil, `["deliver_now","reject"]`, nil, nil,
		protocol.ApprovalStatePending, nil, nil, nil, formatChannelTime(clk.now), nil)
	require.NoError(t, err)

	var beforeCount int64
	require.NoError(t, tx.QueryRowContext(t.Context(), "SELECT count(*) FROM approval_requests").Scan(&beforeCount))
	beforeEvents := len(sink.events)
	clk.now = clk.now.Add(time.Minute)

	changed, err := controller.SupersedeOrchestratorApproval(
		t.Context(), tx, approvalTestMessage, agentA)
	require.NoError(t, err)
	require.True(t, changed)
	changed, err = controller.SupersedeOrchestratorApproval(
		t.Context(), tx, approvalTestMessage, agentA)
	require.NoError(t, err)
	require.False(t, changed, "repeating the exact close is an idempotent no-op")

	var afterCount int64
	require.NoError(t, tx.QueryRowContext(t.Context(), "SELECT count(*) FROM approval_requests").Scan(&afterCount))
	require.Equal(t, beforeCount, afterCount, "superseding does not create a replacement")
	require.Len(t, sink.events, beforeEvents, "superseding emits no control event")

	superseded, err := controller.ApprovalTx(t.Context(), tx, target.ApprovalID)
	require.NoError(t, err)
	require.Equal(t, protocol.ApprovalStateSuperseded, superseded.State)
	require.Nil(t, superseded.Resolution)
	require.Equal(t, "orchestrator", superseded.ResolvedBy)
	require.Equal(t, clk.now, superseded.ResolvedAt)
	untouchedRecipient, err := controller.ApprovalTx(t.Context(), tx, otherRecipient.ApprovalID)
	require.NoError(t, err)
	require.Equal(t, protocol.ApprovalStatePending, untouchedRecipient.State)
	untouchedObject, err := controller.ApprovalTx(t.Context(), tx, otherObject.ApprovalID)
	require.NoError(t, err)
	require.Equal(t, protocol.ApprovalStatePending, untouchedObject.State)
	untouchedSource, err := controller.ApprovalTx(t.Context(), tx, triageApprovalID)
	require.NoError(t, err)
	require.Equal(t, protocol.ApprovalStatePending, untouchedSource.State)

	_, err = controller.ResolveOrchestratorApproval(t.Context(), tx,
		protocol.ResolveApprovalParams{ApprovalID: target.ApprovalID, Option: testApprovalDeliverNow},
		Actor{Type: protocol.ActorTypeHuman, ID: testOperator})
	require.ErrorIs(t, err, ErrInvalidQuery)
	require.NoError(t, tx.Commit())
}

func TestSupersedeOrchestratorApprovalRejectsInvalidIdentity(t *testing.T) {
	t.Parallel()

	controller, _, _ := testController(t)
	tx, err := controller.db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	for _, test := range []struct {
		name      string
		tx        *sql.Tx
		messageID protocol.ULID
		agent     protocol.AgentID
	}{
		{name: "nil transaction", messageID: approvalTestMessage, agent: testAgent2},
		{name: "invalid message id", tx: tx, messageID: "not-a-ulid", agent: testAgent2},
		{name: "invalid agent id", tx: tx, messageID: approvalTestMessage, agent: "invalid agent"},
	} {
		changed, err := controller.SupersedeOrchestratorApproval(
			t.Context(), test.tx, test.messageID, test.agent)
		require.ErrorIs(t, err, ErrInvalidQuery, test.name)
		require.False(t, changed, test.name)
	}
	require.NoError(t, tx.Rollback())
}

func TestSupersedeOrchestratorApprovalRejectsAmbiguousPendingRows(t *testing.T) {
	t.Parallel()

	controller, _, _ := testController(t)
	tx, err := controller.db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(t.Context(), "DROP INDEX approval_requests_orchestrator_recipient")
	require.NoError(t, err)
	_, err = controller.CreateOrchestratorApproval(
		t.Context(), tx, approvalTestMessage, testAgent2, "first")
	require.NoError(t, err)
	_, err = controller.CreateOrchestratorApproval(
		t.Context(), tx, approvalTestMessage, testAgent2, "second")
	require.NoError(t, err)

	changed, err := controller.SupersedeOrchestratorApproval(
		t.Context(), tx, approvalTestMessage, testAgent2)
	require.ErrorIs(t, err, ErrInvalidQuery)
	require.False(t, changed)
	require.NoError(t, tx.Rollback())
}
