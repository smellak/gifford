package channel

import (
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
