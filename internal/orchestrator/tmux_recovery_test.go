package orchestrator

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/daemon"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/tmuxattach"
)

var tmuxOwnNoticeScreen = []byte("$ " + string(tmuxattach.FixedNoticeLine))

// A box that already holds exactly this delivery's notice is finished with
// Enter alone: nothing is typed again and the decision records why.
func TestTmuxOwnNoticeInBoxIsSubmittedWithEnterOnly(t *testing.T) {
	t.Parallel()
	fixture, transport, _ := warmTmuxDeferFixture(t)
	candidate := fixture.seedDelivery(t, 170)
	transport.initial = tmuxOwnNoticeScreen
	require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), candidate))
	require.Zero(t, transport.writes, "the notice is not typed again")
	require.Equal(t, 1, transport.enterOnly)
	require.Equal(t, queueStateDelivered, queueStateOf(t, fixture.store.DB(), candidate))
	guards := latestGuards(t, fixture.store.DB(), candidate.MessageID)
	require.Equal(t, string(tmuxFallbackOwnSubmit),
		guardDetail(guards, protocol.GuardNameDetectorFallback))
}

// Any other text in the box is a draft: nothing is typed or pressed.
func TestTmuxOtherTextInBoxIsNeverSubmitted(t *testing.T) {
	t.Parallel()
	fixture, transport, _ := warmTmuxDeferFixture(t)
	candidate := fixture.seedDelivery(t, 171)
	transport.initial = []byte("$ " + string(tmuxattach.FixedNoticeLine) + " and more")
	require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), candidate))
	require.Zero(t, transport.writes)
	require.Zero(t, transport.enterOnly)
	require.Equal(t, tmuxBoxHoldsText, guardDetail(latestGuards(t, fixture.store.DB(),
		candidate.MessageID), protocol.GuardNameHumanDraftClosed))
}

// The node retries its own uncertain delivery once the box is free, or
// finishes it with Enter when the box still holds its notice. Either way the
// recovery resolves the uncertain attempt and settles its approval as the
// node's own, and the decision records which.
func TestTmuxOwnUncertainDeliveryIsRetriedByTheNode(t *testing.T) {
	t.Parallel()
	for index, test := range []struct {
		name      string
		screen    []byte
		writes    int
		enterOnly int
		reason    tmuxDetectorFallbackReason
	}{
		{"empty box", []byte("$ "), 1, 0, tmuxFallbackOwnRecovery},
		{"notice left in box", tmuxOwnNoticeScreen, 0, 1, tmuxFallbackOwnSubmit},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			fixture, transport, _ := warmTmuxDeferFixture(t)
			candidate := fixture.seedDelivery(t, 172+index)
			leaveTmuxDeliveryUncertain(t, fixture, transport, candidate)

			transport.initial = test.screen
			require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), candidate))
			require.Equal(t, test.writes, transport.writes)
			require.Equal(t, test.enterOnly, transport.enterOnly)
			db := fixture.store.DB()
			require.Equal(t, queueStateDelivered, queueStateOf(t, db, candidate))
			require.Equal(t, 1, countRows(t, db, `SELECT count(*) FROM delivery_attempts
				WHERE message_id = ? AND state = 'UNCERTAIN'
				AND resolution_approval_id = uncertain_approval_id`, candidate.MessageID))
			require.Equal(t, 1, countRows(t, db, `SELECT count(*) FROM delivery_attempts
				WHERE message_id = ? AND state = 'COMMITTED'
				AND recovery_of_delivery_id IS NOT NULL`, candidate.MessageID))
			require.Equal(t, 1, countRows(t, db, `SELECT count(*) FROM approval_requests
				WHERE object_id = ? AND state = 'APPROVED' AND resolved_by = ?`,
				candidate.MessageID, channel.ApprovalSettler))
			require.Equal(t, string(test.reason), guardDetail(latestGuards(t, db,
				candidate.MessageID), protocol.GuardNameDetectorFallback))
		})
	}
}

// A recovery types nothing into a box holding other text: the uncertain
// attempt and its approval stay as they were.
func TestTmuxOwnRecoveryNeverTypesOverOtherText(t *testing.T) {
	t.Parallel()
	fixture, transport, _ := warmTmuxDeferFixture(t)
	candidate := fixture.seedDelivery(t, 174)
	leaveTmuxDeliveryUncertain(t, fixture, transport, candidate)

	transport.initial = []byte("$ human draft")
	require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), candidate))
	require.Zero(t, transport.writes)
	require.Zero(t, transport.enterOnly)
	db := fixture.store.DB()
	require.Equal(t, []string{string(protocol.ApprovalStatePending) + ":"},
		approvalStates(t, db, candidate.MessageID))
	require.Zero(t, countRows(t, db, `SELECT count(*) FROM delivery_attempts
		WHERE message_id = ? AND resolution_approval_id IS NOT NULL`, candidate.MessageID))
}

// leaveTmuxDeliveryUncertain runs one pass whose write ends uncertain, which
// leaves the attempt UNCERTAIN with a pending approval.
func leaveTmuxDeliveryUncertain(
	t *testing.T,
	fixture *executorFixture,
	transport *tmuxTransportStub,
	candidate daemon.QueueDelivery,
) {
	t.Helper()
	transport.preWriteResult = &tmuxattach.Result{
		Outcome: tmuxattach.OutcomeUncertain, Stage: tmuxattach.StageVerify,
	}
	require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), candidate))
	transport.preWriteResult = nil
	require.Equal(t, []string{string(protocol.ApprovalStatePending) + ":"},
		approvalStates(t, fixture.store.DB(), candidate.MessageID))
	require.Equal(t, 1, countRows(t, fixture.store.DB(), `SELECT count(*) FROM delivery_attempts
		WHERE message_id = ? AND state = 'UNCERTAIN'`, candidate.MessageID))
}
