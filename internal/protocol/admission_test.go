package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const testDecisionID = "chd_01JB0000000000000000000000"

func validAdmission() AdmissionResult {
	return AdmissionResult{
		Result:         AdmissionStatusAccepted,
		DecisionID:     testDecisionID,
		MessageID:      testMsgID,
		ThreadID:       testThread,
		ControlVersion: 18,
	}
}

// TestAdmissionRules covers the admission gate results of §6.6.
func TestAdmissionRules(t *testing.T) {
	t.Parallel()

	gen := int64(1)
	turn := int64(2)
	cases := []struct {
		name  string
		valid bool
		patch func(*AdmissionResult)
	}{
		{name: "accepted passes", valid: true, patch: func(*AdmissionResult) {}},
		{name: "accepted carries no reason", patch: func(r *AdmissionResult) {
			r.ControlReason = DecisionReasonLinkHold
		}},
		{name: "artifact instead of message", valid: true, patch: func(r *AdmissionResult) {
			r.MessageID = ""
			r.ArtifactID = "art-1"
		}},
		{name: "exactly one object id", patch: func(r *AdmissionResult) {
			r.ArtifactID = "art-1"
		}},
		{name: "no object id", patch: func(r *AdmissionResult) {
			r.MessageID = ""
		}},
		{name: "held passes", valid: true, patch: func(r *AdmissionResult) {
			r.Result = AdmissionStatusAcceptedHeld
			r.ControlReason = DecisionReasonLoopBudgetExhausted
		}},
		{name: "held needs a hold reason", patch: func(r *AdmissionResult) {
			r.Result = AdmissionStatusAcceptedHeld
			r.ControlReason = DecisionReasonLinkApproval
		}},
		{name: "held names no request", patch: func(r *AdmissionResult) {
			r.Result = AdmissionStatusAcceptedHeld
			r.ControlReason = DecisionReasonLinkHold
			r.ApprovalRequestID = testApprovalID
		}},
		{name: "approval required passes", valid: true, patch: func(r *AdmissionResult) {
			r.Result = AdmissionStatusApprovalRequired
			r.ControlReason = DecisionReasonLinkApproval
			r.ApprovalRequestID = testApprovalID
		}},
		{name: "approval required names the request", patch: func(r *AdmissionResult) {
			r.Result = AdmissionStatusApprovalRequired
			r.ControlReason = DecisionReasonLinkApproval
		}},
		{name: "approval needs an approval reason", patch: func(r *AdmissionResult) {
			r.Result = AdmissionStatusApprovalRequired
			r.ControlReason = DecisionReasonLinkHold
			r.ApprovalRequestID = testApprovalID
		}},
		{name: "loop fields travel together", valid: true, patch: func(r *AdmissionResult) {
			r.LoopID = testLoop
			r.LoopGeneration = &gen
			r.TurnIndex = &turn
		}},
		{name: "partial loop fields", patch: func(r *AdmissionResult) {
			r.LoopID = testLoop
			r.TurnIndex = &turn
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result := validAdmission()
			tc.patch(&result)
			if tc.valid {
				require.NoError(t, result.Validate())
				return
			}
			require.ErrorIs(t, result.Validate(), ErrInvalid)
		})
	}
}

// TestObjectControlStateRules covers the admitted-object states.
func TestObjectControlStateRules(t *testing.T) {
	t.Parallel()

	state := ObjectControlState{
		ObjectType:     DecisionObjectTypeMessage,
		ObjectID:       testMsgID,
		ControlState:   ControlStateHeld,
		ControlReason:  DecisionReasonLinkHold,
		ControlVersion: 18,
	}
	require.NoError(t, state.Validate())

	loop := state
	loop.ObjectType = DecisionObjectTypeLoopAction
	require.ErrorIs(t, loop.Validate(), ErrInvalid)
}
