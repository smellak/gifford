package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const testApprovalID = "apr_01JB0000000000000000000000"

const testStepsOption ApprovalOption = "steps:3"

func validApproval() ApprovalRequest {
	return ApprovalRequest{
		ApprovalID: testApprovalID,
		Source:     ApprovalSourceLink,
		ObjectType: ApprovalObjectTypeMessage,
		ObjectID:   testMsgID,
		Options:    []ApprovalOption{kindApproveOnce, kindReject},
		State:      ApprovalStatePending,
		CreatedAt:  testControlTime(),
	}
}

// TestApprovalRules covers the approvals table of §6.4.
func TestApprovalRules(t *testing.T) {
	t.Parallel()

	resolved := "op-1"
	cases := []struct {
		name  string
		valid bool
		patch func(*ApprovalRequest)
	}{
		{name: "pending passes", valid: true, patch: func(*ApprovalRequest) {}},
		{name: "pending carries no resolution", patch: func(r *ApprovalRequest) {
			r.Resolution = &ApprovalResolution{Option: kindReject}
		}},
		{name: "approved passes", valid: true, patch: func(r *ApprovalRequest) {
			r.State = ApprovalStateApproved
			r.Resolution = &ApprovalResolution{Option: kindApproveOnce}
			r.ResolvedBy = resolved
			r.ResolvedAt = testControlTime()
		}},
		{name: "approved needs all three", patch: func(r *ApprovalRequest) {
			r.State = ApprovalStateApproved
			r.Resolution = &ApprovalResolution{Option: kindApproveOnce}
			r.ResolvedBy = resolved
		}},
		{name: "resolution kind must be offered", patch: func(r *ApprovalRequest) {
			r.State = ApprovalStateRejected
			r.Resolution = &ApprovalResolution{Option: testStepsOption}
			r.ResolvedBy = resolved
			r.ResolvedAt = testControlTime()
		}},
		{name: "expired carries no resolution", patch: func(r *ApprovalRequest) {
			r.State = ApprovalStateExpired
			r.Resolution = &ApprovalResolution{Option: kindReject}
		}},
		{name: "option kind must fit source", patch: func(r *ApprovalRequest) {
			r.Options = []ApprovalOption{testStepsOption}
		}},
		{name: "loop step grants steps", valid: true, patch: func(r *ApprovalRequest) {
			r.Source = ApprovalSourceLoopStep
			r.LoopID = testLoop
			r.Options = []ApprovalOption{testStepsOption, kindStop}
		}},
		{name: "loop step covers messages", patch: func(r *ApprovalRequest) {
			r.Source = ApprovalSourceLoopStep
			r.ObjectType = ApprovalObjectTypeLoop
			r.ObjectID = testLoop
			r.LoopID = testLoop
			r.Options = []ApprovalOption{kindStop}
		}},
		{name: "loop object names its loop", valid: true, patch: func(r *ApprovalRequest) {
			r.Source = ApprovalSourceLoopBudget
			r.ObjectType = ApprovalObjectTypeLoop
			r.ObjectID = testLoop
			r.LoopID = testLoop
			r.Options = []ApprovalOption{"extend:+30", kindStop}
		}},
		{name: "loop object from link source", patch: func(r *ApprovalRequest) {
			r.ObjectType = ApprovalObjectTypeLoop
			r.ObjectID = testLoop
			r.LoopID = testLoop
		}},
		{name: "thread link is approved", valid: true, patch: func(r *ApprovalRequest) {
			r.ObjectType = ApprovalObjectTypeThreadLink
			r.ObjectID = testAgentID + linkArrow + testAgent2 + ":" + testThread
			r.State = ApprovalStateApproved
			r.Resolution = &ApprovalResolution{Option: kindApproveOnce}
			r.ResolvedBy = resolved
			r.ResolvedAt = testControlTime()
			r.ExpiresAt = testControlTime().AddDate(0, 0, 1)
		}},
		{name: "thread link needs expiry", patch: func(r *ApprovalRequest) {
			r.ObjectType = ApprovalObjectTypeThreadLink
			r.ObjectID = testAgentID + linkArrow + testAgent2 + ":" + testThread
			r.State = ApprovalStateApproved
			r.Resolution = &ApprovalResolution{Option: kindApproveOnce}
			r.ResolvedBy = resolved
			r.ResolvedAt = testControlTime()
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			request := validApproval()
			tc.patch(&request)
			if tc.valid {
				require.NoError(t, request.Validate())
				return
			}
			require.ErrorIs(t, request.Validate(), ErrInvalid)
		})
	}
}
