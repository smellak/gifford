package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func validRecord() DecisionRecord {
	explanation := validExplanation()
	explanation.FromAgent = testAgentID
	explanation.ToAgent = testAgent2
	return DecisionRecord{
		DecisionID:        testDecisionID,
		ObjectType:        DecisionObjectTypeMessage,
		ObjectID:          testMsgID,
		Stage:             DecisionStageAdmission,
		Plane:             PlaneMessage,
		FromAgent:         testAgentID,
		ToAgent:           testAgent2,
		Outcome:           DecisionOutcomeAllow,
		Reason:            DecisionReasonAllowed,
		MatchedControlIDs: []ControlID{testControlID},
		EffectiveControl: EffectiveControl{
			GlobalMode: ChannelModeActive, FromNodeMode: ChannelModeActive,
			ToNodeMode: ChannelModeActive, FromTX: true, FromAutoContinue: true,
			ToRX: true, ToAutoDelivery: true, ToDeliverable: true, LinkMode: LinkModeAllow,
		},
		ControlVersions: ControlVersions{"global": 18},
		Explanation:     explanation,
		CreatedAt:       testControlTime(),
	}
}

func heldRecord() DecisionRecord {
	record := validRecord()
	record.Outcome = DecisionOutcomeHold
	record.Reason = DecisionReasonLinkHold
	record.Explanation = validExplanation()
	record.Explanation.Outcome = DecisionOutcomeHold
	record.Explanation.Reason = DecisionReasonLinkHold
	record.Explanation.Blocker = ExplanationScopeLink
	record.Explanation.Lines = []ExplanationLine{
		allowLine(ExplanationScopeGlobal),
		{Scope: ExplanationScopeLink, Key: testAgentID + linkArrow + testAgent2, Value: "HOLD", Blocking: true},
	}
	return record
}

// TestDecisionRecordRules covers the six rules of §6.5.
func TestDecisionRecordRules(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		valid bool
		patch func(*DecisionRecord)
	}{
		{name: "allow passes", valid: true, patch: func(*DecisionRecord) {}},
		{name: "held passes", valid: true, patch: func(r *DecisionRecord) {
			*r = heldRecord()
		}},
		{name: "allow needs an allow reason", patch: func(r *DecisionRecord) {
			r.Reason = DecisionReasonLinkHold
		}},
		{name: "approval reasons", valid: true, patch: func(r *DecisionRecord) {
			*r = heldRecord()
			r.Outcome = DecisionOutcomeApproval
			r.Reason = DecisionReasonLoopStepTokenRequired
			r.Explanation.Outcome = DecisionOutcomeApproval
			r.Explanation.Reason = DecisionReasonLoopStepTokenRequired
		}},
		{name: "approval rejects other reasons", patch: func(r *DecisionRecord) {
			*r = heldRecord()
			r.Outcome = DecisionOutcomeApproval
			r.Reason = DecisionReasonLinkHold
			r.Explanation.Outcome = DecisionOutcomeApproval
			r.Explanation.Reason = DecisionReasonLinkHold
		}},
		{name: "deny at admission passes", valid: true, patch: func(r *DecisionRecord) {
			*r = heldRecord()
			r.Outcome = DecisionOutcomeDeny
			r.Reason = DecisionReasonLinkDenied
			r.Explanation.Outcome = DecisionOutcomeDeny
			r.Explanation.Reason = DecisionReasonLinkDenied
		}},
		{name: "deny rejects queued reasons", patch: func(r *DecisionRecord) {
			*r = heldRecord()
			r.Outcome = DecisionOutcomeDeny
			r.Reason = DecisionReasonObjectHeld
			r.Explanation.Outcome = DecisionOutcomeDeny
			r.Explanation.Reason = DecisionReasonObjectHeld
		}},
		{name: "deny rejects delivery stage", patch: func(r *DecisionRecord) {
			*r = heldRecord()
			r.Outcome = DecisionOutcomeDeny
			r.Reason = DecisionReasonLinkDenied
			r.Stage = DecisionStageDelivery
			r.Explanation.Outcome = DecisionOutcomeDeny
			r.Explanation.Reason = DecisionReasonLinkDenied
		}},
		{name: "unavailable controls hold", valid: true, patch: func(r *DecisionRecord) {
			*r = heldRecord()
			r.Reason = DecisionReasonControlUnavailable
			r.Explanation.Reason = DecisionReasonControlUnavailable
			r.Explanation.Blocker = ExplanationScopeControls
			r.Explanation.Lines = []ExplanationLine{
				{Scope: ExplanationScopeControls, Key: "*", Value: "none", Blocking: true},
			}
		}},
		{name: "unavailable controls never deny", patch: func(r *DecisionRecord) {
			*r = heldRecord()
			r.Outcome = DecisionOutcomeDeny
			r.Reason = DecisionReasonControlUnavailable
			r.Explanation.Outcome = DecisionOutcomeDeny
			r.Explanation.Reason = DecisionReasonControlUnavailable
		}},
		{name: "revalidation hold means changed", valid: true, patch: func(r *DecisionRecord) {
			*r = heldRecord()
			r.Stage = DecisionStageRevalidation
			r.Reason = DecisionReasonControlChanged
			r.Explanation.Reason = DecisionReasonControlChanged
		}},
		{name: "revalidation never denies", patch: func(r *DecisionRecord) {
			*r = heldRecord()
			r.Stage = DecisionStageRevalidation
			r.Outcome = DecisionOutcomeDeny
			r.Reason = DecisionReasonLinkDenied
			r.Explanation.Outcome = DecisionOutcomeDeny
			r.Explanation.Reason = DecisionReasonLinkDenied
		}},
		{name: "explanation matches the record", patch: func(r *DecisionRecord) {
			r.Explanation.Reason = DecisionReasonApprovedOnce
		}},
		{name: "agents match the explanation", patch: func(r *DecisionRecord) {
			r.ToAgent = "agent-9"
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			record := validRecord()
			tc.patch(&record)
			if tc.valid {
				require.NoError(t, record.Validate())
				return
			}
			require.ErrorIs(t, record.Validate(), ErrInvalid)
		})
	}
}
