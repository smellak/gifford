package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func allowLine(scope ExplanationScope) ExplanationLine {
	return ExplanationLine{Scope: scope, Key: "*", Value: string(ControlModeActive)}
}

func validExplanation() Explanation {
	return Explanation{
		FromAgent: testAgentID,
		ToAgent:   testAgent2,
		Plane:     PlaneMessage,
		Outcome:   DecisionOutcomeAllow,
		Reason:    DecisionReasonAllowed,
		Lines: []ExplanationLine{
			allowLine(ExplanationScopeGlobal),
			allowLine(ExplanationScopeObject),
		},
	}
}

// TestExplanationRules covers the explanation invariants of §6.5.
func TestExplanationRules(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		valid bool
		patch func(*Explanation)
	}{
		{name: "allow passes", valid: true, patch: func(*Explanation) {}},
		{name: "held marks the blocker", valid: true, patch: func(e *Explanation) {
			e.Outcome = DecisionOutcomeHold
			e.Reason = DecisionReasonLinkHold
			e.Blocker = ExplanationScopeLink
			e.Lines = []ExplanationLine{
				allowLine(ExplanationScopeGlobal),
				{Scope: ExplanationScopeLink, Key: testAgentID + linkArrow + testAgent2, Value: string(ControlModeHold), Blocking: true},
			}
		}},
		{name: "blocked needs a blocking line", patch: func(e *Explanation) {
			e.Outcome = DecisionOutcomeHold
			e.Reason = DecisionReasonLinkHold
			e.Blocker = ExplanationScopeLink
		}},
		{name: "blocking scope matches blocker", patch: func(e *Explanation) {
			e.Outcome = DecisionOutcomeHold
			e.Reason = DecisionReasonLinkHold
			e.Blocker = ExplanationScopeLink
			e.Lines = []ExplanationLine{
				{Scope: ExplanationScopeGlobal, Key: "*", Value: string(ControlModeActive), Blocking: true},
			}
		}},
		{name: "one blocking line only", patch: func(e *Explanation) {
			e.Outcome = DecisionOutcomeHold
			e.Reason = DecisionReasonLinkHold
			e.Blocker = ExplanationScopeLink
			e.Lines = []ExplanationLine{
				{Scope: ExplanationScopeGlobal, Key: "*", Value: string(ControlModeActive), Blocking: true},
				{Scope: ExplanationScopeLink, Key: "k", Value: string(ControlModeHold), Blocking: true},
			}
		}},
		{name: "lines follow scope order", patch: func(e *Explanation) {
			e.Lines = []ExplanationLine{
				allowLine(ExplanationScopeObject),
				allowLine(ExplanationScopeGlobal),
			}
		}},
		{name: "allow needs an allow reason", patch: func(e *Explanation) {
			e.Reason = DecisionReasonLinkHold
		}},
		{name: "approval needs an approval reason", patch: func(e *Explanation) {
			e.Outcome = DecisionOutcomeApproval
			e.Reason = DecisionReasonLinkHold
			e.Blocker = ExplanationScopeLink
			e.Lines = []ExplanationLine{
				{Scope: ExplanationScopeLink, Key: "k", Value: "REQUIRE_APPROVAL", Blocking: true},
			}
		}},
		{name: "denied never queues", patch: func(e *Explanation) {
			e.Outcome = DecisionOutcomeDeny
			e.Reason = DecisionReasonObjectHeld
			e.Blocker = ExplanationScopeObject
			e.Lines = []ExplanationLine{
				{Scope: ExplanationScopeObject, Key: "k", Value: "HELD", Blocking: true},
			}
		}},
		{name: "policy blocker denies policy", valid: true, patch: func(e *Explanation) {
			e.Outcome = DecisionOutcomeDeny
			e.Reason = DecisionReasonPolicyDenied
			e.Blocker = ExplanationScopePolicy
			e.Lines = []ExplanationLine{
				{Scope: ExplanationScopePolicy, Key: "*", Value: "DENY", Blocking: true},
			}
		}},
		{name: "policy blocker needs policy denied", patch: func(e *Explanation) {
			e.Outcome = DecisionOutcomeDeny
			e.Reason = DecisionReasonLinkDenied
			e.Blocker = ExplanationScopePolicy
			e.Lines = []ExplanationLine{
				{Scope: ExplanationScopePolicy, Key: "*", Value: "DENY", Blocking: true},
			}
		}},
		{name: "controls line closes", valid: true, patch: func(e *Explanation) {
			e.Outcome = DecisionOutcomeHold
			e.Reason = DecisionReasonControlUnavailable
			e.Blocker = ExplanationScopeControls
			e.Lines = []ExplanationLine{
				{Scope: ExplanationScopeControls, Key: "*", Value: noneWord, Blocking: true},
			}
		}},
		{name: "controls blocker holds", patch: func(e *Explanation) {
			e.Outcome = DecisionOutcomeDeny
			e.Reason = DecisionReasonControlUnavailable
			e.Blocker = ExplanationScopeControls
			e.Lines = []ExplanationLine{
				{Scope: ExplanationScopeControls, Key: "*", Value: noneWord, Blocking: true},
			}
		}},
		{name: "controls line skips no reason check", patch: func(e *Explanation) {
			e.Outcome = DecisionOutcomeHold
			e.Reason = DecisionReasonLinkHold
			e.Blocker = ExplanationScopeControls
			e.Lines = []ExplanationLine{
				{Scope: ExplanationScopeControls, Key: "*", Value: noneWord, Blocking: true},
			}
		}},
		{name: "policy line skips no reason check", patch: func(e *Explanation) {
			e.Outcome = DecisionOutcomeDeny
			e.Reason = DecisionReasonChannelOff
			e.Blocker = ExplanationScopePolicy
			e.Lines = []ExplanationLine{
				{Scope: ExplanationScopePolicy, Key: "*", Value: "DENY (policy)", Blocking: true},
				{Scope: ExplanationScopeControls, Key: "*", Value: noneWord},
			}
		}},
		{name: "object id needs object type", patch: func(e *Explanation) {
			e.ObjectID = "01JB0000000000000000000011"
		}},
		{name: "plane is message or artifact", patch: func(e *Explanation) {
			e.Plane = PlaneAll
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			explanation := validExplanation()
			tc.patch(&explanation)
			if tc.valid {
				require.NoError(t, explanation.Validate())
				return
			}
			require.ErrorIs(t, explanation.Validate(), ErrInvalid)
		})
	}
}

// TestEffectiveViewRules covers the delivery-gate preview.
func TestEffectiveViewRules(t *testing.T) {
	t.Parallel()

	view := EffectiveView{
		FromAgent:        testAgentID,
		ToAgent:          testAgent2,
		Plane:            PlaneMessage,
		EffectiveControl: EffectiveControl{GlobalMode: ChannelModeActive, FromNodeMode: ChannelModeActive, ToNodeMode: ChannelModeActive, FromTX: true, FromAutoContinue: true, ToRX: true, ToAutoDelivery: true, ToDeliverable: true, LinkMode: LinkModeAllow},
		EffectiveOutcome: DecisionOutcomeAllow,
		ControlVersions:  ControlVersions{string(ScopeTypeGlobal): 18},
	}
	require.NoError(t, view.Validate())

	blocked := view
	blocked.EffectiveOutcome = DecisionOutcomeHold
	require.ErrorIs(t, blocked.Validate(), ErrInvalid)

	named := view
	named.EffectiveOutcome = DecisionOutcomeHold
	named.Blocker = ExplanationScopeLink
	require.NoError(t, named.Validate())
}
