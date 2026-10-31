package channel

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

// flagOff names the agent flag set to false, or all four.
type flagOff string

// Agent flag selectors.
const (
	flagNone         flagOff = ""
	flagTX           flagOff = "tx"
	flagAutoContinue flagOff = "auto_continue"
	flagAll          flagOff = "all"
	flagRX           flagOff = "rx"
	flagAutoDelivery flagOff = "auto_delivery"
)

// rowSpec describes one control row: empty mode means no row.
type rowSpec struct {
	mode    protocol.ControlMode
	expired bool
	restore protocol.ControlMode
}

// precedenceCase is one row of the §11.1 fixture table.
type precedenceCase struct {
	stage       protocol.DecisionStage
	kind        Kind
	plane       protocol.Plane
	toNode      string
	global      rowSpec
	nodeA       rowSpec
	nodeB       rowSpec
	sender      flagOff
	receiver    flagOff
	linkMsg     rowSpec
	linkArt     rowSpec
	thread      rowSpec
	loopMut     func(*protocol.Loop)
	noLoop      bool
	override    protocol.ControlMode
	object      *Object
	policy      bool
	neverLoaded bool
	hops        int64
	outcome     protocol.DecisionOutcome
	reason      protocol.DecisionReason
	blocker     protocol.ExplanationScope
}

// modeRow builds a mode row spec.
func modeRow(mode string) rowSpec {
	return rowSpec{mode: protocol.ControlMode(mode)}
}

// rows builds the control rows of the case.
func (tc precedenceCase) rows() []protocol.ChannelControl {
	var rows []protocol.ChannelControl
	id := 0
	next := func() protocol.ControlID {
		id++
		return testControlID(1000 + id)
	}
	add := func(root protocol.ChannelControl, spec rowSpec) {
		if spec.expired {
			root.ExpiresAt = testEngineTime.Add(-time.Minute)
			if spec.restore != "" {
				root.Restore = &protocol.ControlSnapshot{
					Mode:      spec.restore,
					ActorType: protocol.ActorTypeHuman,
					CreatedBy: testOperator,
					Version:   1,
				}
			}
		}
		rows = append(rows, root)
	}
	if tc.global.mode != "" {
		root := testEngineRoot(next())
		root.Mode = tc.global.mode
		add(root, tc.global)
	}
	for _, node := range []struct {
		name string
		spec rowSpec
	}{{testNodeA, tc.nodeA}, {testNodeB, tc.nodeB}} {
		if node.spec.mode == "" {
			continue
		}
		root := testNodeRoot(next(), node.name, protocol.ChannelMode(node.spec.mode))
		add(root, node.spec)
	}
	agentRoot := func(agent string, off flagOff) {
		if off == "" {
			return
		}
		root := testEngineRoot(next())
		root.ScopeType = protocol.ScopeTypeAgent
		root.ScopeKey = agent
		root.Mode = ""
		disable := testBool(false)
		switch off {
		case flagNone:
		case flagTX:
			root.TxEnabled = disable
		case flagAutoContinue:
			root.AutoContinueEnabled = disable
		case flagRX:
			root.RxEnabled = disable
		case flagAutoDelivery:
			root.AutoDeliveryEnabled = disable
		case flagAll:
			root.TxEnabled = disable
			root.RxEnabled = disable
			root.AutoDeliveryEnabled = disable
			root.AutoContinueEnabled = disable
		}
		rows = append(rows, root)
	}
	agentRoot(testAgent1, tc.sender)
	agentRoot(testAgent2, tc.receiver)
	linkRow := func(plane protocol.Plane, spec rowSpec) {
		if spec.mode == "" {
			return
		}
		root := testLinkRoot(next(), testAgent1, testAgent2, plane, protocol.LinkMode(spec.mode))
		add(root, spec)
	}
	linkRow(protocol.PlaneMessage, tc.linkMsg)
	linkRow(protocol.PlaneArtifact, tc.linkArt)
	if tc.thread.mode != "" {
		root := testThreadRoot(next(), testThreadID, protocol.LinkMode(tc.thread.mode))
		add(root, tc.thread)
	}
	if tc.override != "" {
		rows = append(rows, testLoopRoot(next(), testLoopID, protocol.LoopMode(tc.override)))
	}
	return rows
}

// query builds the evaluation query of the case.
func (tc precedenceCase) query() Query {
	toNode := tc.toNode
	if toNode == "" {
		toNode = testNodeA
	}
	plane := tc.plane
	if plane == "" {
		plane = protocol.PlaneMessage
	}
	q := Query{
		Stage:        tc.stage,
		Plane:        plane,
		From:         testAgent1,
		FromNode:     testNodeA,
		To:           testAgent2,
		ToNode:       protocol.NodeID(toNode),
		Kind:         tc.kind,
		HopCount:     tc.hops,
		Object:       tc.object,
		PolicyDenied: tc.policy,
	}
	if tc.thread.mode != "" {
		q.ThreadID = testThreadID
	}
	if !tc.noLoop && (tc.kind == KindReply || tc.loopMut != nil) {
		loop := testLoop()
		if tc.loopMut != nil {
			tc.loopMut(loop)
		}
		q.Loop = loop
	}
	return q
}

// Admission stages, short names for the fixture table.
const (
	adm  = protocol.DecisionStageAdmission
	acc  = protocol.DecisionStageAcceptance
	cont = protocol.DecisionStageContinuation
	del  = protocol.DecisionStageDelivery
)

// Shared objects of the fixture table.
var (
	heldObject    = &Object{Type: protocol.DecisionObjectTypeMessage, ID: testObjectID, Held: true}
	pendingObject = &Object{Type: protocol.DecisionObjectTypeMessage, ID: testObjectID, ApprovalPending: true}
)

// Loop mutators of the fixture table.
func loopMode(mode protocol.LoopMode) func(*protocol.Loop) {
	return func(loop *protocol.Loop) { loop.Mode = mode }
}

// loopPaused sets mode PAUSED with the given state reason.
func loopPaused(reason protocol.LoopStateReason) func(*protocol.Loop) {
	return func(loop *protocol.Loop) {
		loop.Mode = protocol.LoopModePaused
		loop.StateReason = reason
	}
}

// loopStepTokens sets mode STEP with the given tokens.
func loopStepTokens(tokens int64) func(*protocol.Loop) {
	return func(loop *protocol.Loop) {
		loop.Mode = protocol.LoopModeStep
		loop.Counters.StepTokens = tokens
	}
}

// precedenceCases is the §11.1 fixture table, one entry per row.
var precedenceCases = []precedenceCase{
	{stage: del, outcome: Allow, reason: protocol.DecisionReasonAllowed},
	{stage: adm, kind: KindRoot, outcome: Allow, reason: protocol.DecisionReasonAllowed},
	{stage: del, global: modeRow("PAUSED"), outcome: Hold, reason: protocol.DecisionReasonChannelPaused, blocker: protocol.ExplanationScopeGlobal},
	{stage: adm, kind: KindRoot, global: modeRow("PAUSED"), outcome: Allow, reason: protocol.DecisionReasonAllowed},
	{stage: del, global: modeRow("OFF"), outcome: Hold, reason: protocol.DecisionReasonChannelOff, blocker: protocol.ExplanationScopeGlobal},
	{stage: adm, kind: KindRoot, global: modeRow("OFF"), outcome: Deny, reason: protocol.DecisionReasonChannelOff, blocker: protocol.ExplanationScopeGlobal},
	{stage: adm, kind: KindRoot, global: modeRow("DRAINING"), outcome: Deny, reason: protocol.DecisionReasonChannelDraining, blocker: protocol.ExplanationScopeGlobal},
	{stage: adm, kind: KindReply, global: modeRow("DRAINING"), noLoop: true, outcome: Deny, reason: protocol.DecisionReasonChannelDraining, blocker: protocol.ExplanationScopeGlobal},
	{stage: adm, kind: KindReply, global: modeRow("DRAINING"), outcome: Allow, reason: protocol.DecisionReasonAllowed},
	{stage: del, global: modeRow("DRAINING"), loopMut: loopMode(protocol.LoopModeAuto), outcome: Allow, reason: protocol.DecisionReasonAllowed},
	{stage: del, nodeB: modeRow("PAUSED"), toNode: testNodeB, outcome: Hold, reason: protocol.DecisionReasonNodePaused, blocker: protocol.ExplanationScopeNode},
	{stage: adm, kind: KindRoot, nodeB: modeRow("PAUSED"), toNode: testNodeB, outcome: Allow, reason: protocol.DecisionReasonAllowed},
	{stage: adm, kind: KindRoot, nodeA: modeRow("OFF"), outcome: Deny, reason: protocol.DecisionReasonNodeOff, blocker: protocol.ExplanationScopeNode},
	{stage: del, global: modeRow("PAUSED"), nodeA: modeRow("OFF"), outcome: Hold, reason: protocol.DecisionReasonNodeOff, blocker: protocol.ExplanationScopeNode},
	{stage: del, global: modeRow("OFF"), nodeA: modeRow("OFF"), outcome: Hold, reason: protocol.DecisionReasonChannelOff, blocker: protocol.ExplanationScopeGlobal},
	{stage: adm, kind: KindRoot, nodeA: modeRow("DRAINING"), outcome: Deny, reason: protocol.DecisionReasonNodeDraining, blocker: protocol.ExplanationScopeNode},
	{stage: adm, kind: KindRoot, sender: flagTX, outcome: Deny, reason: protocol.DecisionReasonAgentTXDisabled, blocker: protocol.ExplanationScopeSender},
	{stage: del, sender: flagTX, outcome: Allow, reason: protocol.DecisionReasonAllowed},
	{stage: adm, kind: KindReply, sender: flagAutoContinue, outcome: Approval, reason: protocol.DecisionReasonAgentAutoContinueOff, blocker: protocol.ExplanationScopeSender},
	{stage: adm, kind: KindRoot, sender: flagAutoContinue, outcome: Allow, reason: protocol.DecisionReasonAllowed},
	{stage: adm, kind: KindRoot, receiver: flagRX, outcome: Deny, reason: protocol.DecisionReasonAgentRXDisabled, blocker: protocol.ExplanationScopeReceiver},
	{stage: acc, kind: KindRoot, receiver: flagRX, outcome: Deny, reason: protocol.DecisionReasonAgentRXDisabled, blocker: protocol.ExplanationScopeReceiver},
	{stage: del, receiver: flagRX, outcome: Allow, reason: protocol.DecisionReasonAllowed},
	{stage: del, receiver: flagAutoDelivery, outcome: Hold, reason: protocol.DecisionReasonAgentAutoDeliveryOff, blocker: protocol.ExplanationScopeReceiver},
	{stage: adm, kind: KindRoot, receiver: flagAutoDelivery, outcome: Allow, reason: protocol.DecisionReasonAllowed},
	{stage: del, receiver: flagAll, outcome: Hold, reason: protocol.DecisionReasonAgentAutoDeliveryOff, blocker: protocol.ExplanationScopeReceiver},
	{stage: adm, kind: KindRoot, sender: flagTX, linkMsg: modeRow("DENY"), outcome: Deny, reason: protocol.DecisionReasonAgentTXDisabled, blocker: protocol.ExplanationScopeSender},
	{stage: del, linkMsg: modeRow("HOLD"), loopMut: loopMode(protocol.LoopModeAuto), outcome: Hold, reason: protocol.DecisionReasonLinkHold, blocker: protocol.ExplanationScopeLink},
	{stage: adm, kind: KindRoot, linkMsg: modeRow("HOLD"), outcome: Hold, reason: protocol.DecisionReasonLinkHold, blocker: protocol.ExplanationScopeLink},
	{stage: adm, kind: KindRoot, linkMsg: modeRow("REQUIRE_APPROVAL"), outcome: Approval, reason: protocol.DecisionReasonLinkApproval, blocker: protocol.ExplanationScopeLink},
	{stage: adm, kind: KindRoot, linkMsg: modeRow("DENY"), outcome: Deny, reason: protocol.DecisionReasonLinkDenied, blocker: protocol.ExplanationScopeLink},
	{stage: del, linkMsg: modeRow("DENY"), outcome: Hold, reason: protocol.DecisionReasonLinkDenied, blocker: protocol.ExplanationScopeLink},
	{stage: adm, kind: KindRoot, global: modeRow("PAUSED"), linkMsg: modeRow("DENY"), outcome: Deny, reason: protocol.DecisionReasonLinkDenied, blocker: protocol.ExplanationScopeLink},
	{stage: del, global: modeRow("PAUSED"), linkMsg: modeRow("DENY"), outcome: Hold, reason: protocol.DecisionReasonLinkDenied, blocker: protocol.ExplanationScopeLink},
	{stage: del, global: modeRow("PAUSED"), linkMsg: modeRow("HOLD"), outcome: Hold, reason: protocol.DecisionReasonChannelPaused, blocker: protocol.ExplanationScopeGlobal},
	{stage: del, linkMsg: modeRow("REQUIRE_APPROVAL"), loopMut: loopPaused(protocol.LoopStateReasonManual), outcome: Hold, reason: protocol.DecisionReasonLoopPaused, blocker: protocol.ExplanationScopeLoop},
	{stage: del, linkMsg: modeRow("HOLD"), loopMut: loopStepTokens(0), outcome: Hold, reason: protocol.DecisionReasonLinkHold, blocker: protocol.ExplanationScopeLink},
	{stage: del, thread: modeRow("HOLD"), outcome: Hold, reason: protocol.DecisionReasonLinkHold, blocker: protocol.ExplanationScopeThread},
	{stage: adm, kind: KindRoot, thread: modeRow("DENY"), outcome: Deny, reason: protocol.DecisionReasonLinkDenied, blocker: protocol.ExplanationScopeThread},
	{stage: adm, kind: KindRoot, linkMsg: modeRow("REQUIRE_APPROVAL"), thread: modeRow("HOLD"), outcome: Hold, reason: protocol.DecisionReasonLinkHold, blocker: protocol.ExplanationScopeThread},
	{stage: adm, kind: KindReply, loopMut: loopMode(protocol.LoopModeStopped), outcome: Deny, reason: protocol.DecisionReasonLoopStopped, blocker: protocol.ExplanationScopeLoop},
	{stage: adm, kind: KindReply, loopMut: loopMode(protocol.LoopModeCompleted), outcome: Deny, reason: protocol.DecisionReasonLoopStopped, blocker: protocol.ExplanationScopeLoop},
	{stage: adm, kind: KindReply, loopMut: loopMode(protocol.LoopModeDraining), outcome: Deny, reason: protocol.DecisionReasonLoopDraining, blocker: protocol.ExplanationScopeLoop},
	{stage: adm, kind: KindReply, loopMut: loopPaused(protocol.LoopStateReasonManual), outcome: Hold, reason: protocol.DecisionReasonLoopPaused, blocker: protocol.ExplanationScopeLoop},
	{stage: adm, kind: KindReply, loopMut: loopPaused(protocol.LoopStateReasonBudgetExhausted), outcome: Hold, reason: protocol.DecisionReasonLoopBudgetExhausted, blocker: protocol.ExplanationScopeLoop},
	{stage: adm, kind: KindReply, loopMut: loopPaused(protocol.LoopStateReasonLoopPatternDetected), outcome: Hold, reason: protocol.DecisionReasonLoopPatternDetected, blocker: protocol.ExplanationScopeLoop},
	{stage: adm, kind: KindReply, loopMut: loopStepTokens(0), outcome: Approval, reason: protocol.DecisionReasonLoopStepTokenRequired, blocker: protocol.ExplanationScopeLoop},
	{stage: adm, kind: KindReply, loopMut: func(loop *protocol.Loop) {
		loop.Mode = protocol.LoopModeStep
		loop.Counters.StepTokens = 2
		loop.Counters.TurnCount = 20
	}, outcome: Allow, reason: protocol.DecisionReasonAllowed},
	{stage: adm, kind: KindReply, loopMut: func(loop *protocol.Loop) {
		loop.Counters.TurnCount = 12
	}, outcome: Hold, reason: protocol.DecisionReasonLoopBudgetExhausted, blocker: protocol.ExplanationScopeLoop},
	{stage: adm, kind: KindReply, loopMut: func(loop *protocol.Loop) {
		loop.Counters.TurnCount = 12
		loop.LastSenderAgent = testAgent1
	}, outcome: Allow, reason: protocol.DecisionReasonAllowed},
	{stage: cont, kind: KindReply, loopMut: func(loop *protocol.Loop) {
		loop.Counters.TurnCount = 12
	}, outcome: Hold, reason: protocol.DecisionReasonLoopBudgetExhausted, blocker: protocol.ExplanationScopeLoop},
	{stage: adm, kind: KindReply, loopMut: func(loop *protocol.Loop) {
		loop.Counters.MessageCount = 24
	}, outcome: Deny, reason: protocol.DecisionReasonLoopBudgetExhausted, blocker: protocol.ExplanationScopeLoop},
	{stage: adm, kind: KindReply, loopMut: func(loop *protocol.Loop) {
		loop.ExpiresAt = testEngineTime
	}, outcome: Deny, reason: protocol.DecisionReasonLoopBudgetExhausted, blocker: protocol.ExplanationScopeLoop},
	{stage: adm, kind: KindReply, hops: 25, outcome: Deny, reason: protocol.DecisionReasonLoopBudgetExhausted, blocker: protocol.ExplanationScopeLoop},
	{stage: adm, kind: KindReply, override: protocol.ControlMode(protocol.LoopModeStopped), outcome: Deny, reason: protocol.DecisionReasonLoopStopped, blocker: protocol.ExplanationScopeLoop},
	{stage: del, loopMut: loopStepTokens(0), outcome: Approval, reason: protocol.DecisionReasonLoopStepTokenRequired, blocker: protocol.ExplanationScopeLoop},
	{stage: del, loopMut: loopStepTokens(2), outcome: Approval, reason: protocol.DecisionReasonLoopStepTokenRequired, blocker: protocol.ExplanationScopeLoop},
	{stage: del, loopMut: loopMode(protocol.LoopModeDraining), outcome: Allow, reason: protocol.DecisionReasonAllowed},
	{stage: del, loopMut: loopMode(protocol.LoopModeStopped), outcome: Hold, reason: protocol.DecisionReasonLoopStopped, blocker: protocol.ExplanationScopeLoop},
	{stage: del, loopMut: loopPaused(protocol.LoopStateReasonBudgetExhausted), outcome: Hold, reason: protocol.DecisionReasonLoopBudgetExhausted, blocker: protocol.ExplanationScopeLoop},
	{stage: del, loopMut: func(loop *protocol.Loop) {
		loop.ExpiresAt = testEngineTime
	}, outcome: Hold, reason: protocol.DecisionReasonLoopBudgetExhausted, blocker: protocol.ExplanationScopeLoop},
	{stage: del, override: protocol.ControlMode(protocol.LoopModePaused), loopMut: loopMode(protocol.LoopModeAuto), outcome: Hold, reason: protocol.DecisionReasonLoopPaused, blocker: protocol.ExplanationScopeLoop},
	{stage: del, object: heldObject, outcome: Hold, reason: protocol.DecisionReasonObjectHeld, blocker: protocol.ExplanationScopeObject},
	{stage: del, object: pendingObject, outcome: Approval, reason: protocol.DecisionReasonObjectApprovalPending, blocker: protocol.ExplanationScopeObject},
	{stage: del, linkMsg: modeRow("REQUIRE_APPROVAL"), object: pendingObject, outcome: Approval, reason: protocol.DecisionReasonLinkApproval, blocker: protocol.ExplanationScopeLink},
	{stage: del, global: modeRow("OFF"), object: heldObject, outcome: Hold, reason: protocol.DecisionReasonChannelOff, blocker: protocol.ExplanationScopeGlobal},
	{stage: del, global: modeRow("PAUSED"), object: heldObject, outcome: Hold, reason: protocol.DecisionReasonChannelPaused, blocker: protocol.ExplanationScopeGlobal},
	{stage: adm, kind: KindRoot, policy: true, outcome: Deny, reason: protocol.DecisionReasonPolicyDenied, blocker: protocol.ExplanationScopePolicy},
	{stage: del, policy: true, outcome: Hold, reason: protocol.DecisionReasonPolicyDenied, blocker: protocol.ExplanationScopePolicy},
	{stage: adm, kind: KindRoot, neverLoaded: true, outcome: Hold, reason: protocol.DecisionReasonControlUnavailable, blocker: protocol.ExplanationScopeControls},
	{stage: del, neverLoaded: true, outcome: Hold, reason: protocol.DecisionReasonControlUnavailable, blocker: protocol.ExplanationScopeControls},
	{stage: adm, kind: KindRoot, policy: true, neverLoaded: true, outcome: Deny, reason: protocol.DecisionReasonPolicyDenied, blocker: protocol.ExplanationScopePolicy},
	{stage: adm, kind: KindRoot, plane: protocol.PlaneArtifact, linkMsg: modeRow("DENY"), outcome: Allow, reason: protocol.DecisionReasonAllowed},
	{stage: adm, kind: KindRoot, plane: protocol.PlaneArtifact, linkArt: modeRow("REQUIRE_APPROVAL"), outcome: Approval, reason: protocol.DecisionReasonLinkApproval, blocker: protocol.ExplanationScopeLink},
	{stage: adm, kind: KindReply, plane: protocol.PlaneArtifact, loopMut: func(loop *protocol.Loop) {
		loop.Counters.ArtifactCount = 3
	}, outcome: Deny, reason: protocol.DecisionReasonLoopBudgetExhausted, blocker: protocol.ExplanationScopeLoop},
	{stage: del, linkMsg: rowSpec{mode: "HOLD", expired: true}, outcome: Hold, reason: protocol.DecisionReasonLinkHold, blocker: protocol.ExplanationScopeLink},
	{stage: del, linkMsg: rowSpec{mode: "ALLOW", expired: true, restore: "DENY"}, outcome: Hold, reason: protocol.DecisionReasonLinkDenied, blocker: protocol.ExplanationScopeLink},
	{stage: adm, kind: KindRoot, global: rowSpec{mode: "ACTIVE", expired: true, restore: "OFF"}, outcome: Deny, reason: protocol.DecisionReasonChannelOff, blocker: protocol.ExplanationScopeGlobal},
}

// TestEvaluatePrecedence runs the §11.1 fixture table.
func TestEvaluatePrecedence(t *testing.T) {
	require.Len(t, precedenceCases, 78)
	for index, tc := range precedenceCases {
		t.Run(fmt.Sprintf("row %d", index+1), func(t *testing.T) {
			engine, err := New(Options{Clock: testClock{now: testEngineTime}})
			require.NoError(t, err)
			if !tc.neverLoaded {
				require.NoError(t, engine.load(tc.rows()))
			}
			query := tc.query()
			verdict, eff, versions, err := engine.evaluate(query)
			require.NoError(t, err)
			require.Equal(t, tc.outcome, verdict.outcome, "outcome")
			require.Equal(t, tc.reason, verdict.reason, "reason")
			require.Equal(t, tc.blocker, verdict.line, "blocker")
			require.NotEmpty(t, versions)
			if tc.neverLoaded {
				require.Equal(t, Effective{}, eff)
			} else {
				require.Equal(t, query.Plane, eff.Plane)
			}
		})
	}
}

// TestReplicaAndOverrideCombine requires the loop replica and the local
// override to combine: the replica adds its restriction without leaving,
// the override is always evaluated, and a grant only stands when the
// override does not restrict. The strongest restriction wins.
func TestReplicaAndOverrideCombine(t *testing.T) {
	t.Parallel()

	replica := func(mode protocol.LoopMode, tokens int64) *protocol.Loop {
		rep := testLoop()
		rep.Mode = mode
		rep.Counters.StepTokens = tokens
		return rep
	}
	for _, tc := range []struct {
		name     string
		delivery bool
		replica  *protocol.Loop
		override protocol.LoopMode
		outcome  protocol.DecisionOutcome
		reason   protocol.DecisionReason
	}{
		{"paused replica, stopped override, reply", false, replica(protocol.LoopModePaused, 0), protocol.LoopModeStopped, Deny, protocol.DecisionReasonLoopStopped},
		{"step without tokens, stopped override, reply", false, replica(protocol.LoopModeStep, 0), protocol.LoopModeStopped, Deny, protocol.DecisionReasonLoopStopped},
		{"paused replica, stopped override, delivery", true, replica(protocol.LoopModePaused, 0), protocol.LoopModeStopped, Hold, protocol.DecisionReasonLoopStopped},
		{"step without tokens, stopped override, delivery", true, replica(protocol.LoopModeStep, 0), protocol.LoopModeStopped, Hold, protocol.DecisionReasonLoopStopped},
		{"step with token, stopped override, delivery", true, replica(protocol.LoopModeStep, 1), protocol.LoopModeStopped, Hold, protocol.DecisionReasonLoopStopped},
		{"step with token, paused override, delivery", true, replica(protocol.LoopModeStep, 1), protocol.LoopModePaused, Hold, protocol.DecisionReasonLoopPaused},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			engine := testEngine(t, testLoopRoot(testControlID(1), testLoopID, tc.override))
			var query Query
			if tc.delivery {
				query = testDeliveryQuery()
			} else {
				query = testQuery()
				query.Kind = KindReply
			}
			query.Loop = tc.replica
			v, _, _, err := engine.evaluate(query)
			require.NoError(t, err)
			require.Equal(t, tc.outcome, v.outcome)
			require.Equal(t, tc.reason, v.reason)
		})
	}
}

// TestHardLimitsDoNotOverflow requires the budget arithmetic to deny
// without wrapping: counters at MaxInt64 never pass by overflow.
func TestHardLimitsDoNotOverflow(t *testing.T) {
	t.Parallel()

	t.Run("messages", func(t *testing.T) {
		t.Parallel()
		engine := testEngine(t)
		query := testQuery()
		query.Kind = KindReply
		query.Loop = testLoop()
		query.Loop.Counters.MessageCount = math.MaxInt64
		v, _, _, err := engine.evaluate(query)
		require.NoError(t, err)
		require.Equal(t, Deny, v.outcome)
		require.Equal(t, protocol.DecisionReasonLoopBudgetExhausted, v.reason)
	})

	t.Run("artifacts", func(t *testing.T) {
		t.Parallel()
		engine := testEngine(t)
		query := testQuery()
		query.Kind = KindReply
		query.Plane = protocol.PlaneArtifact
		query.Loop = testLoop()
		query.Loop.Counters.ArtifactCount = math.MaxInt64
		query.Loop.Counters.ArtifactBytes = math.MaxInt64
		query.ArtifactBytes = math.MaxInt64
		v, _, _, err := engine.evaluate(query)
		require.NoError(t, err)
		require.Equal(t, Deny, v.outcome)
		require.Equal(t, protocol.DecisionReasonLoopBudgetExhausted, v.reason)
	})
}

func TestOpeningReplyIncludesItsMessageReservation(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	query := testQuery()
	query.Kind = KindReply
	query.openingLoop = true
	query.Loop = testLoop()
	query.Loop.Counters.MessageCount = 1
	query.Loop.Counters.TurnCount = 1
	query.Loop.Budget.MaxMessages = 1
	query.Loop.LastSenderAgent = query.From

	verdict, _, _, err := engine.evaluate(query)
	require.NoError(t, err)
	require.Equal(t, Allow, verdict.outcome)
	require.Equal(t, protocol.DecisionReasonAllowed, verdict.reason)
}

// TestLoopExpiredRestoreDenies requires an expired PAUSED loop whose
// restore is STOPPED to deny with LOOP_STOPPED even though the replica is
// only PAUSED: the restore is collected before the override returns.
func TestLoopExpiredRestoreDenies(t *testing.T) {
	t.Parallel()

	for _, delivery := range []bool{false, true} {
		name := "admission"
		want := Deny
		if delivery {
			name = "delivery"
			want = Hold
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			row := testLoopRoot(testControlID(41), testLoopID, protocol.LoopModePaused)
			row.ExpiresAt = testEngineTime.Add(-time.Minute)
			row.Restore = testSnapshot(protocol.ControlModeStopped)
			e := testEngine(t, row)
			q := testQuery()
			if delivery {
				q = testDeliveryQuery()
			}
			q.Kind = KindReply
			q.Loop = testLoop()
			q.Loop.Mode = protocol.LoopModePaused
			v, _, _, err := e.evaluate(q)
			require.NoError(t, err)
			require.Equal(t, want, v.outcome)
			require.Equal(t, restrictionDeny, v.class)
			require.Equal(t, protocol.DecisionReasonLoopStopped, v.reason)
		})
	}
}
