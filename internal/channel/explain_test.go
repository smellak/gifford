package channel

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

// updateGolden rewrites the golden files (R26).
var updateGolden = flag.Bool("update", false, "rewrite golden files")

// requireGolden compares bytes against a golden file.
func requireGolden(t *testing.T, name string, got []byte) {
	t.Helper()
	path := filepath.Join("testdata", "explain", name)
	if *updateGolden {
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, got, 0o644))
	}
	want, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, string(want), string(got))
}

// goldenEngine builds the §8.6 shared fixture: global ACTIVE v18 and node
// ACTIVE v3.
func goldenEngine(t *testing.T) *Engine {
	t.Helper()
	global := testEngineRoot(testControlID(1))
	global.Version = 18
	node := testNodeRoot(testControlID(2), testNodeA, protocol.ChannelModeActive)
	node.Version = 3
	return testEngine(t, global, node)
}

// goldenLoop builds the §8.6 shared loop with control version 5.
func goldenLoop() *protocol.Loop {
	loop := testLoop()
	loop.ControlVersion = 5
	return loop
}

// goldenQuery builds the §8.6 shared query: delivery of msg_42 with thread
// and loop.
func goldenQuery() Query {
	query := testDeliveryQuery()
	query.ToDeliverable = true
	query.ThreadID = testThreadID
	query.Loop = goldenLoop()
	query.Object = &Object{Type: protocol.DecisionObjectTypeMessage, ID: testObjectID}
	return query
}

// applyStored applies a row or fails the test.
func applyStored(t *testing.T, engine *Engine, row protocol.ChannelControl) {
	t.Helper()
	stored, err := engine.apply(row)
	require.NoError(t, err)
	require.True(t, stored)
}

// renderGolden explains and renders a query at the fixture now.
func renderGolden(t *testing.T, engine *Engine, query Query) protocol.Explanation {
	t.Helper()
	x, err := engine.Explain(query)
	require.NoError(t, err)
	require.NoError(t, x.Validate())
	return x
}

// TestExplainGolden checks the eight text goldens and the two JSON goldens
// of §8.6.
func TestExplainGolden(t *testing.T) {
	render := func(t *testing.T, engine *Engine, query Query) string {
		t.Helper()
		return Render(renderGolden(t, engine, query), testEngineTime, time.UTC)
	}

	t.Run("link_hold", func(t *testing.T) {
		link := testLinkRoot(testControlID(3), testAgent1, testAgent2, protocol.PlaneMessage, protocol.LinkModeHold)
		link.Version = 7
		link.Reason = "pairing session"
		link.ExpiresAt = testEngineTime.Add(10 * time.Minute)
		engine := goldenEngine(t)
		applyStored(t, engine, link)
		query := goldenQuery()
		requireGolden(t, "link_hold.golden", []byte(render(t, engine, query)))

		v, eff, vers, err := engine.evaluate(query)
		require.NoError(t, err)
		x, err := engine.Explain(query)
		require.NoError(t, err)
		explanation, err := json.MarshalIndent(x, "", "  ")
		require.NoError(t, err)
		requireGolden(t, "link_hold.json.golden", append(explanation, '\n'))

		rec, err := recordOf("chd_00000000000000000000000001", query, v, eff, vers, x, testEngineTime)
		require.NoError(t, err)
		record, err := json.MarshalIndent(rec, "", "  ")
		require.NoError(t, err)
		requireGolden(t, "record_link_hold.json.golden", append(record, '\n'))
	})

	t.Run("allowed_pair", func(t *testing.T) {
		engine := testEngine(t)
		query := testDeliveryQuery()
		query.ToDeliverable = true
		requireGolden(t, "allowed_pair.golden", []byte(render(t, engine, query)))
	})

	t.Run("strongest_wins", func(t *testing.T) {
		global := testEngineRoot(testControlID(1))
		global.Mode = protocol.ControlMode(protocol.ChannelModePaused)
		global.Version = 19
		node := testNodeRoot(testControlID(2), testNodeA, protocol.ChannelModeActive)
		node.Version = 3
		link := testLinkRoot(testControlID(3), testAgent1, testAgent2, protocol.PlaneMessage, protocol.LinkModeDeny)
		link.Version = 8
		link.Reason = "stop the ping-pong"
		engine := testEngine(t, global, node, link)
		requireGolden(t, "strongest_wins.golden", []byte(render(t, engine, goldenQuery())))
	})

	t.Run("loop_budget", func(t *testing.T) {
		engine := goldenEngine(t)
		query := goldenQuery()
		query.Loop.Mode = protocol.LoopModePaused
		query.Loop.StateReason = protocol.LoopStateReasonBudgetExhausted
		query.Loop.Counters.TurnCount = 12
		query.Loop.Counters.MessageCount = 13
		requireGolden(t, "loop_budget.golden", []byte(render(t, engine, query)))
	})

	t.Run("unavailable", func(t *testing.T) {
		engine, err := New(Options{Clock: testClock{now: testEngineTime}})
		require.NoError(t, err)
		requireGolden(t, "unavailable.golden", []byte(render(t, engine, goldenQuery())))
	})

	t.Run("federated_node", func(t *testing.T) {
		global := testEngineRoot(testControlID(1))
		global.Version = 18
		nodeA := testNodeRoot(testControlID(2), testNodeA, protocol.ChannelModeActive)
		nodeA.Version = 3
		nodeB := testNodeRoot(testControlID(4), testNodeB, protocol.ChannelModePaused)
		nodeB.Version = 4
		nodeB.Reason = "maintenance"
		nodeB.ExpiresAt = time.Date(2026, time.October, 29, 9, 0, 0, 0, time.UTC)
		engine := testEngine(t, global, nodeA, nodeB)
		query := goldenQuery()
		query.ToNode = testNodeB
		requireGolden(t, "federated_node.golden", []byte(render(t, engine, query)))
	})

	t.Run("continue_off", func(t *testing.T) {
		agent := testEngineRoot(testControlID(5))
		agent.ScopeType = protocol.ScopeTypeAgent
		agent.ScopeKey = testAgent1
		agent.Mode = ""
		agent.AutoContinueEnabled = testBool(false)
		agent.Version = 2
		engine := goldenEngine(t)
		applyStored(t, engine, agent)
		query := testQuery()
		query.Kind = KindReply
		query.Loop = goldenLoop()
		query.Object = &Object{Type: protocol.DecisionObjectTypeMessage, ID: "msg_43"}
		requireGolden(t, "continue_off.golden", []byte(render(t, engine, query)))
	})

	t.Run("policy_denied", func(t *testing.T) {
		engine := goldenEngine(t)
		query := testQuery()
		query.Object = &Object{Type: protocol.DecisionObjectTypeMessage, ID: "msg_44"}
		query.PolicyDenied = true
		requireGolden(t, "policy_denied.golden", []byte(render(t, engine, query)))
	})
}

// testDecisionID builds a deterministic decision id.
func testDecisionID(row int) protocol.ChannelDecisionID {
	return protocol.ChannelDecisionID(fmt.Sprintf("chd_%026d", row+1))
}

// TestExplainBlocker replays the §11.1 rows through Explain: the blocking
// line matches the Blocker column, exactly one line blocks unless the
// outcome is ALLOW, the explanation validates, and recordOf plus
// newDecision accept every row with an object added when missing.
func TestExplainBlocker(t *testing.T) {
	for index, tc := range precedenceCases {
		t.Run(fmt.Sprintf("row %d", index+1), func(t *testing.T) {
			engine, err := New(Options{Clock: testClock{now: testEngineTime}})
			require.NoError(t, err)
			if !tc.neverLoaded {
				require.NoError(t, engine.load(tc.rows()))
			}
			query := tc.query()
			v, eff, vers, err := engine.evaluate(query)
			require.NoError(t, err)
			require.Equal(t, tc.outcome, v.outcome)
			require.Equal(t, tc.reason, v.reason)
			x, err := engine.Explain(query)
			require.NoError(t, err)
			require.Equal(t, tc.outcome, x.Outcome)
			require.Equal(t, tc.reason, x.Reason)
			var blocking []protocol.ExplanationLine
			for _, line := range x.Lines {
				if line.Blocking {
					blocking = append(blocking, line)
				}
			}
			if tc.outcome == Allow {
				require.Empty(t, blocking)
				require.Equal(t, protocol.ExplanationScope(""), x.Blocker)
			} else {
				require.Len(t, blocking, 1)
				require.Equal(t, tc.blocker, blocking[0].Scope)
				require.Equal(t, tc.blocker, x.Blocker)
				if tc.blocker == protocol.ExplanationScopeNode {
					node := tc.toNode
					if node == "" {
						node = testNodeA
					}
					require.Equal(t, node, blocking[0].Key)
				}
			}
			require.NoError(t, x.Validate())
			withObject := query
			if withObject.Object == nil {
				objectID := testObjectID
				objectType := protocol.DecisionObjectTypeMessage
				if withObject.Plane == protocol.PlaneArtifact {
					objectType = protocol.DecisionObjectTypeArtifact
					objectID = "art_1"
				}
				withObject.Object = &Object{Type: objectType, ID: protocol.OpaqueID(objectID)}
			}
			again, _, _, err := engine.evaluate(withObject)
			require.NoError(t, err)
			require.Equal(t, v.outcome, again.outcome, "object changes no outcome")
			require.Equal(t, v.reason, again.reason, "object changes no reason")
			rec, err := recordOf(testDecisionID(index), withObject, v, eff, vers, x, testEngineTime)
			require.NoError(t, err)
			_, err = newDecision(rec, v.class, v.line)
			require.NoError(t, err)
		})
	}
}

// TestExplainValues checks values and notes in isolation.
func TestExplainValues(t *testing.T) {
	explain := func(t *testing.T, engine *Engine, query Query) protocol.Explanation {
		t.Helper()
		x, err := engine.Explain(query)
		require.NoError(t, err)
		return x
	}
	noteOf := func(t *testing.T, x protocol.Explanation) string {
		t.Helper()
		for _, line := range x.Lines {
			if line.Blocking {
				return blockNote(x, line, testEngineTime, time.UTC)
			}
		}
		return ""
	}
	lineByScope := func(t *testing.T, x protocol.Explanation, scope protocol.ExplanationScope) protocol.ExplanationLine {
		t.Helper()
		for _, line := range x.Lines {
			if line.Scope == scope {
				return line
			}
		}
		t.Fatalf("no %s line", scope)
		return protocol.ExplanationLine{}
	}

	t.Run("draining to off", func(t *testing.T) {
		row := testNodeRoot(testControlID(30), testNodeA, protocol.ChannelModeDraining)
		row.ThenMode = protocol.ControlModeOff
		engine := testEngine(t, row)
		x := explain(t, engine, testDeliveryQuery())
		require.Equal(t, "DRAINING → OFF", lineByScope(t, x, protocol.ExplanationScopeNode).Value)
	})

	t.Run("step tokens", func(t *testing.T) {
		engine := testEngine(t)
		one := testDeliveryQuery()
		one.Loop = goldenLoop()
		one.Loop.Mode = protocol.LoopModeStep
		one.Loop.Counters.StepTokens = 1
		require.Contains(t, lineByScope(t, explain(t, engine, one), protocol.ExplanationScopeLoop).Value, "· 1 token")
		two := testDeliveryQuery()
		two.Loop = goldenLoop()
		two.Loop.Mode = protocol.LoopModeStep
		two.Loop.Counters.StepTokens = 2
		require.Contains(t, lineByScope(t, explain(t, engine, two), protocol.ExplanationScopeLoop).Value, "· 2 tokens")
	})

	t.Run("local override", func(t *testing.T) {
		engine := goldenEngine(t)
		query := goldenQuery()
		query.Loop.Mode = protocol.LoopModeAuto
		applyStored(t, engine, testLoopRoot(testControlID(31), testLoopID, protocol.LoopModePaused))
		x := explain(t, engine, query)
		require.Contains(t, lineByScope(t, x, protocol.ExplanationScopeLoop).Value, " / local PAUSED")
	})

	t.Run("artifact suffixes", func(t *testing.T) {
		engine := goldenEngine(t)
		query := goldenQuery()
		query.Plane = protocol.PlaneArtifact
		query.Object = &Object{Type: protocol.DecisionObjectTypeArtifact, ID: "art_1"}
		query.Loop.Counters.ArtifactCount = 1
		x := explain(t, engine, query)
		rendered := Render(x, testEngineTime, time.UTC)
		require.Contains(t, rendered, "Link agent-1 → agent-2 (artifact)")
		require.Contains(t, lineByScope(t, x, protocol.ExplanationScopeLoop).Value, "· 1/3 artifacts")
	})

	t.Run("short loop id", func(t *testing.T) {
		require.Equal(t, "loop_01JB…", shortLoopID(testLoopID))
		require.Equal(t, "loop_12345", shortLoopID("loop_12345"))
	})

	t.Run("placeholders", func(t *testing.T) {
		engine := testEngine(t)
		x := explain(t, engine, testDeliveryQuery())
		for _, line := range x.Lines {
			require.NotEqual(t, protocol.ExplanationScopeThread, line.Scope)
			require.NotEqual(t, protocol.ExplanationScopeLoop, line.Scope)
			require.NotEqual(t, protocol.ExplanationScopeObject, line.Scope)
		}
		rendered := Render(x, testEngineTime, time.UTC)
		require.Contains(t, rendered, "Thread                         —\n")
		require.Contains(t, rendered, "Loop                           —\n")
		require.Contains(t, rendered, "Object hold / approval         —\n")
	})

	t.Run("expired row", func(t *testing.T) {
		link := testLinkRoot(testControlID(32), testAgent1, testAgent2, protocol.PlaneMessage, protocol.LinkModeHold)
		link.ExpiresAt = testEngineTime.Add(-time.Minute)
		engine := testEngine(t, link)
		x := explain(t, engine, testDeliveryQuery())
		require.Equal(t, "HOLD (expired)", lineByScope(t, x, protocol.ExplanationScopeLink).Value)
		require.Equal(t, "operator · expired 13:59", noteOf(t, x))
	})

	t.Run("expiry notes", func(t *testing.T) {
		link := testLinkRoot(testControlID(33), testAgent1, testAgent2, protocol.PlaneMessage, protocol.LinkModeHold)
		link.Reason = "session"
		link.ExpiresAt = testEngineTime.Add(10 * time.Minute)
		engine := testEngine(t, link)
		require.Equal(t, `operator · "session" · expires 14:10`, noteOf(t, explain(t, engine, testDeliveryQuery())))

		far := testLinkRoot(testControlID(34), testAgent1, testAgent2, protocol.PlaneMessage, protocol.LinkModeHold)
		far.Reason = "session"
		far.ExpiresAt = time.Date(2026, time.October, 29, 9, 0, 0, 0, time.UTC)
		engine = testEngine(t, far)
		require.Equal(t, `operator · "session" · expires 2026-10-29 09:00`, noteOf(t, explain(t, engine, testDeliveryQuery())))
	})

	t.Run("quoted reason", func(t *testing.T) {
		link := testLinkRoot(testControlID(35), testAgent1, testAgent2, protocol.PlaneMessage, protocol.LinkModeHold)
		link.Reason = `say "hi"`
		engine := testEngine(t, link)
		require.Equal(t, `operator · "say \"hi\""`, noteOf(t, explain(t, engine, testDeliveryQuery())))
	})
}

// TestExplainMarksRestoreControl requires the loop line to carry the
// expired row when its restore is the winning restriction.
func TestExplainMarksRestoreControl(t *testing.T) {
	t.Parallel()

	loopID := protocol.LoopID("loop_01JB0000000000000000000004")
	row := testLoopRoot(testControlID(41), string(loopID), protocol.LoopModePaused)
	row.ExpiresAt = testEngineTime.Add(-time.Minute)
	row.Restore = testSnapshot(protocol.ControlModeStopped)
	e := testEngine(t, row)
	q := testQuery()
	q.Kind = KindReply
	q.Loop = testLoop()
	q.Loop.LoopID = loopID
	q.Loop.Mode = protocol.LoopModePaused
	x, err := e.Explain(q)
	require.NoError(t, err)
	require.NoError(t, x.Validate())
	for _, line := range x.Lines {
		if line.Scope == protocol.ExplanationScopeLoop {
			require.True(t, line.Blocking)
			require.Equal(t, row.ControlID, line.ControlID)
		}
	}
}

// TestExplainMarksWinningNode requires the blocker to be the node the
// evaluation picked by order, not a reconstruction from current modes.
func TestExplainMarksWinningNode(t *testing.T) {
	t.Parallel()

	t.Run("first expired restore wins", func(t *testing.T) {
		t.Parallel()
		first := testNodeRoot(testControlID(42), testNodeA, protocol.ChannelModeActive)
		first.ExpiresAt = testEngineTime.Add(-time.Minute)
		first.Restore = testSnapshot(protocol.ControlModeOff)
		second := testNodeRoot(testControlID(43), testNodeB, protocol.ChannelModeOff)
		e := testEngine(t, first, second)
		q := testQuery()
		q.ToNode = testNodeB
		x, err := e.Explain(q)
		require.NoError(t, err)
		require.Equal(t, testNodeA, explainBlockerKey(t, x))
	})

	t.Run("second expired restore wins", func(t *testing.T) {
		t.Parallel()
		first := testNodeRoot(testControlID(42), testNodeA, protocol.ChannelModeActive)
		first.ExpiresAt = testEngineTime.Add(-time.Minute)
		first.Restore = testSnapshot(protocol.ControlModeActive)
		second := testNodeRoot(testControlID(43), testNodeB, protocol.ChannelModeActive)
		second.ExpiresAt = testEngineTime.Add(-time.Minute)
		second.Restore = testSnapshot(protocol.ControlModeOff)
		e := testEngine(t, first, second)
		q := testQuery()
		q.ToNode = testNodeB
		x, err := e.Explain(q)
		require.NoError(t, err)
		require.Equal(t, testNodeB, explainBlockerKey(t, x))
	})
}

// explainBlockerKey returns the key of the single blocking line.
func explainBlockerKey(t *testing.T, x protocol.Explanation) string {
	t.Helper()
	require.Equal(t, Deny, x.Outcome)
	require.Equal(t, protocol.DecisionReasonNodeOff, x.Reason)
	var keys []string
	for _, line := range x.Lines {
		if line.Blocking {
			keys = append(keys, line.Key)
		}
	}
	require.Len(t, keys, 1)
	return keys[0]
}

// TestAllowRecordKeepsMatched requires an ALLOW record to retain the
// consulted control ids in order.
func TestAllowRecordKeepsMatched(t *testing.T) {
	t.Parallel()

	global := testEngineRoot(testControlID(51))
	node := testNodeRoot(testControlID(52), testNodeA, protocol.ChannelModeActive)
	e := testEngine(t, global, node)
	q := testDeliveryQuery()
	q.ToDeliverable = true
	q.Object = &Object{Type: protocol.DecisionObjectTypeMessage, ID: testObjectID}
	v, eff, vers, err := e.evaluate(q)
	require.NoError(t, err)
	require.Equal(t, Allow, v.outcome)
	x, err := e.Explain(q)
	require.NoError(t, err)
	rec, err := recordOf(testDecisionID(51), q, v, eff, vers, x, testEngineTime)
	require.NoError(t, err)
	require.Equal(t, []protocol.ControlID{global.ControlID, node.ControlID}, rec.MatchedControlIDs)
}

// TestExplainFrozenWinner requires the loop line to keep the evaluated
// winner when the clock crosses the replica's expiry before the lines are
// built: the lines come from the verdict alone.
func TestExplainFrozenWinner(t *testing.T) {
	t.Parallel()

	engine, err := New(Options{Clock: testClock{now: testEngineTime}})
	require.NoError(t, err)
	row := testLoopRoot(testControlID(60), testLoopID, protocol.LoopModePaused)
	require.NoError(t, engine.load([]protocol.ChannelControl{row}))
	query := testDeliveryQuery()
	query.Loop = testLoop()
	query.Loop.Mode = protocol.LoopModeAuto
	query.Loop.ExpiresAt = testEngineTime.Add(time.Minute)
	v, eff, _, err := engine.evaluate(query)
	require.NoError(t, err)
	require.Equal(t, Hold, v.outcome)
	require.Equal(t, protocol.DecisionReasonLoopPaused, v.reason)
	engine.clock = testClock{now: testEngineTime.Add(2 * time.Minute)}
	x := engine.explainResult(query, v, eff)
	require.NoError(t, x.Validate())
	require.Equal(t, Hold, x.Outcome)
	require.Equal(t, protocol.DecisionReasonLoopPaused, x.Reason)
	var blocking []protocol.ExplanationLine
	for _, line := range x.Lines {
		if line.Blocking {
			blocking = append(blocking, line)
		}
	}
	require.Len(t, blocking, 1)
	require.Equal(t, protocol.ExplanationScopeLoop, blocking[0].Scope)
	require.Equal(t, row.ControlID, blocking[0].ControlID)
	require.Equal(t, protocol.ActorTypeHuman, blocking[0].ActorType)
	require.Equal(t, testOperator, blocking[0].ActorID)
	require.Nil(t, blocking[0].Version)
}
