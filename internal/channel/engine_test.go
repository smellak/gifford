package channel

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

// testEngineTime is the fixed now of the engine tests.
var testEngineTime = time.Date(2026, time.October, 28, 14, 0, 0, 0, time.UTC)

// testClock is a fixed clock.
type testClock struct{ now time.Time }

// Now returns the fixed time.
func (c testClock) Now() time.Time { return c.now }

// testControlID builds a deterministic control id.
func testControlID(n int) protocol.ControlID {
	return protocol.ControlID(fmt.Sprintf("ctl_%026d", n))
}

// testEngineRoot builds a valid global ACTIVE root, version 1.
func testEngineRoot(id protocol.ControlID) protocol.ChannelControl {
	return protocol.ChannelControl{
		ControlID: id,
		ScopeType: protocol.ScopeTypeGlobal,
		ScopeKey:  "*",
		Plane:     protocol.PlaneAll,
		Mode:      protocol.ControlModeActive,
		ActorType: protocol.ActorTypeHuman,
		CreatedBy: testOperator,
		CreatedAt: testControlTime,
		UpdatedAt: testControlTime,
		Version:   1,
	}
}

// testNodeRoot builds a valid node root, version 1.
func testNodeRoot(id protocol.ControlID, node string, mode protocol.ChannelMode) protocol.ChannelControl {
	root := testEngineRoot(id)
	root.ScopeType = protocol.ScopeTypeNode
	root.ScopeKey = node
	root.Mode = protocol.ControlMode(mode)
	return root
}

// testBool returns a pointer to its argument.
func testBool(value bool) *bool { return &value }

// testAgentRoot builds a valid agent root with only TX set, version 1.
func testAgentRoot(id protocol.ControlID, agent string, tx bool) protocol.ChannelControl {
	root := testEngineRoot(id)
	root.ScopeType = protocol.ScopeTypeAgent
	root.ScopeKey = agent
	root.Mode = ""
	root.TxEnabled = testBool(tx)
	return root
}

// testLinkRoot builds a valid link root, version 1.
func testLinkRoot(id protocol.ControlID, from, to string, plane protocol.Plane, mode protocol.LinkMode) protocol.ChannelControl {
	root := testEngineRoot(id)
	root.ScopeType = protocol.ScopeTypeLink
	root.ScopeKey = linkKey(protocol.AgentID(from), protocol.AgentID(to))
	root.Plane = plane
	root.Mode = protocol.ControlMode(mode)
	root.FromAgent = protocol.AgentID(from)
	root.ToAgent = protocol.AgentID(to)
	return root
}

// testThreadRoot builds a valid thread root, version 1.
func testThreadRoot(id protocol.ControlID, thread string, mode protocol.LinkMode) protocol.ChannelControl {
	root := testEngineRoot(id)
	root.ScopeType = protocol.ScopeTypeThread
	root.ScopeKey = thread
	root.Mode = protocol.ControlMode(mode)
	return root
}

// testLoopRoot builds a valid loop override root, version 1.
func testLoopRoot(id protocol.ControlID, loop string, mode protocol.LoopMode) protocol.ChannelControl {
	root := testEngineRoot(id)
	root.ScopeType = protocol.ScopeTypeLoop
	root.ScopeKey = loop
	root.Mode = protocol.ControlMode(mode)
	return root
}

// testQuery builds a valid admission root query.
func testQuery() Query {
	return Query{
		Stage:    protocol.DecisionStageAdmission,
		Plane:    protocol.PlaneMessage,
		From:     testAgent1,
		FromNode: testNodeA,
		To:       testAgent2,
		ToNode:   testNodeA,
		Kind:     KindRoot,
	}
}

// testDeliveryQuery builds a valid delivery query without thread or loop.
func testDeliveryQuery() Query {
	query := testQuery()
	query.Stage = protocol.DecisionStageDelivery
	query.Kind = ""
	return query
}

// testLoop builds the base AUTO 7/12 loop of the precedence fixture.
func testLoop() *protocol.Loop {
	return &protocol.Loop{
		LoopID:          testLoopID,
		ThreadID:        testThreadID,
		LoopGeneration:  1,
		Mode:            protocol.LoopModeAuto,
		LastSenderAgent: testAgent2,
		ControlVersion:  1,
		Budget: protocol.LoopBudget{
			MaxTurns:             12,
			MaxMessages:          24,
			MaxHops:              24,
			MaxArtifactCount:     3,
			MaxArtifactBytes:     104857600,
			MaxMessagesPerMinute: 12,
		},
		Counters: protocol.LoopCounters{
			TurnCount:    7,
			MessageCount: 15,
		},
		ExpiresAt: testEngineTime.Add(5 * time.Minute),
	}
}

// testEngine builds a loaded engine or fails the test.
func testEngine(t *testing.T, rows ...protocol.ChannelControl) *Engine {
	t.Helper()
	engine, err := New(Options{Clock: testClock{now: testEngineTime}})
	require.NoError(t, err)
	require.NoError(t, engine.load(rows))
	return engine
}

// TestQueryValidation checks every invalid query shape.
func TestQueryValidation(t *testing.T) {
	validLoop := testLoop()
	validObject := &Object{Type: protocol.DecisionObjectTypeMessage, ID: testObjectID}
	cases := []struct {
		name   string
		mutate func(*Query)
		valid  bool
	}{
		{"valid admission root", func(*Query) {}, true},
		{"bad stage", func(q *Query) { q.Stage = "revalidation" }, false},
		{"bad plane", func(q *Query) { q.Plane = protocol.PlaneAll }, false},
		{"bad from", func(q *Query) { q.From = "!!" }, false},
		{"bad to", func(q *Query) { q.To = "!!" }, false},
		{"from equals to", func(q *Query) { q.To = q.From }, false},
		{"bad from node", func(q *Query) { q.FromNode = "!!" }, false},
		{"bad to node", func(q *Query) { q.ToNode = "!!" }, false},
		{"bad thread", func(q *Query) { q.ThreadID = "!!" }, false},
		{"bad kind", func(q *Query) { q.Kind = "twig" }, false},
		{"continuation with root", func(q *Query) { q.Stage = protocol.DecisionStageContinuation }, false},
		{"continuation without loop", func(q *Query) {
			q.Stage = protocol.DecisionStageContinuation
			q.Kind = KindReply
		}, false},
		{"root with loop", func(q *Query) { q.Loop = validLoop }, false},
		{"loop bad id", func(q *Query) {
			q.Kind = KindReply
			loop := testLoop()
			loop.LoopID = "!!"
			q.Loop = loop
		}, false},
		{"loop generation zero", func(q *Query) {
			q.Kind = KindReply
			loop := testLoop()
			loop.LoopGeneration = 0
			q.Loop = loop
		}, false},
		{"loop bad mode", func(q *Query) {
			q.Kind = KindReply
			loop := testLoop()
			loop.Mode = "WOBBLE"
			q.Loop = loop
		}, false},
		{"loop zero expiry", func(q *Query) {
			q.Kind = KindReply
			loop := testLoop()
			loop.ExpiresAt = time.Time{}
			q.Loop = loop
		}, false},
		{"loop negative counter", func(q *Query) {
			q.Kind = KindReply
			loop := testLoop()
			loop.Counters.TurnCount = -1
			q.Loop = loop
		}, false},
		{"loop negative limit", func(q *Query) {
			q.Kind = KindReply
			loop := testLoop()
			loop.Budget.MaxTurns = -1
			q.Loop = loop
		}, false},
		{"loop bad sender", func(q *Query) {
			q.Kind = KindReply
			loop := testLoop()
			loop.LastSenderAgent = "!!"
			q.Loop = loop
		}, false},
		{"thread differs from loop", func(q *Query) {
			q.Kind = KindReply
			q.ThreadID = "thread-9"
			q.Loop = validLoop
		}, false},
		{"object type mismatches plane", func(q *Query) {
			object := *validObject
			object.Type = protocol.DecisionObjectTypeArtifact
			q.Object = &object
		}, false},
		{"object bad id", func(q *Query) {
			object := *validObject
			object.ID = "!!"
			q.Object = &object
		}, false},
		{"held before delivery", func(q *Query) {
			object := *validObject
			object.Held = true
			q.Object = &object
		}, false},
		{"pending before delivery", func(q *Query) {
			object := *validObject
			object.ApprovalPending = true
			q.Object = &object
		}, false},
		{"negative hops", func(q *Query) { q.HopCount = -1 }, false},
		{"negative bytes", func(q *Query) { q.ArtifactBytes = -1 }, false},
		{"valid reply with loop", func(q *Query) {
			q.Kind = KindReply
			q.Loop = validLoop
		}, true},
		{"valid delivery with object", func(q *Query) {
			q.Stage = protocol.DecisionStageDelivery
			q.Kind = ""
			object := *validObject
			object.Held = true
			q.Object = &object
		}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			query := testQuery()
			tc.mutate(&query)
			err := query.Validate()
			if tc.valid {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.ErrorIs(t, err, ErrInvalidQuery)
		})
	}
}

// TestEffectiveDefaults resolves a query against an empty cache.
func TestEffectiveDefaults(t *testing.T) {
	engine := testEngine(t)
	query := testDeliveryQuery()
	query.ToDeliverable = true

	eff, err := engine.Effective(query)
	require.NoError(t, err)
	require.Equal(t, protocol.ControlMode(""), eff.Global.Mode)
	require.Equal(t, ControlRef{}, eff.Global.ControlRef)
	require.Len(t, eff.Nodes, 1)
	require.Equal(t, protocol.NodeID(testNodeA), eff.Nodes[0].NodeID)
	for _, agent := range []AgentLevel{eff.Sender, eff.Receiver} {
		require.True(t, agent.TX)
		require.True(t, agent.RX)
		require.True(t, agent.AutoDelivery)
		require.True(t, agent.AutoContinue)
		require.Equal(t, ControlRef{}, agent.ControlRef)
	}
	require.Equal(t, protocol.ControlMode(""), eff.Link.Mode)
	require.Nil(t, eff.Thread)
	require.Nil(t, eff.Loop)

	// Every resolution is freshly built: mutating one changes no other.
	eff.Nodes[0].Mode = protocol.ControlMode(protocol.ChannelModeOff)
	again, err := engine.Effective(query)
	require.NoError(t, err)
	require.Equal(t, protocol.ControlMode(""), again.Nodes[0].Mode)

	verdict, _, versions, err := engine.evaluate(query)
	require.NoError(t, err)
	require.Equal(t, Allow, verdict.outcome)
	require.Equal(t, protocol.DecisionReasonAllowed, verdict.reason)
	require.Equal(t, protocol.ControlVersions{
		testGlobalKey():          0,
		testNodeKey(testNodeA):   0,
		testAgentKey(testAgent1): 0,
		testAgentKey(testAgent2): 0,
		testLinkVersionKey:       0,
	}, versions)

	projected := effectiveControl(again, query)
	require.Equal(t, protocol.EffectiveControl{
		GlobalMode:       protocol.ChannelModeActive,
		FromNodeMode:     protocol.ChannelModeActive,
		ToNodeMode:       protocol.ChannelModeActive,
		FromTX:           true,
		FromAutoContinue: true,
		ToRX:             true,
		ToAutoDelivery:   true,
		ToDeliverable:    true,
		LinkMode:         protocol.LinkModeAllow,
	}, projected)
}

// TestEngineCacheVersions drives load, apply, remove and invalidate.
func TestEngineCacheVersions(t *testing.T) {
	globalKey := cacheKey{scope: protocol.ScopeTypeGlobal, key: "*", plane: protocol.PlaneAll}

	t.Run("nil clock", func(t *testing.T) {
		_, err := New(Options{})
		require.ErrorIs(t, err, ErrInvalidOptions)
	})

	t.Run("invalid load fails closed", func(t *testing.T) {
		engine, err := New(Options{Clock: testClock{now: testEngineTime}})
		require.NoError(t, err)
		bad := testEngineRoot(testControlID(1))
		bad.Version = 0
		err = engine.load([]protocol.ChannelControl{bad})
		require.Error(t, err)
		require.ErrorIs(t, err, ErrInvalidControl)
		_, err = engine.Effective(testDeliveryQuery())
		require.ErrorIs(t, err, ErrControlUnavailable)
	})

	t.Run("duplicate load fails closed", func(t *testing.T) {
		engine, err := New(Options{Clock: testClock{now: testEngineTime}})
		require.NoError(t, err)
		dupe := testEngineRoot(testControlID(2))
		err = engine.load([]protocol.ChannelControl{testEngineRoot(testControlID(1)), dupe, dupe})
		require.Error(t, err)
		require.ErrorIs(t, err, ErrInvalidControl)
		_, err = engine.Effective(testDeliveryQuery())
		require.ErrorIs(t, err, ErrControlUnavailable)
	})

	t.Run("apply remove invalidate", func(t *testing.T) {
		engine := testEngine(t, testEngineRoot(testControlID(1)))

		stored, err := engine.apply(testEngineRoot(testControlID(1)))
		require.NoError(t, err)
		require.False(t, stored)

		older := testEngineRoot(testControlID(1))
		older.Version = 0
		_, err = engine.apply(older)
		require.Error(t, err)

		newer := testEngineRoot(testControlID(9))
		newer.Version = 2
		stored, err = engine.apply(newer)
		require.NoError(t, err)
		require.True(t, stored)
		eff, err := engine.Effective(testDeliveryQuery())
		require.NoError(t, err)
		require.Equal(t, int64(2), eff.Global.Version)
		require.Equal(t, testControlID(9), eff.Global.ControlID)

		require.False(t, engine.remove(globalKey, 1))
		require.True(t, engine.remove(globalKey, 3))
		eff, err = engine.Effective(testDeliveryQuery())
		require.NoError(t, err)
		require.Equal(t, ControlRef{}, eff.Global.ControlRef)

		revive := testEngineRoot(testControlID(10))
		revive.Version = 2
		stored, err = engine.apply(revive)
		require.NoError(t, err)
		require.False(t, stored)

		engine.invalidate(globalKey, 2)
		_, err = engine.Effective(testDeliveryQuery())
		require.NoError(t, err)

		engine.invalidate(globalKey, 5)
		_, err = engine.Effective(testDeliveryQuery())
		require.ErrorIs(t, err, ErrControlUnavailable)

		restored := testEngineRoot(testControlID(11))
		restored.Version = 5
		stored, err = engine.apply(restored)
		require.NoError(t, err)
		require.True(t, stored)
		eff, err = engine.Effective(testDeliveryQuery())
		require.NoError(t, err)
		require.Equal(t, int64(5), eff.Global.Version)
	})
}

// TestEngineConcurrent hammers the engine from parallel goroutines; the
// race detector does the asserting.
func TestEngineConcurrent(t *testing.T) {
	rows := []protocol.ChannelControl{
		testEngineRoot(testControlID(1)),
		testNodeRoot(testControlID(2), testNodeA, protocol.ChannelModeActive),
		testAgentRoot(testControlID(3), testAgent1, true),
		testLinkRoot(testControlID(4), testAgent1, testAgent2, protocol.PlaneMessage, protocol.LinkModeAllow),
	}
	engine := testEngine(t, rows...)

	const workers = 8
	const rounds = 50
	var group sync.WaitGroup
	for worker := range workers {
		group.Add(1)
		go func(worker int) {
			defer group.Done()
			for round := range rounds {
				query := testDeliveryQuery()
				if (worker+round)%2 == 0 {
					query = testQuery()
					query.Kind = KindReply
					query.Loop = testLoop()
				}
				if _, err := engine.Effective(query); err != nil {
					t.Error(err)
					return
				}
				if _, _, _, err := engine.evaluate(query); err != nil {
					t.Error(err)
					return
				}
			}
		}(worker)
	}
	for version := int64(2); version <= rounds; version++ {
		next := testEngineRoot(testControlID(100 + int(version)))
		next.Version = version
		if _, err := engine.apply(next); err != nil {
			t.Fatal(err)
		}
	}
	group.Wait()

	eff, err := engine.Effective(testDeliveryQuery())
	require.NoError(t, err)
	require.Equal(t, int64(rounds), eff.Global.Version)
}

// TestControlVersions checks the link_hold golden vector of §7.2.
func TestControlVersions(t *testing.T) {
	rows := []protocol.ChannelControl{
		testEngineRoot(testControlID(1)),
		testNodeRoot(testControlID(2), testNodeA, protocol.ChannelModeActive),
		testLinkRoot(testControlID(3), testAgent1, testAgent2, protocol.PlaneMessage, protocol.LinkModeHold),
	}
	rows[0].Version = 18
	rows[1].Version = 3
	rows[2].Version = 7
	engine := testEngine(t, rows...)

	loop := testLoop()
	loop.ControlVersion = 5
	query := testDeliveryQuery()
	query.ThreadID = testThreadID
	query.Loop = loop

	verdict, _, versions, err := engine.evaluate(query)
	require.NoError(t, err)
	require.Equal(t, Hold, verdict.outcome)
	require.Equal(t, protocol.DecisionReasonLinkHold, verdict.reason)
	require.Equal(t, protocol.ControlVersions{
		testGlobalKey():              18,
		testNodeKey(testNodeA):       3,
		testAgentKey(testAgent1):     0,
		testAgentKey(testAgent2):     0,
		testLinkVersionKey:           7,
		testThreadVersionKey:         0,
		testLoopVersionKey:           5,
		testLoopGenerationVersionKey: 1,
		testLoopOverrideVersionKey:   0,
	}, versions)

	t.Run("no thread or loop", func(t *testing.T) {
		_, _, versions, err := engine.evaluate(testDeliveryQuery())
		require.NoError(t, err)
		require.Equal(t, protocol.ControlVersions{
			testGlobalKey():          18,
			testNodeKey(testNodeA):   3,
			testAgentKey(testAgent1): 0,
			testAgentKey(testAgent2): 0,
			testLinkVersionKey:       7,
		}, versions)
	})

	t.Run("second node", func(t *testing.T) {
		query := testDeliveryQuery()
		query.ToNode = testNodeB
		_, _, versions, err := engine.evaluate(query)
		require.NoError(t, err)
		require.Equal(t, int64(3), versions[testNodeKey(testNodeA)])
		require.Equal(t, int64(0), versions[testNodeKey(testNodeB)])
	})

	t.Run("never loaded", func(t *testing.T) {
		fresh, err := New(Options{Clock: testClock{now: testEngineTime}})
		require.NoError(t, err)
		verdict, _, versions, err := fresh.evaluate(testDeliveryQuery())
		require.NoError(t, err)
		require.Equal(t, Hold, verdict.outcome)
		require.Equal(t, protocol.DecisionReasonControlUnavailable, verdict.reason)
		require.Equal(t, protocol.ControlVersions{
			testGlobalKey():          0,
			testNodeKey(testNodeA):   0,
			testAgentKey(testAgent1): 0,
			testAgentKey(testAgent2): 0,
			testLinkVersionKey:       0,
		}, versions)
	})
}

// TestInvalidationKeepsHighestVersion requires invalidations to keep the
// highest pending version per key: a late lower invalidation never lowers
// it, and applying that lower version leaves the key unavailable.
func TestInvalidationKeepsHighestVersion(t *testing.T) {
	t.Parallel()

	row := testEngineRoot(testControlID(1))
	engine := testEngine(t, row)
	key := cacheKey{scope: protocol.ScopeTypeGlobal, key: "*", plane: protocol.PlaneAll}
	engine.invalidate(key, 10)
	engine.invalidate(key, 5)
	row.Version = 5
	_, err := engine.apply(row)
	require.NoError(t, err)
	_, err = engine.Effective(testQuery())
	require.ErrorIs(t, err, ErrControlUnavailable)
}

// TestFailedLoadLeavesEngineUnavailable requires a failed load to leave
// the engine not loaded, even when the previous load was valid.
func TestFailedLoadLeavesEngineUnavailable(t *testing.T) {
	t.Parallel()

	engine := testEngine(t, testEngineRoot(testControlID(1)))
	require.ErrorIs(t, engine.load([]protocol.ChannelControl{{}}), ErrInvalidControl)
	_, err := engine.Effective(testQuery())
	require.ErrorIs(t, err, ErrControlUnavailable)
}
