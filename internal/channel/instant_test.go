package channel

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

// advancingClock returns a later instant after every read.
type advancingClock struct {
	mu    sync.Mutex
	next  time.Time
	step  time.Duration
	reads int
}

// Now returns the current instant and advances the clock.
func (c *advancingClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.next
	c.next = c.next.Add(c.step)
	c.reads++
	return now
}

// readCount returns the number of clock reads.
func (c *advancingClock) readCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reads
}

// instantEngine builds a loaded engine using an advancing clock.
func instantEngine(t *testing.T, now time.Time, rows ...protocol.ChannelControl) (*Engine, *advancingClock) {
	t.Helper()
	clock := &advancingClock{next: now, step: 2 * time.Second}
	engine, err := New(Options{Clock: clock})
	require.NoError(t, err)
	require.NoError(t, engine.load(rows))
	return engine, clock
}

func TestEvaluationUsesOneInstant(t *testing.T) {
	now := testEngineTime
	expires := now.Add(time.Second)
	global := testEngineRoot(testControlID(71))
	global.ExpiresAt = expires
	link := testLinkRoot(testControlID(72), testAgent1, testAgent2, protocol.PlaneMessage, protocol.LinkModeAllow)
	link.ExpiresAt = expires

	t.Run("Effective keeps global and link coherent", func(t *testing.T) {
		engine, clock := instantEngine(t, now, global, link)
		eff, err := engine.Effective(testQuery())
		require.NoError(t, err)
		require.False(t, eff.Global.Expired)
		require.False(t, eff.Link.Expired)
		require.Equal(t, 1, clock.readCount())
	})

	t.Run("Explain reads the clock once", func(t *testing.T) {
		engine, clock := instantEngine(t, now, global, link)
		x, err := engine.Explain(testQuery())
		require.NoError(t, err)
		require.Equal(t, 1, clock.readCount())
		for _, line := range x.Lines {
			if line.Scope == protocol.ExplanationScopeGlobal || line.Scope == protocol.ExplanationScopeLink {
				require.NotContains(t, line.Value, "(expired)")
			}
		}
	})

	t.Run("loop shares the control instant", func(t *testing.T) {
		engine, clock := instantEngine(t, now, global)
		query := testQuery()
		query.Kind = KindReply
		query.Loop = testLoop()
		query.Loop.ExpiresAt = expires
		v, eff, _, err := engine.evaluate(query)
		require.NoError(t, err)
		require.Equal(t, Allow, v.outcome)
		require.False(t, eff.Global.Expired)
		require.Equal(t, 1, clock.readCount())
	})
}

func TestAgentRestoreUsesEvaluationInstant(t *testing.T) {
	now := testEngineTime

	t.Run("sender", func(t *testing.T) {
		row := testAgentRoot(testControlID(73), testAgent1, true)
		row.ExpiresAt = now
		row.Restore = &protocol.ControlSnapshot{
			TxEnabled: testBool(false),
			ActorType: protocol.ActorTypeSystem,
			CreatedBy: testOperator,
			ExpiresAt: now.Add(time.Second),
			Version:   4,
		}
		engine, clock := instantEngine(t, now, row)
		v, _, _, err := engine.evaluate(testQuery())
		require.NoError(t, err)
		require.Equal(t, protocol.DecisionReasonAgentTXDisabled, v.reason)
		require.False(t, v.winRef.Expired)
		require.Equal(t, 1, clock.readCount())
	})

	t.Run("receiver", func(t *testing.T) {
		row := testAgentRoot(testControlID(74), testAgent2, true)
		row.TxEnabled = nil
		row.RxEnabled = testBool(true)
		row.ExpiresAt = now
		row.Restore = &protocol.ControlSnapshot{
			RxEnabled: testBool(false),
			ActorType: protocol.ActorTypeSystem,
			CreatedBy: testOperator,
			ExpiresAt: now.Add(time.Second),
			Version:   4,
		}
		engine, clock := instantEngine(t, now, row)
		v, _, _, err := engine.evaluate(testQuery())
		require.NoError(t, err)
		require.Equal(t, protocol.DecisionReasonAgentRXDisabled, v.reason)
		require.False(t, v.winRef.Expired)
		require.Equal(t, 1, clock.readCount())
	})
}

func TestEvaluationClockBoundaries(t *testing.T) {
	t.Run("expiration equals now", func(t *testing.T) {
		row := testEngineRoot(testControlID(75))
		row.ExpiresAt = testEngineTime
		engine, clock := instantEngine(t, testEngineTime, row)
		eff, err := engine.Effective(testQuery())
		require.NoError(t, err)
		require.True(t, eff.Global.Expired)
		require.Equal(t, 1, clock.readCount())
	})

	t.Run("loop TTL equals now", func(t *testing.T) {
		engine, clock := instantEngine(t, testEngineTime)
		query := testQuery()
		query.Kind = KindReply
		query.Loop = testLoop()
		query.Loop.ExpiresAt = testEngineTime
		v, _, _, err := engine.evaluate(query)
		require.NoError(t, err)
		require.Equal(t, Deny, v.outcome)
		require.Equal(t, protocol.DecisionReasonLoopBudgetExhausted, v.reason)
		require.Equal(t, 1, clock.readCount())
	})

	t.Run("invalid queries do not read the clock", func(t *testing.T) {
		engine, clock := instantEngine(t, testEngineTime)
		badExplain := testQuery()
		badExplain.Stage = "invalid"
		_, err := engine.Explain(badExplain)
		require.ErrorIs(t, err, ErrInvalidQuery)

		badEffective := testQuery()
		badEffective.From = "!!"
		_, err = engine.Effective(badEffective)
		require.ErrorIs(t, err, ErrInvalidQuery)
		require.Zero(t, clock.readCount())
	})
}
