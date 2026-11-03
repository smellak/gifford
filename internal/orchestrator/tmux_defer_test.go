package orchestrator

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/daemon"
	"github.com/smellak/gifford/internal/detect"
	"github.com/smellak/gifford/internal/procguard"
	"github.com/smellak/gifford/internal/protocol"
)

func advanceDeferClock(fixture *executorFixture, duration time.Duration) {
	fixture.clock.mu.Lock()
	fixture.clock.now = fixture.clock.now.Add(duration)
	fixture.clock.mu.Unlock()
}

func deferredDecisionCount(t *testing.T, fixture *executorFixture, candidate daemon.QueueDelivery) int {
	t.Helper()
	var count int
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM orchestrator_decisions WHERE message_id = ? AND agent_id = ?",
		candidate.MessageID, candidate.AgentID).Scan(&count))
	return count
}

func warmTmuxDeferFixture(t *testing.T) (*executorFixture, *tmuxTransportStub, *tmuxRegistryStub) {
	t.Helper()
	fixture, transport, registry := newTmuxExecutorFixture(t)
	require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), fixture.seedDelivery(t, 150)))
	require.Zero(t, transport.writes)
	return fixture, transport, registry
}

func TestTmuxSustainedDeferCoalescesAndGuardChangeDeliversNextPass(t *testing.T) {
	t.Parallel()
	fixture, transport, registry := warmTmuxDeferFixture(t)
	candidate := fixture.seedDelivery(t, 151)
	transport.initial = []byte("$ human draft")
	const passes = 160
	for range passes {
		require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), candidate))
		advanceDeferClock(fixture, 550*time.Millisecond)
	}
	require.Equal(t, 1, deferredDecisionCount(t, fixture, candidate))
	require.Zero(t, transport.writes)
	require.Equal(t, passes+1, registry.verified, "identity and fresh observations are not backed off")
	var guardWrites int
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
		"SELECT version FROM input_guards WHERE session_id = ?", executorTestSession).Scan(&guardWrites))
	require.Greater(t, guardWrites, 2, "bounded backoff still performs periodic full evaluations")
	require.Less(t, guardWrites, 20, "unchanged passes do not update input guards and control decisions")

	// The deadline is still in the future; emptying the input must wake this
	// message immediately, not after the remaining backoff interval.
	deferred := fixture.executor.tmux.deferred[candidate.AgentID]
	require.True(t, fixture.clock.Now().Before(deferred.next))
	transport.initial = []byte("$ ")
	require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), candidate))
	require.Equal(t, 1, transport.writes)
	require.Equal(t, 2, deferredDecisionCount(t, fixture, candidate))
	require.Empty(t, fixture.executor.tmux.deferred)
}

func TestTmuxPausedUnmeasuredAgentCoalescesForOneMinute(t *testing.T) {
	t.Parallel()
	fixture, transport, registry := newTmuxExecutorFixture(t)
	fixture.executor.tmux.detectorTable = detect.Table{detect.HarnessCodex: {}}
	candidate := fixture.seedDelivery(t, 156)
	off := false
	paused, err := fixture.controller.SetAgent(t.Context(), protocol.SetAgentChannelParams{
		AgentID: candidate.AgentID, AutoDelivery: &off, ExpectedVersion: 0,
	}, channel.Actor{Type: protocol.ActorTypeHuman, ID: executorTestOperator})
	require.NoError(t, err)
	require.False(t, paused.AutoDelivery)
	const interval = 550 * time.Millisecond
	var passes int
	for elapsed := time.Duration(0); elapsed < time.Minute; elapsed += interval {
		require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), candidate))
		advanceDeferClock(fixture, interval)
		passes++
	}
	require.Equal(t, 1, deferredDecisionCount(t, fixture, candidate))
	require.Zero(t, transport.writes)
	require.Equal(t, passes, registry.verified)
	var guardWrites int
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
		"SELECT version FROM input_guards WHERE session_id = ?", executorTestSession).Scan(&guardWrites))
	require.Greater(t, guardWrites, 1)
	require.Less(t, guardWrites, 15, "pause uses bounded backoff rather than full evaluation on every pass")
	guards := latestGuards(t, fixture.store.DB(), candidate.MessageID)
	for _, name := range []protocol.GuardName{
		protocol.GuardNameAdapterConnected, protocol.GuardNamePTYOutputQuiet,
		protocol.GuardNameHumanDraftClosed, protocol.GuardNameNoNewChildOrTool,
	} {
		require.Equal(t, protocol.GuardOutcomeFail, guardOutcome(guards, name))
	}
	t.Logf("paused for %s: %d passes, 1 decision, %d full evaluations, 0 writes",
		time.Duration(passes)*interval, passes, guardWrites)

	// Resuming must invalidate the wait immediately, even when the screen
	// remains unknown. It records the new control state but cannot deliver.
	require.True(t, fixture.clock.Now().Before(fixture.executor.tmux.deferred[candidate.AgentID].next))
	on := true
	_, err = fixture.controller.SetAgent(t.Context(), protocol.SetAgentChannelParams{
		AgentID: candidate.AgentID, AutoDelivery: &on, ExpectedVersion: paused.Version,
	}, channel.Actor{Type: protocol.ActorTypeHuman, ID: executorTestOperator})
	require.NoError(t, err)
	require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), candidate))
	require.Equal(t, 2, deferredDecisionCount(t, fixture, candidate))
	require.Zero(t, transport.writes)
}

func TestTmuxChangedFailedGuardSetRecordsEveryTransition(t *testing.T) {
	t.Parallel()
	fixture, transport, _ := warmTmuxDeferFixture(t)
	candidate := fixture.seedDelivery(t, 152)
	transport.initial = []byte("$ human draft")
	require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), candidate))
	transport.clients = []byte(fmt.Sprintf("%s\t%d\n", tmuxTestPane, fixture.clock.Now().Unix()))
	require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), candidate))
	require.Equal(t, 2, deferredDecisionCount(t, fixture, candidate))
	require.Equal(t, protocol.GuardOutcomeFail,
		guardOutcome(latestGuards(t, fixture.store.DB(), candidate.MessageID), protocol.GuardNameHumanActivityNotActive))
	transport.clients = nil
	require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), candidate))
	require.Equal(t, 3, deferredDecisionCount(t, fixture, candidate), "A -> B -> A is not one observation")
	require.Zero(t, transport.writes)
}

func TestTmuxDeferBackoffIsBoundedAndDedupSurvivesSchedulingReset(t *testing.T) {
	t.Parallel()
	fixture, transport, _ := warmTmuxDeferFixture(t)
	candidate := fixture.seedDelivery(t, 153)
	transport.initial = []byte("$ human draft")
	for _, expected := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 8 * time.Second} {
		require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), candidate))
		entry := fixture.executor.tmux.deferred[candidate.AgentID]
		require.Equal(t, expected, entry.delay)
		advanceDeferClock(fixture, expected)
	}
	fixture.executor.tmux.forgetDefer(candidate.AgentID)
	require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), candidate))
	require.Equal(t, 1, deferredDecisionCount(t, fixture, candidate), "coalescing is durable, not a cache hit")
	var firstObservation string
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
		"SELECT created_at FROM orchestrator_decisions WHERE message_id = ?", candidate.MessageID).Scan(&firstObservation))
	require.Contains(t, firstObservation, "18:30:00", "keep the first observation timestamp")
}

func TestTmuxControlTTLRestoreWakesDeferBeforeItsDeadline(t *testing.T) {
	t.Parallel()
	fixture, transport, _ := warmTmuxDeferFixture(t)
	candidate := fixture.seedDelivery(t, 154)
	off, ttl := false, int64(1500)
	_, err := fixture.controller.SetAgent(t.Context(), protocol.SetAgentChannelParams{
		AgentID: candidate.AgentID, AutoDelivery: &off, TTLMS: &ttl, ExpectedVersion: 0,
	}, channel.Actor{Type: protocol.ActorTypeHuman, ID: executorTestOperator})
	require.NoError(t, err)
	require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), candidate))
	advanceDeferClock(fixture, time.Second)
	require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), candidate))
	require.Zero(t, transport.writes)
	require.Equal(t, 1, deferredDecisionCount(t, fixture, candidate))
	advanceDeferClock(fixture, 500*time.Millisecond)
	require.True(t, fixture.clock.Now().Before(fixture.executor.tmux.deferred[candidate.AgentID].next))
	// The channel stays restrictive until its TTL worker restores the row.
	// Once restored, delivery must not wait for the scheduling deadline.
	expired, err := fixture.controller.Expire(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, expired)
	require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), candidate))
	require.Equal(t, 1, transport.writes, "control TTL expiry must not wait for the backoff")
}

func TestTmuxGuardNamesRequireAttachedModeEvidence(t *testing.T) {
	t.Parallel()
	fixture, transport, _ := warmTmuxDeferFixture(t)
	candidate := fixture.seedDelivery(t, 155)
	require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), candidate))
	require.Equal(t, 1, transport.writes, "no Proxy or native plugin is needed")
	guards := latestGuards(t, fixture.store.DB(), candidate.MessageID)
	for _, name := range []protocol.GuardName{
		protocol.GuardNameAdapterConnected, protocol.GuardNamePTYOutputQuiet, protocol.GuardNameNoNewChildOrTool,
	} {
		require.Equal(t, protocol.GuardOutcomePass, guardOutcome(guards, name))
	}

	// An unmeasured harness profile cannot prove foreground CLI, screen
	// idleness or the resting baseline. These failures are not "N/A".
	state := detect.Classify(detect.HarnessClaude, detect.Table{detect.HarnessClaude: {}}, []byte("❯ draft"), 80, 24, 100,
		[]detect.Process{{PID: 100, Pgrp: 100, TPGID: 100, State: 'S', Argv: []string{"claude"}}})
	rule := fixture.executor.tmux.screenRule(tmuxTestInstance, state, []byte("❯ draft"), fixture.clock.Now())
	guards = tmuxHardGuards(deliveryRecord{
		sessionState: protocol.SessionStateConnected,
		captureMode:  protocol.CaptureModeTmuxAttach, deliverable: true, expiresAt: fixture.clock.Now().Add(time.Hour),
	},
		state.IsCLI, rule, false, true, procguard.Uncertain, false, fixture.clock.Now())
	for _, name := range []protocol.GuardName{
		protocol.GuardNameAdapterConnected, protocol.GuardNamePTYOutputQuiet, protocol.GuardNameNoNewChildOrTool,
	} {
		require.Equal(t, protocol.GuardOutcomeFail, guardOutcome(guards, name))
	}
}

func TestDeferredGuardComparisonIsUnorderedButKeepsOutcomeAndReason(t *testing.T) {
	t.Parallel()
	draft := guard(protocol.GuardNameHumanDraftClosed, false, "draft open")
	busy := guard(protocol.GuardNameNoNewChildOrTool, false, "busy")
	require.True(t, sameDeferredGuards([]protocol.GuardResult{draft, busy}, []protocol.GuardResult{busy, draft}))
	require.False(t, sameDeferredGuards([]protocol.GuardResult{draft}, []protocol.GuardResult{busy}))
	changed := draft
	changed.Detail = "draft unknown"
	require.False(t, sameDeferredGuards([]protocol.GuardResult{draft}, []protocol.GuardResult{changed}))
}
