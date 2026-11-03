package orchestrator

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/detect"
	"github.com/smellak/gifford/internal/protocol"
)

// Each half of the rule fails for its own reason. An unread screen fails
// both: it proves neither a free box nor an idle agent.
func TestTmuxScreenRuleSeparatesBoxAndAgent(t *testing.T) {
	t.Parallel()
	runtime := &tmuxAttachRuntime{output: make(map[protocol.ULID]tmuxOutputSeen)}
	now := time.Date(2026, 11, 3, 21, 0, 0, 0, time.UTC)
	cases := []struct {
		name  string
		state detect.State
		want  tmuxScreenRule
	}{
		{"free", detect.State{Certain: true}, tmuxScreenRule{boxFree: true, agentQuiet: true}},
		{"unread", detect.State{}, tmuxScreenRule{
			boxDetail: tmuxBoxNotLocated, agentDetail: tmuxBoxNotLocated,
		}},
		{"draft", detect.State{Certain: true, BoxBusy: true}, tmuxScreenRule{
			boxDetail: tmuxBoxHoldsText, agentQuiet: true,
		}},
		{"busy", detect.State{Certain: true, Busy: true}, tmuxScreenRule{
			boxFree: true, agentDetail: tmuxBusyMark,
		}},
	}
	for _, tc := range cases {
		rule := runtime.screenRule(tmuxTestInstance, tc.state, []byte("$ "), now)
		require.Equal(t, tc.want, rule, tc.name)
		require.Equal(t, tc.want.boxFree && tc.want.agentQuiet, rule.pass(), tc.name)
	}
}

// The quiet window restarts whenever the visible text changes, while colors
// alone do not count as output.
func TestTmuxOutputQuietRestartsOnChangedText(t *testing.T) {
	t.Parallel()
	runtime := &tmuxAttachRuntime{
		quietWindow: 5 * time.Second, output: make(map[protocol.ULID]tmuxOutputSeen),
	}
	now := time.Date(2026, 11, 3, 21, 0, 0, 0, time.UTC)
	state := detect.State{Certain: true}
	rule := runtime.screenRule(tmuxTestInstance, state, []byte("one\n$ "), now)
	require.Equal(t, tmuxOutputChanging, rule.agentDetail)
	require.False(t, runtime.screenRule(tmuxTestInstance, state,
		[]byte("one\n$ "), now.Add(4*time.Second)).pass())
	require.True(t, runtime.screenRule(tmuxTestInstance, state,
		[]byte("\x1b[1mone\x1b[0m\n$ "), now.Add(5*time.Second)).pass())
	require.False(t, runtime.screenRule(tmuxTestInstance, state,
		[]byte("two\n$ "), now.Add(9*time.Second)).pass())
	require.True(t, runtime.screenRule(tmuxTestInstance, state,
		[]byte("two\n$ "), now.Add(14*time.Second)).pass())
}

// The deterministic route types only after the pane's visible text has stayed
// the same for the quiet window. A moving transcript holds the delivery with
// its reason, and the same screen delivers once the window has passed.
func TestTmuxQuietWindowHoldsChangingOutputThenDelivers(t *testing.T) {
	t.Parallel()
	fixture, transport, _ := warmTmuxDeferFixture(t)
	fixture.executor.tmux.quietWindow = 5 * time.Second
	candidate := fixture.seedDelivery(t, 157)
	for i := range 3 {
		transport.initial = []byte(fmt.Sprintf("output %d\n$ ", i))
		require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), candidate))
		advanceDeferClock(fixture, 2*time.Second)
	}
	require.Zero(t, transport.writes)
	guards := latestGuards(t, fixture.store.DB(), candidate.MessageID)
	require.Equal(t, protocol.GuardOutcomeFail, guardOutcome(guards, protocol.GuardNamePTYOutputQuiet))
	require.Equal(t, tmuxOutputChanging, guardDetail(guards, protocol.GuardNamePTYOutputQuiet))
	require.Equal(t, protocol.GuardOutcomePass, guardOutcome(guards, protocol.GuardNameHumanDraftClosed))

	// The last screen appeared two seconds ago, inside the window.
	require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), candidate))
	require.Zero(t, transport.writes)
	advanceDeferClock(fixture, 3*time.Second)
	require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), candidate))
	require.Equal(t, 1, transport.writes, "a quiet pane delivers without waiting for the backoff")
}

// A box the detector cannot locate is never typed into, however long the
// pane stays quiet.
func TestTmuxQuietWindowNeverTypesIntoUnlocatedBox(t *testing.T) {
	t.Parallel()
	fixture, transport, _ := warmTmuxDeferFixture(t)
	fixture.executor.tmux.quietWindow = 5 * time.Second
	candidate := fixture.seedDelivery(t, 158)
	transport.initial = []byte("no input line here")
	for range 6 {
		require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), candidate))
		advanceDeferClock(fixture, 8*time.Second)
	}
	require.Zero(t, transport.writes)
	guards := latestGuards(t, fixture.store.DB(), candidate.MessageID)
	require.Equal(t, tmuxBoxNotLocated, guardDetail(guards, protocol.GuardNameHumanDraftClosed))
	require.Equal(t, protocol.GuardOutcomeFail, guardOutcome(guards, protocol.GuardNamePTYOutputQuiet))
}
