package protocol

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func validLoop() Loop {
	started := testControlTime()
	return Loop{
		LoopID:         testLoop,
		ThreadID:       testThread,
		LoopGeneration: 1,
		OwnerNode:      testNode,
		Origin:         LoopOriginHuman,
		Participants:   []AgentID{testAgentID, testAgent2},
		Mode:           LoopModeAuto,
		EffectiveMode:  LoopModeAuto,
		BudgetProfile:  BudgetProfileDefault,
		Budget: LoopBudget{
			MaxTurns:               12,
			MaxMessages:            24,
			MaxDurationMS:          3_600_000,
			MaxMessagesPerMinute:   20,
			MaxHops:                6,
			MaxConsecutiveSamePair: 3,
			MaxRetries:             2,
			MaxArtifactCount:       4,
			MaxArtifactBytes:       1_000_000,
		},
		ControlVersion: 1,
		StartedAt:      started,
		ExpiresAt:      started.Add(time.Hour),
		CreatedBy:      testNode,
		UpdatedAt:      started,
	}
}

// TestLoopRules covers the effective-mode rule and the loop invariants.
func TestLoopRules(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		valid bool
		patch func(*Loop)
	}{
		{name: "auto passes", valid: true, patch: func(*Loop) {}},
		{name: "paused override pauses", valid: true, patch: func(l *Loop) {
			l.LocalOverride = ControlModePaused
			l.EffectiveMode = LoopModePaused
		}},
		{name: "stopped override stops", valid: true, patch: func(l *Loop) {
			l.LocalOverride = ControlModeStopped
			l.EffectiveMode = LoopModeStopped
			l.StoppedAt = testControlTime()
		}},
		{name: "stopped mode stays stopped", valid: true, patch: func(l *Loop) {
			l.Mode = LoopModeStopped
			l.EffectiveMode = LoopModeStopped
			l.LocalOverride = ControlModePaused
			l.StoppedAt = testControlTime()
		}},
		{name: "completed mode stays completed", valid: true, patch: func(l *Loop) {
			l.Mode = LoopModeCompleted
			l.EffectiveMode = LoopModeCompleted
			l.LocalOverride = ControlModeStopped
		}},
		{name: "effective must follow the rule", patch: func(l *Loop) {
			l.LocalOverride = ControlModePaused
		}},
		{name: "override is paused or stopped", patch: func(l *Loop) {
			l.LocalOverride = ControlModeAllow
			l.EffectiveMode = LoopModeAuto
		}},
		{name: "expiry is exact", patch: func(l *Loop) {
			l.ExpiresAt = l.ExpiresAt.Add(time.Second)
		}},
		{name: "stopped mode needs stopped_at", patch: func(l *Loop) {
			l.Mode = LoopModeStopped
			l.EffectiveMode = LoopModeStopped
		}},
		{name: "last sender is a participant", valid: true, patch: func(l *Loop) {
			l.LastSenderAgent = testAgent2
		}},
		{name: "last sender outside participants", patch: func(l *Loop) {
			l.LastSenderAgent = "agent-9"
		}},
		{name: "needs two participants", patch: func(l *Loop) {
			l.Participants = []AgentID{testAgentID}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			loop := validLoop()
			tc.patch(&loop)
			if tc.valid {
				require.NoError(t, loop.Validate())
				return
			}
			require.ErrorIs(t, loop.Validate(), ErrInvalid)
		})
	}
}
