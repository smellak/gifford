//go:build linux

package orchestrator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

// The analyst route applies the same rule before it submits a screen: while
// the pane has not stayed quiet for the window it defers with that reason and
// makes no model call, and the same idle screen is then analyzed and
// delivered.
func TestTmuxAnalysisWaitsForQuietOutputBeforeSubmitting(t *testing.T) {
	f := newTmuxContractFixture(t, true)
	f.executor.tmux.quietWindow = 5 * time.Second
	candidate := f.seedDelivery(t, 410)
	f.cycles(t, 3)
	require.Empty(t, f.model.calls(protocol.ModelContractAnalyzeAgentState))
	f.requireNoAttempt(t)
	guards := latestGuards(t, f.store.DB(), candidate.MessageID)
	require.Equal(t, tmuxOutputChanging, guardDetail(guards, protocol.GuardNameHumanDraftClosed))

	f.cycles(t, 8)
	require.Len(t, f.model.calls(protocol.ModelContractAnalyzeAgentState), 1)
	require.Equal(t, 1, attemptCount(t, f.store.DB(), candidate.MessageID))
	require.Equal(t, 2, f.writes(t), "one literal notice and one Enter")
}
