package orchestrator

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/tmuxinstance"
)

func TestOnlyMuseGetsASecondEnter(t *testing.T) {
	t.Parallel()
	for harness, want := range map[protocol.Harness]bool{
		protocol.HarnessMuseCode:   true,
		protocol.HarnessCodex:      false,
		protocol.HarnessClaudeCode: false,
		protocol.HarnessOpencode:   false,
	} {
		guard := &tmuxDeliveryGuard{instance: tmuxinstance.Instance{Harness: harness}}
		require.Equal(t, want, guard.ResubmitOnce(), harness)
	}
}
