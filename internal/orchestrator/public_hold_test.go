package orchestrator

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

// A versioned state policy refusal names its fixed reason for an operator;
// a detail that is not one of those phrases still shows only the guard.
func TestPublicHoldNamesTheStatePolicyReason(t *testing.T) {
	t.Parallel()
	held := pIdleGuard(StateRead{EligibilityReason: StateEligibilityBelowThreshold})
	require.Equal(t,
		"p_idle_threshold: versioned state policy is not eligible: IDLE_BELOW_THRESHOLD",
		PublicHold(held))
	for _, reason := range stateEligibilityReasons {
		require.Contains(t, PublicHold(pIdleGuard(StateRead{EligibilityReason: reason})), reason)
	}
	free := pIdleGuard(StateRead{EligibilityReason: "reason from /some/path"})
	require.Equal(t, string(protocol.GuardNamePIdleThreshold), PublicHold(free))
}
