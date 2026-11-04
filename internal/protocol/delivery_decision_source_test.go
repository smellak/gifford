package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The source of a delivery decision follows what its row records: the
// corroboration policy names itself, a recorded Glimmer call means the
// analyst's model chose, and anything else was the detector alone.
func TestDeliveryDecisionSourceOfFollowsTheRow(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		policy ModelMetadataValue
		call   OpaqueID
		want   DeliveryDecisionSource
	}{
		{"detector alone", DeterministicPTYPolicyVersion, "", DeliveryDecisionSourceDetector},
		{"corroborated, deferred without a call", CorroboratedPTYPolicyVersion, "", DeliveryDecisionSourceCorroboration},
		{"corroborated, the model chose", CorroboratedPTYPolicyVersion, "call-corroborated", DeliveryDecisionSourceCorroboration},
		{"analyst route, the model chose", "automatic-5.8-detector-corroboration:748725a15c5f403247d1a817", "call-2", DeliveryDecisionSourceAnalyst},
		{"analyst route, deferred without a call", "automatic-5.8-detector-corroboration:748725a15c5f403247d1a817", "", DeliveryDecisionSourceDetector},
		{"legacy deterministic policy", "deterministic-5.3c", "", DeliveryDecisionSourceDetector},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := DeliveryDecisionSourceOf(tc.policy, tc.call)
			require.Equal(t, tc.want, got)
			require.True(t, got.Valid())
		})
	}
	require.False(t, DeliveryDecisionSource("operator").Valid())
	require.Equal(t, []string{"detector", "detector+analyst-corroboration", "analyst"},
		DeliveryDecisionSource("").EnumValues())
}
