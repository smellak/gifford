package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The source of a delivery decision follows what its row records: the
// corroboration policy names itself, and with a recorded call the model also
// chose; a recorded Glimmer call means the
// analyst's model chose, an analyst policy whose state came from an analysis
// call means the analyst's reading decided, an analyst policy without any
// call means no analysis arrived, and anything else was the detector alone.
const (
	analystPolicyExample ModelMetadataValue = "automatic-5.8-detector-corroboration:748725a15c5f403247d1a817"
	analyzeCallExample   OpaqueID           = "call-analyze"
)

func TestDeliveryDecisionSourceOfFollowsTheRow(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		policy    ModelMetadataValue
		call      OpaqueID
		stateCall OpaqueID
		want      DeliveryDecisionSource
	}{
		{"detector alone", DeterministicPTYPolicyVersion, "", "", DeliveryDecisionSourceDetector},
		{"detector alone, analysed state", DeterministicPTYPolicyVersion, "", analyzeCallExample, DeliveryDecisionSourceDetector},
		{"corroborated, deferred without a call", CorroboratedPTYPolicyVersion, "", "", DeliveryDecisionSourceCorroboration},
		{"corroborated, the model chose", CorroboratedPTYPolicyVersion, "call-corroborated", "", DeliveryDecisionSourceAnalystCorroborated},
		{"corroborated, the model chose on an analysed state", CorroboratedPTYPolicyVersion, "call-corroborated", analyzeCallExample, DeliveryDecisionSourceAnalystCorroborated},
		{"analyst route, the model chose", analystPolicyExample, "call-2", "", DeliveryDecisionSourceAnalyst},
		{"analyst route, deferred on the analyst's reading", analystPolicyExample, "", analyzeCallExample, DeliveryDecisionSourceAnalyst},
		{"analyst route, deferred without any call", analystPolicyExample, "", "", DeliveryDecisionSourceAnalystUnavailable},
		{"legacy deterministic policy", "deterministic-5.3c", "", "", DeliveryDecisionSourceDetector},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := DeliveryDecisionSourceOf(tc.policy, tc.call, tc.stateCall)
			require.Equal(t, tc.want, got)
			require.True(t, got.Valid())
		})
	}
	require.False(t, DeliveryDecisionSource("operator").Valid())
	require.Equal(t, []string{
		"detector", "detector+analyst-corroboration", "analyst", "analyst-unavailable",
		"analyst+detector-corroboration",
	}, DeliveryDecisionSource("").EnumValues())
}
