package protocol

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIncidentVocabularyIsClosed(t *testing.T) {
	t.Parallel()
	require.Equal(t, []string{"DELIVERY_UNCERTAIN", "APPROVAL_PENDING", "TMUX_INSTANCE_INVALID", "DETECTOR_UNCERTAIN", "ESCALATED"}, IncidentKind("").EnumValues())
	require.Equal(t, []string{"UNCERTAIN", "PENDING", "INVALID", "ESCALATED"}, IncidentState("").EnumValues())
	require.Equal(t, []string{"operator"}, IncidentOwnerKind("").EnumValues())
	require.Equal(t, []string{"delivery", "instances", "detector", "unknown"}, IncidentFixArea("").EnumValues())
	for _, value := range IncidentKind("").EnumValues() {
		require.True(t, IncidentKind(value).Valid())
	}
	for _, value := range IncidentState("").EnumValues() {
		require.True(t, IncidentState(value).Valid())
	}
	for _, value := range []string{"", "BUSY", "OPEN", "RESOLVED", "future"} {
		require.False(t, IncidentKind(value).Valid())
		require.False(t, IncidentState(value).Valid())
	}
	require.False(t, IncidentOwnerKind("agent").Valid())
	require.False(t, IncidentFixArea("orchestrator").Valid())
}

func TestIncidentProtocolBoundsAndReadOnlyMethod(t *testing.T) {
	t.Parallel()
	zero, tooMany := int64(0), MaxListLimit+1
	for _, p := range []IncidentListParams{{Kind: "NEW"}, {AgentID: "Bad"}, {Cursor: "bad.cursor"}, {Limit: &zero}, {Limit: &tooMany}} {
		require.Error(t, p.Validate())
	}
	require.NoError(t, IncidentListParams{}.Validate())
	require.Equal(t, DefaultListLimit, IncidentListParams{}.EffectiveLimit())
	for _, spec := range ControlMethodSpecs() {
		if spec.Method == ControlMethodIncidentList {
			require.False(t, spec.Mutates)
			require.IsType(t, &IncidentListParams{}, spec.NewParams())
			require.IsType(t, &IncidentListResult{}, spec.NewResult())
			return
		}
	}
	t.Fatal("incident.list is absent from method specs")
}

func TestIncidentNullableFactsAndUnknownState(t *testing.T) {
	t.Parallel()
	stamp := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	item := IncidentListItem{
		IncidentID: "DETECTOR_UNCERTAIN:instance-1", Kind: IncidentKindDetectorUncertain,
		AgentID: "worker-1", OwnerKind: IncidentOwnerOperator, FixArea: IncidentFixAreaDetector,
		State: IncidentStateUncertain, Reason: "DRAFT_UNKNOWN", OpenedAt: stamp, UpdatedAt: stamp,
	}
	require.NoError(t, item.Validate(), "unrecorded facts remain null")
	item.State = "invented"
	require.Error(t, item.Validate())
	item.State = IncidentStateUncertain
	item.UpdatedAt = stamp.Add(-time.Second)
	require.Error(t, item.Validate())
}

func TestIncidentRPCOnlyInV17Schema(t *testing.T) {
	t.Parallel()
	request := map[string]any{"jsonrpc": "2.0", "id": float64(1), "method": "incident.list", "params": map[string]any{}}
	for _, root := range Roots() {
		switch root.Name {
		case "rpc_request_v1_6":
			require.Error(t, compileSchema(t, root).Validate(request))
		case "rpc_request_v1_7":
			require.NoError(t, compileSchema(t, root).Validate(request))
		}
	}
}
