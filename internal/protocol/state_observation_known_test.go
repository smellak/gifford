package protocol

import (
	"bytes"
	"encoding/json"
	"testing"

	v6 "github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/stretchr/testify/require"
)

// TestStateObservationV13ReportRoundTripsWithoutKnown pins that the v1.3
// report shape never carries the unknown-cursor mark: a report shaped like
// the ones old frames exchange serializes without it and still validates
// against the published, frozen schema.
func TestStateObservationV13ReportRoundTripsWithoutKnown(t *testing.T) {
	t.Parallel()

	report := StateObservationReport{
		AgentID: "agent-2", SessionID: "session-2",
		ObservationID: "obs-1", ObservedAt: testTimesOnly(),
		Harness: "codex", HarnessVersion: "0.154.0",
		ObservationGeneration: 3,
		InputGuardVersion:     2,
		ScreenState:           StateObservationScreenAvailable,
		ScreenRows:            []string{"completed fixture task", "$ "},
		NativeEvents:          []StateObservationEvent{},
		Cols:                  80,
		Cursor:                &StateObservationCursor{Row: 0, Col: 0},
	}
	raw, err := json.Marshal(report)
	require.NoError(t, err)
	require.NotContains(t, string(raw), `"known"`,
		"an unset cursor mark must not travel on the v1.3 shape")

	var published Root
	for _, root := range Roots() {
		if root.Name == "state_observation_report" {
			published = root
		}
	}
	require.Equal(t, MilestoneV13, published.Since)
	schema := compileSchema(t, published)
	doc, err := v6.UnmarshalJSON(bytes.NewReader(raw))
	require.NoError(t, err)
	require.NoError(t, schema.Validate(doc),
		"the v1.3 report shape must keep validating against the frozen schema")
}
