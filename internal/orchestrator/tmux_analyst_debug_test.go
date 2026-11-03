//go:build linux

package orchestrator

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

func TestTmuxAnalystDebugProbe(t *testing.T) {
	f := newTmuxContractFixture(t, true)
	candidate := f.seedDelivery(t, 901)
	for i := range 3 {
		require.NoError(t, f.cycle(t))
		t.Logf("pass %d: reports=%d demanded=%v", i, len(f.states.reports), f.producer.demanded)
	}
	rows, err := f.store.DB().QueryContext(t.Context(), `SELECT decision_id, selected_action, state_id, hard_guards_json FROM orchestrator_decisions WHERE message_id = ?`, candidate.MessageID)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var id, selected, state, guards string
		require.NoError(t, rows.Scan(&id, &selected, &state, &guards))
		t.Logf("decision selected=%s state=%s guards=%s", selected, state, guards)
	}
	require.NoError(t, rows.Err())
	var manifest string
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT manifest_json FROM sessions WHERE session_id = ?`,
		executorTestSession).Scan(&manifest))
	t.Logf("manifest=%s", manifest)
	var caps protocol.CapabilityManifest
	if err := json.Unmarshal([]byte(manifest), &caps); err != nil {
		t.Logf("manifest unmarshal: %v", err)
	} else if err := caps.Validate(); err != nil {
		t.Logf("manifest validate: %v", err)
	} else {
		t.Logf("manifest ok harness=%s adapter=%s", caps.Harness, caps.AdapterType)
	}
	t.Logf("reports=%d", len(f.states.reports))
	for _, r := range f.states.reports {
		t.Logf("report obs=%s gen=%d rows=%q", r.ObservationID, r.ObservationGeneration, r.ScreenRows)
	}
}
