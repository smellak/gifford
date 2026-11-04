package hub

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

func TestModelNoticeFilesFixedMetadataOnce(t *testing.T) {
	t.Parallel()
	f := noticeFixture(t)
	operator := protocol.AgentID("agent-operator")
	notice := ModelNotice{
		Generation: 9, AdapterName: "analyst", AdapterRevision: "rev-1",
		Reason: "adapter call timed out",
	}
	require.NoError(t, f.service.FileModelNotice(t.Context(), operator, notice))
	require.NoError(t, f.service.FileModelNotice(t.Context(), operator, notice))

	var count int
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM messages WHERE client_msg_id = ?", "model-notice:9:agent-operator").Scan(&count))
	require.Equal(t, 1, count, "a retry converges on the filed notice")
	var rawEnvelope []byte
	require.NoError(t, f.store.DB().QueryRowContext(t.Context(), `SELECT e.envelope
		FROM messages m JOIN message_envelopes e ON e.message_id = m.message_id
		WHERE m.client_msg_id = ?`, "model-notice:9:agent-operator").Scan(&rawEnvelope))
	var envelope protocol.WireEnvelope
	require.NoError(t, json.Unmarshal(rawEnvelope, &envelope))
	var logical protocol.LogicalMessage
	require.NoError(t, json.Unmarshal(envelope.Ciphertext, &logical))
	require.NoError(t, logical.Validate())
	require.Equal(t, registry.EmitterAgentID, logical.FromAgent)
	require.Equal(t, []protocol.AgentID{operator}, logical.ToAgents)
	require.Equal(t, "Glimmer adapter fell back to the base model", logical.Subject)
	require.Equal(t, "The Glimmer adapter fell back to the base model.\nAdapter: analyst@rev-1\n"+
		"Generation: 9\nReason: adapter call timed out\n"+
		"This operator notice carries no message content. No response is needed.", logical.Body)

	back := notice
	back.Reactivated, back.Generation = true, 10
	require.NoError(t, f.service.FileModelNotice(t.Context(), operator, back))
	subject, _ := back.Text()
	require.Equal(t, "Glimmer adapter reactivated", subject)
	require.Error(t, f.service.FileModelNotice(t.Context(), operator, ModelNotice{}))
}
