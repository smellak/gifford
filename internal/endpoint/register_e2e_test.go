package endpoint

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/hub"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

func insertEndpointRegistrationSession(
	t *testing.T,
	store *eventstore.Store,
	agent protocol.AgentID,
	process protocol.ProcessIdentity,
	stamp string,
) {
	t.Helper()
	_, err := store.DB().ExecContext(t.Context(), `INSERT INTO agents
		(agent_id, node_id, harness, created_at, updated_at) VALUES (?, ?, 'test', ?, ?)`,
		agent, e2eNode, stamp, stamp)
	require.NoError(t, err)
	processJSON, err := json.Marshal(process)
	require.NoError(t, err)
	_, err = store.DB().ExecContext(t.Context(), `INSERT INTO sessions
		(session_id, agent_id, state, capture_mode, deliverable, manifest_json,
		 process_json, started_at, updated_at)
		VALUES (?, ?, 'CONNECTED', 'pty_proxy', 1, '{}', ?, ?, ?)`,
		"session-"+string(agent), agent, string(processJSON), stamp, stamp)
	require.NoError(t, err)
}

func TestEndpointRegistersKeyAndKeySubstitutionBlocksSending(t *testing.T) {
	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "gifford.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	clk := &e2eClock{now: time.Date(2026, time.October, 29, 19, 40, 0, 0, time.UTC)}
	engine, err := channel.New(channel.Options{Clock: clk})
	require.NoError(t, err)
	controller, err := channel.NewController(channel.ControllerOptions{
		DB: store.DB(), Engine: engine, Clock: clk, NodeID: e2eNode,
	})
	require.NoError(t, err)
	require.NoError(t, controller.Reload(t.Context()))
	registryDir := t.TempDir()
	require.NoError(t, os.Chmod(registryDir, 0o700))
	keyRegistry, err := registry.New(e2eNode, registryDir, clk)
	require.NoError(t, err)
	service, err := hub.NewService(hub.ServiceOptions{
		DB: store.DB(), NodeID: e2eNode, Engine: engine, Approvals: controller,
		Registry: keyRegistry, Clock: clk,
	})
	require.NoError(t, err)
	hubServer, err := hub.New(t.TempDir(), store.DB(), service.Handlers())
	require.NoError(t, err)
	sock, err := hubServer.Listen()
	require.NoError(t, err)
	go func() { _ = hubServer.Serve(context.Background()) }()
	t.Cleanup(func() { require.NoError(t, hubServer.Close()) })

	self, err := registry.CurrentProcess()
	require.NoError(t, err)
	stamp := eventstore.FormatTime(clk.now)
	const sender = protocol.AgentID("agent-ep")
	insertEndpointRegistrationSession(t, store, sender, self, stamp)
	recipientProcess := self
	recipientProcess.PID = 1
	recipientProcess.ProcessStartTime = 0
	insertEndpointRegistrationSession(t, store, e2eRecipient, recipientProcess, stamp)
	dataDir := t.TempDir()
	require.NoError(t, os.Chmod(dataDir, 0o700))

	first := startEndpoint(t, dataDir, sock)
	first.initialize(t)
	result := first.call(t, 2, toolSendMessage, sendArgs(string(e2eRecipient), "subject", "signed body"))
	require.Equal(t, string(protocol.AdmissionStatusAccepted), result.(map[string]any)["result"])
	first.close(t)
	active, err := keyRegistry.ActiveSigningKey(t.Context(), store.DB(), sender)
	require.NoError(t, err)
	require.Equal(t, endpointKeyID(sender), active.KeyID)
	var messages, envelopes, keys, decisions int
	require.NoError(t, store.DB().QueryRowContext(t.Context(), "SELECT count(*) FROM messages").Scan(&messages))
	require.NoError(t, store.DB().QueryRowContext(t.Context(), "SELECT count(*) FROM message_envelopes").Scan(&envelopes))
	require.NoError(t, store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM channel_control_decisions").Scan(&decisions))
	require.NoError(t, store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM agent_keys WHERE agent_id = ?", sender).Scan(&keys))
	require.Equal(t, 1, messages)
	require.Equal(t, 1, envelopes)
	require.Equal(t, 2, keys)

	second := startEndpoint(t, dataDir, sock)
	second.initialize(t)
	second.close(t)
	require.NoError(t, store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM agent_keys WHERE agent_id = ?", sender).Scan(&keys))
	require.Equal(t, 2, keys)

	_, replacement, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(
		filepath.Join(dataDir, agentKeysDir, string(sender), agentSeedFile), replacement.Seed(), 0o600,
	))

	blocked := startEndpoint(t, dataDir, sock)
	blocked.initialize(t)
	_, err = keyRegistry.ActiveSigningKey(t.Context(), store.DB(), sender)
	require.ErrorIs(t, err, registry.ErrNoUsableKey)
	isBlocked, err := keyRegistry.Blocked(t.Context(), store.DB(), sender)
	require.NoError(t, err)
	require.True(t, isBlocked)
	_, response := blocked.request(t, "tools/call", map[string]any{
		argName: toolSendMessage, argArguments: sendArgs(string(e2eRecipient), "subject", "blocked body"),
	}, 2)
	failure := response["error"].(map[string]any)
	require.Equal(t, -32000, mcpCode(t, failure))
	require.Equal(t, "Request rejected", failure["message"])
	require.Equal(t, string(protocol.ErrorCodeKeyUnavailable),
		failure["data"].(map[string]any)["error_code"])
	blocked.close(t)
	require.NoError(t, store.DB().QueryRowContext(t.Context(), "SELECT count(*) FROM messages").Scan(&messages))
	require.Equal(t, 1, messages)
	var decisionsAfter int
	require.NoError(t, store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM channel_control_decisions").Scan(&decisionsAfter))
	require.Equal(t, decisions, decisionsAfter)
	require.NoError(t, store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM agent_keys WHERE agent_id = ?", sender).Scan(&keys))
	require.Equal(t, 2, keys)
}
