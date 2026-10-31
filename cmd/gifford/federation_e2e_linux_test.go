//go:build linux

package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/hub"
	"github.com/smellak/gifford/internal/protocol"
)

func TestFederationBetweenTwoNodeProcessesOnOneHost(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 75*time.Second)
	defer cancel()

	listenerA := reserveFederationE2EAddress(t)
	listenerB := reserveFederationE2EAddress(t)
	addressA := listenerA.Addr().String()
	addressB := listenerB.Addr().String()
	environmentA := newFederationE2EEnvironment(
		t, "node-a", "org-a", "node-b", "recipient-b", addressA,
	)
	environmentB := newFederationE2EEnvironment(
		t, "node-b", "", "node-a", "recipient-b", addressB,
	)

	require.NoError(t, listenerA.Close())
	hubA, stderrA := startE2EHub(ctx, t, environmentA)
	t.Cleanup(func() { _ = hubA.terminateAndWait() })
	hubB, stderrB := startE2EHub(ctx, t, environmentB)
	t.Cleanup(func() { _ = hubB.terminateAndWait() })

	token := createFederationE2EEnrollmentToken(ctx, t, environmentA.env)
	require.Equal(t, protocol.NodeID("node-a"), token.NodeID)
	require.Equal(t, "https://"+addressA, token.Endpoint)
	require.True(t, token.Fingerprint.Valid())

	var joined protocol.NodeJoinResult
	secret := append([]byte(token.Token), '\n')
	token.Token = ""
	runFederationE2ECLIInput(ctx, t, environmentB.env, secret, &joined,
		"node", "join", token.Endpoint, "--fingerprint", string(token.Fingerprint))
	require.Equal(t, protocol.NodeID("node-a"), joined.Peer.NodeID)
	require.Equal(t, "TRUSTED", joined.Peer.Status)

	require.NoError(t, hubB.terminateAndWait(), "hub B stderr: %s", stderrB.String())
	require.NoError(t, listenerB.Close())
	restartedB, restartedStderrB := startE2EHub(ctx, t, environmentB)
	t.Cleanup(func() { _ = restartedB.terminateAndWait() })

	recipient := startE2EAgent(ctx, t, environmentB.env, "recipient-b")
	t.Cleanup(func() {
		select {
		case <-recipient.process.done:
		default:
			_ = recipient.process.terminateAndWait()
		}
		_ = recipient.outer.Close()
	})
	var pending hub.ListPendingMessagesResult
	recipient.callTool(t, "list_pending_messages", map[string]any{}, &pending)
	require.Empty(t, pending.Messages)

	require.NoError(t, hubA.terminateAndWait(), "hub A stderr: %s", stderrA.String())
	restartedA, restartedStderrA := startE2EHub(ctx, t, environmentA)
	t.Cleanup(func() { _ = restartedA.terminateAndWait() })
	dbA := openE2EReadDB(t, environmentA.database)
	require.Eventually(t, func() bool {
		var signing, encryption bool
		err := dbA.QueryRowContext(ctx, `SELECT
			EXISTS(SELECT 1 FROM agent_keys WHERE agent_id = 'recipient-b'
				AND node_id = 'node-b' AND status = 'ACTIVE'
				AND purpose = 'signing' AND algorithm = 'ed25519'),
			EXISTS(SELECT 1 FROM agent_keys WHERE agent_id = 'recipient-b'
				AND node_id = 'node-b' AND status = 'ACTIVE'
				AND purpose = 'encryption' AND algorithm = 'x25519')`).Scan(&signing, &encryption)
		return err == nil && signing && encryption
	}, e2eRPCTimeout, 20*time.Millisecond, "node A did not import node B's key directory")

	sender := startE2EAgent(ctx, t, environmentA.env, "sender-a")
	t.Cleanup(func() {
		select {
		case <-sender.process.done:
		default:
			_ = sender.process.terminateAndWait()
		}
		_ = sender.outer.Close()
	})
	var admitted protocol.AdmissionResult
	sender.callTool(t, "send_message", map[string]any{
		e2eToField: "recipient-b", e2eSubjectField: "same-host federation",
		e2eBodyField: "H18 two-node delivery", e2eClientIDField: "h18-same-host-1",
	}, &admitted)
	require.Equal(t, protocol.AdmissionStatusAccepted, admitted.Result)
	require.True(t, admitted.MessageID.Valid())

	var stored hub.GetDeliveryStatusResult
	require.Eventually(t, func() bool {
		sender.callTool(t, "get_delivery_status", map[string]any{e2eMessageIDField: admitted.MessageID}, &stored)
		return stored.State == e2eStoredRemote && stored.FederationState == e2eStoredRemote
	}, e2eRPCTimeout, 20*time.Millisecond, "message did not reach STORED_REMOTE")

	require.Eventually(t, func() bool {
		pending = hub.ListPendingMessagesResult{}
		recipient.callTool(t, "list_pending_messages", map[string]any{}, &pending)
		return len(pending.Messages) == 1 && pending.Messages[0].MessageID == admitted.MessageID
	}, e2eRPCTimeout, 20*time.Millisecond, "remote recipient did not receive message")
	generation := pending.Messages[0].DeliveryGeneration
	var message protocol.LogicalMessage
	recipient.callTool(t, "read_message", map[string]any{
		"id": admitted.MessageID, e2eGenerationField: generation,
	}, &message)
	require.Equal(t, "H18 two-node delivery", message.Body)
	var acknowledged map[string]bool
	recipient.callTool(t, "ack_message", map[string]any{
		"id": admitted.MessageID, e2eGenerationField: generation, e2eStatusField: protocol.AckStatusAcknowledged,
	}, &acknowledged)
	require.True(t, acknowledged["acked"])

	var final hub.GetDeliveryStatusResult
	require.Eventually(t, func() bool {
		sender.callTool(t, "get_delivery_status", map[string]any{e2eMessageIDField: admitted.MessageID}, &final)
		return final.State == string(protocol.AckStatusAcknowledged) && len(final.Recipients) == 1 &&
			final.Recipients[0].DeliveryGeneration == 1 &&
			final.Recipients[0].State == e2eStoredRemote &&
			final.Recipients[0].AckStatus == hub.AckMessageAcknowledged
	}, e2eRPCTimeout, 20*time.Millisecond, "remote acknowledgement did not return to origin")

	requireDistinctNodeKeys(t, environmentA, environmentB)
	sender.stop(t)
	recipient.stop(t)
	require.NoError(t, restartedA.terminateAndWait(), "hub A stderr: %s", restartedStderrA.String())
	require.NoError(t, restartedB.terminateAndWait(), "hub B stderr: %s", restartedStderrB.String())
}

func reserveFederationE2EAddress(t *testing.T) net.Listener {
	t.Helper()
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}

func newFederationE2EEnvironment(
	t *testing.T,
	nodeID, organizationID, peerID, peerAgent, address string,
) e2eEnvironment {
	t.Helper()
	root, err := os.MkdirTemp("", "gifford-fed-e2e-*") //nolint:usetesting // Keep socket paths short.
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(root)) })
	configHome := filepath.Join(root, "config")
	dataHome := filepath.Join(root, "data")
	runtimeHome := filepath.Join(root, "runtime")
	configDir := filepath.Join(configHome, "gifford")
	policyDir := filepath.Join(configDir, "policies")
	dataDir := filepath.Join(dataHome, "gifford")
	trustDir := filepath.Join(dataDir, "trust")
	for _, dir := range []string{
		configHome, dataHome, runtimeHome, configDir, policyDir, dataDir, trustDir,
	} {
		require.NoError(t, os.MkdirAll(dir, 0o700))
		require.NoError(t, os.Chmod(dir, 0o700))
	}
	organization := ""
	if organizationID != "" {
		organization = "organization_id: " + organizationID + "\n"
	}
	config := fmt.Sprintf("schema_version: 1\nnode_id: %s\n%sfederation:\n  listen: %s\n  trust_bundle: %s\n  advertise: https://%s\n",
		nodeID, organization, address, trustDir, address)
	require.NoError(t, os.WriteFile(filepath.Join(configDir, "config.yaml"), []byte(config), 0o600))
	policy := fmt.Sprintf("schema_version: 1\nrules:\n  - id: allow-%s-%s\n    effect: allow\n    peer: %s\n    action: deliver_message\n    agent: %s\n",
		peerID, peerAgent, peerID, peerAgent)
	require.NoError(t, os.WriteFile(filepath.Join(policyDir, "federation.yaml"), []byte(policy), 0o600))
	env := append(os.Environ(),
		"XDG_CONFIG_HOME="+configHome,
		"XDG_DATA_HOME="+dataHome,
		"XDG_RUNTIME_DIR="+runtimeHome,
		"GORACE=atexit_sleep_ms=0",
	)
	return e2eEnvironment{
		env: env, runtimeDir: filepath.Join(runtimeHome, "gifford"), dataDir: dataDir,
		database: filepath.Join(dataDir, "gifford.db"),
	}
}

func runFederationE2ECLIInput(
	ctx context.Context,
	t *testing.T,
	env []string,
	input []byte,
	result any,
	args ...string,
) {
	t.Helper()
	defer clear(input)
	cmd := exec.CommandContext(ctx, e2eGiffordBinary, args...)
	cmd.Env = env
	cmd.Stdin = bytes.NewReader(input)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	output, err := cmd.Output()
	require.NoError(t, err, "stderr: %s", stderr.String())
	require.NoError(t, json.Unmarshal(output, result), "output: %s", output)
}

func createFederationE2EEnrollmentToken(
	ctx context.Context,
	t *testing.T,
	env []string,
) protocol.NodeEnrollTokenResult {
	t.Helper()
	cmd := exec.CommandContext(ctx, e2eGiffordBinary, "node", "enroll-token", "--org", "org-b")
	cmd.Env = env
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	output, err := cmd.Output()
	require.NoError(t, err, "stderr: %s", stderr.String())
	defer clear(output)
	var result protocol.NodeEnrollTokenResult
	require.NoError(t, json.Unmarshal(output, &result), "decode enrollment token response")
	return result
}

func requireDistinctNodeKeys(t *testing.T, first, second e2eEnvironment) {
	t.Helper()
	read := func(environment e2eEnvironment) []byte {
		path := filepath.Join(environment.dataDir, "keys", "node", "node.seed")
		info, err := os.Stat(path)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
		seed, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Len(t, seed, 32)
		return seed
	}
	require.NotEqual(t, read(first), read(second))

	firstDB := openE2EReadDB(t, first.database)
	secondDB := openE2EReadDB(t, second.database)
	readPublished := func(database *sql.DB, nodeID protocol.NodeID) []byte {
		var publicKey []byte
		require.NoError(t, database.QueryRowContext(t.Context(),
			"SELECT signing_public_key FROM nodes WHERE node_id = ?", nodeID).Scan(&publicKey))
		require.Len(t, publicKey, 32)
		return publicKey
	}
	firstLocal := readPublished(firstDB, "node-a")
	secondLocal := readPublished(secondDB, "node-b")
	require.Equal(t, firstLocal, readPublished(secondDB, "node-a"))
	require.Equal(t, secondLocal, readPublished(firstDB, "node-b"))
	require.NotEqual(t, firstLocal, secondLocal)
}
