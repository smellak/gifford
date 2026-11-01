//go:build linux

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
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

func TestFederatedArtifactPublicCLILifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
	defer cancel()

	const (
		senderID    = protocol.AgentID("artifact-sender-a")
		recipientID = protocol.AgentID("artifact-recipient-b")
	)
	listenerA := reserveFederationE2EAddress(t)
	listenerB := reserveFederationE2EAddress(t)
	addressA := listenerA.Addr().String()
	addressB := listenerB.Addr().String()
	environmentA := newFederationE2EEnvironment(
		t, "artifact-node-a", "artifact-org-a", "artifact-node-b", string(recipientID), addressA,
	)
	environmentB := newFederationE2EEnvironment(
		t, "artifact-node-b", "", "artifact-node-a", string(recipientID), addressB,
	)

	require.NoError(t, listenerA.Close())
	hubA, stderrA := startE2EHub(ctx, t, environmentA)
	t.Cleanup(func() { _ = hubA.terminateAndWait() })
	hubB, stderrB := startE2EHub(ctx, t, environmentB)
	t.Cleanup(func() { _ = hubB.terminateAndWait() })

	token := createFederationE2EEnrollmentToken(ctx, t, environmentA.env)
	var joined protocol.NodeJoinResult
	secret := append([]byte(token.Token), '\n')
	token.Token = ""
	runFederationE2ECLIInput(ctx, t, environmentB.env, secret, &joined,
		"node", "join", token.Endpoint, "--fingerprint", string(token.Fingerprint))
	require.Equal(t, protocol.NodeID("artifact-node-a"), joined.Peer.NodeID)

	require.NoError(t, hubB.terminateAndWait(), "hub B stderr: %s", stderrB.String())
	require.NoError(t, listenerB.Close())
	restartedB, restartedStderrB := startE2EHub(ctx, t, environmentB)
	t.Cleanup(func() { _ = restartedB.terminateAndWait() })

	recipient := startE2EAgent(ctx, t, environmentB.env, recipientID)
	t.Cleanup(func() {
		_ = recipient.process.terminateAndWait()
		_ = recipient.outer.Close()
	})
	// Node A reads node B's key directory once at startup and then every 60 s,
	// so node B must hold the recipient's keys before node A restarts.
	dbB := openE2EReadDB(t, environmentB.database)
	requireE2EArtifactKeys(ctx, t, dbB, recipientID, "node B did not register the recipient's artifact keys")
	require.NoError(t, hubA.terminateAndWait(), "hub A stderr: %s", stderrA.String())
	restartedA, restartedStderrA := startE2EHub(ctx, t, environmentA)
	t.Cleanup(func() { _ = restartedA.terminateAndWait() })
	dbA := openE2EReadDB(t, environmentA.database)
	requireE2EArtifactKeys(ctx, t, dbA, recipientID, "node A did not import node B's artifact keys")

	sender := startE2EAgent(ctx, t, environmentA.env, senderID)
	t.Cleanup(func() {
		_ = sender.process.terminateAndWait()
		_ = sender.outer.Close()
	})
	source := filepath.Join(t.TempDir(), "federated.bin")
	body := bytes.Repeat([]byte("federated-artifact-marker-"), 90_000)
	body = body[:2*1024*1024+123]
	require.NoError(t, os.WriteFile(source, body, 0o600))
	args := []string{
		e2eArtifactCommand, e2eArtifactSend, source, e2eArtifactToFlag, string(recipientID),
		e2ePurposeFlag, "federated CLI verification",
	}
	type sentArtifact struct {
		ArtifactID  protocol.OpaqueID      `json:"artifact_id"`
		State       protocol.ArtifactState `json:"state"`
		ChunkCount  int64                  `json:"chunk_count"`
		ChunksSent  int64                  `json:"chunks_sent"`
		ResumedFrom int64                  `json:"resumed_from"`
		NoticeSent  bool                   `json:"notice_sent"`
	}
	var first sentArtifact
	sender.artifactCLI(t, &first, args...)
	require.Equal(t, protocol.ArtifactStateStoredRemote, first.State)
	require.Equal(t, int64(3), first.ChunkCount)
	require.Equal(t, first.ChunkCount, first.ChunksSent)
	require.True(t, first.NoticeSent)

	var remoteState protocol.ArtifactState
	var artifactUpdated string
	require.NoError(t, dbB.QueryRowContext(ctx, `SELECT state, updated_at
		FROM artifact_transfers WHERE artifact_id = ?`, first.ArtifactID).
		Scan(&remoteState, &artifactUpdated))
	require.Equal(t, protocol.ArtifactStateLocalAvailable, remoteState)
	var originState protocol.ArtifactState
	require.NoError(t, dbA.QueryRowContext(ctx, `SELECT state FROM artifact_transfers
		WHERE artifact_id = ?`, first.ArtifactID).Scan(&originState))
	require.Equal(t, protocol.ArtifactStateStoredRemote, originState)
	keyPath := filepath.Join(environmentA.dataDir, "artifacts", "outgoing",
		string(first.ArtifactID)+".key")
	_, err := os.Stat(keyPath)
	require.ErrorIs(t, err, os.ErrNotExist)

	var retried sentArtifact
	sender.artifactCLI(t, &retried, args...)
	require.Equal(t, first.ArtifactID, retried.ArtifactID)
	require.Equal(t, protocol.ArtifactStateStoredRemote, retried.State)
	require.Equal(t, first.ChunkCount, retried.ResumedFrom)
	require.Zero(t, retried.ChunksSent)
	require.True(t, retried.NoticeSent)

	noticeClientID := protocol.OpaqueID("notice-" + string(first.ArtifactID))
	var noticeCount int64
	var noticeCreated string
	require.Eventually(t, func() bool {
		err := dbB.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(MIN(created_at), '')
			FROM messages WHERE client_msg_id = ?`, noticeClientID).
			Scan(&noticeCount, &noticeCreated)
		return err == nil && noticeCount == 1 && noticeCreated != ""
	}, e2eRPCTimeout, 20*time.Millisecond, "exactly one FILE_READY did not arrive")
	require.LessOrEqual(t, artifactUpdated, noticeCreated,
		"FILE_READY was created before the recipient artifact became LOCAL_AVAILABLE")

	var noticeMessageID protocol.ULID
	require.NoError(t, dbB.QueryRowContext(ctx, `SELECT message_id FROM messages
		WHERE client_msg_id = ?`, noticeClientID).Scan(&noticeMessageID))
	var pending hub.ListPendingMessagesResult
	var generation int64
	require.Eventually(t, func() bool {
		pending = hub.ListPendingMessagesResult{}
		recipient.callTool(t, "list_pending_messages", map[string]any{}, &pending)
		for _, message := range pending.Messages {
			if message.MessageID == noticeMessageID {
				generation = message.DeliveryGeneration
				return true
			}
		}
		return false
	}, e2eRPCTimeout, 20*time.Millisecond, "FILE_READY was not visible to the recipient")
	var notice protocol.LogicalMessage
	recipient.callTool(t, "read_message", map[string]any{
		"id": noticeMessageID, e2eGenerationField: generation,
	}, &notice)
	require.Equal(t, noticeClientID, notice.ClientMsgID)
	require.Equal(t, "FILE_READY", notice.Subject)
	require.Contains(t, notice.Body, string(first.ArtifactID))

	var received struct {
		Path   string             `json:"path"`
		Size   int64              `json:"size"`
		SHA256 protocol.SHA256Hex `json:"sha256"`
	}
	recipient.artifactCLI(t, &received, e2eArtifactCommand, e2eArtifactReceive,
		string(first.ArtifactID), "--out", t.TempDir())
	require.Equal(t, int64(len(body)), received.Size)
	require.Equal(t, body, mustReadE2EFile(t, received.Path))
	wantDigest := protocol.SHA256Hex(fmt.Sprintf("%x", sha256.Sum256(body)))
	require.Equal(t, wantDigest, received.SHA256)

	originStore := filepath.Join(environmentA.dataDir, "artifacts", "store", string(first.ArtifactID))
	var originChunks int64
	require.Eventually(t, func() bool {
		err := dbA.QueryRowContext(ctx, `SELECT state,
			(SELECT COUNT(*) FROM artifact_chunks WHERE artifact_id = ?)
			FROM artifact_transfers WHERE artifact_id = ?`, first.ArtifactID, first.ArtifactID).
			Scan(&originState, &originChunks)
		_, statErr := os.Stat(originStore)
		return err == nil && originState == protocol.ArtifactStateAcknowledged &&
			originChunks == 0 && os.IsNotExist(statErr)
	}, e2eRPCTimeout, 20*time.Millisecond,
		"artifact ACK did not commit and delete the origin ciphertext")

	var ackInbox int64
	require.NoError(t, dbA.QueryRowContext(ctx, `SELECT COUNT(*)
		FROM federation_artifact_ack_inbox WHERE artifact_id = ?`, first.ArtifactID).
		Scan(&ackInbox))
	require.Equal(t, int64(1), ackInbox)
	require.NoError(t, dbB.QueryRowContext(ctx, `SELECT COUNT(*)
		FROM messages WHERE client_msg_id = ?`, noticeClientID).Scan(&noticeCount))
	require.Equal(t, int64(1), noticeCount)

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

// requireE2EArtifactKeys waits until db holds the ACTIVE ed25519 signing and
// x25519 encryption keys that artifact-node-b issued for agent.
func requireE2EArtifactKeys(ctx context.Context, t *testing.T, db *sql.DB, agent protocol.AgentID, message string) {
	t.Helper()
	require.Eventually(t, func() bool {
		var signing, encryption bool
		err := db.QueryRowContext(ctx, `SELECT
			EXISTS(SELECT 1 FROM agent_keys WHERE agent_id = ?
				AND node_id = 'artifact-node-b' AND status = 'ACTIVE'
				AND purpose = 'signing' AND algorithm = 'ed25519'),
			EXISTS(SELECT 1 FROM agent_keys WHERE agent_id = ?
				AND node_id = 'artifact-node-b' AND status = 'ACTIVE'
				AND purpose = 'encryption' AND algorithm = 'x25519')`,
			agent, agent).Scan(&signing, &encryption)
		return err == nil && signing && encryption
	}, e2eRPCTimeout, 20*time.Millisecond, message)
}
