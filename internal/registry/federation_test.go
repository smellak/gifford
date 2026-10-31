package registry

import (
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

type federationTestNode struct {
	id    protocol.NodeID
	reg   *Registry
	store *eventstore.Store
}

func newFederationTestNode(t *testing.T, id protocol.NodeID) federationTestNode {
	t.Helper()
	store := testStore(t)
	reg, err := New(id, testDataDir(t), clock.System{})
	require.NoError(t, err)
	require.NoError(t, reg.EnsureLocalNode(t.Context(), store.DB(), "org-test"))
	return federationTestNode{id: id, reg: reg, store: store}
}

func trustTestPeer(t *testing.T, local, peer federationTestNode) {
	t.Helper()
	publicKey, err := peer.reg.nodePublic()
	require.NoError(t, err)
	result, err := local.reg.AddPeer(t.Context(), local.store.DB(), peer.id, "org-test", publicKey)
	require.NoError(t, err)
	require.Equal(t, AddPeerRegistered, result.Outcome)
}

func registerTestAgent(
	t *testing.T,
	node federationTestNode,
	agent protocol.AgentID,
) (ed25519.PrivateKey, protocol.ProcessIdentity) {
	t.Helper()
	process, err := CurrentProcess()
	require.NoError(t, err)
	stamp := node.reg.now().Format(timeLayout)
	_, err = node.store.DB().ExecContext(t.Context(), `INSERT INTO agents
		(agent_id, node_id, harness, created_at, updated_at) VALUES (?, ?, 'test', ?, ?)`,
		agent, node.id, stamp, stamp)
	require.NoError(t, err)
	testSession(t.Context(), t, node.store.DB(), "session-"+string(agent), string(agent), &process, nil)
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	_, err = node.reg.Register(t.Context(), node.store.DB(), RegisterRequest{
		Agent: agent, Process: process, PublicKey: publicKey,
		KeyID: "ed25519:" + string(agent) + ":v1",
	})
	require.NoError(t, err)
	return privateKey, process
}

func nextTestKey(
	t *testing.T,
	agent protocol.AgentID,
	process protocol.ProcessIdentity,
	version string,
) (ed25519.PrivateKey, RegisterRequest) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return privateKey, RegisterRequest{
		Agent: agent, Process: process, PublicKey: publicKey,
		KeyID: "ed25519:" + string(agent) + ":v" + version,
	}
}

func directoryForAgent(t *testing.T, node federationTestNode, agent protocol.AgentID) []DirectoryRecord {
	t.Helper()
	records, err := node.reg.ExportDirectory(t.Context(), node.store.DB())
	require.NoError(t, err)
	var found []DirectoryRecord
	for _, record := range records {
		if record.Agent == agent {
			found = append(found, record)
		}
	}
	return found
}

func TestExportDirectoryKeepsEncryptionKeysLocal(t *testing.T) {
	t.Parallel()
	node := newFederationTestNode(t, "node-local-keys")
	agent := protocol.AgentID("agent-local-keys")
	_, process := registerTestAgent(t, node, agent)
	private, err := ecdh.X25519().GenerateKey(rand.Reader)
	require.NoError(t, err)
	result, err := node.reg.Register(t.Context(), node.store.DB(), RegisterRequest{
		Agent: agent, Process: process,
		PublicKey: ed25519.PublicKey(private.PublicKey().Bytes()),
		KeyID:     "x25519:" + string(agent) + ":v1",
		Purpose:   KeyPurposeEncryption, Algorithm: KeyAlgorithmX25519,
	})
	require.NoError(t, err)
	require.Equal(t, OutcomeRegistered, result.Outcome)
	require.Equal(t, int64(2), countNodeKeys(t, node.store.DB(), node.id))

	records, err := node.reg.ExportDirectory(t.Context(), node.store.DB())
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, KeyPurposeSigning, records[0].Purpose)
	require.Equal(t, KeyAlgorithmEd25519, records[0].Algorithm)
}

func countNodeKeys(t *testing.T, db *sql.DB, node protocol.NodeID) int64 {
	t.Helper()
	var count int64
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT count(*) FROM agent_keys WHERE node_id = ?", node).Scan(&count))
	return count
}

func signTestDirectoryRecord(
	t *testing.T,
	node federationTestNode,
	record DirectoryRecord,
) DirectoryRecord {
	t.Helper()
	rec, err := importRecord(node.id, record)
	require.NoError(t, err)
	privateKey, err := node.reg.nodePrivate()
	require.NoError(t, err)
	record.RecordSignature, err = signRecord(privateKey, rec)
	require.NoError(t, err)
	return record
}

func TestEnsureLocalNodePinsSigningKey(t *testing.T) {
	node := newFederationTestNode(t, "node-a")

	var local int64
	var status string
	var stored []byte
	require.NoError(t, node.store.DB().QueryRowContext(t.Context(),
		"SELECT local, status, signing_public_key FROM nodes WHERE node_id = ?", node.id).
		Scan(&local, &status, &stored))
	require.Equal(t, int64(1), local)
	require.Equal(t, localNodeStatus, status)
	publicKey, err := node.reg.nodePublic()
	require.NoError(t, err)
	require.Equal(t, []byte(publicKey), stored)
	require.NoError(t, node.reg.EnsureLocalNode(t.Context(), node.store.DB(), "org-test"))

	replacement, err := New(node.id, testDataDir(t), clock.System{})
	require.NoError(t, err)
	err = replacement.EnsureLocalNode(t.Context(), node.store.DB(), "org-test")
	require.ErrorIs(t, err, ErrNodeKeyMismatch)
}

func TestEnsureLocalNodePinsOrganizationOnce(t *testing.T) {
	store := testStore(t)
	reg, err := New("node-a", testDataDir(t), clock.System{})
	require.NoError(t, err)
	require.NoError(t, reg.EnsureLocalNode(t.Context(), store.DB(), ""))
	var organizationID string
	require.NoError(t, store.DB().QueryRowContext(t.Context(),
		"SELECT organization_id FROM nodes WHERE node_id = 'node-a'").Scan(&organizationID))
	require.Empty(t, organizationID)

	require.NoError(t, reg.EnsureLocalNode(t.Context(), store.DB(), "org-a"))
	require.NoError(t, store.DB().QueryRowContext(t.Context(),
		"SELECT organization_id FROM nodes WHERE node_id = 'node-a'").Scan(&organizationID))
	require.Equal(t, "org-a", organizationID)
	require.NoError(t, reg.EnsureLocalNode(t.Context(), store.DB(), "org-a"))
	err = reg.EnsureLocalNode(t.Context(), store.DB(), "org-other")
	require.ErrorIs(t, err, ErrNodeKeyMismatch)
}

func TestEnrollLocalNodeIsOneWay(t *testing.T) {
	store := testStore(t)
	reg, err := New("node-a", testDataDir(t), clock.System{})
	require.NoError(t, err)
	require.NoError(t, reg.EnsureLocalNode(t.Context(), store.DB(), ""))
	require.NoError(t, reg.EnrollLocalNode(
		t.Context(), store.DB(), "org-a", "policy-v1:000000000000",
	))
	state, err := reg.LocalEnrollment(t.Context(), store.DB())
	require.NoError(t, err)
	require.Equal(t, "org-a", state.OrganizationID)
	require.Equal(t, "policy-v1:000000000000", state.PolicyVersion)
	err = reg.EnrollLocalNode(t.Context(), store.DB(), "org-a", "policy-v1:000000000000")
	require.ErrorIs(t, err, ErrNodeAlreadyEnrolled)
}

func TestSetPeerTransportPinsOnce(t *testing.T) {
	local := newFederationTestNode(t, "node-a")
	peer := newFederationTestNode(t, "node-b")
	trustTestPeer(t, local, peer)

	fingerprint := strings.Repeat("ab", 32)
	require.NoError(t, local.reg.SetPeerTransport(
		t.Context(), local.store.DB(), peer.id, "https://peer.example.invalid:7443", fingerprint,
	))
	require.NoError(t, local.reg.SetPeerTransport(
		t.Context(), local.store.DB(), peer.id, "https://peer.example.invalid:7443", fingerprint,
	))

	for _, change := range []struct {
		endpoint    string
		fingerprint string
	}{
		{"https://other.example.invalid:7443", fingerprint},
		{"https://peer.example.invalid:7443", strings.Repeat("cd", 32)},
	} {
		err := local.reg.SetPeerTransport(
			t.Context(), local.store.DB(), peer.id, change.endpoint, change.fingerprint,
		)
		require.ErrorIs(t, err, ErrPeerTransportMismatch)
	}

	var endpoint, pinned string
	require.NoError(t, local.store.DB().QueryRowContext(t.Context(),
		"SELECT federation_endpoint, certificate_fingerprint FROM nodes WHERE node_id = ?", peer.id).
		Scan(&endpoint, &pinned))
	require.Equal(t, "https://peer.example.invalid:7443", endpoint)
	require.Equal(t, fingerprint, pinned)
}

func TestPeerLifecyclePinsKeyAndNeverRevives(t *testing.T) {
	local := newFederationTestNode(t, "node-b")
	peer := newFederationTestNode(t, "node-a")
	peerKey, err := peer.reg.nodePublic()
	require.NoError(t, err)

	registered, err := local.reg.AddPeer(t.Context(), local.store.DB(), peer.id, "org-a", peerKey)
	require.NoError(t, err)
	require.Equal(t, AddPeerRegistered, registered.Outcome)
	unchanged, err := local.reg.AddPeer(t.Context(), local.store.DB(), peer.id, "org-other", peerKey)
	require.NoError(t, err)
	require.Equal(t, AddPeerUnchanged, unchanged.Outcome)

	otherKey, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	suspended, err := local.reg.AddPeer(t.Context(), local.store.DB(), peer.id, "org-a", otherKey)
	require.NoError(t, err)
	require.Equal(t, AddPeerSuspended, suspended.Outcome)
	require.Equal(t, PeerStatusSuspended, suspended.Status)

	require.NoError(t, local.reg.SetPeerStatus(t.Context(), local.store.DB(), peer.id, PeerStatusRevoked))
	require.NoError(t, local.reg.SetPeerStatus(t.Context(), local.store.DB(), peer.id, PeerStatusRevoked))
	err = local.reg.SetPeerStatus(t.Context(), local.store.DB(), peer.id, PeerStatusSuspended)
	require.ErrorIs(t, err, ErrPeerRevoked)
	_, err = local.reg.AddPeer(t.Context(), local.store.DB(), local.id, "org-local", peerKey)
	require.ErrorIs(t, err, ErrInvalidArgument)
}

func TestDirectoryRoundTripAndPeerRoute(t *testing.T) {
	origin := newFederationTestNode(t, "node-a")
	destination := newFederationTestNode(t, "node-b")
	trustTestPeer(t, destination, origin)
	privateKey, _ := registerTestAgent(t, origin, "agent-a")
	localRoute, err := origin.reg.Route(t.Context(), origin.store.DB(), "agent-a")
	require.NoError(t, err)
	require.Equal(t, RouteLocal, localRoute.Kind)
	missingRoute, err := destination.reg.Route(t.Context(), destination.store.DB(), "agent-missing")
	require.NoError(t, err)
	require.Equal(t, RouteNotFound, missingRoute.Kind)

	directory, err := origin.reg.ExportDirectory(t.Context(), origin.store.DB())
	require.NoError(t, err)
	require.Len(t, directory, 1)
	imported, err := destination.reg.ImportDirectory(t.Context(), destination.store.DB(), origin.id, directory)
	require.NoError(t, err)
	require.Equal(t, 1, imported.Inserted)

	route, err := destination.reg.Route(t.Context(), destination.store.DB(), "agent-a")
	require.NoError(t, err)
	require.Equal(t, RouteFederated, route.Kind)
	require.Equal(t, origin.id, route.Node)
	key, err := destination.reg.ActiveSigningKey(t.Context(), destination.store.DB(), "agent-a")
	require.NoError(t, err)
	payload := []byte("signed by the remote agent")
	require.True(t, ed25519.Verify(key.PublicKey, payload, ed25519.Sign(privateKey, payload)))

	before := countNodeKeys(t, destination.store.DB(), origin.id)
	again, err := destination.reg.ImportDirectory(t.Context(), destination.store.DB(), origin.id, directory)
	require.NoError(t, err)
	require.Equal(t, len(directory), again.Unchanged)
	require.Equal(t, before, countNodeKeys(t, destination.store.DB(), origin.id))

	otherPeerKey, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	suspended, err := destination.reg.AddPeer(
		t.Context(), destination.store.DB(), origin.id, "org-test", otherPeerKey)
	require.NoError(t, err)
	require.Equal(t, AddPeerSuspended, suspended.Outcome)
	route, err = destination.reg.Route(t.Context(), destination.store.DB(), "agent-a")
	require.NoError(t, err)
	require.Equal(t, RoutePeerBlocked, route.Kind)
	_, err = destination.reg.ActiveSigningKey(t.Context(), destination.store.DB(), "agent-a")
	require.ErrorIs(t, err, ErrNoUsableKey)
	blocked, err := destination.reg.Blocked(t.Context(), destination.store.DB(), "agent-a")
	require.NoError(t, err)
	require.True(t, blocked)
	require.NoError(t, destination.reg.SetPeerStatus(
		t.Context(), destination.store.DB(), origin.id, PeerStatusRevoked))
	route, err = destination.reg.Route(t.Context(), destination.store.DB(), "agent-a")
	require.NoError(t, err)
	require.Equal(t, RoutePeerBlocked, route.Kind)
}

func TestRouteImportedDirectoryStates(t *testing.T) {
	origin := newFederationTestNode(t, "node-a")
	destination := newFederationTestNode(t, "node-b")
	trustTestPeer(t, destination, origin)
	registerTestAgent(t, origin, "agent-route")
	records := directoryForAgent(t, origin, "agent-route")
	_, err := destination.reg.ImportDirectory(
		t.Context(), destination.store.DB(), origin.id, records,
	)
	require.NoError(t, err)

	route, err := destination.reg.Route(t.Context(), destination.store.DB(), "agent-route")
	require.NoError(t, err)
	require.Equal(t, RouteResult{Kind: RouteFederated, Node: origin.id}, route)
	missing, err := destination.reg.Route(t.Context(), destination.store.DB(), "agent-absent")
	require.NoError(t, err)
	require.Equal(t, RouteResult{Kind: RouteNotFound}, missing)

	competing := records[0]
	competing.KeyID = "ed25519:agent-route:v2"
	competing.PublicKey, _, err = ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	competing = signTestDirectoryRecord(t, origin, competing)
	result, err := destination.reg.ImportDirectory(
		t.Context(), destination.store.DB(), origin.id, []DirectoryRecord{competing},
	)
	require.NoError(t, err)
	require.Equal(t, []protocol.AgentID{"agent-route"}, result.SuspendedAgents)
	route, err = destination.reg.Route(t.Context(), destination.store.DB(), "agent-route")
	require.NoError(t, err)
	require.Equal(t, RouteResult{Kind: RouteNotFound}, route)

	require.NoError(t, destination.reg.SetPeerStatus(
		t.Context(), destination.store.DB(), origin.id, PeerStatusSuspended,
	))
	route, err = destination.reg.Route(t.Context(), destination.store.DB(), "agent-route")
	require.NoError(t, err)
	require.Equal(t, RouteResult{Kind: RoutePeerBlocked, Node: origin.id}, route)

	expiringOrigin := newFederationTestNode(t, "node-expiring")
	expiringDestination := newFederationTestNode(t, "node-reader")
	trustTestPeer(t, expiringDestination, expiringOrigin)
	registerTestAgent(t, expiringOrigin, "agent-expired")
	expired := directoryForAgent(t, expiringOrigin, "agent-expired")[0]
	validFrom, err := time.Parse(timeLayout, expired.ValidFrom)
	require.NoError(t, err)
	past := validFrom.Add(time.Nanosecond).Format(timeLayout)
	expired.ValidUntil = &past
	expired = signTestDirectoryRecord(t, expiringOrigin, expired)
	_, err = expiringDestination.reg.ImportDirectory(
		t.Context(), expiringDestination.store.DB(), expiringOrigin.id,
		[]DirectoryRecord{expired},
	)
	require.NoError(t, err)
	route, err = expiringDestination.reg.Route(
		t.Context(), expiringDestination.store.DB(), "agent-expired",
	)
	require.NoError(t, err)
	require.Equal(t, RouteResult{Kind: RouteNotFound}, route)
}

func TestImportDirectoryRejectsWholeInvalidBatch(t *testing.T) {
	makeDirectory := func(t *testing.T) (federationTestNode, []DirectoryRecord) {
		t.Helper()
		origin := newFederationTestNode(t, "node-a")
		registerTestAgent(t, origin, "agent-a")
		registerTestAgent(t, origin, "agent-b")
		directory, err := origin.reg.ExportDirectory(t.Context(), origin.store.DB())
		require.NoError(t, err)
		return origin, directory
	}

	t.Run("third node row", func(t *testing.T) {
		origin, directory := makeDirectory(t)
		destination := newFederationTestNode(t, "node-b")
		trustTestPeer(t, destination, origin)
		directory[1].Node = "node-c"
		directory[1].Issuer = "node:node-c"
		_, err := destination.reg.ImportDirectory(t.Context(), destination.store.DB(), origin.id, directory)
		require.ErrorIs(t, err, ErrInvalidArgument)
		require.Zero(t, countNodeKeys(t, destination.store.DB(), origin.id))
	})

	t.Run("bad signature", func(t *testing.T) {
		origin, directory := makeDirectory(t)
		destination := newFederationTestNode(t, "node-b")
		trustTestPeer(t, destination, origin)
		directory[1].RecordSignature[0] ^= 0xff
		_, err := destination.reg.ImportDirectory(t.Context(), destination.store.DB(), origin.id, directory)
		require.Error(t, err)
		require.Zero(t, countNodeKeys(t, destination.store.DB(), origin.id))
	})

	t.Run("local agent claimed", func(t *testing.T) {
		origin, directory := makeDirectory(t)
		destination := newFederationTestNode(t, "node-b")
		trustTestPeer(t, destination, origin)
		stamp := destination.reg.now().Format(timeLayout)
		_, err := destination.store.DB().ExecContext(t.Context(), `INSERT INTO agents
			(agent_id, node_id, harness, created_at, updated_at) VALUES (?, ?, 'test', ?, ?)`,
			directory[0].Agent, destination.id, stamp, stamp)
		require.NoError(t, err)
		_, err = destination.reg.ImportDirectory(t.Context(), destination.store.DB(), origin.id, directory)
		require.ErrorIs(t, err, ErrAgentConflict)
		require.Zero(t, countNodeKeys(t, destination.store.DB(), origin.id))
	})

	t.Run("another peer agent claimed", func(t *testing.T) {
		origin, directory := makeDirectory(t)
		other := newFederationTestNode(t, "node-c")
		registerTestAgent(t, other, directory[0].Agent)
		otherDirectory, err := other.reg.ExportDirectory(t.Context(), other.store.DB())
		require.NoError(t, err)
		destination := newFederationTestNode(t, "node-b")
		trustTestPeer(t, destination, other)
		_, err = destination.reg.ImportDirectory(t.Context(), destination.store.DB(), other.id, otherDirectory)
		require.NoError(t, err)
		trustTestPeer(t, destination, origin)
		before := countNodeKeys(t, destination.store.DB(), other.id)
		_, err = destination.reg.ImportDirectory(t.Context(), destination.store.DB(), origin.id, directory)
		require.ErrorIs(t, err, ErrAgentConflict)
		require.Equal(t, before, countNodeKeys(t, destination.store.DB(), other.id))
		require.Zero(t, countNodeKeys(t, destination.store.DB(), origin.id))
	})

	t.Run("unknown or suspended peer", func(t *testing.T) {
		origin, directory := makeDirectory(t)
		destination := newFederationTestNode(t, "node-b")
		_, err := destination.reg.ImportDirectory(t.Context(), destination.store.DB(), origin.id, directory)
		require.ErrorIs(t, err, ErrPeerNotFound)
		trustTestPeer(t, destination, origin)
		require.NoError(t, destination.reg.SetPeerStatus(
			t.Context(), destination.store.DB(), origin.id, PeerStatusSuspended))
		_, err = destination.reg.ImportDirectory(t.Context(), destination.store.DB(), origin.id, directory)
		require.ErrorIs(t, err, ErrPeerNotTrusted)
		require.Zero(t, countNodeKeys(t, destination.store.DB(), origin.id))
	})
}

func TestImportDirectoryCannotReactivateAgentOwnedByLocalNode(t *testing.T) {
	origin := newFederationTestNode(t, "node-a")
	destination := newFederationTestNode(t, "node-b")
	const agent = protocol.AgentID("agent-local-owner")
	registerTestAgent(t, origin, agent)
	registerTestAgent(t, destination, agent)
	trustTestPeer(t, destination, origin)

	localKey, err := destination.reg.ActiveSigningKey(
		t.Context(), destination.store.DB(), agent,
	)
	require.NoError(t, err)
	directory := directoryForAgent(t, origin, agent)
	require.Len(t, directory, 1)
	_, err = destination.reg.ImportDirectory(
		t.Context(), destination.store.DB(), origin.id, directory,
	)
	require.ErrorIs(t, err, ErrAgentConflict)

	after, err := destination.reg.ActiveSigningKey(t.Context(), destination.store.DB(), agent)
	require.NoError(t, err)
	require.Equal(t, localKey.KeyID, after.KeyID)
	require.Equal(t, localKey.PublicKey, after.PublicKey)
	route, err := destination.reg.Route(t.Context(), destination.store.DB(), agent)
	require.NoError(t, err)
	require.Equal(t, RouteResult{Kind: RouteLocal, Node: destination.id}, route)
	require.Zero(t, countNodeKeys(t, destination.store.DB(), origin.id))
}

func TestImportedRotationAndRevocationAdvanceOnly(t *testing.T) {
	origin := newFederationTestNode(t, "node-a")
	destination := newFederationTestNode(t, "node-b")
	trustTestPeer(t, destination, origin)

	_, process := registerTestAgent(t, origin, "agent-rotate")
	initial := directoryForAgent(t, origin, "agent-rotate")
	_, err := destination.reg.ImportDirectory(t.Context(), destination.store.DB(), origin.id, initial)
	require.NoError(t, err)
	privateKey, replacement := nextTestKey(t, "agent-rotate", process, "2")
	result, err := origin.reg.Register(t.Context(), origin.store.DB(), replacement)
	require.NoError(t, err)
	require.Equal(t, OutcomeSuspended, result.Outcome)
	require.NoError(t, origin.reg.Review(t.Context(), origin.store.DB(), "agent-rotate",
		ReviewDecision{Action: ReviewRotate, KeyID: replacement.KeyID}))
	rotated := directoryForAgent(t, origin, "agent-rotate")
	_, err = destination.reg.ImportDirectory(t.Context(), destination.store.DB(), origin.id, rotated)
	require.NoError(t, err)
	active, err := destination.reg.ActiveSigningKey(t.Context(), destination.store.DB(), "agent-rotate")
	require.NoError(t, err)
	require.Equal(t, replacement.KeyID, active.KeyID)
	payload := []byte("rotated signature")
	require.True(t, ed25519.Verify(active.PublicKey, payload, ed25519.Sign(privateKey, payload)))

	_, revokeProcess := registerTestAgent(t, origin, "agent-revoke")
	revokeInitial := directoryForAgent(t, origin, "agent-revoke")
	_, err = destination.reg.ImportDirectory(t.Context(), destination.store.DB(), origin.id, revokeInitial)
	require.NoError(t, err)
	_, rejected := nextTestKey(t, "agent-revoke", revokeProcess, "2")
	_, err = origin.reg.Register(t.Context(), origin.store.DB(), rejected)
	require.NoError(t, err)
	require.NoError(t, origin.reg.Review(t.Context(), origin.store.DB(), "agent-revoke",
		ReviewDecision{Action: ReviewRevoke}))
	revoked := directoryForAgent(t, origin, "agent-revoke")
	_, err = destination.reg.ImportDirectory(t.Context(), destination.store.DB(), origin.id, revoked)
	require.NoError(t, err)
	_, err = destination.reg.ActiveSigningKey(t.Context(), destination.store.DB(), "agent-revoke")
	require.ErrorIs(t, err, ErrNoUsableKey)
	blocked, err := destination.reg.Blocked(t.Context(), destination.store.DB(), "agent-revoke")
	require.NoError(t, err)
	require.True(t, blocked)

	_, err = destination.reg.ImportDirectory(t.Context(), destination.store.DB(), origin.id, revokeInitial)
	require.NoError(t, err)
	_, err = destination.reg.ActiveSigningKey(t.Context(), destination.store.DB(), "agent-revoke")
	require.ErrorIs(t, err, ErrNoUsableKey, "stale ACTIVE row must not revive REVOKED")
}

func TestUnannouncedRemoteSubstitutionBlocksAgent(t *testing.T) {
	origin := newFederationTestNode(t, "node-a")
	destination := newFederationTestNode(t, "node-b")
	trustTestPeer(t, destination, origin)
	registerTestAgent(t, origin, "agent-a")
	initial := directoryForAgent(t, origin, "agent-a")
	_, err := destination.reg.ImportDirectory(t.Context(), destination.store.DB(), origin.id, initial)
	require.NoError(t, err)

	publicKey, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	unannounced := initial[0]
	unannounced.KeyID = "ed25519:agent-a:v2"
	unannounced.PublicKey = publicKey
	unannounced = signTestDirectoryRecord(t, origin, unannounced)
	result, err := destination.reg.ImportDirectory(
		t.Context(), destination.store.DB(), origin.id, []DirectoryRecord{unannounced})
	require.NoError(t, err)
	require.Equal(t, []protocol.AgentID{"agent-a"}, result.SuspendedAgents)
	_, err = destination.reg.ActiveSigningKey(t.Context(), destination.store.DB(), "agent-a")
	require.ErrorIs(t, err, ErrNoUsableKey)
	blocked, err := destination.reg.Blocked(t.Context(), destination.store.DB(), "agent-a")
	require.NoError(t, err)
	require.True(t, blocked)
}

func TestConflictingRemoteKeyIDSuspendsPeer(t *testing.T) {
	origin := newFederationTestNode(t, "node-a")
	destination := newFederationTestNode(t, "node-b")
	trustTestPeer(t, destination, origin)
	registerTestAgent(t, origin, "agent-a")
	initial := directoryForAgent(t, origin, "agent-a")
	_, err := destination.reg.ImportDirectory(t.Context(), destination.store.DB(), origin.id, initial)
	require.NoError(t, err)

	conflict := initial[0]
	conflict.PublicKey, _, err = ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	conflict = signTestDirectoryRecord(t, origin, conflict)
	result, err := destination.reg.ImportDirectory(
		t.Context(), destination.store.DB(), origin.id, []DirectoryRecord{conflict})
	require.NoError(t, err)
	require.True(t, result.PeerSuspended)
	require.Equal(t, []protocol.AgentID{"agent-a"}, result.SuspendedAgents)
	require.Equal(t, int64(1), countNodeKeys(t, destination.store.DB(), origin.id))
	route, err := destination.reg.Route(t.Context(), destination.store.DB(), "agent-a")
	require.NoError(t, err)
	require.Equal(t, RoutePeerBlocked, route.Kind)
	_, err = destination.reg.ActiveSigningKey(t.Context(), destination.store.DB(), "agent-a")
	require.ErrorIs(t, err, ErrNoUsableKey)
}

func TestSetLocalPolicyVersionUpdatesOnlyLocalNode(t *testing.T) {
	local := newFederationTestNode(t, "node-a")
	peer := newFederationTestNode(t, "node-b")
	trustTestPeer(t, local, peer)

	const version = "policy-v1:0123456789ab"
	require.NoError(t, local.reg.SetLocalPolicyVersion(t.Context(), local.store.DB(), version))
	require.NoError(t, local.reg.SetLocalPolicyVersion(t.Context(), local.store.DB(), version))
	var localVersion, peerVersion string
	require.NoError(t, local.store.DB().QueryRowContext(t.Context(),
		"SELECT policy_version FROM nodes WHERE node_id = ?", local.id).Scan(&localVersion))
	require.NoError(t, local.store.DB().QueryRowContext(t.Context(),
		"SELECT policy_version FROM nodes WHERE node_id = ?", peer.id).Scan(&peerVersion))
	require.Equal(t, version, localVersion)
	require.Empty(t, peerVersion)
	err := local.reg.SetLocalPolicyVersion(t.Context(), local.store.DB(), "policy-v1:not-hex")
	require.ErrorIs(t, err, ErrInvalidArgument)
}
