package federation

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

const (
	enrollmentTestOrgB          = "org-b"
	enrollmentTestPolicyVersion = "policy-v1:000000000000"
)

type mutableFederationClock struct{ now time.Time }

func (c *mutableFederationClock) Now() time.Time { return c.now }

type enrollmentTestNode struct {
	id        protocol.NodeID
	dataDir   string
	bundleDir string
	identity  *NodeTLS
	store     *eventstore.Store
	registry  *registry.Registry
	bundle    *TrustBundle
	manager   *NodeManager
	clock     *mutableFederationClock
}

func newEnrollmentTestNode(
	t *testing.T,
	id protocol.NodeID,
	organizationID string,
) enrollmentTestNode {
	t.Helper()
	dataDir := privateDataDir(t)
	identity, err := EnsureNodeTLS(dataDir, id)
	require.NoError(t, err)
	store, err := eventstore.Open(t.Context(), filepath.Join(dataDir, "gifford.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	clk := &mutableFederationClock{now: time.Date(2026, time.October, 29, 21, 0, 0, 0, time.UTC)}
	reg, err := registry.New(id, dataDir, clk)
	require.NoError(t, err)
	require.NoError(t, reg.EnsureLocalNode(t.Context(), store.DB(), organizationID))
	require.NoError(t, reg.SetLocalPolicyVersion(t.Context(), store.DB(), enrollmentTestPolicyVersion))
	bundleDir := filepath.Join(dataDir, "peers")
	require.NoError(t, os.Mkdir(bundleDir, 0o700))
	bundle, err := LoadTrustBundle(bundleDir)
	require.NoError(t, err)
	manager, err := NewNodeManager(NodeManagerOptions{
		DB: store.DB(), Registry: reg, Bundle: bundle, Identity: identity, Clock: clk,
		Listen:        loopbackEphemeral,
		Endpoint:      "https://" + string(id) + ".example.invalid:7443",
		PolicyVersion: enrollmentTestPolicyVersion,
	})
	require.NoError(t, err)
	return enrollmentTestNode{
		id: id, dataDir: dataDir, bundleDir: bundleDir, identity: identity,
		store: store, registry: reg, bundle: bundle, manager: manager, clock: clk,
	}
}

func startEnrollmentServer(t *testing.T, node enrollmentTestNode) string {
	t.Helper()
	server, err := NewServer(ServerOptions{
		Listen: loopbackEphemeral, Identity: node.identity, TrustBundle: node.bundle,
		DB: node.store.DB(), Clock: node.clock, Enrollment: node.manager.Enrollment(),
	})
	require.NoError(t, err)
	address, err := server.Listen(t.Context())
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		require.NoError(t, <-done)
	})
	return "https://" + address
}

func TestEnrollmentJoinsTwoNodesAndRevokesLive(t *testing.T) {
	nodeA := newEnrollmentTestNode(t, "node-a", "org-a")
	nodeB := newEnrollmentTestNode(t, "node-b", "")
	urlA := startEnrollmentServer(t, nodeA)

	token, err := nodeA.manager.EnrollToken(t.Context(), protocol.NodeEnrollTokenParams{OrganizationID: enrollmentTestOrgB})
	require.NoError(t, err)
	require.NotEmpty(t, token.Token)
	assertSecretAbsentFromFiles(t, nodeA.dataDir, token.Token)
	joined, err := nodeB.manager.Join(t.Context(), protocol.NodeJoinParams{
		URL: urlA, Token: token.Token, Fingerprint: protocol.SHA256Hex(nodeA.identity.Fingerprint),
	})
	require.NoError(t, err)
	assertSecretAbsentFromFiles(t, nodeA.dataDir, token.Token)
	assertSecretAbsentFromFiles(t, nodeB.dataDir, token.Token)
	require.Equal(t, nodeA.id, joined.Peer.NodeID)
	require.Equal(t, "org-a", joined.Peer.OrganizationID)
	organizationID, err := nodeB.registry.LocalOrganizationID(t.Context(), nodeB.store.DB())
	require.NoError(t, err)
	require.Equal(t, enrollmentTestOrgB, organizationID)
	localEnrollment, err := nodeB.registry.LocalEnrollment(t.Context(), nodeB.store.DB())
	require.NoError(t, err)
	require.Equal(t, enrollmentTestPolicyVersion, localEnrollment.PolicyVersion)
	urlB := startEnrollmentServer(t, nodeB)

	peersA, err := nodeA.manager.Peers(t.Context())
	require.NoError(t, err)
	peersB, err := nodeB.manager.Peers(t.Context())
	require.NoError(t, err)
	require.Equal(t, "node-b", string(peersA.Peers[0].NodeID))
	require.Equal(t, "TRUSTED", peersA.Peers[0].Status)
	require.Equal(t, enrollmentTestPolicyVersion, peersA.Peers[0].PolicyVersion)
	require.Equal(t, "node-a", string(peersB.Peers[0].NodeID))
	require.Equal(t, "TRUSTED", peersB.Peers[0].Status)
	require.Equal(t, enrollmentTestPolicyVersion, peersB.Peers[0].PolicyVersion)
	require.True(t, nodeA.bundle.ContainsNode(nodeB.id))
	require.True(t, nodeB.bundle.ContainsNode(nodeA.id))

	require.Equal(t, http.StatusNotImplemented, authenticatedStatus(t, nodeA, nodeB, urlA))
	require.Equal(t, http.StatusNotImplemented, authenticatedStatus(t, nodeB, nodeA, urlB))

	duplicate := newEnrollmentTestNode(t, "node-c", "")
	_, err = duplicate.manager.Join(t.Context(), protocol.NodeJoinParams{
		URL: urlA, Token: token.Token, Fingerprint: protocol.SHA256Hex(nodeA.identity.Fingerprint),
	})
	require.ErrorIs(t, err, ErrEnrollmentRejected)
	duplicatePeers, err := duplicate.manager.Peers(t.Context())
	require.NoError(t, err)
	require.Empty(t, duplicatePeers.Peers)

	revoked, err := nodeA.manager.Revoke(t.Context(), protocol.NodeRevokeParams{NodeID: nodeB.id})
	require.NoError(t, err)
	require.Equal(t, "REVOKED", revoked.Peer.Status)
	require.False(t, nodeA.bundle.ContainsNode(nodeB.id))
	require.Equal(t, http.StatusForbidden, authenticatedStatus(t, nodeA, nodeB, urlA))
}

func TestEnrollmentRejectsExpiredAndDuplicateNode(t *testing.T) {
	t.Run("expired", func(t *testing.T) {
		nodeA := newEnrollmentTestNode(t, "node-a", "org-a")
		nodeB := newEnrollmentTestNode(t, "node-b", "")
		urlA := startEnrollmentServer(t, nodeA)
		token, err := nodeA.manager.EnrollToken(t.Context(), protocol.NodeEnrollTokenParams{
			OrganizationID: enrollmentTestOrgB,
		})
		require.NoError(t, err)
		nodeA.clock.now = nodeA.clock.now.Add(registry.EnrollmentTokenTTL)
		_, err = nodeB.manager.Join(t.Context(), protocol.NodeJoinParams{
			URL: urlA, Token: token.Token, Fingerprint: protocol.SHA256Hex(nodeA.identity.Fingerprint),
		})
		require.ErrorIs(t, err, ErrEnrollmentRejected)
		assertNoPeerWrites(t, nodeA, nodeB)
	})

	t.Run("malformed token", func(t *testing.T) {
		nodeA := newEnrollmentTestNode(t, "node-a", "org-a")
		nodeB := newEnrollmentTestNode(t, "node-b", "")
		urlA := startEnrollmentServer(t, nodeA)
		_, err := nodeB.manager.Join(t.Context(), protocol.NodeJoinParams{
			URL: urlA, Token: "not-a-token",
			Fingerprint: protocol.SHA256Hex(nodeA.identity.Fingerprint),
		})
		require.ErrorIs(t, err, ErrEnrollmentRejected)
		assertNoPeerWrites(t, nodeA, nodeB)
	})

	nodeA := newEnrollmentTestNode(t, "node-a", "org-a")
	firstB := newEnrollmentTestNode(t, "node-b", "")
	urlA := startEnrollmentServer(t, nodeA)
	firstToken, err := nodeA.manager.EnrollToken(t.Context(), protocol.NodeEnrollTokenParams{OrganizationID: enrollmentTestOrgB})
	require.NoError(t, err)
	_, err = firstB.manager.Join(t.Context(), protocol.NodeJoinParams{
		URL: urlA, Token: firstToken.Token, Fingerprint: protocol.SHA256Hex(nodeA.identity.Fingerprint),
	})
	require.NoError(t, err)
	duplicateB := newEnrollmentTestNode(t, "node-b", "")
	secondToken, err := nodeA.manager.EnrollToken(t.Context(), protocol.NodeEnrollTokenParams{OrganizationID: enrollmentTestOrgB})
	require.NoError(t, err)
	_, err = duplicateB.manager.Join(t.Context(), protocol.NodeJoinParams{
		URL: urlA, Token: secondToken.Token, Fingerprint: protocol.SHA256Hex(nodeA.identity.Fingerprint),
	})
	require.ErrorIs(t, err, ErrEnrollmentRejected)
	var unused int
	require.NoError(t, nodeA.store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM enrollment_tokens WHERE used_at IS NULL").Scan(&unused))
	require.Equal(t, 1, unused)
}

func TestJoinBadPinSendsNoToken(t *testing.T) {
	serverNode := newEnrollmentTestNode(t, "node-a", "org-a")
	joiningNode := newEnrollmentTestNode(t, "node-b", "")
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", loopbackEphemeral)
	require.NoError(t, err)
	var received atomic.Bool
	server := &http.Server{Handler: http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		received.Store(true)
		_, _ = io.Copy(io.Discard, request.Body)
		response.WriteHeader(http.StatusInternalServerError)
	})}
	tlsListener := tls.NewListener(listener, &tls.Config{
		MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{serverNode.identity.Certificate},
		ClientAuth: tls.RequestClientCert,
	})
	done := make(chan error, 1)
	go func() { done <- server.Serve(tlsListener) }()
	t.Cleanup(func() {
		require.NoError(t, server.Close())
		err := <-done
		require.ErrorIs(t, err, http.ErrServerClosed)
	})

	_, err = joiningNode.manager.Join(t.Context(), protocol.NodeJoinParams{
		URL: "https://" + listener.Addr().String(), Token: "must-not-be-sent",
		Fingerprint: protocol.SHA256Hex(strings.Repeat("cd", 32)),
	})
	require.Error(t, err)
	require.False(t, received.Load())
}

func TestNodeManagerEnrollmentStateGates(t *testing.T) {
	t.Parallel()

	unenrolled := newEnrollmentTestNode(t, "node-b", "")
	_, err := unenrolled.manager.EnrollToken(t.Context(), protocol.NodeEnrollTokenParams{})
	require.ErrorIs(t, err, registry.ErrNodeUnenrolled)
	_, err = unenrolled.manager.Revoke(t.Context(), protocol.NodeRevokeParams{NodeID: "node-a"})
	require.ErrorIs(t, err, registry.ErrNodeUnenrolled)
	peers, err := unenrolled.manager.Peers(t.Context())
	require.NoError(t, err)
	require.Empty(t, peers.Peers)

	enrolled := newEnrollmentTestNode(t, "node-a", "org-a")
	_, err = enrolled.manager.Join(t.Context(), protocol.NodeJoinParams{})
	require.ErrorIs(t, err, registry.ErrNodeAlreadyEnrolled)

	minimal, err := NewNodeManager(NodeManagerOptions{
		DB: unenrolled.store.DB(), Registry: unenrolled.registry, Clock: unenrolled.clock,
		PolicyVersion: enrollmentTestPolicyVersion,
	})
	require.NoError(t, err)
	_, err = minimal.Join(t.Context(), protocol.NodeJoinParams{})
	require.ErrorIs(t, err, registry.ErrFederationUnavailable)
}

func TestEnrollmentURLUsesAbsoluteQueryFreeBase(t *testing.T) {
	t.Parallel()

	got, err := enrollmentURL("https://node-a.example.invalid/federation/")
	require.NoError(t, err)
	require.Equal(t, "https://node-a.example.invalid/federation/v1/enroll", got)
	_, err = enrollmentURL("https://node-a.example.invalid?secret=no")
	require.Error(t, err)
}

func authenticatedStatus(
	t *testing.T,
	server enrollmentTestNode,
	client enrollmentTestNode,
	baseURL string,
) int {
	t.Helper()
	config, err := NewClientTLSConfig(t.Context(), ClientOptions{
		Identity: client.identity, TrustBundle: client.bundle,
		DB: client.store.DB(), TargetNode: server.id,
	})
	require.NoError(t, err)
	return requestStatus(t, testHTTPClient(config), http.MethodPost, baseURL+"/v1/deliver")
}

func assertNoPeerWrites(t *testing.T, nodes ...enrollmentTestNode) {
	t.Helper()
	for _, node := range nodes {
		var peers int
		require.NoError(t, node.store.DB().QueryRowContext(t.Context(),
			"SELECT count(*) FROM nodes WHERE local = 0").Scan(&peers))
		require.Zero(t, peers)
	}
}

func assertSecretAbsentFromFiles(t *testing.T, root, secret string) {
	t.Helper()
	require.NoError(t, filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		require.NotContains(t, string(raw), secret, path)
		return nil
	}))
}
