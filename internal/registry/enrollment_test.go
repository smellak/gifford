package registry

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

type enrollmentClock struct{ now time.Time }

func (c *enrollmentClock) Now() time.Time { return c.now }

func newEnrollmentRegistry(t *testing.T) (*Registry, federationTestNode, *enrollmentClock) {
	t.Helper()
	clk := &enrollmentClock{now: time.Date(2026, time.October, 29, 21, 0, 0, 0, time.UTC)}
	store := testStore(t)
	reg, err := New("node-a", testDataDir(t), clk)
	require.NoError(t, err)
	require.NoError(t, reg.EnsureLocalNode(t.Context(), store.DB(), "org-a"))
	return reg, federationTestNode{id: "node-a", reg: reg, store: store}, clk
}

func testEnrollmentRequest(
	t *testing.T,
	token string,
	node protocol.NodeID,
) EnrollPeerRequest {
	t.Helper()
	publicKey, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return EnrollPeerRequest{
		Token: token, Node: node,
		SigningPublicKey: publicKey, Endpoint: "https://" + string(node) + ".example.invalid:7443",
		CertificateFingerprint: "abababababababababababababababababababababababababababababababab",
		PolicyVersion:          "policy-v1:000000000000",
	}
}

func TestEnrollmentTokenStoredAsDigestAndConsumedAtomically(t *testing.T) {
	reg, local, _ := newEnrollmentRegistry(t)
	token, err := reg.CreateEnrollmentToken(t.Context(), local.store.DB(), "org-b", EnrollmentTokenTTL)
	require.NoError(t, err)
	raw, err := base64.RawURLEncoding.DecodeString(token.Token)
	require.NoError(t, err)
	require.Len(t, raw, enrollmentTokenSize)
	digest := sha256.Sum256(raw)

	var storedHash []byte
	var storedOrg, usedBy string
	var usedAt any
	require.NoError(t, local.store.DB().QueryRowContext(t.Context(),
		"SELECT hash, org, used_at, coalesce(used_by, '') FROM enrollment_tokens").
		Scan(&storedHash, &storedOrg, &usedAt, &usedBy))
	require.Equal(t, digest[:], storedHash)
	require.Equal(t, "org-b", storedOrg)
	require.Nil(t, usedAt)
	require.NotContains(t, string(storedHash), token.Token)

	request := testEnrollmentRequest(t, token.Token, "node-b")
	result, err := reg.EnrollPeer(t.Context(), local.store.DB(), request)
	require.NoError(t, err)
	require.Equal(t, AddPeerRegistered, result.Peer.Outcome)
	require.Equal(t, "org-b", result.OrganizationID)

	var status, endpoint, fingerprint string
	require.NoError(t, local.store.DB().QueryRowContext(t.Context(), `SELECT status,
		federation_endpoint, certificate_fingerprint FROM nodes WHERE node_id = ?`, request.Node).
		Scan(&status, &endpoint, &fingerprint))
	require.Equal(t, string(PeerStatusTrusted), status)
	require.Equal(t, request.Endpoint, endpoint)
	require.Equal(t, request.CertificateFingerprint, fingerprint)
	var policyVersion string
	require.NoError(t, local.store.DB().QueryRowContext(t.Context(),
		"SELECT policy_version FROM nodes WHERE node_id = ?", request.Node).Scan(&policyVersion))
	require.Equal(t, request.PolicyVersion, policyVersion)
	require.NoError(t, reg.SetPeerPolicyVersion(t.Context(), local.store.DB(), request.Node, request.PolicyVersion))
	err = reg.SetPeerPolicyVersion(t.Context(), local.store.DB(), request.Node, "policy-v1:111111111111")
	require.ErrorIs(t, err, ErrPeerPolicyMismatch)
	require.NoError(t, local.store.DB().QueryRowContext(t.Context(),
		"SELECT used_by FROM enrollment_tokens WHERE hash = ?", digest[:]).Scan(&usedBy))
	require.Equal(t, "node-b", usedBy)

	_, err = reg.EnrollPeer(t.Context(), local.store.DB(),
		testEnrollmentRequest(t, token.Token, "node-c"))
	require.ErrorIs(t, err, ErrEnrollmentDenied)
	var nodeCount int
	require.NoError(t, local.store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM nodes WHERE node_id = 'node-c'").Scan(&nodeCount))
	require.Zero(t, nodeCount)
}

func TestEnrollmentTokenExpiryFailsWithoutWrites(t *testing.T) {
	reg, local, clk := newEnrollmentRegistry(t)
	token, err := reg.CreateEnrollmentToken(t.Context(), local.store.DB(), "org-b", EnrollmentTokenTTL)
	require.NoError(t, err)
	clk.now = clk.now.Add(EnrollmentTokenTTL)
	_, err = reg.EnrollPeer(t.Context(), local.store.DB(),
		testEnrollmentRequest(t, token.Token, "node-b"))
	require.ErrorIs(t, err, ErrEnrollmentDenied)
	var peers, used int
	require.NoError(t, local.store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM nodes WHERE local = 0").Scan(&peers))
	require.NoError(t, local.store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM enrollment_tokens WHERE used_at IS NOT NULL OR used_by IS NOT NULL").Scan(&used))
	require.Zero(t, peers)
	require.Zero(t, used)
}

func TestEnrollmentMalformedTokenIsDeniedWithoutWrites(t *testing.T) {
	reg, local, _ := newEnrollmentRegistry(t)
	_, err := reg.EnrollPeer(t.Context(), local.store.DB(),
		testEnrollmentRequest(t, "not-a-token", "node-b"))
	require.ErrorIs(t, err, ErrEnrollmentDenied)
	require.NotErrorIs(t, err, ErrInvalidArgument)
	var peers int
	require.NoError(t, local.store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM nodes WHERE local = 0").Scan(&peers))
	require.Zero(t, peers)
}

func TestEnrollmentRejectsExistingNodeWithoutConsumingToken(t *testing.T) {
	reg, local, _ := newEnrollmentRegistry(t)
	publicKey, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	_, err = reg.AddPeer(t.Context(), local.store.DB(), "node-b", "org-b", publicKey)
	require.NoError(t, err)
	token, err := reg.CreateEnrollmentToken(t.Context(), local.store.DB(), "org-b", EnrollmentTokenTTL)
	require.NoError(t, err)
	_, err = reg.EnrollPeer(t.Context(), local.store.DB(),
		testEnrollmentRequest(t, token.Token, "node-b"))
	require.ErrorIs(t, err, ErrPeerAlreadyExists)
	var used int
	require.NoError(t, local.store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM enrollment_tokens WHERE used_at IS NOT NULL OR used_by IS NOT NULL").Scan(&used))
	require.Zero(t, used)
}

func TestPeersReturnsStablePublicView(t *testing.T) {
	reg, local, _ := newEnrollmentRegistry(t)
	for _, node := range []protocol.NodeID{"node-c", "node-b"} {
		publicKey, _, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err)
		_, err = reg.AddPeer(t.Context(), local.store.DB(), node, "org-"+string(node), publicKey)
		require.NoError(t, err)
	}
	peers, err := reg.Peers(t.Context(), local.store.DB())
	require.NoError(t, err)
	require.Len(t, peers, 2)
	require.Equal(t, protocol.NodeID("node-b"), peers[0].Node)
	require.Equal(t, protocol.NodeID("node-c"), peers[1].Node)
}
