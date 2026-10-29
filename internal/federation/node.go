package federation

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

const (
	joinTimeout          = 15 * time.Second
	federationHTTPScheme = "https"
)

// ErrEnrollmentRejected means the pinned remote node refused enrollment. It
// intentionally does not expose whether the token was unknown, malformed,
// expired, or used.
var ErrEnrollmentRejected = errors.New("enrollment rejected")

// NodeManagerOptions configures node operations served over the local control
// socket and the remote /v1/enroll endpoint.
type NodeManagerOptions struct {
	DB            *sql.DB
	Registry      *registry.Registry
	Bundle        *TrustBundle
	Identity      *NodeTLS
	Clock         clock.Clock
	Listen        string
	Endpoint      string
	PolicyVersion string
}

// NodeManager is the hub-owned implementation of node enrollment operations.
type NodeManager struct {
	db            *sql.DB
	registry      *registry.Registry
	bundle        *TrustBundle
	identity      *NodeTLS
	clock         clock.Clock
	nodeID        protocol.NodeID
	listen        string
	endpoint      string
	policyVersion string
	enrollment    *EnrollmentManager
}

// NewNodeManager validates the complete local federation identity.
func NewNodeManager(options NodeManagerOptions) (*NodeManager, error) {
	if options.DB == nil || options.Registry == nil || options.Clock == nil ||
		!validEnrollmentPolicyVersion(options.PolicyVersion) {
		return nil, fmt.Errorf("new node manager: incomplete options")
	}
	configured := options.Listen != "" || options.Endpoint != "" ||
		options.Identity != nil || options.Bundle != nil
	var enrollment *EnrollmentManager
	if configured {
		if options.Listen == "" || options.Identity == nil || options.Bundle == nil ||
			!validEnrollmentEndpoint(options.Endpoint) {
			return nil, fmt.Errorf("new node manager: incomplete federation options")
		}
		nodeID, err := nodeIDFromCertificate(options.Identity.Leaf)
		if err != nil {
			return nil, fmt.Errorf("new node manager: %w", err)
		}
		if nodeID != options.Registry.NodeID() {
			return nil, fmt.Errorf("new node manager: TLS identity and registry node differ")
		}
		enrollment, err = NewEnrollmentManager(EnrollmentOptions{
			DB: options.DB, Registry: options.Registry, Bundle: options.Bundle,
			Identity: options.Identity, Endpoint: options.Endpoint,
		})
		if err != nil {
			return nil, err
		}
	}
	return &NodeManager{
		db: options.DB, registry: options.Registry, bundle: options.Bundle,
		identity: options.Identity, clock: options.Clock, nodeID: options.Registry.NodeID(),
		listen: options.Listen, endpoint: options.Endpoint, policyVersion: options.PolicyVersion,
		enrollment: enrollment,
	}, nil
}

// Enrollment returns the server-side HTTP enrollment operation.
func (m *NodeManager) Enrollment() *EnrollmentManager { return m.enrollment }

// EnrollToken creates a one-use token and returns the local identity an
// operator must pass to the joining node out of band.
func (m *NodeManager) EnrollToken(
	ctx context.Context,
	params protocol.NodeEnrollTokenParams,
) (protocol.NodeEnrollTokenResult, error) {
	localOrganization, err := m.registry.LocalOrganizationID(ctx, m.db)
	if err != nil {
		return protocol.NodeEnrollTokenResult{}, fmt.Errorf("read local enrollment state: %w", err)
	}
	if localOrganization == "" {
		return protocol.NodeEnrollTokenResult{}, registry.ErrNodeUnenrolled
	}
	if !m.configured() {
		return protocol.NodeEnrollTokenResult{}, registry.ErrFederationUnavailable
	}
	organizationID := params.OrganizationID
	if organizationID == "" {
		organizationID = localOrganization
	}
	ttl := registry.EnrollmentTokenTTL
	if params.TTLMS != nil {
		ttl = time.Duration(*params.TTLMS) * time.Millisecond
	}
	token, err := m.registry.CreateEnrollmentToken(ctx, m.db, organizationID, ttl)
	if err != nil {
		return protocol.NodeEnrollTokenResult{}, fmt.Errorf("create enrollment token: %w", err)
	}
	return protocol.NodeEnrollTokenResult{
		Token: token.Token, NodeID: m.nodeID, Endpoint: m.endpoint,
		Fingerprint: protocol.SHA256Hex(m.identity.Fingerprint),
		ExpiresAt:   token.ExpiresAt.Format(time.RFC3339Nano),
	}, nil
}

// Join contacts a pinned accepting node, then atomically pins its identity in
// the local registry and live trust bundle.
func (m *NodeManager) Join(
	ctx context.Context,
	params protocol.NodeJoinParams,
) (protocol.NodeJoinResult, error) {
	localOrganization, err := m.registry.LocalOrganizationID(ctx, m.db)
	if err != nil {
		return protocol.NodeJoinResult{}, fmt.Errorf("read local enrollment state: %w", err)
	}
	if localOrganization != "" {
		return protocol.NodeJoinResult{}, registry.ErrNodeAlreadyEnrolled
	}
	if !m.configured() {
		return protocol.NodeJoinResult{}, registry.ErrFederationUnavailable
	}
	target, err := enrollmentURL(params.URL)
	if err != nil {
		return protocol.NodeJoinResult{}, err
	}
	config, observed, err := m.enrollmentClientTLSConfig(string(params.Fingerprint))
	if err != nil {
		return protocol.NodeJoinResult{}, err
	}
	publicKey, err := m.registry.NodeSigningPublicKey()
	if err != nil {
		return protocol.NodeJoinResult{}, fmt.Errorf("read local node signing key: %w", err)
	}
	body, err := json.Marshal(EnrollmentRequest{
		Token: params.Token, SigningPublicKey: publicKey, Endpoint: m.endpoint,
		PolicyVersion: m.policyVersion,
	})
	if err != nil {
		return protocol.NodeJoinResult{}, fmt.Errorf("encode enrollment request: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target,
		io.NopCloser(bytes.NewReader(body)))
	if err != nil {
		return protocol.NodeJoinResult{}, fmt.Errorf("build enrollment request: %w", err)
	}
	request.ContentLength = int64(len(body))
	request.Header.Set("Content-Type", "application/json")
	client := &http.Client{
		Transport: &http.Transport{TLSClientConfig: config, DisableKeepAlives: true},
		Timeout:   joinTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	response, err := client.Do(request)
	if err != nil {
		return protocol.NodeJoinResult{}, fmt.Errorf("join pinned node: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode == http.StatusForbidden {
		return protocol.NodeJoinResult{}, ErrEnrollmentRejected
	}
	if response.StatusCode != http.StatusOK {
		return protocol.NodeJoinResult{}, fmt.Errorf("join pinned node returned HTTP %d", response.StatusCode)
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, maxEnrollmentBody+1))
	decoder.DisallowUnknownFields()
	var accepted EnrollmentResponse
	if err := decoder.Decode(&accepted); err != nil {
		return protocol.NodeJoinResult{}, fmt.Errorf("decode enrollment response: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return protocol.NodeJoinResult{}, fmt.Errorf("decode enrollment response: %w", err)
	}
	serverCert := observed()
	if serverCert == nil {
		return protocol.NodeJoinResult{}, fmt.Errorf("pinned server certificate was not observed")
	}
	serverNode, err := nodeIDFromCertificate(serverCert)
	if err != nil || accepted.Node != serverNode || accepted.Node == m.nodeID ||
		!validEnrollmentOrganization(accepted.OrganizationID) ||
		!validEnrollmentOrganization(accepted.AssignedOrganizationID) ||
		len(accepted.SigningPublicKey) != ed25519.PublicKeySize ||
		!validEnrollmentEndpoint(accepted.Endpoint) ||
		!validEnrollmentPolicyVersion(accepted.PolicyVersion) ||
		!fingerprintsEqual(string(accepted.Fingerprint), Fingerprint(serverCert)) {
		return protocol.NodeJoinResult{}, fmt.Errorf("pinned server returned an invalid enrollment identity")
	}
	peer, err := m.registerAcceptedPeer(ctx, serverCert, accepted)
	if err != nil {
		return protocol.NodeJoinResult{}, err
	}
	return protocol.NodeJoinResult{Peer: peer}, nil
}

// Peers returns the stable public peer list.
func (m *NodeManager) Peers(ctx context.Context) (protocol.NodePeersResult, error) {
	peers, err := m.registry.Peers(ctx, m.db)
	if err != nil {
		return protocol.NodePeersResult{}, fmt.Errorf("list registry peers: %w", err)
	}
	result := protocol.NodePeersResult{Peers: make([]protocol.NodePeer, 0, len(peers))}
	for _, peer := range peers {
		result.Peers = append(result.Peers, protocolPeer(peer))
	}
	return result, nil
}

// Revoke makes peer trust terminal and removes its certificate from disk and
// the live verification pool before returning.
func (m *NodeManager) Revoke(
	ctx context.Context,
	params protocol.NodeRevokeParams,
) (protocol.NodeRevokeResult, error) {
	localOrganization, err := m.registry.LocalOrganizationID(ctx, m.db)
	if err != nil {
		return protocol.NodeRevokeResult{}, fmt.Errorf("read local enrollment state: %w", err)
	}
	if localOrganization == "" {
		return protocol.NodeRevokeResult{}, registry.ErrNodeUnenrolled
	}
	if !m.configured() {
		return protocol.NodeRevokeResult{}, registry.ErrFederationUnavailable
	}
	peer, err := m.registry.Peer(ctx, m.db, params.NodeID)
	if err != nil {
		return protocol.NodeRevokeResult{}, fmt.Errorf("read peer for revocation: %w", err)
	}
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return protocol.NodeRevokeResult{}, fmt.Errorf("begin peer revocation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := m.registry.SetPeerStatus(ctx, tx, params.NodeID, registry.PeerStatusRevoked); err != nil {
		return protocol.NodeRevokeResult{}, fmt.Errorf("revoke registry peer: %w", err)
	}
	removed, err := m.bundle.RemoveCertificate(params.NodeID)
	if err != nil {
		return protocol.NodeRevokeResult{}, err
	}
	if err := tx.Commit(); err != nil {
		restoreErr := m.bundle.restoreCertificates(removed)
		return protocol.NodeRevokeResult{}, errors.Join(fmt.Errorf("commit peer revocation: %w", err), restoreErr)
	}
	peer.Status = registry.PeerStatusRevoked
	return protocol.NodeRevokeResult{Peer: protocolPeer(peer)}, nil
}

func (m *NodeManager) configured() bool {
	return m.listen != "" && m.endpoint != "" && m.identity != nil && m.bundle != nil &&
		m.enrollment != nil
}

// NewReplicator builds the hub's authenticated delivery worker from the same
// pinned identity and trust bundle used by this node manager.
func (m *NodeManager) NewReplicator(
	leaseOwner string,
	alert func(ReplicatorAlert),
) (*Replicator, error) {
	if !m.configured() {
		return nil, registry.ErrFederationUnavailable
	}
	return NewReplicator(ReplicatorOptions{
		DB: m.db, Registry: m.registry, Identity: m.identity, TrustBundle: m.bundle,
		Clock: m.clock, LeaseOwner: leaseOwner, Alert: alert,
	})
}

func (m *NodeManager) enrollmentClientTLSConfig(
	pinnedFingerprint string,
) (*tls.Config, func() *x509.Certificate, error) {
	if !validFingerprint(pinnedFingerprint) {
		return nil, nil, fmt.Errorf("invalid enrollment certificate fingerprint")
	}
	var observed *x509.Certificate
	config := &tls.Config{
		MinVersion:         tls.VersionTLS13,
		Certificates:       []tls.Certificate{m.identity.Certificate},
		InsecureSkipVerify: true, //nolint:gosec // Exact out-of-band pin replaces DNS verification.
		VerifyConnection: func(state tls.ConnectionState) error {
			nodeID, cert, err := verifyEnrollmentCertificate(state.PeerCertificates, m.clock.Now().UTC())
			if err != nil {
				return fmt.Errorf("verify enrollment server certificate: %w", err)
			}
			if nodeID == m.nodeID {
				return fmt.Errorf("enrollment server identifies the local node")
			}
			if !fingerprintsEqual(pinnedFingerprint, Fingerprint(cert)) {
				return fmt.Errorf("enrollment server certificate fingerprint does not match pin")
			}
			observed = cert
			return nil
		},
	}
	return config, func() *x509.Certificate { return observed }, nil
}

func (m *NodeManager) registerAcceptedPeer(
	ctx context.Context,
	cert *x509.Certificate,
	accepted EnrollmentResponse,
) (protocol.NodePeer, error) {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return protocol.NodePeer{}, fmt.Errorf("begin accepted peer transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := m.registry.EnrollLocalNode(
		ctx, tx, accepted.AssignedOrganizationID, m.policyVersion,
	); err != nil {
		return protocol.NodePeer{}, fmt.Errorf("publish local enrollment: %w", err)
	}
	_, err = m.registry.RegisterEnrolledPeer(ctx, tx, registry.PeerEnrollment{
		OrganizationID: accepted.OrganizationID, Node: accepted.Node,
		SigningPublicKey: accepted.SigningPublicKey, Endpoint: accepted.Endpoint,
		CertificateFingerprint: Fingerprint(cert), PolicyVersion: accepted.PolicyVersion,
	})
	if err != nil {
		return protocol.NodePeer{}, fmt.Errorf("register accepting peer: %w", err)
	}
	if err := m.bundle.AddCertificate(cert); err != nil {
		return protocol.NodePeer{}, fmt.Errorf("add accepting certificate: %w", err)
	}
	if err := tx.Commit(); err != nil {
		_, removeErr := m.bundle.RemoveCertificate(accepted.Node)
		return protocol.NodePeer{}, errors.Join(fmt.Errorf("commit accepted peer: %w", err), removeErr)
	}
	return protocol.NodePeer{
		NodeID: accepted.Node, OrganizationID: accepted.OrganizationID,
		Status: string(registry.PeerStatusTrusted), Endpoint: accepted.Endpoint,
		Fingerprint: protocol.SHA256Hex(Fingerprint(cert)), PolicyVersion: accepted.PolicyVersion,
	}, nil
}

func protocolPeer(peer registry.Peer) protocol.NodePeer {
	return protocol.NodePeer{
		NodeID: peer.Node, OrganizationID: peer.OrganizationID, Status: string(peer.Status),
		Endpoint: peer.Endpoint, Fingerprint: protocol.SHA256Hex(peer.CertificateFingerprint),
		PolicyVersion: peer.PolicyVersion,
	}
}

func enrollmentURL(raw string) (string, error) {
	parsed, err := url.ParseRequestURI(raw)
	if err != nil || parsed.Scheme != federationHTTPScheme || parsed.Host == "" || parsed.User != nil ||
		parsed.Fragment != "" || parsed.RawQuery != "" {
		return "", fmt.Errorf("invalid enrollment URL: %w", registry.ErrInvalidArgument)
	}
	parsed.Path = strings.TrimSuffix(parsed.Path, "/") + "/v1/enroll"
	return parsed.String(), nil
}

func validEnrollmentPolicyVersion(version string) bool {
	const prefix = "policy-v1:"
	digest := strings.TrimPrefix(version, prefix)
	if digest == version || len(digest) != 12 {
		return false
	}
	for _, character := range digest {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return false
		}
	}
	return true
}
