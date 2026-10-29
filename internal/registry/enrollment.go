package registry

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"github.com/smellak/gifford/internal/protocol"
)

const (
	// EnrollmentTokenTTL is both the default and the longest lifetime a
	// caller may request for an enrollment token.
	EnrollmentTokenTTL  = 15 * time.Minute
	enrollmentTokenSize = 32
)

// EnrollmentToken is the one-time secret returned by CreateEnrollmentToken.
// Token is never written to storage; callers must display it exactly once.
type EnrollmentToken struct {
	Token     string
	CreatedAt time.Time
	ExpiresAt time.Time
}

// EnrollPeerRequest is the identity and transport a joining node presents.
type EnrollPeerRequest struct {
	Token                  string
	Node                   protocol.NodeID
	SigningPublicKey       ed25519.PublicKey
	Endpoint               string
	CertificateFingerprint string
	PolicyVersion          string
}

// EnrollPeerResult reports both the registered peer and the organization
// assigned by the one-use token. The joining node learns that assignment only
// from the accepting node's authenticated response.
type EnrollPeerResult struct {
	Peer           AddPeerResult
	OrganizationID string
}

// PeerEnrollment is a peer identity already authenticated by an enrollment
// exchange. The joining side uses it to persist the accepting node without a
// second local token.
type PeerEnrollment struct {
	OrganizationID         string
	Node                   protocol.NodeID
	SigningPublicKey       ed25519.PublicKey
	Endpoint               string
	CertificateFingerprint string
	PolicyVersion          string
}

// Peer is the public registry view used by `gifford node peers`.
type Peer struct {
	Node                   protocol.NodeID
	OrganizationID         string
	Status                 PeerStatus
	Endpoint               string
	CertificateFingerprint string
	PolicyVersion          string
}

// NodeID returns this registry's immutable local node identity.
func (r *Registry) NodeID() protocol.NodeID { return r.nodeID }

// CreateEnrollmentToken creates 32 random bytes, stores only their SHA-256
// digest, and returns the base64url secret once. The injected registry clock
// drives both token creation and expiry.
func (r *Registry) CreateEnrollmentToken(
	ctx context.Context,
	ex Execer,
	organizationID string,
	ttl time.Duration,
) (EnrollmentToken, error) {
	if !validOrganizationID(organizationID) || ttl <= 0 || ttl > EnrollmentTokenTTL {
		return EnrollmentToken{}, fmt.Errorf("create enrollment token: %w", ErrInvalidArgument)
	}
	raw := make([]byte, enrollmentTokenSize)
	if _, err := rand.Read(raw); err != nil {
		return EnrollmentToken{}, fmt.Errorf("generate enrollment token: %w", err)
	}
	plaintext := base64.RawURLEncoding.EncodeToString(raw)
	digest := sha256.Sum256(raw)
	createdAt := r.now()
	expiresAt := createdAt.Add(ttl)
	text, err := queryText("enrollment_token_insert.sql")
	if err != nil {
		return EnrollmentToken{}, err
	}
	if err := withTx(ctx, ex, func(tx Execer) error {
		if _, err := tx.ExecContext(ctx, text, digest[:], organizationID,
			createdAt.Format(timeLayout), expiresAt.Format(timeLayout)); err != nil {
			return fmt.Errorf("insert enrollment token digest: %w", err)
		}
		return nil
	}); err != nil {
		return EnrollmentToken{}, err
	}
	return EnrollmentToken{Token: plaintext, CreatedAt: createdAt, ExpiresAt: expiresAt}, nil
}

// EnrollPeer atomically consumes a valid token and pins a previously unknown
// peer's signing key and transport identity. Any failure rolls every database
// write back, including the token consumption.
func (r *Registry) EnrollPeer(
	ctx context.Context,
	ex Execer,
	req EnrollPeerRequest,
) (EnrollPeerResult, error) {
	digest, err := enrollmentTokenDigest(req.Token)
	if err != nil {
		return EnrollPeerResult{}, fmt.Errorf("enroll peer: %w", err)
	}
	if !req.Node.Valid() || req.Node == r.nodeID ||
		len(req.SigningPublicKey) != ed25519.PublicKeySize ||
		!validFederationEndpoint(req.Endpoint) ||
		!validCertificateFingerprint(req.CertificateFingerprint) ||
		!validPolicyVersion(req.PolicyVersion) {
		return EnrollPeerResult{}, fmt.Errorf("enroll peer: %w", ErrInvalidArgument)
	}
	var result EnrollPeerResult
	err = withTx(ctx, ex, func(tx Execer) error {
		organizationID, err := r.consumeEnrollmentToken(ctx, tx, digest, req.Node)
		if err != nil {
			return err
		}
		peer, err := r.registerEnrolledPeer(ctx, tx, PeerEnrollment{
			OrganizationID: organizationID, Node: req.Node,
			SigningPublicKey: req.SigningPublicKey, Endpoint: req.Endpoint,
			CertificateFingerprint: req.CertificateFingerprint, PolicyVersion: req.PolicyVersion,
		})
		result = EnrollPeerResult{Peer: peer, OrganizationID: organizationID}
		return err
	})
	if err != nil {
		return EnrollPeerResult{}, err
	}
	return result, nil
}

// RegisterEnrolledPeer atomically pins a previously unknown peer after the
// remote side has accepted this node's token. Any existing node id is a
// conflict, even if all presented values match.
func (r *Registry) RegisterEnrolledPeer(
	ctx context.Context,
	ex Execer,
	req PeerEnrollment,
) (AddPeerResult, error) {
	if !r.validPeerEnrollment(req) {
		return AddPeerResult{}, fmt.Errorf("register enrolled peer: %w", ErrInvalidArgument)
	}
	var result AddPeerResult
	err := withTx(ctx, ex, func(tx Execer) error {
		var registerErr error
		result, registerErr = r.registerEnrolledPeer(ctx, tx, req)
		return registerErr
	})
	if err != nil {
		return AddPeerResult{}, err
	}
	return result, nil
}

func (r *Registry) registerEnrolledPeer(
	ctx context.Context,
	tx Execer,
	req PeerEnrollment,
) (AddPeerResult, error) {
	if _, err := nodeByID(ctx, tx, req.Node); err == nil {
		return AddPeerResult{}, fmt.Errorf("enroll peer %s: %w", req.Node, ErrPeerAlreadyExists)
	} else if !errors.Is(err, sql.ErrNoRows) {
		return AddPeerResult{}, err
	}
	result, err := r.AddPeer(ctx, tx, req.Node, req.OrganizationID, req.SigningPublicKey)
	if err != nil {
		return AddPeerResult{}, err
	}
	if result.Outcome != AddPeerRegistered {
		return AddPeerResult{}, fmt.Errorf("enroll peer %s was not newly registered: %w", req.Node, ErrPeerAlreadyExists)
	}
	if err := r.SetPeerTransport(ctx, tx, req.Node, req.Endpoint, req.CertificateFingerprint); err != nil {
		return AddPeerResult{}, err
	}
	if err := r.SetPeerPolicyVersion(ctx, tx, req.Node, req.PolicyVersion); err != nil {
		return AddPeerResult{}, err
	}
	return result, nil
}

func (r *Registry) validPeerEnrollment(req PeerEnrollment) bool {
	return validOrganizationID(req.OrganizationID) && req.Node.Valid() && req.Node != r.nodeID &&
		len(req.SigningPublicKey) == ed25519.PublicKeySize && validFederationEndpoint(req.Endpoint) &&
		validCertificateFingerprint(req.CertificateFingerprint) && validPolicyVersion(req.PolicyVersion)
}

func (r *Registry) consumeEnrollmentToken(
	ctx context.Context,
	tx Execer,
	digest []byte,
	nodeID protocol.NodeID,
) (string, error) {
	selectText, err := queryText("enrollment_token_select.sql")
	if err != nil {
		return "", err
	}
	var storedOrg, createdAt, expiresAt string
	var usedAt, usedBy sql.NullString
	err = tx.QueryRowContext(ctx, selectText, digest).
		Scan(&storedOrg, &createdAt, &expiresAt, &usedAt, &usedBy)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrEnrollmentDenied
	}
	if err != nil {
		return "", fmt.Errorf("select enrollment token digest: %w", err)
	}
	created, createdErr := time.Parse(timeLayout, createdAt)
	expires, expiresErr := time.Parse(timeLayout, expiresAt)
	now := r.now()
	if createdErr != nil || expiresErr != nil || expires.Before(created) ||
		!validOrganizationID(storedOrg) || usedAt.Valid || usedBy.Valid || !now.Before(expires) {
		return "", ErrEnrollmentDenied
	}
	consumeText, err := queryText("enrollment_token_consume.sql")
	if err != nil {
		return "", err
	}
	result, err := tx.ExecContext(ctx, consumeText,
		now.Format(timeLayout), nodeID, digest, storedOrg, now.Format(timeLayout))
	if err != nil {
		return "", fmt.Errorf("consume enrollment token digest: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return "", fmt.Errorf("count consumed enrollment tokens: %w", err)
	}
	if affected != 1 {
		return "", ErrEnrollmentDenied
	}
	return storedOrg, nil
}

// LocalEnrollmentState is the local identity state needed for enrollment.
// An empty OrganizationID is the explicit UNENROLLED state.
type LocalEnrollmentState struct {
	OrganizationID string
	PolicyVersion  string
}

// LocalEnrollment returns the organization and compiled policy version pinned
// on the local node row.
func (r *Registry) LocalEnrollment(ctx context.Context, q Querier) (LocalEnrollmentState, error) {
	node, err := nodeByID(ctx, q, r.nodeID)
	if err != nil {
		return LocalEnrollmentState{}, fmt.Errorf("read local node enrollment: %w", err)
	}
	if !node.local || node.status != localNodeStatus ||
		(node.organizationID != "" && !validOrganizationID(node.organizationID)) ||
		(node.policyVersion != "" && !validPolicyVersion(node.policyVersion)) {
		return LocalEnrollmentState{}, fmt.Errorf("local node enrollment is invalid: %w", ErrNodeKeyMismatch)
	}
	return LocalEnrollmentState{
		OrganizationID: node.organizationID,
		PolicyVersion:  node.policyVersion,
	}, nil
}

// LocalOrganizationID returns the immutable organization pinned on the local
// node. An empty value is the explicit UNENROLLED state.
func (r *Registry) LocalOrganizationID(ctx context.Context, q Querier) (string, error) {
	state, err := r.LocalEnrollment(ctx, q)
	return state.OrganizationID, err
}

// NodeSigningPublicKey returns a defensive copy of this node's signing key.
func (r *Registry) NodeSigningPublicKey() (ed25519.PublicKey, error) {
	publicKey, err := r.nodePublic()
	if err != nil {
		return nil, err
	}
	return append(ed25519.PublicKey(nil), publicKey...), nil
}

// Peers returns all remote nodes in stable node-id order.
func (r *Registry) Peers(ctx context.Context, q Querier) ([]Peer, error) {
	text, err := queryText("nodes_select_peers.sql")
	if err != nil {
		return nil, err
	}
	rows, err := q.QueryContext(ctx, text)
	if err != nil {
		return nil, fmt.Errorf("select peers: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var peers []Peer
	for rows.Next() {
		var peer Peer
		var status string
		var endpoint, fingerprint sql.NullString
		if err := rows.Scan(&peer.Node, &peer.OrganizationID, &status, &endpoint,
			&fingerprint, &peer.PolicyVersion); err != nil {
			return nil, fmt.Errorf("scan peer: %w", err)
		}
		peer.Status, err = parsePeerStatus(status)
		if err != nil {
			return nil, err
		}
		if endpoint.Valid {
			peer.Endpoint = endpoint.String
		}
		if fingerprint.Valid {
			peer.CertificateFingerprint = fingerprint.String
		}
		peers = append(peers, peer)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read peers: %w", err)
	}
	return peers, nil
}

// Peer returns one remote node's public registry view.
func (r *Registry) Peer(ctx context.Context, q Querier, nodeID protocol.NodeID) (Peer, error) {
	if !nodeID.Valid() || nodeID == r.nodeID {
		return Peer{}, fmt.Errorf("read peer %q: %w", nodeID, ErrInvalidArgument)
	}
	node, err := nodeByID(ctx, q, nodeID)
	if errors.Is(err, sql.ErrNoRows) {
		return Peer{}, fmt.Errorf("read peer %s: %w", nodeID, ErrPeerNotFound)
	}
	if err != nil {
		return Peer{}, err
	}
	if node.local {
		return Peer{}, fmt.Errorf("read peer %s names local node: %w", nodeID, ErrInvalidArgument)
	}
	status, err := parsePeerStatus(node.status)
	if err != nil {
		return Peer{}, err
	}
	return Peer{
		Node: node.nodeID, OrganizationID: node.organizationID, Status: status,
		Endpoint: node.federationEndpoint, CertificateFingerprint: node.certificateFingerprint,
		PolicyVersion: node.policyVersion,
	}, nil
}

func enrollmentTokenDigest(token string) ([]byte, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(raw) != enrollmentTokenSize ||
		base64.RawURLEncoding.EncodeToString(raw) != token {
		return nil, ErrEnrollmentDenied
	}
	digest := sha256.Sum256(raw)
	return digest[:], nil
}

func validOrganizationID(value string) bool {
	return protocol.NodeID(value).Valid()
}
