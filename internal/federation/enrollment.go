package federation

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

const maxEnrollmentBody = 64 << 10

// EnrollmentRequest is the JSON body a joining node sends after TLS has
// pinned the server. The joining node id and certificate fingerprint come
// only from the TLS connection, never from this body.
type EnrollmentRequest struct {
	Token            string `json:"token"`
	SigningPublicKey []byte `json:"signing_public_key"`
	Endpoint         string `json:"endpoint"`
	PolicyVersion    string `json:"policy_version"`
}

// EnrollmentResponse is the local node identity returned after enrollment.
type EnrollmentResponse struct {
	Node                   protocol.NodeID    `json:"node_id"`
	OrganizationID         string             `json:"organization_id"`
	AssignedOrganizationID string             `json:"assigned_organization_id"`
	SigningPublicKey       []byte             `json:"signing_public_key"`
	Endpoint               string             `json:"endpoint"`
	Fingerprint            protocol.SHA256Hex `json:"fingerprint"`
	PolicyVersion          string             `json:"policy_version"`
}

// EnrollmentOptions configures the atomic server-side enrollment operation.
type EnrollmentOptions struct {
	DB       *sql.DB
	Registry *registry.Registry
	Bundle   *TrustBundle
	Identity *NodeTLS
	Endpoint string
}

// EnrollmentManager consumes tokens, pins peers, and updates the live trust
// bundle. It is kept separate from HTTP parsing so transaction behavior is
// directly testable.
type EnrollmentManager struct {
	db          *sql.DB
	registry    *registry.Registry
	bundle      *TrustBundle
	nodeID      protocol.NodeID
	signingKey  ed25519.PublicKey
	endpoint    string
	fingerprint protocol.SHA256Hex
}

// NewEnrollmentManager validates the local identity advertised to joining
// nodes. The immutable organization is read from the registry per enrollment.
func NewEnrollmentManager(options EnrollmentOptions) (*EnrollmentManager, error) {
	if options.DB == nil || options.Registry == nil || options.Bundle == nil || options.Identity == nil ||
		!validEnrollmentEndpoint(options.Endpoint) {
		return nil, fmt.Errorf("new enrollment manager: incomplete options")
	}
	nodeID, err := nodeIDFromCertificate(options.Identity.Leaf)
	if err != nil {
		return nil, fmt.Errorf("new enrollment manager: %w", err)
	}
	publicKey, err := options.Registry.NodeSigningPublicKey()
	if err != nil {
		return nil, fmt.Errorf("new enrollment manager: read local node signing key: %w", err)
	}
	return &EnrollmentManager{
		db: options.DB, registry: options.Registry, bundle: options.Bundle,
		nodeID: nodeID, signingKey: publicKey,
		endpoint: options.Endpoint, fingerprint: protocol.SHA256Hex(options.Identity.Fingerprint),
	}, nil
}

func (m *EnrollmentManager) enroll(
	ctx context.Context,
	cert *x509.Certificate,
	req EnrollmentRequest,
) (EnrollmentResponse, error) {
	peerID, err := nodeIDFromCertificate(cert)
	if err != nil {
		return EnrollmentResponse{}, err
	}
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return EnrollmentResponse{}, fmt.Errorf("begin enrollment transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	local, err := m.registry.LocalEnrollment(ctx, tx)
	if err != nil {
		return EnrollmentResponse{}, fmt.Errorf("read local enrollment: %w", err)
	}
	if local.OrganizationID == "" {
		return EnrollmentResponse{}, registry.ErrNodeUnenrolled
	}
	if local.PolicyVersion == "" {
		return EnrollmentResponse{}, fmt.Errorf("local policy version is not pinned")
	}
	enrolled, err := m.registry.EnrollPeer(ctx, tx, registry.EnrollPeerRequest{
		Token: req.Token, Node: peerID,
		SigningPublicKey: ed25519.PublicKey(req.SigningPublicKey), Endpoint: req.Endpoint,
		CertificateFingerprint: Fingerprint(cert), PolicyVersion: req.PolicyVersion,
	})
	if err != nil {
		return EnrollmentResponse{}, fmt.Errorf("persist enrolled peer: %w", err)
	}
	if err := m.bundle.AddCertificate(cert); err != nil {
		return EnrollmentResponse{}, fmt.Errorf("add enrolled certificate: %w", err)
	}
	if err := tx.Commit(); err != nil {
		_, removeErr := m.bundle.RemoveCertificate(peerID)
		return EnrollmentResponse{}, errors.Join(fmt.Errorf("commit enrollment: %w", err), removeErr)
	}
	return EnrollmentResponse{
		Node: m.nodeID, OrganizationID: local.OrganizationID,
		AssignedOrganizationID: enrolled.OrganizationID,
		SigningPublicKey:       append([]byte(nil), m.signingKey...), Endpoint: m.endpoint,
		Fingerprint: m.fingerprint, PolicyVersion: local.PolicyVersion,
	}, nil
}

func (m *EnrollmentManager) handler(response http.ResponseWriter, request *http.Request) {
	cert, ok := enrollmentCertificate(request.Context())
	if !ok {
		http.Error(response, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}
	defer func() { _ = request.Body.Close() }()
	decoder := json.NewDecoder(io.LimitReader(request.Body, maxEnrollmentBody+1))
	decoder.DisallowUnknownFields()
	var body EnrollmentRequest
	if err := decoder.Decode(&body); err != nil {
		http.Error(response, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	if err := requireJSONEOF(decoder); err != nil {
		http.Error(response, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	result, err := m.enroll(request.Context(), cert, body)
	if err != nil {
		switch {
		case errors.Is(err, registry.ErrEnrollmentDenied):
			http.Error(response, http.StatusText(http.StatusForbidden), http.StatusForbidden)
		case errors.Is(err, registry.ErrPeerAlreadyExists):
			http.Error(response, http.StatusText(http.StatusConflict), http.StatusConflict)
		case errors.Is(err, registry.ErrInvalidArgument):
			http.Error(response, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		case errors.Is(err, registry.ErrNodeUnenrolled):
			http.Error(response, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
		default:
			http.Error(response, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		}
		return
	}
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(response).Encode(result)
}

func requireJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("unexpected second JSON value")
		}
		return fmt.Errorf("decode trailing JSON: %w", err)
	}
	return nil
}

func verifyEnrollmentCertificate(
	certificates []*x509.Certificate,
	now time.Time,
) (protocol.NodeID, *x509.Certificate, error) {
	if len(certificates) != 1 {
		return "", nil, fmt.Errorf("enrollment requires exactly one self-signed certificate")
	}
	cert := certificates[0]
	nodeID, err := nodeIDFromCertificate(cert)
	if err != nil {
		return "", nil, err
	}
	if !bytes.Equal(cert.RawIssuer, cert.RawSubject) ||
		cert.CheckSignature(cert.SignatureAlgorithm, cert.RawTBSCertificate, cert.Signature) != nil {
		return "", nil, fmt.Errorf("enrollment certificate is not self-signed")
	}
	if now.Before(cert.NotBefore) || now.After(cert.NotAfter) {
		return "", nil, fmt.Errorf("enrollment certificate is not currently valid")
	}
	if !hasExtKeyUsage(cert, x509.ExtKeyUsageClientAuth) ||
		!hasExtKeyUsage(cert, x509.ExtKeyUsageServerAuth) {
		return "", nil, fmt.Errorf("enrollment certificate lacks client or server authentication usage")
	}
	if len(cert.UnhandledCriticalExtensions) != 0 {
		return "", nil, fmt.Errorf("enrollment certificate has unhandled critical extensions")
	}
	return nodeID, cert, nil
}

func validEnrollmentOrganization(value string) bool {
	return protocol.NodeID(value).Valid()
}

func validEnrollmentEndpoint(raw string) bool {
	parsed, err := url.ParseRequestURI(raw)
	return err == nil && parsed.Scheme == "https" && parsed.Host != "" &&
		parsed.User == nil && parsed.RawQuery == "" && parsed.Fragment == ""
}
