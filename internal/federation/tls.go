package federation

import (
	"context"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/protocol"
)

const trustedPeerStatus = "TRUSTED"

// Querier is the read surface the transport needs from the node registry.
type Querier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// ServerOptions configures the federation HTTPS listener.
type ServerOptions struct {
	Listen      string
	Identity    *NodeTLS
	TrustBundle *TrustBundle
	DB          Querier
	Clock       clock.Clock
	Enrollment  *EnrollmentManager
	Delivery    DeliveryReceiver
	Ack         AckReceiver
	Artifact    ArtifactReceiver
	Directory   DirectoryExporter
}

// Server is the authenticated federation HTTP transport.
type Server struct {
	listen    string
	identity  *NodeTLS
	bundle    *TrustBundle
	db        Querier
	clock     clock.Clock
	enroll    *EnrollmentManager
	delivery  DeliveryReceiver
	ack       AckReceiver
	artifact  ArtifactReceiver
	directory DirectoryExporter

	mu       sync.Mutex
	listener net.Listener
	http     *http.Server
}

// NewServer validates options and constructs a stopped server.
func NewServer(options ServerOptions) (*Server, error) {
	if options.Listen == "" || options.Identity == nil || options.TrustBundle == nil ||
		options.DB == nil || options.Clock == nil {
		return nil, fmt.Errorf("new federation server: incomplete options")
	}
	s := &Server{
		listen: options.Listen, identity: options.Identity,
		bundle: options.TrustBundle, db: options.DB, clock: options.Clock,
		enroll: options.Enrollment, delivery: options.Delivery, ack: options.Ack,
		artifact: options.Artifact, directory: options.Directory,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/health", healthHandler)
	mux.HandleFunc("POST /v1/enroll", s.enrollmentHandler)
	mux.HandleFunc("POST /v1/deliver", s.deliveryHandler)
	mux.HandleFunc("POST /v1/ack", s.ackHandler)
	mux.HandleFunc("POST /v1/artifacts/manifest", s.artifactManifestHandler)
	mux.HandleFunc("POST /v1/artifacts/chunks", s.artifactChunkHandler)
	mux.HandleFunc("POST /v1/artifacts/complete", s.artifactCompleteHandler)
	mux.HandleFunc("POST /v1/revoke", notImplementedHandler)
	mux.HandleFunc("GET /v1/keys", s.keysHandler)
	mux.HandleFunc("GET /v1/artifacts/{path...}", notImplementedHandler)
	s.http = &http.Server{
		Handler:           s.authenticate(mux),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	return s, nil
}

// Listen binds the configured address with TLS 1.3 and begins accepting only
// when Serve is called. It returns the concrete address, including an assigned
// port when the configured port is zero.
func (s *Server) Listen(ctx context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		return s.listener.Addr().String(), nil
	}
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", s.listen)
	if err != nil {
		return "", fmt.Errorf("listen for federation: %w", err)
	}
	config := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{s.identity.Certificate},
		// Request every client certificate, including a joining node whose
		// self-signed certificate cannot be in the bundle yet. Route-specific
		// verification below decides which trust model applies.
		ClientAuth: tls.RequestClientCert,
	}
	s.listener = tls.NewListener(listener, config)
	return listener.Addr().String(), nil
}

// Serve serves the listener until ctx is canceled or Close is called.
func (s *Server) Serve(ctx context.Context) error {
	s.mu.Lock()
	listener := s.listener
	s.mu.Unlock()
	if listener == nil {
		if _, err := s.Listen(ctx); err != nil {
			return err
		}
		s.mu.Lock()
		listener = s.listener
		s.mu.Unlock()
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = s.Close()
		case <-done:
		}
	}()
	err := s.http.Serve(listener)
	close(done)
	if errors.Is(err, http.ErrServerClosed) || errors.Is(err, net.ErrClosed) {
		return nil
	}
	return fmt.Errorf("serve federation: %w", err)
}

// Close stops the federation listener.
func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return nil
	}
	err := s.http.Close()
	s.listener = nil
	if errors.Is(err, http.ErrServerClosed) || errors.Is(err, net.ErrClosed) {
		return nil
	}
	return fmt.Errorf("close federation server: %w", err)
}

type (
	verifiedNodeKey          struct{}
	enrollmentCertificateKey struct{}
)

// VerifiedNodeID returns the authenticated peer identity installed by the
// server before dispatching a protected endpoint.
func VerifiedNodeID(ctx context.Context) (protocol.NodeID, bool) {
	nodeID, ok := ctx.Value(verifiedNodeKey{}).(protocol.NodeID)
	return nodeID, ok
}

func enrollmentCertificate(ctx context.Context) (*x509.Certificate, bool) {
	cert, ok := ctx.Value(enrollmentCertificateKey{}).(*x509.Certificate)
	return cert, ok
}

func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodGet && request.URL.Path == "/v1/health" {
			next.ServeHTTP(response, request)
			return
		}
		if request.TLS == nil || len(request.TLS.PeerCertificates) == 0 {
			http.Error(response, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
			return
		}
		cert := request.TLS.PeerCertificates[0]
		if request.Method == http.MethodPost && request.URL.Path == "/v1/enroll" {
			nodeID, verified, err := verifyEnrollmentCertificate(
				request.TLS.PeerCertificates, s.clock.Now().UTC(),
			)
			if err != nil {
				http.Error(response, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
				return
			}
			if s.bundle.ContainsNode(nodeID) {
				http.Error(response, http.StatusText(http.StatusForbidden), http.StatusForbidden)
				return
			}
			ctx := context.WithValue(request.Context(), verifiedNodeKey{}, nodeID)
			ctx = context.WithValue(ctx, enrollmentCertificateKey{}, verified)
			next.ServeHTTP(response, request.WithContext(ctx))
			return
		}
		nodeID, err := verifyPeerCertificate(request.TLS.PeerCertificates, s.bundle, s.clock.Now().UTC())
		if err != nil {
			// A revoked certificate is deliberately removed from the live
			// bundle, but its pinned identity remains in the registry. Recognize
			// that exact blocked identity so authorization returns 403; every
			// unknown or merely unbundled certificate remains a 401.
			nodeID, err = blockedPinnedPeerIdentity(
				request.Context(), s.db, cert, Fingerprint(cert),
			)
			if err != nil {
				http.Error(response, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
				return
			}
		}
		if err := authorizePeer(request.Context(), s.db, nodeID, Fingerprint(cert)); err != nil {
			http.Error(response, http.StatusText(http.StatusForbidden), http.StatusForbidden)
			return
		}
		ctx := context.WithValue(request.Context(), verifiedNodeKey{}, nodeID)
		next.ServeHTTP(response, request.WithContext(ctx))
	})
}

func blockedPinnedPeerIdentity(
	ctx context.Context,
	db Querier,
	cert *x509.Certificate,
	fingerprint string,
) (protocol.NodeID, error) {
	nodeID, err := nodeIDFromCertificate(cert)
	if err != nil {
		return "", err
	}
	var status string
	var local int64
	var pinned sql.NullString
	err = db.QueryRowContext(ctx,
		"SELECT status, local, certificate_fingerprint FROM nodes WHERE node_id = ?", nodeID).
		Scan(&status, &local, &pinned)
	if err != nil || local != 0 || status == trustedPeerStatus || !pinned.Valid ||
		!fingerprintsEqual(pinned.String, fingerprint) {
		return "", fmt.Errorf("certificate is not a known blocked peer identity")
	}
	return nodeID, nil
}

func (s *Server) enrollmentHandler(response http.ResponseWriter, request *http.Request) {
	if s.enroll == nil {
		http.Error(response, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
		return
	}
	s.enroll.handler(response, request)
}

func authorizePeer(ctx context.Context, db Querier, nodeID protocol.NodeID, fingerprint string) error {
	var status string
	var local int64
	var pinned sql.NullString
	err := db.QueryRowContext(ctx,
		"SELECT status, local, certificate_fingerprint FROM nodes WHERE node_id = ?", nodeID).
		Scan(&status, &local, &pinned)
	if err != nil {
		return fmt.Errorf("select peer TLS pin: %w", err)
	}
	if local != 0 || status != trustedPeerStatus || !pinned.Valid ||
		!fingerprintsEqual(pinned.String, fingerprint) {
		return fmt.Errorf("peer TLS identity is not trusted")
	}
	return nil
}

func verifyPeerCertificate(
	certificates []*x509.Certificate,
	bundle *TrustBundle,
	now time.Time,
) (protocol.NodeID, error) {
	if len(certificates) == 0 {
		return "", fmt.Errorf("peer presented no certificate")
	}
	leaf := certificates[0]
	intermediates := x509.NewCertPool()
	for _, cert := range certificates[1:] {
		intermediates.AddCert(cert)
	}
	if _, err := leaf.Verify(x509.VerifyOptions{
		Roots: bundle.rootPool(), Intermediates: intermediates, CurrentTime: now,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}); err != nil {
		return "", fmt.Errorf("verify client certificate chain: %w", err)
	}
	return nodeIDFromCertificate(leaf)
}

func healthHandler(response http.ResponseWriter, _ *http.Request) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(http.StatusOK)
	_, _ = response.Write([]byte("{\"status\":\"ok\"}\n"))
}

func notImplementedHandler(response http.ResponseWriter, request *http.Request) {
	if _, ok := VerifiedNodeID(request.Context()); !ok {
		http.Error(response, http.StatusText(http.StatusForbidden), http.StatusForbidden)
		return
	}
	http.Error(response, http.StatusText(http.StatusNotImplemented), http.StatusNotImplemented)
}

// ClientOptions names a target peer whose certificate pin is read from DB.
type ClientOptions struct {
	Identity    *NodeTLS
	TrustBundle *TrustBundle
	DB          Querier
	TargetNode  protocol.NodeID
	Clock       clock.Clock
}

// NewClientTLSConfig builds a TLS 1.3 client configuration that verifies the
// chain, exact target URI SAN, and registry-pinned certificate fingerprint.
func NewClientTLSConfig(ctx context.Context, options ClientOptions) (*tls.Config, error) {
	if options.Identity == nil || options.TrustBundle == nil ||
		options.DB == nil || !options.TargetNode.Valid() {
		return nil, fmt.Errorf("new federation client TLS config: incomplete options")
	}
	var status string
	var local int64
	var pinned sql.NullString
	err := options.DB.QueryRowContext(ctx,
		"SELECT status, local, certificate_fingerprint FROM nodes WHERE node_id = ?", options.TargetNode).
		Scan(&status, &local, &pinned)
	if err != nil {
		return nil, fmt.Errorf("select target TLS pin: %w", err)
	}
	if local != 0 || status != trustedPeerStatus || !pinned.Valid || !validFingerprint(pinned.String) {
		return nil, fmt.Errorf("target node is not a trusted pinned peer")
	}
	targetNode := options.TargetNode
	pinnedFingerprint := pinned.String
	clientClock := options.Clock
	if clientClock == nil {
		clientClock = clock.System{}
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{options.Identity.Certificate},
		// URI identity has no DNS name. VerifyConnection below replaces all
		// standard verification with chain, URI SAN, and pin checks.
		InsecureSkipVerify: true, //nolint:gosec
		VerifyConnection: func(state tls.ConnectionState) error {
			if len(state.PeerCertificates) == 0 {
				return fmt.Errorf("server presented no certificate")
			}
			leaf := state.PeerCertificates[0]
			intermediates := x509.NewCertPool()
			for _, cert := range state.PeerCertificates[1:] {
				intermediates.AddCert(cert)
			}
			if _, err := leaf.Verify(x509.VerifyOptions{
				Roots: options.TrustBundle.rootPool(), Intermediates: intermediates,
				CurrentTime: clientClock.Now().UTC(),
				KeyUsages:   []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			}); err != nil {
				return fmt.Errorf("verify server certificate chain: %w", err)
			}
			gotNode, err := nodeIDFromCertificate(leaf)
			if err != nil {
				return err
			}
			if gotNode != targetNode {
				return fmt.Errorf("server certificate identifies node %s, want %s", gotNode, targetNode)
			}
			if !fingerprintsEqual(pinnedFingerprint, Fingerprint(leaf)) {
				return fmt.Errorf("server certificate fingerprint does not match its pin")
			}
			return nil
		},
	}, nil
}

func validFingerprint(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32 && hex.EncodeToString(decoded) == value
}

func fingerprintsEqual(left, right string) bool {
	if !validFingerprint(left) || !validFingerprint(right) {
		return false
	}
	leftBytes, leftErr := hex.DecodeString(left)
	rightBytes, rightErr := hex.DecodeString(right)
	if leftErr != nil || rightErr != nil || len(leftBytes) != 32 || len(rightBytes) != 32 {
		return false
	}
	return subtle.ConstantTimeCompare(leftBytes, rightBytes) == 1
}
