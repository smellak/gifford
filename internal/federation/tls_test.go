package federation

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

const loopbackEphemeral = "127.0.0.1:0"

type transportNode struct {
	id       protocol.NodeID
	identity *NodeTLS
	registry *registry.Registry
	store    *eventstore.Store
}

func newTransportNode(t *testing.T, id protocol.NodeID) transportNode {
	t.Helper()
	dataDir := privateDataDir(t)
	identity, err := EnsureNodeTLS(dataDir, id)
	require.NoError(t, err)
	store, err := eventstore.Open(t.Context(), filepath.Join(dataDir, "gifford.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	reg, err := registry.New(id, dataDir, clock.System{})
	require.NoError(t, err)
	require.NoError(t, reg.EnsureLocalNode(t.Context(), store.DB(), "example-org"))
	return transportNode{id: id, identity: identity, registry: reg, store: store}
}

func (node transportNode) trustPeer(t *testing.T, peer transportNode) {
	t.Helper()
	publicKey, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	_, err = node.registry.AddPeer(t.Context(), node.store.DB(), peer.id, "example-org", publicKey)
	require.NoError(t, err)
	require.NoError(t, node.registry.SetPeerTransport(
		t.Context(), node.store.DB(), peer.id, "https://peer.example.invalid:7443", peer.identity.Fingerprint,
	))
}

func startTestServer(t *testing.T, node transportNode, bundle *TrustBundle) string {
	t.Helper()
	server, err := NewServer(ServerOptions{
		Listen: loopbackEphemeral, Identity: node.identity, TrustBundle: bundle, DB: node.store.DB(),
		Clock: clock.System{}, Ack: ackReceiverFunc(func(
			context.Context, protocol.NodeID, AckRequest,
		) ([]byte, error) {
			return []byte("{}"), nil
		}),
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

func testHTTPClient(config *tls.Config) *http.Client {
	return &http.Client{
		Transport: &http.Transport{TLSClientConfig: config},
		Timeout:   5 * time.Second,
	}
}

func requestStatus(t *testing.T, client *http.Client, method, target string) int {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), method, target, strings.NewReader("payload"))
	require.NoError(t, err)
	response, err := client.Do(request)
	require.NoError(t, err)
	t.Cleanup(func() { _ = response.Body.Close() })
	_, err = io.Copy(io.Discard, response.Body)
	require.NoError(t, err)
	return response.StatusCode
}

func TestFederationTLSAuthorization(t *testing.T) {
	serverNode := newTransportNode(t, "node-server")
	clientNode := newTransportNode(t, "node-client")
	missingNode := newTransportNode(t, "node-missing")
	outsiderNode := newTransportNode(t, "node-outsider")
	serverNode.trustPeer(t, clientNode)
	clientNode.trustPeer(t, serverNode)

	serverBundle, err := LoadTrustBundle(writeTrustBundle(t, clientNode.identity, missingNode.identity))
	require.NoError(t, err)
	clientBundle, err := LoadTrustBundle(writeTrustBundle(t, serverNode.identity))
	require.NoError(t, err)
	baseURL := startTestServer(t, serverNode, serverBundle)
	clientTLS, err := NewClientTLSConfig(t.Context(), ClientOptions{
		Identity: clientNode.identity, TrustBundle: clientBundle,
		DB: clientNode.store.DB(), TargetNode: serverNode.id,
	})
	require.NoError(t, err)

	trusted := testHTTPClient(clientTLS)
	require.Equal(t, http.StatusOK, requestStatus(t, trusted, http.MethodGet, baseURL+"/v1/health"))
	for _, endpoint := range []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/v1/deliver"},
		{http.MethodPost, "/v1/revoke"},
		{http.MethodGet, "/v1/keys"},
		{http.MethodGet, "/v1/artifacts/object"},
	} {
		require.Equal(t, http.StatusNotImplemented, requestStatus(
			t, trusted, endpoint.method, baseURL+endpoint.path,
		))
	}
	require.Equal(t, http.StatusBadRequest, requestStatus(
		t, trusted, http.MethodPost, baseURL+"/v1/ack",
	))

	withoutCertificate := clientTLS.Clone()
	withoutCertificate.Certificates = nil
	anonymous := testHTTPClient(withoutCertificate)
	require.Equal(t, http.StatusOK, requestStatus(t, anonymous, http.MethodGet, baseURL+"/v1/health"))
	require.Equal(t, http.StatusUnauthorized, requestStatus(t, anonymous, http.MethodPost, baseURL+"/v1/deliver"))

	tls12 := withoutCertificate.Clone()
	tls12.MinVersion = tls.VersionTLS12
	tls12.MaxVersion = tls.VersionTLS12
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, baseURL+"/v1/health", nil)
	require.NoError(t, err)
	response, err := testHTTPClient(tls12).Do(request)
	if response != nil {
		_ = response.Body.Close()
	}
	require.Error(t, err)

	missingRow := clientTLS.Clone()
	missingRow.Certificates = []tls.Certificate{missingNode.identity.Certificate}
	require.Equal(t, http.StatusForbidden, requestStatus(
		t, testHTTPClient(missingRow), http.MethodPost, baseURL+"/v1/deliver",
	))

	unbundled := clientTLS.Clone()
	unbundled.Certificates = []tls.Certificate{outsiderNode.identity.Certificate}
	require.Equal(t, http.StatusUnauthorized, requestStatus(
		t, testHTTPClient(unbundled), http.MethodPost, baseURL+"/v1/deliver",
	))

	_, err = serverNode.store.DB().ExecContext(t.Context(),
		"UPDATE nodes SET certificate_fingerprint = NULL WHERE node_id = ?", clientNode.id)
	require.NoError(t, err)
	require.Equal(t, http.StatusForbidden, requestStatus(t, trusted, http.MethodPost, baseURL+"/v1/ack"))
	_, err = serverNode.store.DB().ExecContext(t.Context(),
		"UPDATE nodes SET certificate_fingerprint = ? WHERE node_id = ?", strings.Repeat("ab", 32), clientNode.id)
	require.NoError(t, err)
	require.Equal(t, http.StatusForbidden, requestStatus(t, trusted, http.MethodPost, baseURL+"/v1/revoke"))
	_, err = serverNode.store.DB().ExecContext(t.Context(),
		"UPDATE nodes SET certificate_fingerprint = ?, status = 'SUSPENDED' WHERE node_id = ?",
		clientNode.identity.Fingerprint, clientNode.id)
	require.NoError(t, err)
	require.Equal(t, http.StatusForbidden, requestStatus(t, trusted, http.MethodGet, baseURL+"/v1/keys"))
}

func TestClientRejectsWrongServerURISAN(t *testing.T) {
	serverNode := newTransportNode(t, "node-wrong")
	clientNode := newTransportNode(t, "node-client")

	publicKey, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	_, err = clientNode.registry.AddPeer(
		t.Context(), clientNode.store.DB(), "node-expected", "example-org", publicKey,
	)
	require.NoError(t, err)
	require.NoError(t, clientNode.registry.SetPeerTransport(
		t.Context(), clientNode.store.DB(), "node-expected",
		"https://peer.example.invalid:7443", serverNode.identity.Fingerprint,
	))

	emptyBundleDir := filepath.Join(t.TempDir(), "empty-trust")
	require.NoError(t, os.Mkdir(emptyBundleDir, 0o700))
	serverBundle, err := LoadTrustBundle(emptyBundleDir)
	require.NoError(t, err)
	baseURL := startTestServer(t, serverNode, serverBundle)
	clientBundle, err := LoadTrustBundle(writeTrustBundle(t, serverNode.identity))
	require.NoError(t, err)
	clientTLS, err := NewClientTLSConfig(t.Context(), ClientOptions{
		Identity: clientNode.identity, TrustBundle: clientBundle,
		DB: clientNode.store.DB(), TargetNode: "node-expected",
	})
	require.NoError(t, err)
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, baseURL+"/v1/health", nil)
	require.NoError(t, err)
	response, err := testHTTPClient(clientTLS).Do(request)
	if response != nil {
		_ = response.Body.Close()
	}
	require.Error(t, err)
	require.Contains(t, err.Error(), "identifies node node-wrong, want node-expected")
}
