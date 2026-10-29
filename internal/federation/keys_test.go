package federation

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

type fixedDirectoryExporter struct {
	records []registry.DirectoryRecord
}

func (e fixedDirectoryExporter) ExportDirectory(
	context.Context,
	registry.Querier,
) ([]registry.DirectoryRecord, error) {
	return cloneDirectoryRecords(e.records), nil
}

type mutableDirectoryExporter struct {
	mu      sync.RWMutex
	records []registry.DirectoryRecord
}

func (e *mutableDirectoryExporter) ExportDirectory(
	context.Context,
	registry.Querier,
) ([]registry.DirectoryRecord, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return cloneDirectoryRecords(e.records), nil
}

func (e *mutableDirectoryExporter) set(records []registry.DirectoryRecord) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.records = cloneDirectoryRecords(records)
}

func cloneDirectoryRecords(records []registry.DirectoryRecord) []registry.DirectoryRecord {
	cloned := make([]registry.DirectoryRecord, len(records))
	for index, record := range records {
		cloned[index] = record
		cloned[index].PublicKey = append([]byte(nil), record.PublicKey...)
		cloned[index].RecordSignature = append([]byte(nil), record.RecordSignature...)
		if record.ValidUntil != nil {
			validUntil := *record.ValidUntil
			cloned[index].ValidUntil = &validUntil
		}
	}
	return cloned
}

func registerDirectoryAgent(
	t *testing.T,
	node enrollmentTestNode,
	agent protocol.AgentID,
) ed25519.PublicKey {
	t.Helper()
	process, err := registry.CurrentProcess()
	require.NoError(t, err)
	processJSON, err := json.Marshal(process)
	require.NoError(t, err)
	stamp := eventstore.FormatTime(node.clock.Now())
	_, err = node.store.DB().ExecContext(t.Context(), `INSERT INTO agents
		(agent_id, node_id, harness, created_at, updated_at)
		VALUES (?, ?, 'test', ?, ?)`, agent, node.id, stamp, stamp)
	require.NoError(t, err)
	_, err = node.store.DB().ExecContext(t.Context(), `INSERT INTO sessions
		(session_id, agent_id, state, capture_mode, deliverable, manifest_json,
		 process_json, started_at, updated_at)
		VALUES (?, ?, 'CONNECTED', 'pty_proxy', 1, '{}', ?, ?, ?)`,
		"session-"+string(agent), agent, string(processJSON), stamp, stamp)
	require.NoError(t, err)
	publicKey, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	result, err := node.registry.Register(t.Context(), node.store.DB(), registry.RegisterRequest{
		Agent: agent, Process: process, PublicKey: publicKey,
		KeyID: "ed25519:" + string(agent) + ":v1",
	})
	require.NoError(t, err)
	require.Equal(t, registry.OutcomeRegistered, result.Outcome)
	return publicKey
}

func exportedDirectory(t *testing.T, node enrollmentTestNode) []registry.DirectoryRecord {
	t.Helper()
	records, err := node.registry.ExportDirectory(t.Context(), node.store.DB())
	require.NoError(t, err)
	return records
}

func trustDirectoryPeer(
	t *testing.T,
	local enrollmentTestNode,
	peer enrollmentTestNode,
	endpoint string,
) {
	t.Helper()
	publicKey, err := peer.registry.NodeSigningPublicKey()
	require.NoError(t, err)
	result, err := local.registry.AddPeer(
		t.Context(), local.store.DB(), peer.id, "org-test", publicKey,
	)
	require.NoError(t, err)
	require.Equal(t, registry.AddPeerRegistered, result.Outcome)
	require.NoError(t, local.registry.SetPeerTransport(
		t.Context(), local.store.DB(), peer.id, endpoint, peer.identity.Fingerprint,
	))
	require.NoError(t, local.bundle.AddCertificate(peer.identity.Leaf))
}

func trustSigningPeer(t *testing.T, local, peer enrollmentTestNode) {
	t.Helper()
	publicKey, err := peer.registry.NodeSigningPublicKey()
	require.NoError(t, err)
	result, err := local.registry.AddPeer(
		t.Context(), local.store.DB(), peer.id, "org-test", publicKey,
	)
	require.NoError(t, err)
	require.Equal(t, registry.AddPeerRegistered, result.Outcome)
}

func startKeysServer(
	t *testing.T,
	node enrollmentTestNode,
	exporter DirectoryExporter,
) string {
	t.Helper()
	server, err := NewServer(ServerOptions{
		Listen: loopbackEphemeral, Identity: node.identity, TrustBundle: node.bundle,
		DB: node.store.DB(), Clock: node.clock, Directory: exporter,
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

func startDirectoryPair(
	t *testing.T,
	client enrollmentTestNode,
	server enrollmentTestNode,
	exporter DirectoryExporter,
) string {
	t.Helper()
	trustDirectoryPeer(t, server, client, "https://client.example.invalid:7443")
	baseURL := startKeysServer(t, server, exporter)
	trustDirectoryPeer(t, client, server, baseURL)
	return baseURL
}

func directoryClientTLS(
	t *testing.T,
	client enrollmentTestNode,
	server enrollmentTestNode,
) *tls.Config {
	t.Helper()
	config, err := NewClientTLSConfig(t.Context(), ClientOptions{
		Identity: client.identity, TrustBundle: client.bundle,
		DB: client.store.DB(), TargetNode: server.id, Clock: client.clock,
	})
	require.NoError(t, err)
	return config
}

func getDirectory(
	t *testing.T,
	client *http.Client,
	target string,
) (int, []byte) {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, target+"/v1/keys", nil)
	require.NoError(t, err)
	response, err := client.Do(request)
	require.NoError(t, err)
	defer func() { require.NoError(t, response.Body.Close()) }()
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	return response.StatusCode, body
}

func countImportedKeys(t *testing.T, node enrollmentTestNode, peer protocol.NodeID) int64 {
	t.Helper()
	var count int64
	require.NoError(t, node.store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM agent_keys WHERE node_id = ?", peer).Scan(&count))
	return count
}

func TestDirectorySyncImportsOnlyServingNodeKeys(t *testing.T) {
	nodeA := newEnrollmentTestNode(t, "node-a", "org-a")
	nodeB := newEnrollmentTestNode(t, "node-b", "org-b")
	nodeC := newEnrollmentTestNode(t, "node-c", "org-c")
	wantPublic := registerDirectoryAgent(t, nodeB, "agent-b")
	registerDirectoryAgent(t, nodeC, "agent-c")
	trustSigningPeer(t, nodeB, nodeC)
	_, err := nodeB.registry.ImportDirectory(
		t.Context(), nodeB.store.DB(), nodeC.id, exportedDirectory(t, nodeC),
	)
	require.NoError(t, err)
	startDirectoryPair(t, nodeA, nodeB, nodeB.registry)

	require.NoError(t, nodeA.manager.SyncDirectories(t.Context()))
	key, err := nodeA.registry.ActiveSigningKey(t.Context(), nodeA.store.DB(), "agent-b")
	require.NoError(t, err)
	require.Equal(t, []byte(wantPublic), []byte(key.PublicKey))
	route, err := nodeA.registry.Route(t.Context(), nodeA.store.DB(), "agent-b")
	require.NoError(t, err)
	require.Equal(t, registry.RouteResult{Kind: registry.RouteFederated, Node: nodeB.id}, route)
	require.Zero(t, countImportedKeys(t, nodeA, nodeC.id))
	route, err = nodeA.registry.Route(t.Context(), nodeA.store.DB(), "agent-c")
	require.NoError(t, err)
	require.Equal(t, registry.RouteNotFound, route.Kind)
}

func TestKeysEndpointRevealsNothingWithoutTrustedClientCertificate(t *testing.T) {
	nodeA := newEnrollmentTestNode(t, "node-a", "org-a")
	nodeB := newEnrollmentTestNode(t, "node-b", "org-b")
	outsider := newEnrollmentTestNode(t, "node-outsider", "org-outsider")
	registerDirectoryAgent(t, nodeB, "agent-secret")
	baseURL := startDirectoryPair(t, nodeA, nodeB, nodeB.registry)
	require.NoError(t, nodeB.bundle.AddCertificate(outsider.identity.Leaf))
	trustedTLS := directoryClientTLS(t, nodeA, nodeB)

	withoutCertificate := trustedTLS.Clone()
	withoutCertificate.Certificates = nil
	status, body := getDirectory(t, testHTTPClient(withoutCertificate), baseURL)
	require.Equal(t, http.StatusUnauthorized, status)
	require.NotContains(t, string(body), "ed25519:agent-secret:v1")

	untrustedCertificate := trustedTLS.Clone()
	untrustedCertificate.Certificates = []tls.Certificate{outsider.identity.Certificate}
	status, body = getDirectory(t, testHTTPClient(untrustedCertificate), baseURL)
	require.Equal(t, http.StatusForbidden, status)
	require.NotContains(t, string(body), "ed25519:agent-secret:v1")
}

func TestDirectorySyncRejectsInvalidBatchAtomically(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		mutate func([]registry.DirectoryRecord, registry.DirectoryRecord) []registry.DirectoryRecord
	}{
		{
			name: "tampered record",
			mutate: func(records []registry.DirectoryRecord, _ registry.DirectoryRecord) []registry.DirectoryRecord {
				records[1].RecordSignature[0] ^= 0xff
				return records
			},
		},
		{
			name: "signature from another node",
			mutate: func(records []registry.DirectoryRecord, other registry.DirectoryRecord) []registry.DirectoryRecord {
				records[1].RecordSignature = append([]byte(nil), other.RecordSignature...)
				return records
			},
		},
		{
			name: "record from another node",
			mutate: func(records []registry.DirectoryRecord, other registry.DirectoryRecord) []registry.DirectoryRecord {
				return append(records, other)
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			nodeA := newEnrollmentTestNode(t, "node-a", "org-a")
			nodeB := newEnrollmentTestNode(t, "node-b", "org-b")
			nodeC := newEnrollmentTestNode(t, "node-c", "org-c")
			registerDirectoryAgent(t, nodeB, "agent-one")
			registerDirectoryAgent(t, nodeB, "agent-two")
			registerDirectoryAgent(t, nodeC, "agent-other")
			records := testCase.mutate(
				cloneDirectoryRecords(exportedDirectory(t, nodeB)), exportedDirectory(t, nodeC)[0],
			)
			startDirectoryPair(t, nodeA, nodeB, fixedDirectoryExporter{records: records})

			require.Error(t, nodeA.manager.SyncDirectories(t.Context()))
			require.Zero(t, countImportedKeys(t, nodeA, nodeB.id))
			require.Zero(t, countImportedKeys(t, nodeA, nodeC.id))
			route, err := nodeA.registry.Route(t.Context(), nodeA.store.DB(), "agent-one")
			require.NoError(t, err)
			require.Equal(t, registry.RouteNotFound, route.Kind)
		})
	}
}

func TestDirectorySyncQuarantinesConflictingKeyID(t *testing.T) {
	nodeA := newEnrollmentTestNode(t, "node-a", "org-a")
	nodeB := newEnrollmentTestNode(t, "node-b", "org-b")
	wantPublic := registerDirectoryAgent(t, nodeB, "agent-collision")
	exporter := &mutableDirectoryExporter{}
	exporter.set(exportedDirectory(t, nodeB))
	startDirectoryPair(t, nodeA, nodeB, exporter)
	require.NoError(t, nodeA.manager.SyncDirectories(t.Context()))

	otherStore, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "other.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, otherStore.Close()) })
	otherRegistry, err := registry.New(nodeB.id, nodeB.dataDir, nodeB.clock)
	require.NoError(t, err)
	require.NoError(t, otherRegistry.EnsureLocalNode(t.Context(), otherStore.DB(), "org-b"))
	otherNode := enrollmentTestNode{
		id: nodeB.id, dataDir: nodeB.dataDir, store: otherStore,
		registry: otherRegistry, clock: nodeB.clock,
	}
	replacementPublic := registerDirectoryAgent(t, otherNode, "agent-collision")
	require.NotEqual(t, []byte(wantPublic), []byte(replacementPublic))
	exporter.set(exportedDirectory(t, otherNode))

	err = nodeA.manager.SyncDirectories(t.Context())
	require.Error(t, err)
	require.Contains(t, err.Error(), "quarantined peer")
	peer, err := nodeA.registry.Peer(t.Context(), nodeA.store.DB(), nodeB.id)
	require.NoError(t, err)
	require.Equal(t, registry.PeerStatusSuspended, peer.Status)
	require.Equal(t, int64(1), countImportedKeys(t, nodeA, nodeB.id))
	var storedPublic []byte
	require.NoError(t, nodeA.store.DB().QueryRowContext(t.Context(),
		"SELECT public_key FROM agent_keys WHERE key_id = 'ed25519:agent-collision:v1'").
		Scan(&storedPublic))
	require.Equal(t, []byte(wantPublic), storedPublic)
	route, err := nodeA.registry.Route(t.Context(), nodeA.store.DB(), "agent-collision")
	require.NoError(t, err)
	require.Equal(t, registry.RouteResult{Kind: registry.RoutePeerBlocked, Node: nodeB.id}, route)
}

func TestDirectorySyncContinuesAfterPeerFailure(t *testing.T) {
	nodeA := newEnrollmentTestNode(t, "node-a", "org-a")
	down := newEnrollmentTestNode(t, "node-b-down", "org-b")
	live := newEnrollmentTestNode(t, "node-c-live", "org-c")
	registerDirectoryAgent(t, live, "agent-live")
	trustDirectoryPeer(t, live, nodeA, "https://client.example.invalid:7443")
	liveURL := startKeysServer(t, live, live.registry)
	downListener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", loopbackEphemeral)
	require.NoError(t, err)
	downURL := "https://" + downListener.Addr().String()
	require.NoError(t, downListener.Close())
	trustDirectoryPeer(t, nodeA, down, downURL)
	trustDirectoryPeer(t, nodeA, live, liveURL)

	err = nodeA.manager.SyncDirectories(t.Context())
	require.Error(t, err)
	require.Contains(t, err.Error(), string(down.id))
	route, routeErr := nodeA.registry.Route(t.Context(), nodeA.store.DB(), "agent-live")
	require.NoError(t, routeErr)
	require.Equal(t, registry.RouteResult{Kind: registry.RouteFederated, Node: live.id}, route)
}

func TestKeysEndpointRefusesDirectoryLargerThanOneMiB(t *testing.T) {
	nodeA := newEnrollmentTestNode(t, "node-a", "org-a")
	nodeB := newEnrollmentTestNode(t, "node-b", "org-b")
	oversized := registry.DirectoryRecord{
		Node: nodeB.id, Issuer: "node:" + string(nodeB.id),
		PublicKey: make([]byte, maxDirectoryBody),
	}
	baseURL := startDirectoryPair(
		t, nodeA, nodeB, fixedDirectoryExporter{records: []registry.DirectoryRecord{oversized}},
	)
	status, body := getDirectory(t, testHTTPClient(directoryClientTLS(t, nodeA, nodeB)), baseURL)
	require.Equal(t, http.StatusInternalServerError, status)
	require.Less(t, len(body), maxDirectoryBody)
}
