package federation

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

const (
	maxDirectoryBody        = 1 << 20
	directoryRequestTimeout = 15 * time.Second
)

// DirectoryExporter is the registry read surface exposed by GET /v1/keys.
// Registry.ExportDirectory returns only locally issued, node-signed records.
type DirectoryExporter interface {
	ExportDirectory(context.Context, registry.Querier) ([]registry.DirectoryRecord, error)
}

func (s *Server) keysHandler(response http.ResponseWriter, request *http.Request) {
	if s.directory == nil {
		notImplementedHandler(response, request)
		return
	}
	if _, ok := VerifiedNodeID(request.Context()); !ok {
		http.Error(response, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}
	records, err := s.directory.ExportDirectory(request.Context(), s.db)
	if err != nil {
		http.Error(response, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	if records == nil {
		records = make([]registry.DirectoryRecord, 0)
	}
	localNode, err := nodeIDFromCertificate(s.identity.Leaf)
	if err != nil || !localDirectory(localNode, records) {
		http.Error(response, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	body, err := json.Marshal(records)
	if err != nil || len(body) > maxDirectoryBody {
		http.Error(response, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(http.StatusOK)
	_, _ = response.Write(body)
}

func localDirectory(node protocol.NodeID, records []registry.DirectoryRecord) bool {
	for _, record := range records {
		if record.Node != node || record.Issuer != "node:"+string(node) {
			return false
		}
	}
	return true
}

// SyncDirectories fetches and atomically imports the signed key directory of
// every trusted peer. A failed peer is reported without preventing attempts
// against the remaining trusted peers.
func (m *NodeManager) SyncDirectories(ctx context.Context) error {
	if !m.configured() {
		return registry.ErrFederationUnavailable
	}
	peers, err := m.registry.Peers(ctx, m.db)
	if err != nil {
		return fmt.Errorf("list peers for directory sync: %w", err)
	}
	var syncErrors []error
	for _, peer := range peers {
		if peer.Status != registry.PeerStatusTrusted {
			continue
		}
		if err := ctx.Err(); err != nil {
			syncErrors = append(syncErrors, err)
			break
		}
		if err := m.syncDirectory(ctx, peer); err != nil {
			syncErrors = append(syncErrors, fmt.Errorf("sync directory from %s: %w", peer.Node, err))
		}
	}
	return errors.Join(syncErrors...)
}

func (m *NodeManager) syncDirectory(ctx context.Context, peer registry.Peer) error {
	target, err := directoryURL(peer.Endpoint)
	if err != nil {
		return err
	}
	clientTLS, err := NewClientTLSConfig(ctx, ClientOptions{
		Identity: m.identity, TrustBundle: m.bundle, DB: m.db,
		TargetNode: peer.Node, Clock: m.clock,
	})
	if err != nil {
		return fmt.Errorf("configure authenticated client: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return fmt.Errorf("build directory request: %w", err)
	}
	client := &http.Client{
		Transport: &http.Transport{TLSClientConfig: clientTLS, DisableKeepAlives: true},
		Timeout:   directoryRequestTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	result, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("fetch directory: %w", err)
	}
	defer func() { _ = result.Body.Close() }()
	if result.StatusCode != http.StatusOK {
		return fmt.Errorf("fetch directory returned HTTP %d", result.StatusCode)
	}
	if result.ContentLength > maxDirectoryBody {
		return fmt.Errorf("directory response exceeds %d bytes", maxDirectoryBody)
	}
	body, err := io.ReadAll(io.LimitReader(result.Body, maxDirectoryBody+1))
	if err != nil {
		return fmt.Errorf("read directory response: %w", err)
	}
	if len(body) > maxDirectoryBody {
		return fmt.Errorf("directory response exceeds %d bytes", maxDirectoryBody)
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var records []registry.DirectoryRecord
	if err := decoder.Decode(&records); err != nil {
		return fmt.Errorf("decode directory response: %w", err)
	}
	if records == nil {
		return fmt.Errorf("decode directory response: expected an array")
	}
	if err := requireJSONEOF(decoder); err != nil {
		return fmt.Errorf("decode directory response: %w", err)
	}
	imported, err := m.registry.ImportDirectory(ctx, m.db, peer.Node, records)
	if err != nil {
		return fmt.Errorf("import directory: %w", err)
	}
	if imported.PeerSuspended {
		return fmt.Errorf("import directory quarantined peer after a key conflict")
	}
	return nil
}

func directoryURL(raw string) (string, error) {
	parsed, err := url.ParseRequestURI(raw)
	if err != nil || parsed.Scheme != federationHTTPScheme || parsed.Host == "" || parsed.User != nil ||
		parsed.Fragment != "" || parsed.RawQuery != "" {
		return "", fmt.Errorf("invalid peer directory endpoint: %w", registry.ErrInvalidArgument)
	}
	parsed.Path = strings.TrimSuffix(parsed.Path, "/") + "/v1/keys"
	return parsed.String(), nil
}
