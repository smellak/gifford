package federation

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	// Register the pure-Go SQLite driver for the probe's read-only database view.
	_ "modernc.org/sqlite"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	defaultProbeTimeout = 10 * time.Second
	maxProbeDetailBytes = 240
)

// ErrProbeFailed means at least one federation boundary accepted prohibited
// behavior or did not prove the expected behavior.
var ErrProbeFailed = errors.New("federation probe failed")

// ProbeOptions identifies a live peer and the local enrolled identity used to
// exercise its federation boundary. The database is opened read-only.
type ProbeOptions struct {
	Endpoint    string
	Fingerprint string
	DataDir     string
	Timeout     time.Duration
	Clock       clock.Clock
	Output      io.Writer
}

type probeInputs struct {
	localNode       protocol.NodeID
	targetNode      protocol.NodeID
	targetPublicKey ed25519.PublicKey
	identity        *NodeTLS
	request         DeliveryRequest
	exactRequest    []byte
	exactReceipt    []byte
	nextSequence    int64
}

type probeRunner struct {
	baseURL     string
	fingerprint string
	timeout     time.Duration
	clock       clock.Clock
	output      io.Writer
	inputs      probeInputs
}

// ProbeMain parses the standalone operator probe flags and returns its process
// status. The command entry point contains no probing logic.
func ProbeMain(args []string, stdout, stderr io.Writer) int {
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}
	flags := flag.NewFlagSet("fedprobe", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var options ProbeOptions
	flags.StringVar(&options.Endpoint, "endpoint", "", "peer HTTPS federation origin")
	flags.StringVar(&options.Fingerprint, "fingerprint", "", "expected lowercase certificate SHA-256")
	flags.StringVar(&options.DataDir, "data-dir", "", "local node data directory")
	flags.DurationVar(&options.Timeout, "timeout", defaultProbeTimeout, "timeout for each network request")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 || options.Endpoint == "" || options.Fingerprint == "" || options.DataDir == "" {
		_, _ = fmt.Fprintln(stderr, "fedprobe: --endpoint, --fingerprint and --data-dir are required")
		return 2
	}
	options.Output = stdout
	if err := RunProbe(context.Background(), options); err != nil {
		if !errors.Is(err, ErrProbeFailed) {
			_, _ = fmt.Fprintf(stderr, "fedprobe: %s\n", probeDetail(err))
		}
		return 1
	}
	return 0
}

// RunProbe executes the live negative federation matrix and prints one
// content-free PASS or FAIL line per case. It never writes to the local node.
func RunProbe(ctx context.Context, options ProbeOptions) error {
	baseURL, err := validateProbeOptions(options)
	if err != nil {
		return err
	}
	probeClock := options.Clock
	if probeClock == nil {
		probeClock = clock.System{}
	}
	timeout := options.Timeout
	if timeout == 0 {
		timeout = defaultProbeTimeout
	}
	inputs, err := loadProbeInputs(ctx, options.DataDir, options.Fingerprint)
	if err != nil {
		return err
	}
	runner := &probeRunner{
		baseURL: baseURL, fingerprint: options.Fingerprint, timeout: timeout,
		clock: probeClock, output: options.Output, inputs: inputs,
	}
	return runner.run(ctx)
}

func validateProbeOptions(options ProbeOptions) (string, error) {
	if options.Output == nil || options.DataDir == "" || options.Timeout < 0 {
		return "", fmt.Errorf("configure federation probe: invalid options")
	}
	if !validFingerprint(options.Fingerprint) {
		return "", fmt.Errorf("configure federation probe: fingerprint must be lowercase SHA-256")
	}
	parsed, err := url.ParseRequestURI(options.Endpoint)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil ||
		parsed.RawQuery != "" || parsed.Fragment != "" || parsed.RawPath != "" ||
		(parsed.Path != "" && parsed.Path != "/") {
		return "", fmt.Errorf("configure federation probe: endpoint must be an HTTPS origin")
	}
	return strings.TrimSuffix(options.Endpoint, "/"), nil
}

func loadProbeInputs(ctx context.Context, dataDir, fingerprint string) (probeInputs, error) {
	cleanDataDir, err := filepath.Abs(filepath.Clean(dataDir))
	if err != nil {
		return probeInputs{}, fmt.Errorf("resolve probe data directory: %w", err)
	}
	databasePath := filepath.Join(cleanDataDir, "gifford.db")
	db, err := openProbeDB(ctx, databasePath)
	if err != nil {
		return probeInputs{}, err
	}
	defer func() { _ = db.Close() }()

	localNode, err := probeLocalNode(ctx, db)
	if err != nil {
		return probeInputs{}, err
	}
	targetNode, targetPublicKey, err := probeTargetNode(ctx, db, fingerprint)
	if err != nil {
		return probeInputs{}, err
	}
	identityPath := filepath.Join(cleanDataDir, tlsKeysDir, tlsIdentityFile)
	if info, statErr := os.Lstat(identityPath); statErr != nil {
		return probeInputs{}, fmt.Errorf("read existing local TLS identity: %w", statErr)
	} else if !info.Mode().IsRegular() {
		return probeInputs{}, fmt.Errorf("read existing local TLS identity: not a regular file")
	}
	identity, err := EnsureNodeTLS(cleanDataDir, localNode)
	if err != nil {
		return probeInputs{}, fmt.Errorf("load local TLS identity: %w", err)
	}
	exactRequest, exactReceipt, nextSequence, err := probeStoredDelivery(ctx, db, targetNode)
	if err != nil {
		return probeInputs{}, err
	}
	request, err := DecodeDeliveryRequest(exactRequest)
	if err != nil {
		return probeInputs{}, fmt.Errorf("decode stored federation probe request: %w", err)
	}
	canonical, err := request.CanonicalBytes()
	if err != nil || !bytes.Equal(canonical, exactRequest) {
		return probeInputs{}, fmt.Errorf("load federation probe request: stored request is not canonical")
	}
	envelope, _, err := request.DecodeEnvelope()
	if err != nil {
		return probeInputs{}, fmt.Errorf("decode stored federation probe envelope: %w", err)
	}
	if envelope.OriginNode != localNode || envelope.DestinationNode != targetNode {
		return probeInputs{}, fmt.Errorf("load federation probe request: route does not match local and target nodes")
	}
	if err := verifyProbeReceipt(request, exactReceipt, targetPublicKey); err != nil {
		return probeInputs{}, fmt.Errorf("verify stored federation probe receipt: %w", err)
	}
	return probeInputs{
		localNode: localNode, targetNode: targetNode, targetPublicKey: targetPublicKey,
		identity: identity, request: request, exactRequest: exactRequest,
		exactReceipt: exactReceipt, nextSequence: nextSequence,
	}, nil
}

func openProbeDB(ctx context.Context, path string) (*sql.DB, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("open federation probe database: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("open federation probe database: not a regular file")
	}
	dsn := (&url.URL{Scheme: "file", Path: path}).String() +
		"?mode=ro&_pragma=query_only(1)&_pragma=busy_timeout(5000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open federation probe database: %w", err)
	}
	db.SetMaxOpenConns(1)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("open federation probe database: %w", err)
	}
	return db, nil
}

func probeLocalNode(ctx context.Context, db *sql.DB) (protocol.NodeID, error) {
	rows, err := db.QueryContext(ctx, "SELECT node_id FROM nodes WHERE local = 1 ORDER BY node_id")
	if err != nil {
		return "", fmt.Errorf("read local federation node: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var nodes []protocol.NodeID
	for rows.Next() {
		var node protocol.NodeID
		if err := rows.Scan(&node); err != nil {
			return "", fmt.Errorf("read local federation node: %w", err)
		}
		nodes = append(nodes, node)
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("read local federation node: %w", err)
	}
	if len(nodes) != 1 || !nodes[0].Valid() {
		return "", fmt.Errorf("read local federation node: expected one valid local node")
	}
	return nodes[0], nil
}

func probeTargetNode(
	ctx context.Context,
	db *sql.DB,
	fingerprint string,
) (protocol.NodeID, ed25519.PublicKey, error) {
	rows, err := db.QueryContext(ctx, `SELECT node_id, status, local, signing_public_key
		FROM nodes WHERE certificate_fingerprint = ? ORDER BY node_id`, fingerprint)
	if err != nil {
		return "", nil, fmt.Errorf("read federation probe target: %w", err)
	}
	defer func() { _ = rows.Close() }()
	type target struct {
		node      protocol.NodeID
		status    string
		local     int64
		publicKey []byte
	}
	var targets []target
	for rows.Next() {
		var candidate target
		if err := rows.Scan(&candidate.node, &candidate.status, &candidate.local, &candidate.publicKey); err != nil {
			return "", nil, fmt.Errorf("read federation probe target: %w", err)
		}
		targets = append(targets, candidate)
	}
	if err := rows.Err(); err != nil {
		return "", nil, fmt.Errorf("read federation probe target: %w", err)
	}
	if len(targets) != 1 || !targets[0].node.Valid() || targets[0].local != 0 ||
		targets[0].status != trustedPeerStatus || len(targets[0].publicKey) != ed25519.PublicKeySize {
		return "", nil, fmt.Errorf("read federation probe target: fingerprint must identify one trusted peer")
	}
	return targets[0].node, ed25519.PublicKey(bytes.Clone(targets[0].publicKey)), nil
}

func probeStoredDelivery(
	ctx context.Context,
	db *sql.DB,
	target protocol.NodeID,
) ([]byte, []byte, int64, error) {
	var request, receipt []byte
	err := db.QueryRowContext(ctx, `SELECT request.request, receipt.receipt
		FROM federation_outbox_requests AS request
		JOIN federation_outbox AS outbox ON outbox.outbox_id = request.outbox_id
		JOIN federation_receipts AS receipt ON receipt.outbox_id = outbox.outbox_id
		WHERE request.destination_node = ? AND receipt.state = 'STORED_REMOTE'
		ORDER BY outbox.created_at DESC, outbox.outbox_id DESC LIMIT 1`, target).
		Scan(&request, &receipt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil, 0, fmt.Errorf("load federation probe request: no successful delivery to target")
	}
	if err != nil {
		return nil, nil, 0, fmt.Errorf("load federation probe request: %w", err)
	}
	var nextSequence int64
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence), 0) + 1
		FROM federation_outbox_requests WHERE destination_node = ?`, target).Scan(&nextSequence); err != nil {
		return nil, nil, 0, fmt.Errorf("allocate federation probe sequence: %w", err)
	}
	if nextSequence < 1 {
		return nil, nil, 0, fmt.Errorf("allocate federation probe sequence: invalid sequence")
	}
	return bytes.Clone(request), bytes.Clone(receipt), nextSequence, nil
}

func (r *probeRunner) run(ctx context.Context) error {
	baselinePassed := false
	cases := []struct {
		name string
		run  func(context.Context) error
	}{
		{"tls-1.3", func(ctx context.Context) error {
			err := r.probeTLS13(ctx)
			baselinePassed = err == nil
			return err
		}},
		{"tls-1.2-rejected", func(ctx context.Context) error {
			if !baselinePassed {
				return fmt.Errorf("TLS 1.3 baseline did not pass")
			}
			return r.probeTLS12Rejected(ctx)
		}},
		{"no-client-certificate", r.probeNoClientCertificate},
		{"unenrolled-client-certificate", r.probeUnenrolledClientCertificate},
		{"forged-origin", r.probeForgedOrigin},
		{"invalid-signature", r.probeInvalidSignature},
		{"exact-replay", r.probeExactReplay},
	}
	failed := false
	for _, test := range cases {
		err := test.run(ctx)
		if err == nil {
			if _, writeErr := fmt.Fprintf(r.output, "PASS %s\n", test.name); writeErr != nil {
				return fmt.Errorf("write federation probe result: %w", writeErr)
			}
			continue
		}
		failed = true
		if _, writeErr := fmt.Fprintf(r.output, "FAIL %s: %s\n", test.name, probeDetail(err)); writeErr != nil {
			return fmt.Errorf("write federation probe result: %w", writeErr)
		}
	}
	if failed {
		return ErrProbeFailed
	}
	return nil
}

func probeDetail(err error) string {
	detail := strings.Join(strings.Fields(err.Error()), " ")
	if len(detail) <= maxProbeDetailBytes {
		return detail
	}
	return detail[:maxProbeDetailBytes] + "..."
}

func (r *probeRunner) probeTLS13(ctx context.Context) error {
	client := r.client(&r.inputs.identity.Certificate, tls.VersionTLS13, 0)
	defer client.CloseIdleConnections()
	response, err := r.request(ctx, client, http.MethodGet, "/v1/health", nil)
	if err != nil {
		return err
	}
	if response.status != http.StatusOK {
		return fmt.Errorf("health returned HTTP %d", response.status)
	}
	if response.tlsVersion != tls.VersionTLS13 {
		return fmt.Errorf("health negotiated TLS version 0x%x", response.tlsVersion)
	}
	return nil
}

func (r *probeRunner) probeTLS12Rejected(ctx context.Context) error {
	client := r.client(&r.inputs.identity.Certificate, tls.VersionTLS12, tls.VersionTLS12)
	defer client.CloseIdleConnections()
	_, err := r.request(ctx, client, http.MethodGet, "/v1/health", nil)
	if err == nil {
		return fmt.Errorf("peer accepted TLS 1.2")
	}
	return nil
}

func (r *probeRunner) probeNoClientCertificate(ctx context.Context) error {
	client := r.client(nil, tls.VersionTLS13, 0)
	defer client.CloseIdleConnections()
	response, err := r.request(ctx, client, http.MethodPost, "/v1/deliver", []byte("{}"))
	if err != nil {
		return err
	}
	if response.status != http.StatusUnauthorized {
		return fmt.Errorf("request without client certificate returned HTTP %d", response.status)
	}
	return nil
}

func (r *probeRunner) probeUnenrolledClientCertificate(ctx context.Context) error {
	identity, err := ephemeralProbeIdentity()
	if err != nil {
		return err
	}
	client := r.client(&identity.Certificate, tls.VersionTLS13, 0)
	defer client.CloseIdleConnections()
	response, err := r.request(ctx, client, http.MethodPost, "/v1/deliver", []byte("{}"))
	if err != nil {
		return err
	}
	if response.status != http.StatusUnauthorized {
		return fmt.Errorf("unenrolled client certificate returned HTTP %d", response.status)
	}
	return nil
}

func ephemeralProbeIdentity() (*NodeTLS, error) {
	random := make([]byte, 8)
	if _, err := rand.Read(random); err != nil {
		return nil, fmt.Errorf("generate ephemeral probe node: %w", err)
	}
	node := protocol.NodeID("fedprobe-" + hex.EncodeToString(random))
	raw, err := makeNodeTLSPEM(node)
	if err != nil {
		return nil, fmt.Errorf("generate ephemeral probe identity: %w", err)
	}
	identity, err := parseNodeTLS(raw, node)
	if err != nil {
		return nil, fmt.Errorf("parse ephemeral probe identity: %w", err)
	}
	return identity, nil
}

func (r *probeRunner) probeForgedOrigin(ctx context.Context) error {
	request, err := r.freshRequest()
	if err != nil {
		return err
	}
	envelope, _, err := request.DecodeEnvelope()
	if err != nil {
		return err
	}
	envelope.OriginNode = "fedprobe-forged"
	envelope.RouteDigest = probeRouteDigest(envelope.MessageID, envelope.OriginNode,
		envelope.OriginAgent, envelope.RecipientAgents)
	request.Envelope, err = json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("encode forged-origin envelope: %w", err)
	}
	request.Sender.Node = envelope.OriginNode
	request.Sender.Issuer = "node:" + string(envelope.OriginNode)
	exact, err := request.CanonicalBytes()
	if err != nil {
		return fmt.Errorf("encode forged-origin request: %w", err)
	}
	client := r.client(&r.inputs.identity.Certificate, tls.VersionTLS13, 0)
	defer client.CloseIdleConnections()
	response, err := r.request(ctx, client, http.MethodPost, "/v1/deliver", exact)
	if err != nil {
		return err
	}
	if response.status != http.StatusForbidden {
		return fmt.Errorf("forged origin returned HTTP %d", response.status)
	}
	decoder := json.NewDecoder(bytes.NewReader(response.body))
	decoder.DisallowUnknownFields()
	var refusal deliveryErrorResponse
	if err := decoder.Decode(&refusal); err != nil || requireJSONEOF(decoder) != nil ||
		refusal.ErrorCode != protocol.ErrorCodeNotFound {
		return fmt.Errorf("forged origin did not return the bounded NOT_FOUND refusal")
	}
	return nil
}

func probeRouteDigest(
	messageID protocol.ULID,
	node protocol.NodeID,
	sender protocol.AgentID,
	recipients []protocol.AgentID,
) protocol.Digest {
	names := make([]string, len(recipients))
	for i, recipient := range recipients {
		names[i] = string(recipient)
	}
	sort.Strings(names)
	digest := sha256.Sum256([]byte(string(messageID) + "|" + string(node) + "|" +
		string(sender) + "|" + strings.Join(names, ",")))
	return protocol.Digest("sha256:" + hex.EncodeToString(digest[:]))
}

func (r *probeRunner) probeInvalidSignature(ctx context.Context) error {
	request, err := r.freshRequest()
	if err != nil {
		return err
	}
	envelope, _, err := request.DecodeEnvelope()
	if err != nil {
		return err
	}
	envelope.Signature = bytes.Clone(envelope.Signature)
	envelope.Signature[0] ^= 0xff
	request.Envelope, err = json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("encode invalid-signature envelope: %w", err)
	}
	exact, err := request.CanonicalBytes()
	if err != nil {
		return fmt.Errorf("encode invalid-signature request: %w", err)
	}
	client := r.client(&r.inputs.identity.Certificate, tls.VersionTLS13, 0)
	defer client.CloseIdleConnections()
	response, err := r.request(ctx, client, http.MethodPost, "/v1/deliver", exact)
	if err != nil {
		return err
	}
	if response.status != http.StatusBadRequest {
		return fmt.Errorf("invalid signature returned HTTP %d", response.status)
	}
	return nil
}

func (r *probeRunner) freshRequest() (DeliveryRequest, error) {
	request, err := DecodeDeliveryRequest(r.inputs.exactRequest)
	if err != nil {
		return DeliveryRequest{}, fmt.Errorf("clone stored probe request: %w", err)
	}
	nonce := make([]byte, deliveryNonceBytes)
	if _, err := rand.Read(nonce); err != nil {
		return DeliveryRequest{}, fmt.Errorf("generate federation probe nonce: %w", err)
	}
	request.Nonce = nonce
	request.Sequence = r.inputs.nextSequence
	r.inputs.nextSequence++
	return request, nil
}

func (r *probeRunner) probeExactReplay(ctx context.Context) error {
	client := r.client(&r.inputs.identity.Certificate, tls.VersionTLS13, 0)
	defer client.CloseIdleConnections()
	first, err := r.request(ctx, client, http.MethodPost, "/v1/deliver", r.inputs.exactRequest)
	if err != nil {
		return err
	}
	second, err := r.request(ctx, client, http.MethodPost, "/v1/deliver", r.inputs.exactRequest)
	if err != nil {
		return err
	}
	if first.status != http.StatusOK || second.status != http.StatusOK {
		return fmt.Errorf("exact replay returned HTTP %d then HTTP %d", first.status, second.status)
	}
	if !bytes.Equal(first.body, second.body) || !bytes.Equal(first.body, r.inputs.exactReceipt) {
		return fmt.Errorf("exact replay did not return the original byte-identical receipt")
	}
	if err := verifyProbeReceipt(r.inputs.request, first.body, r.inputs.targetPublicKey); err != nil {
		return fmt.Errorf("verify replay receipt: %w", err)
	}
	return nil
}

func verifyProbeReceipt(
	request DeliveryRequest,
	raw []byte,
	publicKey ed25519.PublicKey,
) error {
	receipt, err := DecodeDeliveryReceipt(raw)
	if err != nil {
		return err
	}
	if receipt.Status != DeliveryReceiptStoredRemote {
		return fmt.Errorf("receipt is not STORED_REMOTE")
	}
	if err := receipt.Verify(publicKey); err != nil {
		return err
	}
	envelope, _, err := request.DecodeEnvelope()
	if err != nil {
		return err
	}
	requestDigest, err := request.RequestSHA256()
	if err != nil {
		return err
	}
	envelopeDigest, err := request.EnvelopeSHA256()
	if err != nil {
		return err
	}
	if receipt.MessageID != envelope.MessageID || receipt.OriginNode != envelope.OriginNode ||
		receipt.DestinationNode != envelope.DestinationNode ||
		receipt.DeliveryGeneration != request.DeliveryGeneration ||
		receipt.Sequence != request.Sequence || !bytes.Equal(receipt.Nonce, request.Nonce) ||
		receipt.RequestSHA256 != requestDigest || receipt.EnvelopeSHA256 != envelopeDigest ||
		len(receipt.RecipientAgents) != len(request.Recipients) {
		return fmt.Errorf("receipt does not match delivery request")
	}
	for i, recipient := range request.Recipients {
		if receipt.RecipientAgents[i] != recipient.Agent {
			return fmt.Errorf("receipt recipients do not match delivery request")
		}
	}
	return nil
}

type probeHTTPResponse struct {
	status     int
	tlsVersion uint16
	body       []byte
}

func (r *probeRunner) request(
	ctx context.Context,
	client *http.Client,
	method, path string,
	body []byte,
) (probeHTTPResponse, error) {
	request, err := http.NewRequestWithContext(ctx, method, r.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return probeHTTPResponse{}, fmt.Errorf("build probe request: %w", err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := client.Do(request)
	if err != nil {
		return probeHTTPResponse{}, fmt.Errorf("send probe request: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(response.Body, maxReceiptBody+1))
	if err != nil {
		return probeHTTPResponse{}, fmt.Errorf("read probe response: %w", err)
	}
	if len(raw) > maxReceiptBody {
		return probeHTTPResponse{}, fmt.Errorf("read probe response: body exceeds limit")
	}
	var version uint16
	if response.TLS != nil {
		version = response.TLS.Version
	}
	return probeHTTPResponse{status: response.StatusCode, tlsVersion: version, body: raw}, nil
}

func (r *probeRunner) client(certificate *tls.Certificate, minVersion, maxVersion uint16) *http.Client {
	config := &tls.Config{
		MinVersion: minVersion, MaxVersion: maxVersion,
		InsecureSkipVerify: true, //nolint:gosec // Exact certificate pin and URI identity are verified below.
		VerifyConnection: func(state tls.ConnectionState) error {
			if len(state.PeerCertificates) == 0 {
				return fmt.Errorf("server presented no certificate")
			}
			leaf := state.PeerCertificates[0]
			now := r.clock.Now().UTC()
			if now.Before(leaf.NotBefore) || now.After(leaf.NotAfter) ||
				!hasExtKeyUsage(leaf, x509.ExtKeyUsageServerAuth) {
				return fmt.Errorf("server certificate is not currently valid for server authentication")
			}
			node, err := nodeIDFromCertificate(leaf)
			if err != nil {
				return err
			}
			if node != r.inputs.targetNode {
				return fmt.Errorf("server certificate identifies an unexpected node")
			}
			if !fingerprintsEqual(r.fingerprint, Fingerprint(leaf)) {
				return fmt.Errorf("server certificate fingerprint does not match its pin")
			}
			return nil
		},
	}
	if certificate != nil {
		config.Certificates = []tls.Certificate{*certificate}
	}
	transport := &http.Transport{TLSClientConfig: config}
	return &http.Client{
		Transport: transport,
		Timeout:   r.timeout,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}
