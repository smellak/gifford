package federation

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"io/fs"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/smellak/gifford/internal/protocol"
)

const (
	tlsKeysDir      = "keys"
	tlsIdentityFile = "node-tls.pem"
)

var tlsStageCounter atomic.Uint64

var (
	certificateNotBefore = time.Date(2025, time.January, 1, 0, 0, 0, 0, time.UTC)
	certificateNotAfter  = time.Date(2045, time.January, 1, 0, 0, 0, 0, time.UTC)
)

// NodeTLS is the local node's persistent TLS identity.
type NodeTLS struct {
	Certificate tls.Certificate
	Leaf        *x509.Certificate
	Fingerprint string
}

// EnsureNodeTLS loads the local TLS identity or creates it on first use. The
// combined private key and certificate file is mode 0600 inside data/keys,
// which must be mode 0700. Existing broader modes are refused, never repaired.
func EnsureNodeTLS(dataDir string, nodeID protocol.NodeID) (*NodeTLS, error) {
	if dataDir == "" || !nodeID.Valid() {
		return nil, fmt.Errorf("ensure node TLS identity: invalid data directory or node id")
	}
	if err := ensurePrivateDir(dataDir); err != nil {
		return nil, err
	}
	keysDir := filepath.Join(dataDir, tlsKeysDir)
	if err := ensurePrivateDir(keysDir); err != nil {
		return nil, err
	}
	path := filepath.Join(keysDir, tlsIdentityFile)
	raw, err := os.ReadFile(path)
	if err == nil {
		if err := requireMode(path, 0o600); err != nil {
			return nil, err
		}
		return parseNodeTLS(raw, nodeID)
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("read TLS identity: %w", err)
	}
	raw, err = makeNodeTLSPEM(nodeID)
	if err != nil {
		return nil, err
	}
	if err := publishIdentity(keysDir, path, raw); err != nil {
		return nil, err
	}
	raw, err = os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read published TLS identity: %w", err)
	}
	if err := requireMode(path, 0o600); err != nil {
		return nil, err
	}
	return parseNodeTLS(raw, nodeID)
}

func ensurePrivateDir(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return fmt.Errorf("create TLS key directory %s: %w", path, err)
	}
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat TLS key directory %s: %w", path, err)
	}
	if !info.IsDir() || info.Mode().Perm() != 0o700 {
		return fmt.Errorf("TLS key directory %s has mode %o, want 700", path, info.Mode().Perm())
	}
	return nil
}

func requireMode(path string, want os.FileMode) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat TLS identity %s: %w", path, err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != want {
		return fmt.Errorf("TLS identity %s has mode %o, want %o", path, info.Mode().Perm(), want)
	}
	return nil
}

func publishIdentity(dir, path string, raw []byte) error {
	staging := filepath.Join(dir, ".node-tls.tmp."+strconv.Itoa(os.Getpid())+"."+
		strconv.FormatUint(tlsStageCounter.Add(1), 10))
	file, err := os.OpenFile(staging, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("stage TLS identity: %w", err)
	}
	cleanup := func() { _ = os.Remove(staging) }
	if _, err := file.Write(raw); err != nil {
		_ = file.Close()
		cleanup()
		return fmt.Errorf("write TLS identity: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		cleanup()
		return fmt.Errorf("sync TLS identity: %w", err)
	}
	if err := file.Close(); err != nil {
		cleanup()
		return fmt.Errorf("close TLS identity: %w", err)
	}
	if err := os.Link(staging, path); err != nil && !errors.Is(err, fs.ErrExist) {
		cleanup()
		return fmt.Errorf("publish TLS identity: %w", err)
	}
	cleanup()
	return nil
}

func makeNodeTLSPEM(nodeID protocol.NodeID) ([]byte, error) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate TLS private key: %w", err)
	}
	uri := nodeURI(nodeID)
	serialHash := sha256.Sum256([]byte("gifford-node-tls:" + nodeID))
	serial := new(big.Int).SetBytes(serialHash[:20])
	serial.SetBit(serial, 159, 0)
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: string(nodeID)},
		NotBefore:    certificateNotBefore,
		NotAfter:     certificateNotAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		URIs:         []*url.URL{uri},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, publicKey, privateKey)
	if err != nil {
		return nil, fmt.Errorf("create TLS certificate: %w", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return nil, fmt.Errorf("marshal TLS private key: %w", err)
	}
	var out bytes.Buffer
	if err := pem.Encode(&out, &pem.Block{Type: "CERTIFICATE", Bytes: der}); err != nil {
		return nil, fmt.Errorf("encode TLS certificate: %w", err)
	}
	if err := pem.Encode(&out, &pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}); err != nil {
		return nil, fmt.Errorf("encode TLS private key: %w", err)
	}
	return out.Bytes(), nil
}

func parseNodeTLS(raw []byte, nodeID protocol.NodeID) (*NodeTLS, error) {
	pair, err := tls.X509KeyPair(raw, raw)
	if err != nil {
		return nil, fmt.Errorf("parse TLS identity: %w", err)
	}
	if len(pair.Certificate) != 1 {
		return nil, fmt.Errorf("TLS identity has %d certificates, want 1", len(pair.Certificate))
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return nil, fmt.Errorf("parse TLS certificate: %w", err)
	}
	got, err := nodeIDFromCertificate(leaf)
	if err != nil {
		return nil, err
	}
	if got != nodeID {
		return nil, fmt.Errorf("TLS certificate identifies node %s, want %s", got, nodeID)
	}
	if !hasExtKeyUsage(leaf, x509.ExtKeyUsageServerAuth) ||
		!hasExtKeyUsage(leaf, x509.ExtKeyUsageClientAuth) {
		return nil, fmt.Errorf("TLS certificate lacks server or client authentication usage")
	}
	pair.Leaf = leaf
	return &NodeTLS{Certificate: pair, Leaf: leaf, Fingerprint: Fingerprint(leaf)}, nil
}

func hasExtKeyUsage(cert *x509.Certificate, want x509.ExtKeyUsage) bool {
	for _, usage := range cert.ExtKeyUsage {
		if usage == want {
			return true
		}
	}
	return false
}

func nodeURI(nodeID protocol.NodeID) *url.URL {
	return &url.URL{Scheme: "gifford", Host: "node", Path: "/" + string(nodeID)}
}

func nodeIDFromCertificate(cert *x509.Certificate) (protocol.NodeID, error) {
	if cert == nil || len(cert.URIs) != 1 {
		return "", fmt.Errorf("TLS certificate must have exactly one URI SAN")
	}
	uri := cert.URIs[0]
	if uri.Scheme != "gifford" || uri.Host != "node" || uri.User != nil || uri.Opaque != "" ||
		uri.RawQuery != "" || uri.Fragment != "" {
		return "", fmt.Errorf("TLS certificate URI SAN is not a gifford node identity")
	}
	if len(uri.Path) < 2 || uri.Path[0] != '/' || bytes.ContainsRune([]byte(uri.Path[1:]), '/') {
		return "", fmt.Errorf("TLS certificate URI SAN carries an invalid node id")
	}
	id := protocol.NodeID(uri.Path[1:])
	if !id.Valid() {
		return "", fmt.Errorf("TLS certificate URI SAN carries an invalid node id")
	}
	return id, nil
}

// Fingerprint returns the lowercase SHA-256 fingerprint of a certificate's
// DER encoding.
func Fingerprint(cert *x509.Certificate) string {
	if cert == nil {
		return ""
	}
	sum := sha256.Sum256(cert.Raw)
	return hex.EncodeToString(sum[:])
}
