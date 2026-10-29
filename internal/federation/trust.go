package federation

import (
	"bytes"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/smellak/gifford/internal/protocol"
)

var bundleStageCounter atomic.Uint64

// TrustBundle is the live set of peer certificates accepted for TLS chain
// verification. Mutations update disk first and then replace the in-memory
// snapshot, so new handshakes observe enrollment and revocation immediately.
type TrustBundle struct {
	dir string

	mu    sync.RWMutex
	files map[string][]*x509.Certificate
	certs map[protocol.NodeID]map[string]*x509.Certificate
}

// LoadTrustBundle loads every regular file in dir as strict certificate PEM.
// An unreadable entry, malformed PEM block, non-certificate block, or
// certificate without an unambiguous gifford node URI SAN rejects the bundle.
func LoadTrustBundle(dir string) (*TrustBundle, error) {
	if dir == "" {
		return nil, fmt.Errorf("trust bundle directory is empty")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read trust bundle directory: %w", err)
	}
	bundle := &TrustBundle{
		dir: filepath.Clean(dir), files: make(map[string][]*x509.Certificate),
		certs: make(map[protocol.NodeID]map[string]*x509.Certificate),
	}
	for _, entry := range entries {
		if entry.IsDir() || entry.Type()&fs.ModeType != 0 {
			return nil, fmt.Errorf("trust bundle entry %s is not a regular file", entry.Name())
		}
		path := filepath.Join(dir, entry.Name())
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read trust bundle entry %s: %w", entry.Name(), err)
		}
		certificates, err := parseCertificatePEM(raw)
		if err != nil {
			return nil, fmt.Errorf("parse trust bundle entry %s: %w", entry.Name(), err)
		}
		for _, cert := range certificates {
			nodeID, err := nodeIDFromCertificate(cert)
			if err != nil {
				return nil, fmt.Errorf("trust bundle entry %s: %w", entry.Name(), err)
			}
			bundle.addMemory(nodeID, cert)
		}
		bundle.files[entry.Name()] = certificates
	}
	return bundle, nil
}

// AddCertificate persists one newly enrolled peer certificate as strict PEM
// and makes it visible to new handshakes. A node already represented anywhere
// in the bundle is rejected instead of silently replacing trust.
func (b *TrustBundle) AddCertificate(cert *x509.Certificate) error {
	nodeID, err := nodeIDFromCertificate(cert)
	if err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.certs[nodeID]) != 0 {
		return fmt.Errorf("trust bundle already contains node %s", nodeID)
	}
	name := string(nodeID) + ".pem"
	if _, ok := b.files[name]; ok {
		return fmt.Errorf("trust bundle file %s already exists", name)
	}
	path := filepath.Join(b.dir, name)
	if _, err := os.Lstat(path); err == nil {
		return fmt.Errorf("trust bundle file %s already exists", name)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("stat trust bundle file %s: %w", name, err)
	}
	raw := pem.EncodeToMemory(&pem.Block{Type: certificatePEMType, Bytes: cert.Raw})
	if err := atomicWriteBundleFile(b.dir, path, raw); err != nil {
		return err
	}
	b.files[name] = []*x509.Certificate{cert}
	b.addMemory(nodeID, cert)
	return nil
}

func (b *TrustBundle) restoreCertificates(certificates []*x509.Certificate) error {
	if len(certificates) == 0 {
		return nil
	}
	nodeID, err := nodeIDFromCertificate(certificates[0])
	if err != nil {
		return err
	}
	for _, cert := range certificates[1:] {
		id, err := nodeIDFromCertificate(cert)
		if err != nil || id != nodeID {
			return fmt.Errorf("restore trust bundle certificates: mixed node identities")
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.certs[nodeID]) != 0 {
		return fmt.Errorf("trust bundle already contains node %s", nodeID)
	}
	name := string(nodeID) + ".pem"
	path := filepath.Join(b.dir, name)
	if _, err := os.Lstat(path); err == nil {
		return fmt.Errorf("trust bundle file %s already exists", name)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("stat trust bundle file %s: %w", name, err)
	}
	if err := atomicWriteBundleFile(b.dir, path, encodeCertificatesPEM(certificates)); err != nil {
		return err
	}
	b.files[name] = append([]*x509.Certificate(nil), certificates...)
	for _, cert := range certificates {
		b.addMemory(nodeID, cert)
	}
	return nil
}

// RemoveCertificate removes every certificate for nodeID from disk and the
// live snapshot. It returns the removed certificates so a caller can restore
// them if a surrounding database transaction fails to commit.
func (b *TrustBundle) RemoveCertificate(nodeID protocol.NodeID) ([]*x509.Certificate, error) {
	if !nodeID.Valid() {
		return nil, fmt.Errorf("remove trust bundle certificate: invalid node id")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	byFingerprint := b.certs[nodeID]
	if len(byFingerprint) == 0 {
		return nil, nil
	}
	removed := make([]*x509.Certificate, 0, len(byFingerprint))
	for _, cert := range byFingerprint {
		removed = append(removed, cert)
	}
	sort.Slice(removed, func(i, j int) bool { return Fingerprint(removed[i]) < Fingerprint(removed[j]) })
	for name, certificates := range b.files {
		kept := make([]*x509.Certificate, 0, len(certificates))
		changed := false
		for _, cert := range certificates {
			id, err := nodeIDFromCertificate(cert)
			if err != nil {
				return nil, fmt.Errorf("revalidate trust bundle entry %s: %w", name, err)
			}
			if id == nodeID {
				changed = true
				continue
			}
			kept = append(kept, cert)
		}
		if !changed {
			continue
		}
		path := filepath.Join(b.dir, name)
		if len(kept) == 0 {
			if err := os.Remove(path); err != nil {
				return nil, fmt.Errorf("remove trust bundle entry %s: %w", name, err)
			}
			delete(b.files, name)
			continue
		}
		raw := encodeCertificatesPEM(kept)
		if err := atomicWriteBundleFile(b.dir, path, raw); err != nil {
			return nil, fmt.Errorf("rewrite trust bundle entry %s: %w", name, err)
		}
		b.files[name] = append([]*x509.Certificate(nil), kept...)
	}
	delete(b.certs, nodeID)
	return removed, nil
}

// ContainsNode reports whether any certificate for nodeID is in the current
// in-memory bundle.
func (b *TrustBundle) ContainsNode(nodeID protocol.NodeID) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.certs[nodeID]) != 0
}

// rootPool returns an independent snapshot safe for verification while the
// live bundle changes.
func (b *TrustBundle) rootPool() *x509.CertPool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	pool := x509.NewCertPool()
	for _, certificates := range b.certs {
		for _, cert := range certificates {
			pool.AddCert(cert)
		}
	}
	return pool
}

func (b *TrustBundle) addMemory(nodeID protocol.NodeID, cert *x509.Certificate) {
	if b.certs[nodeID] == nil {
		b.certs[nodeID] = make(map[string]*x509.Certificate)
	}
	b.certs[nodeID][Fingerprint(cert)] = cert
}

func atomicWriteBundleFile(dir, path string, raw []byte) error {
	staging := filepath.Join(dir, ".bundle.tmp."+strconv.Itoa(os.Getpid())+"."+
		strconv.FormatUint(bundleStageCounter.Add(1), 10))
	file, err := os.OpenFile(staging, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("stage trust bundle entry: %w", err)
	}
	cleanup := func() { _ = os.Remove(staging) }
	if _, err := file.Write(raw); err != nil {
		_ = file.Close()
		cleanup()
		return fmt.Errorf("write trust bundle entry: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		cleanup()
		return fmt.Errorf("sync trust bundle entry: %w", err)
	}
	if err := file.Close(); err != nil {
		cleanup()
		return fmt.Errorf("close trust bundle entry: %w", err)
	}
	if err := os.Rename(staging, path); err != nil {
		cleanup()
		return fmt.Errorf("publish trust bundle entry: %w", err)
	}
	return nil
}

func encodeCertificatesPEM(certificates []*x509.Certificate) []byte {
	size := 0
	for _, cert := range certificates {
		size += len(cert.Raw) + 64
	}
	raw := make([]byte, 0, size)
	for _, cert := range certificates {
		raw = append(raw, pem.EncodeToMemory(&pem.Block{Type: certificatePEMType, Bytes: cert.Raw})...)
	}
	return raw
}

func parseCertificatePEM(raw []byte) ([]*x509.Certificate, error) {
	var certs []*x509.Certificate
	for len(bytes.TrimSpace(raw)) > 0 {
		block, rest := pem.Decode(raw)
		if block == nil {
			return nil, fmt.Errorf("invalid PEM data")
		}
		if block.Type != certificatePEMType || len(block.Headers) != 0 {
			return nil, fmt.Errorf("unexpected PEM block %q", block.Type)
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parse certificate: %w", err)
		}
		certs = append(certs, cert)
		raw = rest
	}
	if len(certs) == 0 {
		return nil, fmt.Errorf("file has no certificates")
	}
	return certs, nil
}
