package federation

import (
	"bytes"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// TrustBundle is the immutable set of peer certificates accepted for TLS
// chain verification.
type TrustBundle struct {
	roots *x509.CertPool
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
	roots := x509.NewCertPool()
	for _, entry := range entries {
		if entry.IsDir() || entry.Type()&fs.ModeType != 0 {
			return nil, fmt.Errorf("trust bundle entry %s is not a regular file", entry.Name())
		}
		path := filepath.Join(dir, entry.Name())
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read trust bundle entry %s: %w", entry.Name(), err)
		}
		certs, err := parseCertificatePEM(raw)
		if err != nil {
			return nil, fmt.Errorf("parse trust bundle entry %s: %w", entry.Name(), err)
		}
		for _, cert := range certs {
			if _, err := nodeIDFromCertificate(cert); err != nil {
				return nil, fmt.Errorf("trust bundle entry %s: %w", entry.Name(), err)
			}
			roots.AddCert(cert)
		}
	}
	return &TrustBundle{roots: roots}, nil
}

func parseCertificatePEM(raw []byte) ([]*x509.Certificate, error) {
	var certs []*x509.Certificate
	for len(bytes.TrimSpace(raw)) > 0 {
		block, rest := pem.Decode(raw)
		if block == nil {
			return nil, fmt.Errorf("invalid PEM data")
		}
		if block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
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
