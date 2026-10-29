package federation

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func privateDataDir(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "data")
	require.NoError(t, os.Mkdir(dir, 0o700))
	return dir
}

func certificatePEM(identity *NodeTLS) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: identity.Leaf.Raw})
}

func writeTrustBundle(t *testing.T, identities ...*NodeTLS) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "trust")
	require.NoError(t, os.Mkdir(dir, 0o700))
	for index, identity := range identities {
		name := filepath.Join(dir, identity.Leaf.Subject.CommonName+"-"+string(rune('a'+index))+".pem")
		require.NoError(t, os.WriteFile(name, certificatePEM(identity), 0o600))
	}
	return dir
}

func TestEnsureNodeTLSPersistsStrictIdentity(t *testing.T) {
	t.Parallel()

	dataDir := privateDataDir(t)
	first, err := EnsureNodeTLS(dataDir, "node-a")
	require.NoError(t, err)
	second, err := EnsureNodeTLS(dataDir, "node-a")
	require.NoError(t, err)
	require.Equal(t, first.Fingerprint, second.Fingerprint)
	require.Equal(t, certificateNotBefore, first.Leaf.NotBefore)
	require.Equal(t, certificateNotAfter, first.Leaf.NotAfter)
	require.Len(t, first.Leaf.URIs, 1)
	require.Equal(t, "gifford://node/node-a", first.Leaf.URIs[0].String())
	require.True(t, hasExtKeyUsage(first.Leaf, x509.ExtKeyUsageServerAuth))
	require.True(t, hasExtKeyUsage(first.Leaf, x509.ExtKeyUsageClientAuth))

	info, err := os.Stat(filepath.Join(dataDir, "keys", tlsIdentityFile))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}

func TestEnsureNodeTLSRefusesBroadKeysDirectory(t *testing.T) {
	t.Parallel()

	dataDir := privateDataDir(t)
	keysDir := filepath.Join(dataDir, "keys")
	require.NoError(t, os.Mkdir(keysDir, 0o755))
	_, err := EnsureNodeTLS(dataDir, "node-a")
	require.Error(t, err)
	require.Contains(t, err.Error(), "mode 755")
}

func TestLoadTrustBundleRejectsInvalidCertificates(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "trust")
	require.NoError(t, os.Mkdir(dir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "bad.pem"), []byte("not PEM\n"), 0o600))
	_, err := LoadTrustBundle(dir)
	require.Error(t, err)
}

func TestLoadTrustBundleRejectsCertificateWithoutNodeURI(t *testing.T) {
	t.Parallel()

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour),
		NotAfter: time.Now().Add(time.Hour), ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, publicKey, privateKey)
	require.NoError(t, err)
	dir := filepath.Join(t.TempDir(), "trust")
	require.NoError(t, os.Mkdir(dir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "no-uri.pem"), pem.EncodeToMemory(
		&pem.Block{Type: "CERTIFICATE", Bytes: der},
	), 0o600))
	_, err = LoadTrustBundle(dir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exactly one URI SAN")
}
