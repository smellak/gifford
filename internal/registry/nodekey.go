package registry

import (
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// This file holds the node signing key: an Ed25519 key under the data
// directory, created on first use. Key material never leaves this file in
// arguments, the environment, or logs: the package logs nothing.

const (
	// nodeKeyDirNames locates the key: <data dir>/keys/node.
	nodeKeysDir = "keys"
	nodeNameDir = "node"
	// nodeSeedFile holds the raw 32-byte seed, mode 0600.
	nodeSeedFile = "node.seed"
)

// nodeKey stores the loaded node key with the directory it lives in.
type nodeKey struct {
	dir  string
	priv ed25519.PrivateKey
}

// loadNodeKey returns the node key under dataDir, creating it on first
// use. The seed file holds the raw 32-byte seed with mode 0600 inside
// directories with mode 0700. A present file with any other length is an
// error: the key is never silently replaced, so existing rows keep
// verifying against the key that signed them.
func loadNodeKey(dataDir string) (nodeKey, error) {
	dir := filepath.Join(dataDir, nodeKeysDir, nodeNameDir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nodeKey{}, fmt.Errorf("create node key directory: %w", err)
	}
	path := filepath.Join(dir, nodeSeedFile)
	seed, err := os.ReadFile(path)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return nodeKey{}, fmt.Errorf("read node key: %w", err)
		}
		return createNodeKey(path)
	}
	if len(seed) != ed25519.SeedSize {
		return nodeKey{}, fmt.Errorf("node key holds %d bytes, want %d: %w",
			len(seed), ed25519.SeedSize, ErrInvalidArgument)
	}
	return nodeKey{dir: dir, priv: ed25519.NewKeyFromSeed(seed)}, nil
}

// createNodeKey generates a fresh seed and stores it with mode 0600. The
// exclusive create lets two racing first uses agree: the loser reads the
// winner's file.
func createNodeKey(path string) (nodeKey, error) {
	seed := make([]byte, ed25519.SeedSize)
	if _, err := rand.Read(seed); err != nil {
		return nodeKey{}, fmt.Errorf("generate node key: %w", err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		if !errors.Is(err, fs.ErrExist) {
			return nodeKey{}, fmt.Errorf("create node key: %w", err)
		}
		seed, err := os.ReadFile(path)
		if err != nil {
			return nodeKey{}, fmt.Errorf("read raced node key: %w", err)
		}
		if len(seed) != ed25519.SeedSize {
			return nodeKey{}, fmt.Errorf("node key holds %d bytes, want %d: %w",
				len(seed), ed25519.SeedSize, ErrInvalidArgument)
		}
		return nodeKey{dir: filepath.Dir(path), priv: ed25519.NewKeyFromSeed(seed)}, nil
	}
	if _, err := file.Write(seed); err != nil {
		_ = file.Close()
		return nodeKey{}, fmt.Errorf("write node key: %w", err)
	}
	if err := file.Close(); err != nil {
		return nodeKey{}, fmt.Errorf("close node key: %w", err)
	}
	return nodeKey{dir: filepath.Dir(path), priv: ed25519.NewKeyFromSeed(seed)}, nil
}
