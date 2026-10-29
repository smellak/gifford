package registry

import (
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
)

// This file holds the node signing key: an Ed25519 key under the data
// directory, created on first use. Key material never leaves this file in
// arguments, the environment, or logs: the package logs nothing.
//
// The key directory and the seed file double as a trust signal: loading
// refuses insecure modes instead of repairing them, so a loosened
// directory never goes unnoticed. Creation publishes through a temporary
// file and an atomic link, so readers never observe a partial seed.

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

// stageCounter disambiguates staging files of racing first uses in one
// process; the atomic link still decides the single winner.
var stageCounter atomic.Uint64

// loadNodeKey returns the node key under dataDir, creating it on first
// use. The seed file holds the raw 32-byte seed with mode 0600 inside a
// directory with mode 0700; anything else is refused. A present file with
// any other length is an error: the key is never silently replaced, so
// existing rows keep verifying against the key that signed them.
func loadNodeKey(dataDir string) (nodeKey, error) {
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return nodeKey{}, fmt.Errorf("create data directory: %w", err)
	}
	if err := checkMode(dataDir, 0o700); err != nil {
		return nodeKey{}, err
	}
	keys := filepath.Join(dataDir, nodeKeysDir)
	if err := os.MkdirAll(keys, 0o700); err != nil {
		return nodeKey{}, fmt.Errorf("create node key directory: %w", err)
	}
	if err := checkMode(keys, 0o700); err != nil {
		return nodeKey{}, err
	}
	dir := filepath.Join(keys, nodeNameDir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nodeKey{}, fmt.Errorf("create node key directory: %w", err)
	}
	if err := checkMode(dir, 0o700); err != nil {
		return nodeKey{}, err
	}
	path := filepath.Join(dir, nodeSeedFile)
	seed, err := os.ReadFile(path)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return nodeKey{}, fmt.Errorf("read node key: %w", err)
		}
		return createNodeKey(dir, path)
	}
	if err := checkMode(path, 0o600); err != nil {
		return nodeKey{}, err
	}
	if len(seed) != ed25519.SeedSize {
		return nodeKey{}, fmt.Errorf("node key holds %d bytes, want %d: %w",
			len(seed), ed25519.SeedSize, ErrInvalidArgument)
	}
	return nodeKey{dir: dir, priv: ed25519.NewKeyFromSeed(seed)}, nil
}

// checkMode refuses a path whose permission bits differ from want. The
// check reads metadata, so it holds for any effective user id.
func checkMode(path string, want os.FileMode) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat node key path: %w", err)
	}
	if info.Mode().Perm() != want {
		return fmt.Errorf("node key path %s holds mode %o, want %o: %w",
			path, info.Mode().Perm(), want, ErrInvalidArgument)
	}
	return nil
}

// createNodeKey generates a fresh seed and publishes it under path. The
// seed lands complete: it is fully written and synced to a temporary file
// first, then linked into place, so the final name never shows a partial
// seed. A lost link race means another first use won: the loser drops its
// temporary file and loads the winner's key.
func createNodeKey(dir, path string) (nodeKey, error) {
	seed := make([]byte, ed25519.SeedSize)
	if _, err := rand.Read(seed); err != nil {
		return nodeKey{}, fmt.Errorf("generate node key: %w", err)
	}
	staging := path + ".tmp." + strconv.Itoa(os.Getpid()) + "." + strconv.FormatUint(stageCounter.Add(1), 10)
	staged, err := os.OpenFile(staging, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nodeKey{}, fmt.Errorf("stage node key: %w", err)
	}
	if err := writeFull(staged, seed); err != nil {
		_ = staged.Close()
		_ = os.Remove(staging)
		return nodeKey{}, err
	}
	if err := staged.Close(); err != nil {
		_ = os.Remove(staging)
		return nodeKey{}, fmt.Errorf("close node key: %w", err)
	}
	if err := os.Link(staging, path); err != nil {
		_ = os.Remove(staging)
		if !errors.Is(err, fs.ErrExist) {
			return nodeKey{}, fmt.Errorf("publish node key: %w", err)
		}
		return loadPublishedKey(dir, path)
	}
	_ = os.Remove(staging)
	return nodeKey{dir: dir, priv: ed25519.NewKeyFromSeed(seed)}, nil
}

// loadPublishedKey loads the winner's key after losing a creation race.
// The link publishes complete files only, so the read observes a whole
// seed or nothing.
func loadPublishedKey(dir, path string) (nodeKey, error) {
	seed, err := os.ReadFile(path)
	if err != nil {
		return nodeKey{}, fmt.Errorf("read raced node key: %w", err)
	}
	if err := checkMode(path, 0o600); err != nil {
		return nodeKey{}, err
	}
	if len(seed) != ed25519.SeedSize {
		return nodeKey{}, fmt.Errorf("node key holds %d bytes, want %d: %w",
			len(seed), ed25519.SeedSize, ErrInvalidArgument)
	}
	return nodeKey{dir: dir, priv: ed25519.NewKeyFromSeed(seed)}, nil
}

// writeFull writes every byte of seed and syncs the file, so a crash
// cannot leave a published partial seed behind.
func writeFull(file *os.File, seed []byte) error {
	written := 0
	for written < len(seed) {
		n, err := file.Write(seed[written:])
		if err != nil {
			return fmt.Errorf("write node key: %w", err)
		}
		written += n
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync node key: %w", err)
	}
	return nil
}
