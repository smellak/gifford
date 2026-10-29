package endpoint

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

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/xdg"
)

// This file holds the agent signing keys: one Ed25519 key per agent under
// keys/<agent_id>/ in the data directory, created on first use. The
// design calls this from gifford run at the agent's first start; the
// wiring lands later. The private key never travels in argv, the
// environment, or a log: this package logs nothing, and callers receive
// only the public key for registry.Register.
//
// Like the node key, loading refuses insecure modes and creation
// publishes atomically, so readers never observe a partial seed.

const (
	// agentKeysDir locates the keys: <data dir>/keys/<agent_id>.
	agentKeysDir = "keys"
	// agentSeedFile holds the raw 32-byte seed, mode 0600.
	agentSeedFile = "signing.seed"
)

// agentStageCounter disambiguates staging files of racing first uses in
// one process; the atomic link still decides the single winner.
var agentStageCounter atomic.Uint64

// EnsureAgentKey returns the agent's public signing key, creating the key
// on first use. An empty dataDir resolves through internal/xdg. The
// agent id must be canonical.
func EnsureAgentKey(dataDir string, agent protocol.AgentID) (ed25519.PublicKey, error) {
	if !agent.Valid() {
		return nil, fmt.Errorf("agent key for %q: %w", agent, ErrInvalidArgument)
	}
	resolved, err := resolveDataDir(dataDir)
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(resolved, agentKeysDir, string(agent))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create agent key directory: %w", err)
	}
	for _, path := range []struct {
		name string
		want os.FileMode
	}{
		{resolved, 0o700},
		{filepath.Join(resolved, agentKeysDir), 0o700},
		{dir, 0o700},
	} {
		if err := checkKeyMode(path.name, path.want); err != nil {
			return nil, err
		}
	}
	path := filepath.Join(dir, agentSeedFile)
	seed, err := os.ReadFile(path)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("read agent key: %w", err)
		}
		return createAgentKey(path)
	}
	if err := checkKeyMode(path, 0o600); err != nil {
		return nil, err
	}
	if len(seed) != ed25519.SeedSize {
		return nil, fmt.Errorf("agent key holds %d bytes, want %d: %w",
			len(seed), ed25519.SeedSize, ErrInvalidArgument)
	}
	return ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey), nil
}

// LoadAgentPrivate returns the agent's private signing key for envelope
// signing. It never creates: missing keys are an error, so a connection
// without a key offers no tools instead of minting identity.
func LoadAgentPrivate(dataDir string, agent protocol.AgentID) (ed25519.PrivateKey, error) {
	if !agent.Valid() {
		return nil, fmt.Errorf("agent key for %q: %w", agent, ErrInvalidArgument)
	}
	resolved, err := resolveDataDir(dataDir)
	if err != nil {
		return nil, err
	}
	path := filepath.Join(resolved, agentKeysDir, string(agent), agentSeedFile)
	//nolint:gosec // the agent id is charset-validated above: no separators.
	seed, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read agent key: %w", ErrNoIdentity)
	}
	if err := checkKeyMode(path, 0o600); err != nil {
		return nil, err
	}
	if len(seed) != ed25519.SeedSize {
		return nil, fmt.Errorf("agent key holds %d bytes, want %d: %w",
			len(seed), ed25519.SeedSize, ErrInvalidArgument)
	}
	return ed25519.NewKeyFromSeed(seed), nil
}

// resolveDataDir returns dataDir, or the XDG data directory when empty.
func resolveDataDir(dataDir string) (string, error) {
	if dataDir != "" {
		return dataDir, nil
	}
	dirs, err := xdg.Resolve()
	if err != nil {
		return "", fmt.Errorf("resolve data directory: %w", err)
	}
	return dirs.Data, nil
}

// checkKeyMode refuses a path whose permission bits differ from want.
func checkKeyMode(path string, want os.FileMode) error {
	//nolint:gosec // every path derives from a charset-validated agent id.
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat agent key path: %w", err)
	}
	if info.Mode().Perm() != want {
		return fmt.Errorf("agent key path %s holds mode %o, want %o: %w",
			path, info.Mode().Perm(), want, ErrInvalidArgument)
	}
	return nil
}

// createAgentKey generates a fresh seed and publishes it atomically: a
// fully written and synced staging file linked into place, so the final
// name never shows a partial seed. Losing the link race loads the
// winner's key.
func createAgentKey(path string) (ed25519.PublicKey, error) {
	seed := make([]byte, ed25519.SeedSize)
	if _, err := rand.Read(seed); err != nil {
		return nil, fmt.Errorf("generate agent key: %w", err)
	}
	staging := path + ".tmp." + strconv.Itoa(os.Getpid()) + "." +
		strconv.FormatUint(agentStageCounter.Add(1), 10)
	staged, err := os.OpenFile(staging, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("stage agent key: %w", err)
	}
	if err := writeKeyFull(staged, seed); err != nil {
		_ = staged.Close()
		_ = os.Remove(staging)
		return nil, err
	}
	if err := staged.Close(); err != nil {
		_ = os.Remove(staging)
		return nil, fmt.Errorf("close agent key: %w", err)
	}
	if err := os.Link(staging, path); err != nil {
		_ = os.Remove(staging)
		if !errors.Is(err, fs.ErrExist) {
			return nil, fmt.Errorf("publish agent key: %w", err)
		}
		seed, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read raced agent key: %w", err)
		}
		if err := checkKeyMode(path, 0o600); err != nil {
			return nil, err
		}
		if len(seed) != ed25519.SeedSize {
			return nil, fmt.Errorf("agent key holds %d bytes, want %d: %w",
				len(seed), ed25519.SeedSize, ErrInvalidArgument)
		}
		return ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey), nil
	}
	_ = os.Remove(staging)
	return ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey), nil
}

// writeKeyFull writes every byte of seed and syncs the file.
func writeKeyFull(file *os.File, seed []byte) error {
	written := 0
	for written < len(seed) {
		n, err := file.Write(seed[written:])
		if err != nil {
			return fmt.Errorf("write agent key: %w", err)
		}
		written += n
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync agent key: %w", err)
	}
	return nil
}
