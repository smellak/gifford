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
	"strings"

	"github.com/smellak/gifford/internal/protocol"
)

// This file holds the node's system sender: the local-only identity
// that files operator escalation notices as normal signed messages.
// The loop owns its private key, so its signature is legitimate, never
// forged. The sender is never an agent: it has no registry record, no
// session and no peer announcement; the deny list below keeps agents
// from claiming its id, and federated rows carrying it are refused.
//
// The key lives like the node key: a raw 32-byte seed with mode 0600
// inside 0700 directories, created on first use through a temporary
// file and an atomic link. Rotation moves the active seed aside and
// creates a fresh one; retired seeds stay readable beside it, so
// notices signed before a rotation keep verifying. Retired seeds are
// 32 bytes each and rotations are exceptional, so no pruning runs.
//
// Nothing here leaves the file in arguments, the environment or logs:
// the package logs nothing, and errors never carry key material.

// EmitterAgentID is the reserved sender of operator escalation
// notices. It fits the agent grammar on purpose, so the reservation
// is a deny list, never a shape: anything conforming is registrable
// except the ids named here.
const EmitterAgentID = protocol.AgentID("system-notifier")

// ReservedAgentID reports whether id is withheld from agents. Only
// the node's own paths may emit as a reserved id.
func ReservedAgentID(id protocol.AgentID) bool {
	return id == EmitterAgentID
}

// The emitter key id carries the sender and the rotation generation:
// ed25519:system-notifier:v<generation>. The wire contract pins the
// agent part to the origin agent, so rotations differ only in the
// version suffix; the retired files stay addressable by it.
func emitterKeyID(generation int) protocol.SigningKeyID {
	return protocol.SigningKeyID("ed25519:" + string(EmitterAgentID) +
		":v" + strconv.Itoa(generation))
}

// emitterKeyGeneration reads the rotation generation back from a key
// id the wire contract already validated.
func emitterKeyGeneration(keyID string) (int, error) {
	agent, version, ok := strings.Cut(strings.TrimPrefix(keyID, "ed25519:"), ":v")
	if !ok || agent != string(EmitterAgentID) {
		return 0, fmt.Errorf("emitter key id %q is not the system sender: %w", keyID, ErrInvalidArgument)
	}
	generation, err := strconv.Atoi(version)
	if err != nil || generation < 1 {
		return 0, fmt.Errorf("emitter key id %q holds no generation: %w", keyID, ErrInvalidArgument)
	}
	return generation, nil
}

// EmitterKey is the active system sender key: its id for envelopes
// and its private half for signing.
type EmitterKey struct {
	KeyID protocol.SigningKeyID
	Priv  ed25519.PrivateKey
}

// emitterLayout locates the sender directory and its files.
func emitterLayout(dataDir string) (dir, seed, version, retired string) {
	dir = filepath.Join(dataDir, nodeKeysDir, string(EmitterAgentID))
	seed = filepath.Join(dir, "emitter.seed")
	version = filepath.Join(dir, "emitter.version")
	retired = filepath.Join(dir, "retired")
	return dir, seed, version, retired
}

// ensureEmitterDir creates the sender directory with node key modes.
func ensureEmitterDir(dataDir string) (dir, seed, version, retired string, err error) {
	dir, seed, version, retired = emitterLayout(dataDir)
	for _, path := range []string{dataDir, filepath.Join(dataDir, nodeKeysDir), dir, retired} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			return "", "", "", "", fmt.Errorf("create emitter key directory: %w", err)
		}
		if err := checkMode(path, 0o700); err != nil {
			return "", "", "", "", err
		}
	}
	return dir, seed, version, retired, nil
}

// readEmitterVersion reads the active generation, defaulting to zero
// when no key was ever created.
func readEmitterVersion(versionPath string) (int, error) {
	raw, err := os.ReadFile(versionPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return 0, nil
		}
		return 0, fmt.Errorf("read emitter key version: %w", err)
	}
	generation, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil || generation < 0 {
		return 0, fmt.Errorf("emitter key version holds %q: %w", raw, ErrInvalidArgument)
	}
	return generation, nil
}

// writeEmitterVersion publishes a generation number, complete and
// synced before it replaces the previous one.
func writeEmitterVersion(versionPath string, generation int) error {
	staging := versionPath + ".tmp"
	if err := os.WriteFile(staging, []byte(strconv.Itoa(generation)+"\n"), 0o600); err != nil {
		return fmt.Errorf("stage emitter key version: %w", err)
	}
	if err := os.Rename(staging, versionPath); err != nil {
		_ = os.Remove(staging)
		return fmt.Errorf("publish emitter key version: %w", err)
	}
	return nil
}

// EmitterSigningKey returns the active system sender key, creating it
// on first use. Concurrent first uses race on the same atomic link
// the node key uses: one seed wins and every caller loads it.
func (r *Registry) EmitterSigningKey() (EmitterKey, error) {
	_, seed, versionPath, _, err := ensureEmitterDir(r.dataDir)
	if err != nil {
		return EmitterKey{}, err
	}
	generation, err := readEmitterVersion(versionPath)
	if err != nil {
		return EmitterKey{}, err
	}
	if _, statErr := os.Stat(seed); statErr == nil && generation < 1 {
		// A seed without a version never signs under a guessed
		// generation: refuse instead of forking the key id.
		return EmitterKey{}, fmt.Errorf("emitter key without version: %w", ErrInvalidArgument)
	} else if statErr != nil && !errors.Is(statErr, fs.ErrNotExist) {
		return EmitterKey{}, fmt.Errorf("stat emitter key: %w", statErr)
	}
	if generation < 1 {
		generation = 1
		if err := writeEmitterVersion(versionPath, generation); err != nil {
			return EmitterKey{}, err
		}
	}
	seedBytes, err := os.ReadFile(seed)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return EmitterKey{}, fmt.Errorf("read emitter key: %w", err)
		}
		created, err := createEmitterSeed(seed)
		if err != nil {
			return EmitterKey{}, err
		}
		seedBytes = created
	}
	if err := checkMode(seed, 0o600); err != nil {
		return EmitterKey{}, err
	}
	if len(seedBytes) != ed25519.SeedSize {
		return EmitterKey{}, fmt.Errorf("emitter key holds %d bytes, want %d: %w",
			len(seedBytes), ed25519.SeedSize, ErrInvalidArgument)
	}
	priv := ed25519.NewKeyFromSeed(seedBytes)
	return EmitterKey{
		KeyID: emitterKeyID(generation),
		Priv:  priv,
	}, nil
}

// createEmitterSeed generates one seed through a temporary file and
// an atomic link, the node key creation race included.
func createEmitterSeed(path string) ([]byte, error) {
	seed := make([]byte, ed25519.SeedSize)
	if _, err := rand.Read(seed); err != nil {
		return nil, fmt.Errorf("generate emitter key: %w", err)
	}
	staging := path + ".tmp." + strconv.Itoa(os.Getpid()) + "." + strconv.FormatUint(stageCounter.Add(1), 10)
	staged, err := os.OpenFile(staging, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("stage emitter key: %w", err)
	}
	if err := writeFull(staged, seed); err != nil {
		_ = staged.Close()
		_ = os.Remove(staging)
		return nil, err
	}
	if err := staged.Close(); err != nil {
		_ = os.Remove(staging)
		return nil, fmt.Errorf("close emitter key: %w", err)
	}
	if err := os.Link(staging, path); err != nil {
		_ = os.Remove(staging)
		if !errors.Is(err, fs.ErrExist) {
			return nil, fmt.Errorf("publish emitter key: %w", err)
		}
		raced, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read raced emitter key: %w", err)
		}
		return raced, nil
	}
	_ = os.Remove(staging)
	return seed, nil
}

// EmitterPublicKey resolves the verification key for an envelope key
// id: the active seed, else the retired one filed under its
// generation. Unknown ids are an error, never a guess.
func (r *Registry) EmitterPublicKey(keyID string) (ed25519.PublicKey, error) {
	if !protocol.SigningKeyID(keyID).Valid() {
		return nil, fmt.Errorf("emitter key id %q: %w", keyID, ErrInvalidArgument)
	}
	_, seed, versionPath, retiredDir, err := ensureEmitterDir(r.dataDir)
	if err != nil {
		return nil, err
	}
	wanted, err := emitterKeyGeneration(keyID)
	if err != nil {
		return nil, err
	}
	generation, err := readEmitterVersion(versionPath)
	if err != nil {
		return nil, err
	}
	// Verification never creates: a missing seed means no key of any
	// generation exists, and the id stays unknown.
	path := seed
	if wanted != generation {
		path = filepath.Join(retiredDir, "v"+strconv.Itoa(wanted)+".seed")
	}
	seedBytes, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("emitter key id %q: %w", keyID, ErrInvalidArgument)
		}
		return nil, fmt.Errorf("read emitter key: %w", err)
	}
	if len(seedBytes) != ed25519.SeedSize {
		return nil, fmt.Errorf("emitter key v%d holds %d bytes: %w",
			wanted, len(seedBytes), ErrInvalidArgument)
	}
	return ed25519.NewKeyFromSeed(seedBytes).Public().(ed25519.PublicKey), nil
}

// RotateEmitterKey retires the active seed and creates a fresh one.
// Notices signed before the rotation keep verifying through the
// retired file; the new generation signs from now on. The rename and
// the version bump share no transaction with signing, so callers
// created before the rotation finish with the key they loaded: the
// retired copy still verifies them.
func (r *Registry) RotateEmitterKey() (oldID, newID protocol.SigningKeyID, err error) {
	_, seed, versionPath, retiredDir, err := ensureEmitterDir(r.dataDir)
	if err != nil {
		return "", "", err
	}
	active, err := r.EmitterSigningKey()
	if err != nil {
		return "", "", err
	}
	generation, err := readEmitterVersion(versionPath)
	if err != nil {
		return "", "", err
	}
	if generation < 1 {
		generation = 1
	}
	retired := filepath.Join(retiredDir, "v"+strconv.Itoa(generation)+".seed")
	if err := os.Rename(seed, retired); err != nil {
		return "", "", fmt.Errorf("retire emitter key: %w", err)
	}
	next := generation + 1
	if err := writeEmitterVersion(versionPath, next); err != nil {
		return "", "", err
	}
	if _, err := createEmitterSeed(seed); err != nil {
		return "", "", err
	}
	return active.KeyID, emitterKeyID(next), nil
}
