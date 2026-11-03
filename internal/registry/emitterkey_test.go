package registry

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/protocol"
)

// The system sender id is withheld from agents but stays a valid id:
// the reservation is a list, never a shape.
func TestEmitterIDReservedButValid(t *testing.T) {
	t.Parallel()

	require.True(t, EmitterAgentID.Valid())
	require.True(t, ReservedAgentID(EmitterAgentID))
	require.False(t, ReservedAgentID(protocol.AgentID("agent-9")))
	require.True(t, protocol.SigningKeyID("ed25519:system-notifier:v1").Valid())
}

// The first load creates the seed with node key modes: 0700
// directories, 0600 seed. Anything else refuses.
func TestEmitterKeyFirstUseModes(t *testing.T) {
	t.Parallel()

	dir := testDataDir(t)
	reg, err := New(protocol.NodeID("node-1"), dir, clock.System{})
	require.NoError(t, err)
	key, err := reg.EmitterSigningKey()
	require.NoError(t, err)
	require.Equal(t, protocol.SigningKeyID("ed25519:system-notifier:v1"), key.KeyID)
	require.Len(t, key.Priv, 64)

	seedPath := filepath.Join(dir, "keys", "system-notifier", "emitter.seed")
	info, err := os.Stat(seedPath)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())

	public, err := reg.EmitterPublicKey(string(key.KeyID))
	require.NoError(t, err)
	require.Equal(t, key.Priv.Public(), public)

	// A loosened seed refuses instead of repairing.
	require.NoError(t, os.Chmod(seedPath, 0o640))
	loose, err := New(protocol.NodeID("node-1"), dir, clock.System{})
	require.NoError(t, err)
	_, err = loose.EmitterSigningKey()
	require.Error(t, err)
}

// Verification resolves the active key and the retired ones, and
// nothing else: unknown generations and foreign senders fail closed,
// and resolving never creates a key.
func TestEmitterKeyResolution(t *testing.T) {
	t.Parallel()

	dir := testDataDir(t)
	reg, err := New(protocol.NodeID("node-1"), dir, clock.System{})
	require.NoError(t, err)
	first, err := reg.EmitterSigningKey()
	require.NoError(t, err)

	_, err = reg.EmitterPublicKey("ed25519:system-notifier:v9")
	require.Error(t, err, "unknown generation resolves nothing")
	_, err = reg.EmitterPublicKey("ed25519:agent-9:v1")
	require.Error(t, err, "a foreign sender never verifies as the emitter")
	_, err = reg.EmitterPublicKey("not-a-key-id")
	require.Error(t, err, "garbage fails closed")

	// Resolving the unknown must not have created anything new.
	second, err := reg.EmitterSigningKey()
	require.NoError(t, err)
	require.Equal(t, first.KeyID, second.KeyID, "verification creates nothing")
}

// Rotation retires the active seed and signs with a fresh one, while
// notices signed before keep verifying through the retired file.
func TestEmitterKeyRotationKeepsOldNoticesVerifying(t *testing.T) {
	t.Parallel()

	dir := testDataDir(t)
	reg, err := New(protocol.NodeID("node-1"), dir, clock.System{})
	require.NoError(t, err)
	first, err := reg.EmitterSigningKey()
	require.NoError(t, err)

	oldID, newID, err := reg.RotateEmitterKey()
	require.NoError(t, err)
	require.Equal(t, first.KeyID, oldID)
	require.Equal(t, protocol.SigningKeyID("ed25519:system-notifier:v2"), newID)

	current, err := reg.EmitterSigningKey()
	require.NoError(t, err)
	require.Equal(t, newID, current.KeyID)
	require.NotEqual(t, first.Priv.Public(), current.Priv.Public(), "rotation replaces the seed")

	oldPublic, err := reg.EmitterPublicKey(string(oldID))
	require.NoError(t, err, "the retired generation still resolves")
	require.Equal(t, first.Priv.Public(), oldPublic)
	newPublic, err := reg.EmitterPublicKey(string(newID))
	require.NoError(t, err)
	require.Equal(t, current.Priv.Public(), newPublic)
}
