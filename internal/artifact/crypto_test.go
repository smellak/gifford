package artifact

import (
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

func TestArtifactCryptographicProfile(t *testing.T) {
	t.Parallel()
	artifactID := protocol.OpaqueID("art-0123456789abcdef0123456789abcdef")
	clientID := protocol.OpaqueID("file-profile-test")
	sender := protocol.AgentID("sender-agent")
	recipient := protocol.AgentID("recipient-agent")
	key, err := NewContentKey()
	require.NoError(t, err)

	for _, plaintext := range [][]byte{nil, {0x7f}, make([]byte, protocol.ArtifactChunkSize)} {
		count, err := ChunkCount(int64(len(plaintext)))
		require.NoError(t, err)
		ciphertext, err := EncryptChunk(key, artifactID, 0, count, plaintext)
		require.NoError(t, err)
		repeated, err := EncryptChunk(key, artifactID, 0, count, plaintext)
		require.NoError(t, err)
		require.Equal(t, ciphertext, repeated, "chunk encryption must be deterministic for resume")
		opened, err := DecryptChunk(key, artifactID, 0, count, ciphertext)
		require.NoError(t, err)
		require.Equal(t, plaintext, opened)
		tampered := append([]byte(nil), ciphertext...)
		tampered[len(tampered)-1] ^= 1
		_, err = DecryptChunk(key, artifactID, 0, count, tampered)
		require.ErrorIs(t, err, ErrVerification)
	}

	manifest := PrivateManifest{
		Filename: "report.txt", MediaType: "text/plain", Purpose: "review",
		PlaintextSize: 1, PlaintextSHA256: Digest([]byte{0x7f}),
	}
	encryptedManifest, err := EncryptManifest(key, artifactID, manifest)
	require.NoError(t, err)
	openedManifest, err := DecryptManifest(key, artifactID, encryptedManifest)
	require.NoError(t, err)
	require.Equal(t, manifest, openedManifest)

	recipientPrivate, err := ecdh.X25519().GenerateKey(rand.Reader)
	require.NoError(t, err)
	envelope, err := WrapContentKey(recipientPrivate.PublicKey().Bytes(), artifactID, recipient, key)
	require.NoError(t, err)
	openedKey, err := UnwrapContentKey(recipientPrivate, artifactID, recipient, envelope)
	require.NoError(t, err)
	require.Equal(t, key, openedKey)

	public, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	metadata := EnvelopeMetadata{
		ArtifactID: artifactID, ClientMsgID: clientID, Sender: sender, Recipient: recipient,
		CiphertextSize: 17, ChunkCount: 1, CiphertextSHA256: Digest(make([]byte, 17)),
		EncryptedManifest: encryptedManifest, KeyEnvelope: envelope,
		EncryptionProfile: protocol.ArtifactEncryptionProfile,
		ExpiresAt:         time.Date(2026, time.November, 1, 12, 0, 0, 0, time.UTC),
	}
	signature, err := SignEnvelope(private, metadata)
	require.NoError(t, err)
	require.NoError(t, VerifyEnvelope(public, metadata, signature))
	metadata.CiphertextSize++
	require.ErrorIs(t, VerifyEnvelope(public, metadata, signature), ErrVerification)
}

func TestArtifactSizesAndSafeNames(t *testing.T) {
	t.Parallel()
	tests := []struct {
		size       int64
		wantChunks int64
		wantLast   int64
	}{
		{size: 0, wantChunks: 1, wantLast: 16},
		{size: 1, wantChunks: 1, wantLast: 17},
		{
			size: 3*protocol.ArtifactChunkSize + protocol.ArtifactChunkSize/2, wantChunks: 4,
			wantLast: protocol.ArtifactChunkSize/2 + 16,
		},
	}
	for _, test := range tests {
		chunks, err := ChunkCount(test.size)
		require.NoError(t, err)
		require.Equal(t, test.wantChunks, chunks)
		size, err := CiphertextSize(test.size, chunks)
		require.NoError(t, err)
		last, err := ExpectedChunkSize(size, chunks, chunks-1)
		require.NoError(t, err)
		require.Equal(t, test.wantLast, last)
	}

	id := protocol.OpaqueID("art-0123456789abcdef0123456789abcdef")
	require.Equal(t, "x", SanitizeFilename("../../x", id))
	require.Equal(t, "b", SanitizeFilename("a/b", id))
	require.Equal(t, "ab", SanitizeFilename("a\x00\x1fb", id))
	require.Equal(t, "artifact-"+string(id), SanitizeFilename("..", id))
	require.Equal(t, "artifact-"+string(id), SanitizeFilename("a..b", id))
}
