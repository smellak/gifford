package artifact

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	artifactformat "github.com/smellak/gifford/internal/artifact"
	"github.com/smellak/gifford/internal/endpoint"
	hubapi "github.com/smellak/gifford/internal/hub"
	"github.com/smellak/gifford/internal/protocol"
)

const receiveTestTimeout = 10 * time.Second

type blockingDownloadClient struct {
	entered  chan struct{}
	release  <-chan struct{}
	download protocol.DownloadArtifactChunkResult
}

func (c *blockingDownloadClient) call(
	ctx context.Context,
	method string,
	_ any,
	result any,
) error {
	if method != hubapi.MethodDownloadArtifactChunk {
		return fmt.Errorf("unexpected artifact method %q", method)
	}
	close(c.entered)
	select {
	case <-c.release:
	case <-ctx.Done():
		return fmt.Errorf("wait to release download: %w", ctx.Err())
	}
	download, ok := result.(*protocol.DownloadArtifactChunkResult)
	if !ok {
		return fmt.Errorf("unexpected artifact result %T", result)
	}
	*download = c.download
	return nil
}

type receiveOutcome struct {
	result receiveResult
	err    error
}

func TestReceiveVerifiedConcurrentSameArtifact(t *testing.T) {
	for _, first := range []string{"a", "b"} {
		t.Run(first+"_finishes_first", func(t *testing.T) {
			dataDir, recipient, status, download, plaintext := receiveTestFixture(t)
			outputDirectory := t.TempDir()
			ctx, cancel := context.WithTimeout(t.Context(), receiveTestTimeout)
			defer cancel()

			releaseA := make(chan struct{})
			releaseB := make(chan struct{})
			clientA := &blockingDownloadClient{
				entered: make(chan struct{}), release: releaseA, download: download,
			}
			clientB := &blockingDownloadClient{
				entered: make(chan struct{}), release: releaseB, download: download,
			}
			doneA := startVerifiedReceive(
				ctx, clientA, recipient, dataDir, status, outputDirectory,
			)
			waitForDownload(t, clientA.entered)
			doneB := startVerifiedReceive(
				ctx, clientB, recipient, dataDir, status, outputDirectory,
			)
			waitForDownload(t, clientB.entered)

			var outcomeA, outcomeB receiveOutcome
			if first == "a" {
				close(releaseA)
				outcomeA = waitForReceive(t, doneA)
				close(releaseB)
				outcomeB = waitForReceive(t, doneB)
			} else {
				close(releaseB)
				outcomeB = waitForReceive(t, doneB)
				close(releaseA)
				outcomeA = waitForReceive(t, doneA)
			}

			require.NoError(t, outcomeA.err)
			require.NoError(t, outcomeB.err)
			require.NotEqual(t, outcomeA.result.Path, outcomeB.result.Path)
			require.ElementsMatch(t, []string{"report.txt", "report.txt (1)"}, []string{
				filepath.Base(outcomeA.result.Path), filepath.Base(outcomeB.result.Path),
			})
			for _, outcome := range []receiveOutcome{outcomeA, outcomeB} {
				require.Equal(t, artifactformat.Digest(plaintext), outcome.result.SHA256)
				require.Equal(t, plaintext, mustReadArtifactFile(t, outcome.result.Path))
				info, err := os.Stat(outcome.result.Path)
				require.NoError(t, err)
				require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
			}
			temporaries, err := filepath.Glob(filepath.Join(outputDirectory, ".gifford-*.part"))
			require.NoError(t, err)
			require.Empty(t, temporaries)
		})
	}
}

func TestReceiveVerifiedIgnoresStaleTemporary(t *testing.T) {
	dataDir, recipient, status, download, plaintext := receiveTestFixture(t)
	outputDirectory := t.TempDir()
	stalePath := filepath.Join(
		outputDirectory, ".gifford-"+string(status.ArtifactID)+"-stale.part",
	)
	staleContent := []byte("interrupted earlier reception")
	require.NoError(t, os.WriteFile(stalePath, staleContent, 0o600))
	release := make(chan struct{})
	close(release)
	client := &blockingDownloadClient{
		entered: make(chan struct{}), release: release, download: download,
	}

	result, err := receiveVerified(
		t.Context(), client, recipient, dataDir, status, outputDirectory,
	)
	require.NoError(t, err)
	require.Equal(t, plaintext, mustReadArtifactFile(t, result.Path))
	require.Equal(t, staleContent, mustReadArtifactFile(t, stalePath))
}

func startVerifiedReceive(
	ctx context.Context,
	client artifactDownloadClient,
	recipient protocol.AgentID,
	dataDir string,
	status protocol.ArtifactStatusResult,
	outputDirectory string,
) <-chan receiveOutcome {
	done := make(chan receiveOutcome, 1)
	go func() {
		result, err := receiveVerified(
			ctx, client, recipient, dataDir, status, outputDirectory,
		)
		done <- receiveOutcome{result: result, err: err}
	}()
	return done
}

func waitForDownload(t *testing.T, entered <-chan struct{}) {
	t.Helper()
	select {
	case <-entered:
	case <-time.After(receiveTestTimeout):
		t.Fatal("timed out waiting for artifact download")
	}
}

func waitForReceive(t *testing.T, done <-chan receiveOutcome) receiveOutcome {
	t.Helper()
	select {
	case outcome := <-done:
		return outcome
	case <-time.After(receiveTestTimeout):
		t.Fatal("timed out waiting for artifact reception")
		return receiveOutcome{}
	}
}

func receiveTestFixture(t *testing.T) (
	string,
	protocol.AgentID,
	protocol.ArtifactStatusResult,
	protocol.DownloadArtifactChunkResult,
	[]byte,
) {
	t.Helper()
	dataDir := t.TempDir()
	require.NoError(t, os.Chmod(dataDir, 0o700))
	artifactID := protocol.OpaqueID("art-0123456789abcdef0123456789abcdef")
	sender := protocol.AgentID("sender-agent")
	recipient := protocol.AgentID("recipient-agent")
	recipientPublic, err := endpoint.EnsureAgentEncryptionKey(dataDir, recipient)
	require.NoError(t, err)
	contentKey, err := artifactformat.NewContentKey()
	require.NoError(t, err)
	plaintext := []byte("concurrent encrypted artifact payload")
	ciphertext, err := artifactformat.EncryptChunk(contentKey, artifactID, 0, 1, plaintext)
	require.NoError(t, err)
	manifest, err := artifactformat.EncryptManifest(contentKey, artifactID, artifactformat.PrivateManifest{
		Filename: "report.txt", MediaType: "text/plain", Purpose: "review",
		PlaintextSize: int64(len(plaintext)), PlaintextSHA256: artifactformat.Digest(plaintext),
	})
	require.NoError(t, err)
	keyEnvelope, err := artifactformat.WrapContentKey(
		recipientPublic, artifactID, recipient, contentKey,
	)
	require.NoError(t, err)
	senderPublic, senderPrivate, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	expiresAt := time.Date(2026, time.November, 1, 18, 0, 0, 0, time.UTC)
	metadata := artifactformat.EnvelopeMetadata{
		ArtifactID: artifactID, ClientMsgID: protocol.OpaqueID("file-concurrent-receive"),
		Sender: sender, Recipient: recipient, CiphertextSize: int64(len(ciphertext)),
		ChunkCount: 1, CiphertextSHA256: artifactformat.Digest(ciphertext),
		EncryptedManifest: manifest, KeyEnvelope: keyEnvelope,
		EncryptionProfile: protocol.ArtifactEncryptionProfile, ExpiresAt: expiresAt,
	}
	signature, err := artifactformat.SignEnvelope(senderPrivate, metadata)
	require.NoError(t, err)
	status := protocol.ArtifactStatusResult{
		ArtifactID: artifactID, ClientMsgID: metadata.ClientMsgID, Sender: sender,
		State:             protocol.ArtifactStateLocalAvailable,
		EncryptionProfile: protocol.ArtifactEncryptionProfile,
		CiphertextSize:    int64(len(ciphertext)), ChunkCount: 1,
		CiphertextSHA256: metadata.CiphertextSHA256, EncryptedManifest: manifest,
		SenderKeyID:     protocol.SigningKeyID("ed25519:sender-agent:v1"),
		SenderPublicKey: senderPublic, Signature: signature, ExpiresAt: expiresAt, NextChunk: 1,
		Recipients: []protocol.ArtifactRecipientStatus{{
			Agent: recipient, KeyID: protocol.EncryptionKeyID("x25519:recipient-agent:v1"),
			KeyEnvelope: keyEnvelope, State: protocol.ArtifactStateLocalAvailable,
		}},
	}
	require.NoError(t, status.Validate())
	download := protocol.DownloadArtifactChunkResult{
		ArtifactID: artifactID, ChunkIndex: 0, Ciphertext: ciphertext,
		SHA256: artifactformat.Digest(ciphertext),
	}
	return dataDir, recipient, status, download, plaintext
}
