package hub

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	artifactformat "github.com/smellak/gifford/internal/artifact"
	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

func TestLocalArtifactResumeRoundTrip(t *testing.T) {
	t.Parallel()

	fixture := newHubFixture(t)
	fixture.service.dataDir = t.TempDir()
	recipientPrivate, err := ecdh.X25519().GenerateKey(rand.Reader)
	require.NoError(t, err)
	fixture.keys.encryption[testHubRecipient] = registry.KeyInfo{
		KeyID: testHubRecipientEncryptionKeyID, Agent: testHubRecipient,
		PublicKey: recipientPrivate.PublicKey().Bytes(),
		Purpose:   registry.KeyPurposeEncryption, Algorithm: registry.KeyAlgorithmX25519,
	}

	plaintext := bytes.Repeat([]byte("artifact-marker-"), 230_000)
	plainDigest := sha256.Sum256(plaintext)
	clientID := protocol.OpaqueID("file-round-trip")
	artifactID, err := artifactformat.DeriveArtifactID(testHubSender, clientID)
	require.NoError(t, err)
	key, err := artifactformat.NewContentKey()
	require.NoError(t, err)
	chunkCount, err := artifactformat.ChunkCount(int64(len(plaintext)))
	require.NoError(t, err)
	ciphertextChunks := make([][]byte, 0, chunkCount)
	ciphertextHash := sha256.New()
	var ciphertextSize int64
	for index := range chunkCount {
		start := index * protocol.ArtifactChunkSize
		end := start + protocol.ArtifactChunkSize
		if end > int64(len(plaintext)) {
			end = int64(len(plaintext))
		}
		chunk, err := artifactformat.EncryptChunk(key, artifactID, index, chunkCount, plaintext[start:end])
		require.NoError(t, err)
		ciphertextChunks = append(ciphertextChunks, chunk)
		ciphertextSize += int64(len(chunk))
		_, _ = ciphertextHash.Write(chunk)
	}
	ciphertextDigest := protocol.SHA256Hex(hex.EncodeToString(ciphertextHash.Sum(nil)))
	manifest, err := artifactformat.EncryptManifest(key, artifactID, artifactformat.PrivateManifest{
		Filename: "report.txt", MediaType: "text/plain", Purpose: "review",
		PlaintextSize:   int64(len(plaintext)),
		PlaintextSHA256: protocol.SHA256Hex(hex.EncodeToString(plainDigest[:])),
	})
	require.NoError(t, err)
	envelope, err := artifactformat.WrapContentKey(
		recipientPrivate.PublicKey().Bytes(), artifactID, testHubRecipient, key,
	)
	require.NoError(t, err)
	params := protocol.ReserveArtifactParams{
		ClientMsgID: clientID, Recipient: testHubRecipient,
		EncryptionProfile: protocol.ArtifactEncryptionProfile,
		CiphertextSize:    ciphertextSize, ChunkCount: chunkCount,
		CiphertextSHA256: ciphertextDigest, EncryptedManifest: manifest,
		RecipientKeyID: testHubRecipientEncryptionKeyID, KeyEnvelope: envelope,
		SenderKeyID: testHubSenderKeyID, ReservedAt: fixture.clock.now,
		ExpiresAt: fixture.clock.now.Add(24 * time.Hour),
	}
	params.Signature, err = artifactformat.SignEnvelope(fixture.private, artifactformat.EnvelopeMetadata{
		ArtifactID: artifactID, ClientMsgID: clientID, Sender: testHubSender,
		Recipient: testHubRecipient, CiphertextSize: ciphertextSize, ChunkCount: chunkCount,
		CiphertextSHA256: params.CiphertextSHA256, EncryptedManifest: manifest,
		KeyEnvelope: envelope, EncryptionProfile: params.EncryptionProfile,
		ExpiresAt: params.ExpiresAt,
	})
	require.NoError(t, err)

	reserved := callArtifact[protocol.ReserveArtifactResult](
		t, fixture.service.ReserveArtifact, testHubSender, params,
	)
	require.Equal(t, artifactID, reserved.ArtifactID)
	require.Equal(t, int64(0), reserved.NextChunk)
	require.Equal(t, protocol.DecisionOutcomeAllow, reserved.Admission.Outcome)

	uploaded := callArtifact[protocol.UploadArtifactChunkResult](
		t, fixture.service.UploadArtifactChunk, testHubSender,
		protocol.UploadArtifactChunkParams{
			ArtifactID: artifactID, ChunkIndex: 0, Ciphertext: ciphertextChunks[0],
			SHA256: artifactformat.Digest(ciphertextChunks[0]),
		},
	)
	require.True(t, uploaded.Stored)
	require.Equal(t, int64(1), uploaded.NextChunk)

	resumed := callArtifact[protocol.ReserveArtifactResult](
		t, fixture.service.ReserveArtifact, testHubSender, params,
	)
	require.Equal(t, int64(1), resumed.NextChunk)
	for index := int64(1); index < chunkCount; index++ {
		uploaded = callArtifact[protocol.UploadArtifactChunkResult](
			t, fixture.service.UploadArtifactChunk, testHubSender,
			protocol.UploadArtifactChunkParams{
				ArtifactID: artifactID, ChunkIndex: index, Ciphertext: ciphertextChunks[index],
				SHA256: artifactformat.Digest(ciphertextChunks[index]),
			},
		)
		require.True(t, uploaded.Stored)
	}

	completed := callArtifact[protocol.ArtifactMutationResult](
		t, fixture.service.CompleteArtifactUpload, testHubSender,
		protocol.ArtifactIDParams{ArtifactID: artifactID},
	)
	require.Equal(t, protocol.ArtifactStateLocalAvailable, completed.TransferState)

	status := callArtifact[protocol.ArtifactStatusResult](
		t, fixture.service.GetArtifactStatus, testHubRecipient,
		protocol.ArtifactIDParams{ArtifactID: artifactID},
	)
	require.Equal(t, protocol.ArtifactStateLocalAvailable, status.State)
	require.True(t, bytes.Equal(fixture.keys.keys[testHubSender].PublicKey, status.SenderPublicKey))

	openedKey, err := artifactformat.UnwrapContentKey(
		recipientPrivate, artifactID, testHubRecipient, status.Recipients[0].KeyEnvelope,
	)
	require.NoError(t, err)
	var opened []byte
	for index := range chunkCount {
		downloaded := callArtifact[protocol.DownloadArtifactChunkResult](
			t, fixture.service.DownloadArtifactChunk, testHubRecipient,
			protocol.DownloadArtifactChunkParams{ArtifactID: artifactID, ChunkIndex: index},
		)
		chunk, err := artifactformat.DecryptChunk(
			openedKey, artifactID, index, chunkCount, downloaded.Ciphertext,
		)
		require.NoError(t, err)
		opened = append(opened, chunk...)
	}
	require.Equal(t, plaintext, opened)

	acked := callArtifact[protocol.ArtifactMutationResult](
		t, fixture.service.AckArtifact, testHubRecipient,
		protocol.ArtifactIDParams{ArtifactID: artifactID},
	)
	require.Equal(t, protocol.ArtifactStateAcknowledged, acked.TransferState)
	var chunks int64
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM artifact_chunks WHERE artifact_id = ?", artifactID).Scan(&chunks))
	require.Zero(t, chunks)
	dir, err := fixture.service.artifactDirectory(artifactID)
	require.NoError(t, err)
	_, err = os.Stat(dir)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func callArtifact[T any](
	t *testing.T,
	call func(context.Context, protocol.AgentID, json.RawMessage) (any, error),
	agent protocol.AgentID,
	params any,
) T {
	t.Helper()
	raw, err := json.Marshal(params)
	require.NoError(t, err)
	result, err := call(t.Context(), agent, raw)
	require.NoError(t, err)
	typed, ok := result.(T)
	require.True(t, ok, "unexpected result type %T", result)
	return typed
}

type hubArtifactScenario struct {
	fixture          *hubFixture
	recipientPrivate *ecdh.PrivateKey
	artifactID       protocol.OpaqueID
	key              []byte
	chunks           [][]byte
	params           protocol.ReserveArtifactParams
}

func newHubArtifactScenario(
	t *testing.T,
	plaintext []byte,
	clientID protocol.OpaqueID,
) *hubArtifactScenario {
	t.Helper()
	fixture := newHubFixture(t)
	fixture.service.dataDir = t.TempDir()
	recipientPrivate, err := ecdh.X25519().GenerateKey(rand.Reader)
	require.NoError(t, err)
	fixture.keys.encryption[testHubRecipient] = registry.KeyInfo{
		KeyID: testHubRecipientEncryptionKeyID, Agent: testHubRecipient,
		PublicKey: recipientPrivate.PublicKey().Bytes(),
		Purpose:   registry.KeyPurposeEncryption, Algorithm: registry.KeyAlgorithmX25519,
	}
	artifactID, err := artifactformat.DeriveArtifactID(testHubSender, clientID)
	require.NoError(t, err)
	key, err := artifactformat.NewContentKey()
	require.NoError(t, err)
	chunkCount, err := artifactformat.ChunkCount(int64(len(plaintext)))
	require.NoError(t, err)
	chunks := make([][]byte, 0, chunkCount)
	ciphertextHash := sha256.New()
	var ciphertextSize int64
	for index := range chunkCount {
		start := index * protocol.ArtifactChunkSize
		end := start + protocol.ArtifactChunkSize
		if end > int64(len(plaintext)) {
			end = int64(len(plaintext))
		}
		chunk, err := artifactformat.EncryptChunk(
			key, artifactID, index, chunkCount, plaintext[start:end],
		)
		require.NoError(t, err)
		chunks = append(chunks, chunk)
		ciphertextSize += int64(len(chunk))
		_, _ = ciphertextHash.Write(chunk)
	}
	plainDigest := sha256.Sum256(plaintext)
	manifest, err := artifactformat.EncryptManifest(key, artifactID, artifactformat.PrivateManifest{
		Filename: "report.txt", MediaType: "text/plain", Purpose: "test",
		PlaintextSize:   int64(len(plaintext)),
		PlaintextSHA256: protocol.SHA256Hex(hex.EncodeToString(plainDigest[:])),
	})
	require.NoError(t, err)
	envelope, err := artifactformat.WrapContentKey(
		recipientPrivate.PublicKey().Bytes(), artifactID, testHubRecipient, key,
	)
	require.NoError(t, err)
	params := protocol.ReserveArtifactParams{
		ClientMsgID: clientID, Recipient: testHubRecipient,
		EncryptionProfile: protocol.ArtifactEncryptionProfile,
		CiphertextSize:    ciphertextSize, ChunkCount: chunkCount,
		CiphertextSHA256:  protocol.SHA256Hex(hex.EncodeToString(ciphertextHash.Sum(nil))),
		EncryptedManifest: manifest, RecipientKeyID: testHubRecipientEncryptionKeyID,
		KeyEnvelope: envelope, SenderKeyID: testHubSenderKeyID,
		ReservedAt: fixture.clock.now, ExpiresAt: fixture.clock.now.Add(24 * time.Hour),
	}
	params.Signature, err = artifactformat.SignEnvelope(fixture.private, artifactformat.EnvelopeMetadata{
		ArtifactID: artifactID, ClientMsgID: clientID, Sender: testHubSender,
		Recipient: testHubRecipient, CiphertextSize: ciphertextSize, ChunkCount: chunkCount,
		CiphertextSHA256: params.CiphertextSHA256, EncryptedManifest: manifest,
		KeyEnvelope: envelope, EncryptionProfile: params.EncryptionProfile,
		ExpiresAt: params.ExpiresAt,
	})
	require.NoError(t, err)
	return &hubArtifactScenario{
		fixture: fixture, recipientPrivate: recipientPrivate, artifactID: artifactID,
		key: key, chunks: chunks, params: params,
	}
}

func (s *hubArtifactScenario) reserve(t *testing.T) protocol.ReserveArtifactResult {
	t.Helper()
	return callArtifact[protocol.ReserveArtifactResult](
		t, s.fixture.service.ReserveArtifact, testHubSender, s.params,
	)
}

func (s *hubArtifactScenario) upload(t *testing.T, index int64) {
	t.Helper()
	callArtifact[protocol.UploadArtifactChunkResult](
		t, s.fixture.service.UploadArtifactChunk, testHubSender,
		protocol.UploadArtifactChunkParams{
			ArtifactID: s.artifactID, ChunkIndex: index, Ciphertext: s.chunks[index],
			SHA256: artifactformat.Digest(s.chunks[index]),
		},
	)
}

func (s *hubArtifactScenario) uploadAll(t *testing.T) {
	t.Helper()
	for index := range s.chunks {
		s.upload(t, int64(index))
	}
}

func (s *hubArtifactScenario) complete(t *testing.T) protocol.ArtifactMutationResult {
	t.Helper()
	return callArtifact[protocol.ArtifactMutationResult](
		t, s.fixture.service.CompleteArtifactUpload, testHubSender,
		protocol.ArtifactIDParams{ArtifactID: s.artifactID},
	)
}

func artifactCallError(
	t *testing.T,
	call func(context.Context, protocol.AgentID, json.RawMessage) (any, error),
	agent protocol.AgentID,
	params any,
) *callError {
	t.Helper()
	raw, err := json.Marshal(params)
	require.NoError(t, err)
	_, err = call(t.Context(), agent, raw)
	return requireCallCode(t, err, codeDomainError)
}

func TestArtifactTypedFailuresAndAuthorization(t *testing.T) {
	t.Parallel()
	scenario := newHubArtifactScenario(t, []byte{0x7f}, "file-auth")
	scenario.reserve(t)
	third := protocol.AgentID("agent-third")

	got := artifactCallError(t, scenario.fixture.service.UploadArtifactChunk,
		testHubRecipient, protocol.UploadArtifactChunkParams{
			ArtifactID: scenario.artifactID, ChunkIndex: 0, Ciphertext: scenario.chunks[0],
			SHA256: artifactformat.Digest(scenario.chunks[0]),
		})
	require.Equal(t, protocol.ErrorCodeNotSender, got.domain)
	got = artifactCallError(t, scenario.fixture.service.CompleteArtifactUpload,
		testHubRecipient, protocol.ArtifactIDParams{ArtifactID: scenario.artifactID})
	require.Equal(t, protocol.ErrorCodeNotSender, got.domain)
	got = artifactCallError(t, scenario.fixture.service.AckArtifact,
		testHubRecipient, protocol.ArtifactIDParams{ArtifactID: scenario.artifactID})
	require.Equal(t, protocol.ErrorCodeUploadIncomplete, got.domain)
	got = artifactCallError(t, scenario.fixture.service.CompleteArtifactUpload,
		testHubSender, protocol.ArtifactIDParams{ArtifactID: scenario.artifactID})
	require.Equal(t, protocol.ErrorCodeUploadIncomplete, got.domain)

	got = artifactCallError(t, scenario.fixture.service.UploadArtifactChunk,
		testHubSender, protocol.UploadArtifactChunkParams{
			ArtifactID: scenario.artifactID, ChunkIndex: 1, Ciphertext: scenario.chunks[0],
			SHA256: artifactformat.Digest(scenario.chunks[0]),
		})
	require.Equal(t, protocol.ErrorCodeChunkOutOfRange, got.domain)
	got = artifactCallError(t, scenario.fixture.service.UploadArtifactChunk,
		testHubSender, protocol.UploadArtifactChunkParams{
			ArtifactID: scenario.artifactID, ChunkIndex: 0, Ciphertext: make([]byte, 16),
			SHA256: artifactformat.Digest(make([]byte, 16)),
		})
	require.Equal(t, protocol.ErrorCodeChunkSizeInvalid, got.domain)
	got = artifactCallError(t, scenario.fixture.service.UploadArtifactChunk,
		testHubSender, protocol.UploadArtifactChunkParams{
			ArtifactID: scenario.artifactID, ChunkIndex: 0, Ciphertext: scenario.chunks[0],
			SHA256: artifactformat.Digest([]byte("wrong")),
		})
	require.Equal(t, protocol.ErrorCodeChunkDigestMismatch, got.domain)

	scenario.uploadAll(t)
	scenario.complete(t)
	for _, test := range []struct {
		name  string
		call  func(context.Context, protocol.AgentID, json.RawMessage) (any, error)
		agent protocol.AgentID
		param any
	}{
		{
			name: "third status", call: scenario.fixture.service.GetArtifactStatus, agent: third,
			param: protocol.ArtifactIDParams{ArtifactID: scenario.artifactID},
		},
		{
			name: "third download", call: scenario.fixture.service.DownloadArtifactChunk, agent: third,
			param: protocol.DownloadArtifactChunkParams{ArtifactID: scenario.artifactID, ChunkIndex: 0},
		},
		{
			name: "third ack", call: scenario.fixture.service.AckArtifact, agent: third,
			param: protocol.ArtifactIDParams{ArtifactID: scenario.artifactID},
		},
		{
			name: "sender ack", call: scenario.fixture.service.AckArtifact, agent: testHubSender,
			param: protocol.ArtifactIDParams{ArtifactID: scenario.artifactID},
		},
	} {
		got := artifactCallError(t, test.call, test.agent, test.param)
		require.Equal(t, protocol.ErrorCodeNotRecipient, got.domain, test.name)
	}
	raw, err := scenario.fixture.service.ListPendingArtifacts(
		t.Context(), third, json.RawMessage(`{}`),
	)
	require.NoError(t, err)
	require.Empty(t, raw.(protocol.ListPendingArtifactsResult).Artifacts)

	callArtifact[protocol.ArtifactMutationResult](t, scenario.fixture.service.AckArtifact,
		testHubRecipient, protocol.ArtifactIDParams{ArtifactID: scenario.artifactID})
	got = artifactCallError(t, scenario.fixture.service.UploadArtifactChunk,
		testHubSender, protocol.UploadArtifactChunkParams{
			ArtifactID: scenario.artifactID, ChunkIndex: 0, Ciphertext: scenario.chunks[0],
			SHA256: artifactformat.Digest(scenario.chunks[0]),
		})
	require.Equal(t, protocol.ErrorCodeUploadIncomplete, got.domain)
}

func TestArtifactReservationConflictAndCiphertextMismatch(t *testing.T) {
	t.Parallel()
	scenario := newHubArtifactScenario(t, []byte("signed content"), "file-conflict")
	scenario.reserve(t)
	conflict := scenario.params
	conflict.Signature = append([]byte(nil), conflict.Signature...)
	conflict.Signature[0] ^= 1
	got := artifactCallError(t, scenario.fixture.service.ReserveArtifact, testHubSender, conflict)
	require.Equal(t, protocol.ErrorCodeReservationConflict, got.domain)

	mutated := append([]byte(nil), scenario.chunks[0]...)
	mutated[0] ^= 1
	callArtifact[protocol.UploadArtifactChunkResult](t,
		scenario.fixture.service.UploadArtifactChunk, testHubSender,
		protocol.UploadArtifactChunkParams{
			ArtifactID: scenario.artifactID, ChunkIndex: 0, Ciphertext: mutated,
			SHA256: artifactformat.Digest(mutated),
		})
	got = artifactCallError(t, scenario.fixture.service.CompleteArtifactUpload,
		testHubSender, protocol.ArtifactIDParams{ArtifactID: scenario.artifactID})
	require.Equal(t, protocol.ErrorCodeCiphertextDigestMismatch, got.domain)
}

func TestArtifactChannelGateStoresOnlyAllow(t *testing.T) {
	for _, test := range []struct {
		name     string
		clientID protocol.OpaqueID
		block    func(*testing.T, *hubArtifactScenario)
		reason   protocol.DecisionReason
	}{
		{
			name: "sender tx off", clientID: "file-blocked-tx",
			reason: protocol.DecisionReasonAgentTXDisabled,
			block: func(t *testing.T, scenario *hubArtifactScenario) {
				off := false
				_, err := scenario.fixture.controller.SetAgent(t.Context(), protocol.SetAgentChannelParams{
					AgentID: testHubSender, TX: &off, ExpectedVersion: 0,
				}, channel.Actor{Type: protocol.ActorTypeHuman, ID: testOperatorID})
				require.NoError(t, err)
			},
		},
		{
			name: "artifact link deny", clientID: "file-blocked-link",
			reason: protocol.DecisionReasonLinkDenied,
			block: func(t *testing.T, scenario *hubArtifactScenario) {
				_, err := scenario.fixture.controller.SetLink(t.Context(), protocol.SetLinkParams{
					FromAgent: testHubSender, ToAgent: testHubRecipient,
					Mode: protocol.LinkModeDeny, Plane: protocol.PlaneArtifact,
					ExpectedVersions: protocol.ControlVersions{
						"link:agent-sender→agent-recipient:artifact": 0,
					},
				}, channel.Actor{Type: protocol.ActorTypeHuman, ID: testOperatorID})
				require.NoError(t, err)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			scenario := newHubArtifactScenario(t, []byte("blocked"), test.clientID)
			test.block(t, scenario)
			got := artifactCallError(t, scenario.fixture.service.ReserveArtifact,
				testHubSender, scenario.params)
			require.Equal(t, protocol.ErrorCodeChannelNotAllowed, got.domain)
			require.NotNil(t, got.admission)
			require.Equal(t, protocol.DecisionOutcomeDeny, got.admission.Outcome)
			require.Equal(t, test.reason, got.admission.ControlReason)
			var transfers, decisions int64
			require.NoError(t, scenario.fixture.store.DB().QueryRowContext(t.Context(),
				"SELECT COUNT(*) FROM artifact_transfers WHERE artifact_id = ?", scenario.artifactID).
				Scan(&transfers))
			require.Zero(t, transfers)
			require.NoError(t, scenario.fixture.store.DB().QueryRowContext(t.Context(),
				`SELECT COUNT(*) FROM channel_control_decisions
				 WHERE object_type = ? AND object_id = ?`,
				protocol.DecisionObjectTypeArtifact, scenario.artifactID).Scan(&decisions))
			require.Positive(t, decisions)
		})
	}
}

func TestArtifactReconcileRestoresResumePoint(t *testing.T) {
	t.Parallel()
	plaintext := bytes.Repeat([]byte("reconcile-marker"), 230_000)
	scenario := newHubArtifactScenario(t, plaintext, "file-reconcile")
	scenario.reserve(t)
	scenario.upload(t, 0)
	scenario.upload(t, 1)
	missing, err := scenario.fixture.service.artifactChunkPath(scenario.artifactID, 1)
	require.NoError(t, err)
	require.NoError(t, os.Remove(missing))
	directory, err := scenario.fixture.service.artifactDirectory(scenario.artifactID)
	require.NoError(t, err)
	orphan := filepath.Join(directory, ".interrupted.tmp")
	require.NoError(t, os.WriteFile(orphan, []byte("orphan"), 0o600))

	require.NoError(t, scenario.fixture.service.ReconcileArtifacts(t.Context()))
	var chunks int64
	require.NoError(t, scenario.fixture.store.DB().QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM artifact_chunks WHERE artifact_id = ?", scenario.artifactID).
		Scan(&chunks))
	require.Equal(t, int64(1), chunks)
	require.NoFileExists(t, orphan)
	resumed := scenario.reserve(t)
	require.Equal(t, int64(1), resumed.NextChunk)
	for index := int64(1); index < int64(len(scenario.chunks)); index++ {
		scenario.upload(t, index)
	}
	require.Equal(t, protocol.ArtifactStateLocalAvailable, scenario.complete(t).TransferState)
}
