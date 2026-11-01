package hub

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	artifactformat "github.com/smellak/gifford/internal/artifact"
	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/federation"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

type federatedArtifactScenario struct {
	base              *outboundFederationFixture
	remoteService     *Service
	recipientPrivate  *ecdh.PrivateKey
	artifactID        protocol.OpaqueID
	plaintext         []byte
	chunks            [][]byte
	params            protocol.ReserveArtifactParams
	originAckReceiver federation.ArtifactAckReceiver
	ackReplicator     *federation.Replicator
}

func newFederatedArtifactScenario(t *testing.T, clientID protocol.OpaqueID, plaintext []byte) *federatedArtifactScenario {
	t.Helper()
	f := newOutboundFederationFixture(t)
	f.service.dataDir = privateHubDataDir(t)
	recipientPrivate, err := ecdh.X25519().GenerateKey(rand.Reader)
	require.NoError(t, err)
	registerOutboundEncryption(t, f, recipientPrivate.PublicKey().Bytes())
	directory, err := f.remoteRegistry.ExportDirectory(t.Context(), f.remote.DB())
	require.NoError(t, err)
	_, err = f.localRegistry.ImportDirectory(
		t.Context(), f.store.DB(), outboxDestinationNode, directory,
	)
	require.NoError(t, err)

	artifactID, err := artifactformat.DeriveArtifactID(outboxSender, clientID)
	require.NoError(t, err)
	contentKey, err := artifactformat.NewContentKey()
	require.NoError(t, err)
	chunkCount, err := artifactformat.ChunkCount(int64(len(plaintext)))
	require.NoError(t, err)
	chunks := make([][]byte, 0, chunkCount)
	ciphertextHash := sha256.New()
	var ciphertextSize int64
	for index := range chunkCount {
		start := index * protocol.ArtifactChunkSize
		end := min(start+protocol.ArtifactChunkSize, int64(len(plaintext)))
		chunk, err := artifactformat.EncryptChunk(
			contentKey, artifactID, index, chunkCount, plaintext[start:end],
		)
		require.NoError(t, err)
		chunks = append(chunks, chunk)
		ciphertextSize += int64(len(chunk))
		_, _ = ciphertextHash.Write(chunk)
	}
	plainDigest := sha256.Sum256(plaintext)
	manifest, err := artifactformat.EncryptManifest(contentKey, artifactID, artifactformat.PrivateManifest{
		Filename: "federated.bin", MediaType: "application/octet-stream", Purpose: "replicate",
		PlaintextSize:   int64(len(plaintext)),
		PlaintextSHA256: protocol.SHA256Hex(hex.EncodeToString(plainDigest[:])),
	})
	require.NoError(t, err)
	envelope, err := artifactformat.WrapContentKey(
		recipientPrivate.PublicKey().Bytes(), artifactID, outboxRecipient, contentKey,
	)
	require.NoError(t, err)
	params := protocol.ReserveArtifactParams{
		ClientMsgID: clientID, Recipient: outboxRecipient,
		EncryptionProfile: protocol.ArtifactEncryptionProfile,
		CiphertextSize:    ciphertextSize, ChunkCount: chunkCount,
		CiphertextSHA256:  protocol.SHA256Hex(hex.EncodeToString(ciphertextHash.Sum(nil))),
		EncryptedManifest: manifest,
		RecipientKeyID:    protocol.EncryptionKeyID("x25519:" + string(outboxRecipient) + ":v1"),
		KeyEnvelope:       envelope,
		SenderKeyID:       protocol.SigningKeyID("ed25519:" + string(outboxSender) + ":v1"),
		ReservedAt:        f.clock.now, ExpiresAt: f.clock.now.Add(24 * time.Hour),
	}
	params.Signature, err = artifactformat.SignEnvelope(f.senderKey, artifactformat.EnvelopeMetadata{
		ArtifactID: artifactID, ClientMsgID: clientID, Sender: outboxSender,
		Recipient: outboxRecipient, CiphertextSize: ciphertextSize, ChunkCount: chunkCount,
		CiphertextSHA256: params.CiphertextSHA256, EncryptedManifest: manifest,
		KeyEnvelope: envelope, EncryptionProfile: params.EncryptionProfile,
		ExpiresAt: params.ExpiresAt,
	})
	require.NoError(t, err)
	remoteService := f.remoteServiceWithoutTransport(t)
	remoteService.dataDir = privateHubDataDir(t)
	return &federatedArtifactScenario{
		base: f, remoteService: remoteService, recipientPrivate: recipientPrivate,
		artifactID: artifactID, plaintext: plaintext,
		chunks: chunks, params: params, originAckReceiver: f.service,
	}
}

func registerOutboundEncryption(
	t *testing.T,
	f *outboundFederationFixture,
	publicKey []byte,
) {
	t.Helper()
	process, err := registry.CurrentProcess()
	require.NoError(t, err)
	_, err = f.remoteRegistry.Register(t.Context(), f.remote.DB(), registry.RegisterRequest{
		Agent: outboxRecipient, Process: process, PublicKey: publicKey,
		KeyID:   "x25519:" + string(outboxRecipient) + ":v1",
		Purpose: registry.KeyPurposeEncryption, Algorithm: registry.KeyAlgorithmX25519,
	})
	require.NoError(t, err)
}

func (s *federatedArtifactScenario) queue(t *testing.T) {
	t.Helper()
	reserved := callArtifact[protocol.ReserveArtifactResult](
		t, s.base.service.ReserveArtifact, outboxSender, s.params,
	)
	require.Equal(t, s.artifactID, reserved.ArtifactID)
	for index, chunk := range s.chunks {
		uploaded := callArtifact[protocol.UploadArtifactChunkResult](
			t, s.base.service.UploadArtifactChunk, outboxSender,
			protocol.UploadArtifactChunkParams{
				ArtifactID: s.artifactID, ChunkIndex: int64(index), Ciphertext: chunk,
				SHA256: artifactformat.Digest(chunk),
			},
		)
		require.True(t, uploaded.Stored)
	}
	completed := callArtifact[protocol.ArtifactMutationResult](
		t, s.base.service.CompleteArtifactUpload, outboxSender,
		protocol.ArtifactIDParams{ArtifactID: s.artifactID},
	)
	require.Equal(t, protocol.ArtifactStateQueuedRemote, completed.TransferState)
}

func (s *federatedArtifactScenario) start(
	t *testing.T,
	receiver federation.ArtifactReceiver,
) *federation.Replicator {
	t.Helper()
	originServer, err := federation.NewServer(federation.ServerOptions{
		Listen: testLoopback, Identity: s.base.originIdentity,
		TrustBundle: federationTestBundle(t, s.base.remoteIdentity),
		DB:          s.base.store.DB(), Clock: s.base.clock, ArtifactAck: s.originAckReceiver,
		Directory: s.base.localRegistry,
	})
	require.NoError(t, err)
	originAddress, err := originServer.Listen(t.Context())
	require.NoError(t, err)
	startOutboundFederationServer(t, originServer)
	require.NoError(t, s.base.remoteRegistry.SetPeerTransport(
		t.Context(), s.base.remote.DB(), outboxOriginNode,
		"https://"+originAddress, s.base.originIdentity.Fingerprint,
	))
	s.ackReplicator, err = federation.NewReplicator(federation.ReplicatorOptions{
		DB: s.base.remote.DB(), Registry: s.base.remoteRegistry,
		Identity:    s.base.remoteIdentity,
		TrustBundle: federationTestBundle(t, s.base.originIdentity), Clock: s.base.clock,
		Random: outboundZeroReader{}, LeaseOwner: "artifact-ack-e2e-worker",
	})
	require.NoError(t, err)

	server, err := federation.NewServer(federation.ServerOptions{
		Listen: testLoopback, Identity: s.base.remoteIdentity,
		TrustBundle: federationTestBundle(t, s.base.originIdentity),
		DB:          s.base.remote.DB(), Clock: s.base.clock, Artifact: receiver,
		Directory: s.base.remoteRegistry,
	})
	require.NoError(t, err)
	address, err := server.Listen(t.Context())
	require.NoError(t, err)
	startOutboundFederationServer(t, server)
	return s.base.replicator(t, address)
}

func (s *federatedArtifactScenario) replicateAll(t *testing.T, replicator *federation.Replicator) {
	t.Helper()
	for index := range len(s.chunks) + 2 {
		claimed, err := replicator.ReplicateOnce(t.Context())
		require.NoError(t, err)
		var state string
		var lastError string
		require.NoError(t, s.base.store.DB().QueryRowContext(t.Context(),
			"SELECT state, COALESCE(last_error, '') FROM federation_outbox WHERE object_type = 'artifact'").
			Scan(&state, &lastError))
		require.True(t, claimed, "iteration %d: state=%s last_error=%v", index, state, lastError)
	}
	claimed, err := replicator.ReplicateOnce(t.Context())
	require.NoError(t, err)
	require.False(t, claimed)
}

func TestFederatedArtifactRoundTripOverMTLS(t *testing.T) {
	plaintext := bytes.Repeat([]byte("federated-artifact-marker"), 90_000)
	scenario := newFederatedArtifactScenario(t, "artifact-e2e", plaintext)
	scenario.queue(t)
	replicator := scenario.start(t, scenario.remoteService)
	scenario.replicateAll(t, replicator)

	var originState, remoteState string
	require.NoError(t, scenario.base.store.DB().QueryRowContext(t.Context(),
		"SELECT state FROM artifact_transfers WHERE artifact_id = ?", scenario.artifactID).
		Scan(&originState))
	require.Equal(t, string(protocol.ArtifactStateStoredRemote), originState)
	require.NoError(t, scenario.base.remote.DB().QueryRowContext(t.Context(),
		"SELECT state FROM artifact_transfers WHERE artifact_id = ?", scenario.artifactID).
		Scan(&remoteState))
	require.Equal(t, string(protocol.ArtifactStateLocalAvailable), remoteState)

	status := callArtifact[protocol.ArtifactStatusResult](
		t, scenario.remoteService.GetArtifactStatus, outboxRecipient,
		protocol.ArtifactIDParams{ArtifactID: scenario.artifactID},
	)
	openedKey, err := artifactformat.UnwrapContentKey(
		scenario.recipientPrivate, scenario.artifactID, outboxRecipient,
		status.Recipients[0].KeyEnvelope,
	)
	require.NoError(t, err)
	var opened []byte
	for index := range status.ChunkCount {
		downloaded := callArtifact[protocol.DownloadArtifactChunkResult](
			t, scenario.remoteService.DownloadArtifactChunk, outboxRecipient,
			protocol.DownloadArtifactChunkParams{ArtifactID: scenario.artifactID, ChunkIndex: index},
		)
		chunk, err := artifactformat.DecryptChunk(
			openedKey, scenario.artifactID, index, status.ChunkCount, downloaded.Ciphertext,
		)
		require.NoError(t, err)
		opened = append(opened, chunk...)
	}
	require.Equal(t, scenario.plaintext, opened)
	privateManifest, err := artifactformat.DecryptManifest(
		openedKey, scenario.artifactID, status.EncryptedManifest,
	)
	require.NoError(t, err)
	require.Equal(t, privateManifest.PlaintextSHA256, artifactformat.Digest(opened))

	acknowledged := callArtifact[protocol.ArtifactMutationResult](
		t, scenario.remoteService.AckArtifact, outboxRecipient,
		protocol.ArtifactIDParams{ArtifactID: scenario.artifactID},
	)
	require.Equal(t, protocol.ArtifactStateAcknowledged, acknowledged.TransferState)
	require.Zero(t, federationTableCount(t, scenario.base.remote, "artifact_chunks"))
	remoteDir, err := scenario.remoteService.artifactDirectory(scenario.artifactID)
	require.NoError(t, err)
	_, err = os.Stat(remoteDir)
	require.ErrorIs(t, err, os.ErrNotExist)

	ackReplicator := scenario.ackReplicator
	claimed, err := ackReplicator.ReplicateOnce(t.Context())
	require.NoError(t, err)
	require.True(t, claimed)
	claimed, err = ackReplicator.ReplicateOnce(t.Context())
	require.NoError(t, err)
	require.False(t, claimed)
	require.NoError(t, scenario.base.store.DB().QueryRowContext(t.Context(),
		"SELECT state FROM artifact_transfers WHERE artifact_id = ?", scenario.artifactID).
		Scan(&originState))
	require.Equal(t, string(protocol.ArtifactStateAcknowledged), originState)
	require.Zero(t, federationTableCount(t, scenario.base.store, "artifact_chunks"))
	originDir, err := scenario.base.service.artifactDirectory(scenario.artifactID)
	require.NoError(t, err)
	_, err = os.Stat(originDir)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.Equal(t, int64(1),
		federationTableCount(t, scenario.base.store, "federation_artifact_ack_inbox"))
	require.Equal(t, int64(1),
		federationTableCount(t, scenario.base.remote, "federation_artifact_ack_receipts"))
	var cursor int64
	var accepted int64
	require.NoError(t, scenario.base.store.DB().QueryRowContext(t.Context(),
		"SELECT next_chunk, manifest_accepted FROM federation_artifact_outbox").
		Scan(&cursor, &accepted))
	require.Equal(t, int64(len(scenario.chunks)), cursor)
	require.Equal(t, int64(1), accepted)
	require.Equal(t, int64(len(scenario.chunks)+2),
		federationTableCount(t, scenario.base.store, "federation_artifact_receipts"))
}

type artifactAckResponseSender struct {
	status   int
	calls    int
	requests [][]byte
}

func (s *artifactAckResponseSender) Send(
	_ context.Context,
	_ registry.Peer,
	exact []byte,
) (federation.DeliveryResponse, error) {
	s.calls++
	s.requests = append(s.requests, bytes.Clone(exact))
	return federation.DeliveryResponse{StatusCode: s.status}, nil
}

type invalidArtifactAckReceiptSender struct {
	registry *registry.Registry
	clock    *hubClock
}

func (s *invalidArtifactAckReceiptSender) Send(
	_ context.Context,
	_ registry.Peer,
	exact []byte,
) (federation.DeliveryResponse, error) {
	request, err := federation.DecodeArtifactAckRequest(exact)
	if err != nil {
		return federation.DeliveryResponse{}, err
	}
	digest, err := request.RequestSHA256()
	if err != nil {
		return federation.DeliveryResponse{}, err
	}
	receipt := federation.ArtifactAckReceipt{
		ArtifactID: request.ArtifactID, Recipient: request.Recipient,
		OriginNode: outboxOriginNode, DestinationNode: outboxDestinationNode,
		RequestSHA256: digest, Nonce: bytes.Clone(request.Nonce), Sequence: request.Sequence,
		ReceivedAt: s.clock.now,
	}
	if err := receipt.Sign(s.registry); err != nil {
		return federation.DeliveryResponse{}, err
	}
	receipt.Signature[0] ^= 0xff
	raw, err := json.Marshal(receipt)
	if err != nil {
		return federation.DeliveryResponse{}, err
	}
	return federation.DeliveryResponse{StatusCode: http.StatusOK, Body: raw}, nil
}

func TestArtifactAckReplicatorRejectsInvalidSignedReceipt(t *testing.T) {
	scenario := newFederatedArtifactScenario(t, "artifact-ack-bad-receipt", []byte("bad ack receipt"))
	scenario.queue(t)
	replicator := scenario.start(t, scenario.remoteService)
	scenario.replicateAll(t, replicator)
	callArtifact[protocol.ArtifactMutationResult](
		t, scenario.remoteService.AckArtifact, outboxRecipient,
		protocol.ArtifactIDParams{ArtifactID: scenario.artifactID},
	)
	sender := &invalidArtifactAckReceiptSender{
		registry: scenario.base.localRegistry, clock: scenario.base.clock,
	}
	ackReplicator, err := federation.NewReplicator(federation.ReplicatorOptions{
		DB: scenario.base.remote.DB(), Registry: scenario.base.remoteRegistry,
		Clock: scenario.base.clock, Random: outboundZeroReader{},
		LeaseOwner: "artifact-ack-bad-receipt", Sender: sender,
	})
	require.NoError(t, err)
	claimed, err := ackReplicator.ReplicateOnce(t.Context())
	require.NoError(t, err)
	require.True(t, claimed)
	var state string
	require.NoError(t, scenario.base.remote.DB().QueryRowContext(t.Context(),
		"SELECT state FROM federation_outbox WHERE object_type = 'artifact_ack'").Scan(&state))
	require.Equal(t, "FAILED_RETRYABLE", state)
	require.Zero(t, federationTableCount(t, scenario.base.remote, "federation_artifact_ack_receipts"))
}

func TestArtifactAck404StaysPendingWithBackoff(t *testing.T) {
	scenario := newFederatedArtifactScenario(t, "artifact-ack-old-peer", []byte("old peer ack"))
	scenario.queue(t)
	replicator := scenario.start(t, scenario.remoteService)
	scenario.replicateAll(t, replicator)
	acked := callArtifact[protocol.ArtifactMutationResult](
		t, scenario.remoteService.AckArtifact, outboxRecipient,
		protocol.ArtifactIDParams{ArtifactID: scenario.artifactID},
	)
	require.Equal(t, protocol.ArtifactStateAcknowledged, acked.TransferState)
	sender := &artifactAckResponseSender{status: http.StatusNotFound}
	ackReplicator, err := federation.NewReplicator(federation.ReplicatorOptions{
		DB: scenario.base.remote.DB(), Registry: scenario.base.remoteRegistry,
		Clock: scenario.base.clock, Random: outboundZeroReader{},
		LeaseOwner: "artifact-ack-old-peer", MaxAge: time.Hour, Sender: sender,
	})
	require.NoError(t, err)
	claimed, err := ackReplicator.ReplicateOnce(t.Context())
	require.NoError(t, err)
	require.True(t, claimed)
	require.Equal(t, 1, sender.calls)
	var outboxState string
	var nextAttempt string
	require.NoError(t, scenario.base.remote.DB().QueryRowContext(t.Context(), `SELECT state,
		next_attempt_at FROM federation_outbox WHERE object_type = 'artifact_ack'`).
		Scan(&outboxState, &nextAttempt))
	require.Equal(t, "FAILED_RETRYABLE", outboxState)
	require.NotEmpty(t, nextAttempt)
	claimed, err = ackReplicator.ReplicateOnce(t.Context())
	require.NoError(t, err)
	require.False(t, claimed)
	require.Equal(t, 1, sender.calls, "backoff must prevent a hot loop")
	require.Zero(t, federationTableCount(t, scenario.base.remote, "federation_artifact_ack_receipts"))

	scenario.base.clock.now = scenario.base.clock.now.Add(2 * time.Hour)
	claimed, err = ackReplicator.ReplicateOnce(t.Context())
	require.NoError(t, err)
	require.True(t, claimed)
	require.Equal(t, 2, sender.calls)
	require.NoError(t, scenario.base.remote.DB().QueryRowContext(t.Context(),
		"SELECT state FROM federation_outbox WHERE object_type = 'artifact_ack'").
		Scan(&outboxState))
	require.Equal(t, "FAILED_RETRYABLE", outboxState, "artifact ACKs never max-age closed")
	var remoteState string
	require.NoError(t, scenario.base.remote.DB().QueryRowContext(t.Context(),
		"SELECT state FROM artifact_transfers WHERE artifact_id = ?", scenario.artifactID).
		Scan(&remoteState))
	require.Equal(t, string(protocol.ArtifactStateAcknowledged), remoteState)
	var originState string
	require.NoError(t, scenario.base.store.DB().QueryRowContext(t.Context(),
		"SELECT state FROM artifact_transfers WHERE artifact_id = ?", scenario.artifactID).
		Scan(&originState))
	require.Equal(t, string(protocol.ArtifactStateStoredRemote), originState)
}

type lostArtifactAckReceipt struct {
	delegate federation.ArtifactAckReceiver
	dropped  bool
	receipts [][]byte
}

func (r *lostArtifactAckReceipt) ReceiveFederatedArtifactAck(
	ctx context.Context,
	peer protocol.NodeID,
	request federation.ArtifactAckRequest,
) ([]byte, error) {
	receipt, err := r.delegate.ReceiveFederatedArtifactAck(ctx, peer, request)
	if err != nil {
		return nil, err
	}
	r.receipts = append(r.receipts, bytes.Clone(receipt))
	if !r.dropped {
		r.dropped = true
		panic(http.ErrAbortHandler)
	}
	return receipt, nil
}

func TestFederatedArtifactAckReplaysReceiptAfterLostResponse(t *testing.T) {
	scenario := newFederatedArtifactScenario(t, "artifact-ack-lost-receipt", []byte("lost ack receipt"))
	lost := &lostArtifactAckReceipt{delegate: scenario.base.service}
	scenario.originAckReceiver = lost
	scenario.queue(t)
	replicator := scenario.start(t, scenario.remoteService)
	scenario.replicateAll(t, replicator)
	callArtifact[protocol.ArtifactMutationResult](
		t, scenario.remoteService.AckArtifact, outboxRecipient,
		protocol.ArtifactIDParams{ArtifactID: scenario.artifactID},
	)
	ackReplicator := scenario.ackReplicator
	claimed, err := ackReplicator.ReplicateOnce(t.Context())
	require.NoError(t, err)
	require.True(t, claimed)
	require.Equal(t, int64(1),
		federationTableCount(t, scenario.base.store, "federation_artifact_ack_inbox"))
	require.Zero(t, federationTableCount(t, scenario.base.store, "artifact_chunks"))
	require.Zero(t, federationTableCount(t, scenario.base.remote, "federation_artifact_ack_receipts"))
	scenario.base.clock.now = scenario.base.clock.now.Add(2 * time.Second)
	claimed, err = ackReplicator.ReplicateOnce(t.Context())
	require.NoError(t, err)
	require.True(t, claimed)
	require.Len(t, lost.receipts, 2)
	require.Equal(t, lost.receipts[0], lost.receipts[1])
	require.Equal(t, int64(1),
		federationTableCount(t, scenario.base.remote, "federation_artifact_ack_receipts"))
}

func TestFederatedArtifactAckRejectsPeerAndReplayConflicts(t *testing.T) {
	scenario := newFederatedArtifactScenario(t, "artifact-ack-binding", []byte("ack binding"))
	scenario.queue(t)
	replicator := scenario.start(t, scenario.remoteService)
	scenario.replicateAll(t, replicator)
	request := federation.ArtifactAckRequest{
		ArtifactID: scenario.artifactID, Recipient: outboxRecipient,
		AckedAt: scenario.base.clock.now, Nonce: bytes.Repeat([]byte{0x51}, 16), Sequence: 1,
	}
	_, err := scenario.base.service.ReceiveFederatedArtifactAck(
		t.Context(), outboxOriginNode, request,
	)
	require.Error(t, err)
	require.Zero(t, federationTableCount(t, scenario.base.store, "federation_artifact_ack_inbox"))
	first, err := scenario.base.service.ReceiveFederatedArtifactAck(
		t.Context(), outboxDestinationNode, request,
	)
	require.NoError(t, err)
	replayed, err := scenario.base.service.ReceiveFederatedArtifactAck(
		t.Context(), outboxDestinationNode, request,
	)
	require.NoError(t, err)
	require.Equal(t, first, replayed)
	conflict := request
	conflict.Sequence = 2
	conflict.Nonce = bytes.Repeat([]byte{0x52}, 16)
	_, err = scenario.base.service.ReceiveFederatedArtifactAck(
		t.Context(), outboxDestinationNode, conflict,
	)
	require.Error(t, err)
	require.Equal(t, int64(1),
		federationTableCount(t, scenario.base.store, "federation_artifact_ack_inbox"))
}

type lostArtifactChunkReceipt struct {
	delegate federation.ArtifactReceiver
	dropped  bool
	indexes  []int64
}

func (r *lostArtifactChunkReceipt) ReceiveArtifactManifest(
	ctx context.Context,
	peer protocol.NodeID,
	request federation.ArtifactManifestRequest,
) ([]byte, error) {
	return r.delegate.ReceiveArtifactManifest(ctx, peer, request)
}

func (r *lostArtifactChunkReceipt) ReceiveArtifactChunk(
	ctx context.Context,
	peer protocol.NodeID,
	request federation.ArtifactChunkRequest,
) ([]byte, error) {
	r.indexes = append(r.indexes, request.ChunkIndex)
	receipt, err := r.delegate.ReceiveArtifactChunk(ctx, peer, request)
	if err == nil && !r.dropped {
		r.dropped = true
		panic(http.ErrAbortHandler)
	}
	return receipt, err
}

func (r *lostArtifactChunkReceipt) CompleteArtifact(
	ctx context.Context,
	peer protocol.NodeID,
	request federation.ArtifactCompleteRequest,
) ([]byte, error) {
	return r.delegate.CompleteArtifact(ctx, peer, request)
}

func TestFederatedArtifactResumesFromReceiverCursorAfterLostReceipt(t *testing.T) {
	plaintext := bytes.Repeat([]byte("resume-artifact-marker"), 80_000)
	scenario := newFederatedArtifactScenario(t, "artifact-resume", plaintext)
	scenario.queue(t)
	receiver := &lostArtifactChunkReceipt{delegate: scenario.remoteService}
	replicator := scenario.start(t, receiver)

	claimed, err := replicator.ReplicateOnce(t.Context())
	require.NoError(t, err)
	require.True(t, claimed)
	claimed, err = replicator.ReplicateOnce(t.Context())
	require.NoError(t, err)
	var outboxState string
	var lastError string
	require.NoError(t, scenario.base.store.DB().QueryRowContext(t.Context(),
		"SELECT state, COALESCE(last_error, '') FROM federation_outbox WHERE object_type = 'artifact'").
		Scan(&outboxState, &lastError))
	require.True(t, claimed, "state=%s last_error=%v", outboxState, lastError)
	require.Equal(t, int64(1), federationTableCount(t, scenario.base.remote, "artifact_chunks"))
	var cursor int64
	require.NoError(t, scenario.base.store.DB().QueryRowContext(t.Context(),
		"SELECT next_chunk FROM federation_artifact_outbox").Scan(&cursor))
	require.Zero(t, cursor)

	scenario.base.clock.now = scenario.base.clock.now.Add(2 * time.Second)
	claimed, err = replicator.ReplicateOnce(t.Context())
	require.NoError(t, err)
	require.True(t, claimed)
	require.Equal(t, []int64{0, 0}, receiver.indexes)
	require.Equal(t, int64(1), federationTableCount(t, scenario.base.remote, "artifact_chunks"))
	for range len(scenario.chunks) - 1 + 1 {
		claimed, err = replicator.ReplicateOnce(t.Context())
		require.NoError(t, err)
		require.True(t, claimed)
	}
	require.NoError(t, scenario.base.store.DB().QueryRowContext(t.Context(),
		"SELECT next_chunk FROM federation_artifact_outbox").Scan(&cursor))
	require.Equal(t, int64(len(scenario.chunks)), cursor)
}

func TestFederatedArtifactExpiryPurgesReceiverPartialState(t *testing.T) {
	scenario := newFederatedArtifactScenario(t, "artifact-expiry", []byte("expires remotely"))
	scenario.queue(t)
	var manifestRaw []byte
	require.NoError(t, scenario.base.store.DB().QueryRowContext(t.Context(),
		"SELECT manifest FROM federation_artifact_outbox").Scan(&manifestRaw))
	manifest, err := federation.DecodeArtifactManifestRequest(manifestRaw)
	require.NoError(t, err)
	_, err = scenario.remoteService.ReceiveArtifactManifest(t.Context(), outboxOriginNode, manifest)
	require.NoError(t, err)
	chunk := federation.ArtifactChunkRequest{
		ArtifactID: scenario.artifactID, OriginNode: outboxOriginNode,
		DestinationNode: outboxDestinationNode, ChunkIndex: 0,
		Ciphertext: scenario.chunks[0], SHA256: artifactformat.Digest(scenario.chunks[0]),
	}
	_, err = scenario.remoteService.ReceiveArtifactChunk(t.Context(), outboxOriginNode, chunk)
	require.NoError(t, err)
	scenario.base.clock.now = scenario.params.ExpiresAt.Add(time.Second)
	expired, err := scenario.remoteService.ExpireFederatedArtifacts(t.Context())
	require.NoError(t, err)
	require.Equal(t, int64(1), expired)
	require.Zero(t, federationTableCount(t, scenario.base.remote, "artifact_transfers"))
	require.Zero(t, federationTableCount(t, scenario.base.remote, "artifact_chunks"))
	require.Zero(t, federationTableCount(t, scenario.base.remote, "federation_artifact_inbox"))
	dir, err := scenario.remoteService.artifactDirectory(scenario.artifactID)
	require.NoError(t, err)
	_, err = os.Stat(dir)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestFederatedArtifactExpiryPurgesAvailableReceiverState(t *testing.T) {
	scenario := newFederatedArtifactScenario(t, "artifact-available-expiry", []byte("available then expired"))
	scenario.queue(t)
	replicator := scenario.start(t, scenario.remoteService)
	scenario.replicateAll(t, replicator)
	require.NotZero(t, federationTableCount(t, scenario.base.remote, "artifact_chunks"))
	scenario.base.clock.now = scenario.params.ExpiresAt.Add(time.Second)
	pending := callArtifact[protocol.ListPendingArtifactsResult](
		t, scenario.remoteService.ListPendingArtifacts, outboxRecipient,
		protocol.ListPendingArtifactsParams{},
	)
	require.Empty(t, pending.Artifacts)
	require.Zero(t, federationTableCount(t, scenario.base.remote, "artifact_transfers"))
	require.Zero(t, federationTableCount(t, scenario.base.remote, "artifact_chunks"))
	require.Zero(t, federationTableCount(t, scenario.base.remote, "federation_artifact_inbox"))
	dir, err := scenario.remoteService.artifactDirectory(scenario.artifactID)
	require.NoError(t, err)
	_, err = os.Stat(dir)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestFederatedArtifactRejectsPeerSignatureChunkAndReplayConflicts(t *testing.T) {
	scenario := newFederatedArtifactScenario(t, "artifact-negatives", []byte("negative artifact"))
	scenario.queue(t)
	var manifestRaw []byte
	require.NoError(t, scenario.base.store.DB().QueryRowContext(t.Context(),
		"SELECT manifest FROM federation_artifact_outbox").Scan(&manifestRaw))
	manifest, err := federation.DecodeArtifactManifestRequest(manifestRaw)
	require.NoError(t, err)
	_, err = scenario.remoteService.ReceiveArtifactManifest(t.Context(), outboxDestinationNode, manifest)
	require.Error(t, err)
	badSignature := manifest
	badSignature.Reservation.Signature = append([]byte(nil), manifest.Reservation.Signature...)
	badSignature.Reservation.Signature[0] ^= 0xff
	_, err = scenario.remoteService.ReceiveArtifactManifest(t.Context(), outboxOriginNode, badSignature)
	require.Error(t, err)
	require.Zero(t, federationTableCount(t, scenario.base.remote, "artifact_transfers"))
	firstReceipt, err := scenario.remoteService.ReceiveArtifactManifest(
		t.Context(), outboxOriginNode, manifest,
	)
	require.NoError(t, err)
	scenario.base.clock.now = scenario.base.clock.now.Add(time.Hour)
	prematureExpiry := manifest
	prematureExpiry.Reservation.ExpiresAt = scenario.base.clock.now.Add(-time.Second)
	prematureExpiry.Reservation.ReservedAt = prematureExpiry.Reservation.ExpiresAt.Add(-24 * time.Hour)
	_, err = scenario.remoteService.ReceiveArtifactManifest(
		t.Context(), outboxOriginNode, prematureExpiry,
	)
	require.Error(t, err)
	require.Equal(t, int64(1), federationTableCount(t, scenario.base.remote, "artifact_transfers"))
	replayedReceipt, err := scenario.remoteService.ReceiveArtifactManifest(
		t.Context(), outboxOriginNode, manifest,
	)
	require.NoError(t, err)
	require.NotEmpty(t, firstReceipt)
	require.NotEmpty(t, replayedReceipt)
	require.Equal(t, int64(1), federationTableCount(t, scenario.base.remote, "artifact_transfers"))

	conflict := manifest
	conflict.Reservation.EncryptedManifest = append([]byte(nil), manifest.Reservation.EncryptedManifest...)
	conflict.Reservation.EncryptedManifest[0] ^= 0xff
	_, err = scenario.remoteService.ReceiveArtifactManifest(t.Context(), outboxOriginNode, conflict)
	require.Error(t, err)
	badChunk := federation.ArtifactChunkRequest{
		ArtifactID: scenario.artifactID, OriginNode: outboxOriginNode,
		DestinationNode: outboxDestinationNode, ChunkIndex: 0,
		Ciphertext: scenario.chunks[0],
		SHA256:     protocol.SHA256Hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
	}
	_, err = scenario.remoteService.ReceiveArtifactChunk(t.Context(), outboxOriginNode, badChunk)
	require.Error(t, err)
	require.Zero(t, federationTableCount(t, scenario.base.remote, "artifact_chunks"))
	badChunk.SHA256 = artifactformat.Digest(badChunk.Ciphertext)
	_, err = scenario.remoteService.ReceiveArtifactChunk(t.Context(), outboxOriginNode, badChunk)
	require.NoError(t, err)
	_, err = scenario.remoteService.ReceiveArtifactChunk(t.Context(), outboxOriginNode, badChunk)
	require.NoError(t, err)
	require.Equal(t, int64(1), federationTableCount(t, scenario.base.remote, "artifact_chunks"))
	badChunk.Ciphertext = append([]byte(nil), badChunk.Ciphertext...)
	badChunk.Ciphertext[0] ^= 0xff
	badChunk.SHA256 = artifactformat.Digest(badChunk.Ciphertext)
	_, err = scenario.remoteService.ReceiveArtifactChunk(t.Context(), outboxOriginNode, badChunk)
	require.Error(t, err)
	require.Equal(t, int64(1), federationTableCount(t, scenario.base.remote, "artifact_chunks"))
}

func TestFederatedArtifactReceiverAppliesArtifactChannel(t *testing.T) {
	scenario := newFederatedArtifactScenario(t, "artifact-channel-deny", []byte("denied artifact"))
	scenario.queue(t)
	_, err := scenario.remoteService.approvals.SetLink(t.Context(), protocol.SetLinkParams{
		FromAgent: outboxSender, ToAgent: outboxRecipient,
		Plane: protocol.PlaneArtifact, Mode: protocol.LinkModeDeny,
		ExpectedVersions: protocol.ControlVersions{
			"link:" + string(outboxSender) + "→" + string(outboxRecipient) + ":artifact": 0,
		},
	}, channel.Actor{Type: protocol.ActorTypeHuman, ID: "operator-artifact"})
	require.NoError(t, err)
	var manifestRaw []byte
	require.NoError(t, scenario.base.store.DB().QueryRowContext(t.Context(),
		"SELECT manifest FROM federation_artifact_outbox").Scan(&manifestRaw))
	manifest, err := federation.DecodeArtifactManifestRequest(manifestRaw)
	require.NoError(t, err)
	_, err = scenario.remoteService.ReceiveArtifactManifest(t.Context(), outboxOriginNode, manifest)
	require.Error(t, err)
	require.Zero(t, federationTableCount(t, scenario.base.remote, "artifact_transfers"))
	var decisions int64
	require.NoError(t, scenario.base.remote.DB().QueryRowContext(t.Context(), `SELECT COUNT(*)
		FROM channel_control_decisions WHERE object_type = 'artifact' AND object_id = ?`,
		scenario.artifactID).Scan(&decisions))
	require.Equal(t, int64(1), decisions)
}

type artifactReceiptFault string

const (
	artifactReceiptForeignID    artifactReceiptFault = "foreign-id"
	artifactReceiptBadSignature artifactReceiptFault = "bad-signature"
	artifactReceiptBadCursor    artifactReceiptFault = "bad-cursor"
)

type faultyArtifactReceiptSender struct {
	registry *registry.Registry
	clock    *hubClock
	fault    artifactReceiptFault
	calls    int
}

func (s *faultyArtifactReceiptSender) Send(
	_ context.Context,
	_ registry.Peer,
	exact []byte,
) (federation.DeliveryResponse, error) {
	s.calls++
	var receipt federation.ArtifactReceipt
	var err error
	if s.calls == 1 {
		receipt, err = s.manifestReceipt(exact)
	} else {
		receipt, err = s.chunkReceipt(exact)
	}
	if err != nil {
		return federation.DeliveryResponse{}, err
	}
	if err := receipt.Sign(s.registry); err != nil {
		return federation.DeliveryResponse{}, err
	}
	if s.fault == artifactReceiptBadSignature {
		receipt.Signature[0] ^= 0xff
	}
	raw, err := json.Marshal(receipt)
	if err != nil {
		return federation.DeliveryResponse{}, err
	}
	return federation.DeliveryResponse{StatusCode: http.StatusOK, Body: raw}, nil
}

func (s *faultyArtifactReceiptSender) manifestReceipt(
	exact []byte,
) (federation.ArtifactReceipt, error) {
	request, err := federation.DecodeArtifactManifestRequest(exact)
	if err != nil {
		return federation.ArtifactReceipt{}, err
	}
	digest, err := request.RequestSHA256()
	if err != nil {
		return federation.ArtifactReceipt{}, err
	}
	receipt := federation.ArtifactReceipt{
		ArtifactID: request.ArtifactID, OriginNode: request.OriginNode,
		DestinationNode: request.DestinationNode,
		Phase:           federation.ArtifactReceiptManifest, RequestSHA256: digest,
		NextChunk: 0, ChunkCount: request.Reservation.ChunkCount,
		Status: federation.ArtifactReceiptReplicating, ReceivedAt: s.clock.now,
	}
	if s.fault == artifactReceiptForeignID {
		receipt.ArtifactID = "art-foreign"
	}
	return receipt, nil
}

func (s *faultyArtifactReceiptSender) chunkReceipt(
	exact []byte,
) (federation.ArtifactReceipt, error) {
	request, err := federation.DecodeArtifactChunkRequest(exact)
	if err != nil {
		return federation.ArtifactReceipt{}, err
	}
	digest, err := request.RequestSHA256()
	if err != nil {
		return federation.ArtifactReceipt{}, err
	}
	return federation.ArtifactReceipt{
		ArtifactID: request.ArtifactID, OriginNode: request.OriginNode,
		DestinationNode: request.DestinationNode,
		Phase:           federation.ArtifactReceiptChunk, RequestSHA256: digest,
		NextChunk: request.ChunkIndex + 2, ChunkCount: 2,
		Status: federation.ArtifactReceiptReplicating, ReceivedAt: s.clock.now,
	}, nil
}

func TestArtifactReplicatorDoesNotAdvanceOnInvalidReceipt(t *testing.T) {
	tests := []struct {
		name      string
		fault     artifactReceiptFault
		plaintext []byte
		calls     int
		accepted  int64
	}{
		{"foreign artifact", artifactReceiptForeignID, []byte("foreign receipt"), 1, 0},
		{"bad signature", artifactReceiptBadSignature, []byte("bad signature"), 1, 0},
		{
			"bad chunk cursor", artifactReceiptBadCursor,
			bytes.Repeat([]byte("cursor"), 200_000), 2, 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scenario := newFederatedArtifactScenario(
				t, protocol.OpaqueID("artifact-receipt-"+string(test.fault)), test.plaintext,
			)
			scenario.queue(t)
			sender := &faultyArtifactReceiptSender{
				registry: scenario.base.remoteRegistry, clock: scenario.base.clock, fault: test.fault,
			}
			replicator, err := federation.NewReplicator(federation.ReplicatorOptions{
				DB: scenario.base.store.DB(), Registry: scenario.base.localRegistry,
				Clock: scenario.base.clock, Random: outboundZeroReader{},
				LeaseOwner: "invalid-artifact-receipt", Sender: sender,
			})
			require.NoError(t, err)
			for range test.calls {
				claimed, err := replicator.ReplicateOnce(t.Context())
				require.NoError(t, err)
				require.True(t, claimed)
			}
			var cursor, accepted int64
			require.NoError(t, scenario.base.store.DB().QueryRowContext(t.Context(),
				"SELECT next_chunk, manifest_accepted FROM federation_artifact_outbox").
				Scan(&cursor, &accepted))
			require.Zero(t, cursor)
			require.Equal(t, test.accepted, accepted)
		})
	}
}
