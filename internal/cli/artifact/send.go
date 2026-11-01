package artifact

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"mime"
	"os"
	"path/filepath"
	"time"
	"unicode/utf8"

	artifactformat "github.com/smellak/gifford/internal/artifact"
	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/endpoint"
	hubapi "github.com/smellak/gifford/internal/hub"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	remoteArtifactWaitTimeout = 30 * time.Second
	remoteArtifactPollEvery   = 250 * time.Millisecond
)

type artifactCallClient interface {
	call(context.Context, string, any, any) error
}

type artifactSendClient interface {
	artifactCallClient
	notify(context.Context, endpoint.NotificationRequest) error
}

type artifactSendOptions struct {
	remoteWait time.Duration
	pollEvery  time.Duration
}

type sendResult struct {
	ArtifactID  protocol.OpaqueID      `json:"artifact_id"`
	State       protocol.ArtifactState `json:"state"`
	ChunkCount  int64                  `json:"chunk_count"`
	ChunksSent  int64                  `json:"chunks_sent"`
	ResumedFrom int64                  `json:"resumed_from"`
	NoticeSent  bool                   `json:"notice_sent"`
}

func (c *artifactClient) notify(ctx context.Context, request endpoint.NotificationRequest) error {
	_, err := endpoint.SendNotification(ctx, c.hub, c.dirs.Data, request)
	if err != nil {
		return fmt.Errorf("send artifact notification: %w", err)
	}
	return nil
}

func sendFile(
	ctx context.Context,
	path string,
	recipient protocol.AgentID,
	purpose string,
) (sendResult, error) {
	if !recipient.Valid() || !utf8.ValidString(purpose) || utf8.RuneCountInString(purpose) > 200 {
		return sendResult{}, fmt.Errorf("invalid artifact recipient or purpose")
	}
	plaintextSize, plaintextDigest, err := scanPlaintext(path)
	if err != nil {
		return sendResult{}, err
	}
	client, err := openArtifactClient(ctx)
	if err != nil {
		return sendResult{}, err
	}
	defer client.close()
	if recipient == client.agent {
		return sendResult{}, fmt.Errorf("artifact recipient must differ from sender")
	}
	clientMsgID, err := artifactformat.DefaultClientMessageID(recipient, plaintextDigest, purpose)
	if err != nil {
		return sendResult{}, fmt.Errorf("derive artifact client id: %w", err)
	}
	artifactID, err := artifactformat.DeriveArtifactID(client.agent, clientMsgID)
	if err != nil {
		return sendResult{}, fmt.Errorf("derive artifact id: %w", err)
	}
	chunkCount, err := artifactformat.ChunkCount(plaintextSize)
	if err != nil {
		return sendResult{}, fmt.Errorf("count artifact chunks: %w", err)
	}
	ciphertextSize, err := artifactformat.CiphertextSize(plaintextSize, chunkCount)
	if err != nil {
		return sendResult{}, fmt.Errorf("size artifact ciphertext: %w", err)
	}
	record, recorded, err := loadOutgoingRecord(client.dirs.Data, artifactID)
	if err != nil {
		return sendResult{}, err
	}
	key, keyFound, err := loadContentKey(client.dirs.Data, artifactID)
	if err != nil {
		return sendResult{}, err
	}
	// Durable resume validation intentionally keeps the existing and first-send
	// paths together so they cannot drift on identity or cryptographic metadata.
	//nolint:nestif
	if recorded {
		if record.Sender != client.agent || record.Reserve.Recipient != recipient ||
			record.Reserve.ClientMsgID != clientMsgID || record.Reserve.ChunkCount != chunkCount ||
			record.Reserve.CiphertextSize != ciphertextSize {
			return sendResult{}, fmt.Errorf("outgoing artifact reservation conflicts with this file")
		}
	} else {
		if !keyFound {
			key, err = loadOrCreateContentKey(client.dirs.Data, artifactID)
			if err != nil {
				return sendResult{}, err
			}
			keyFound = true
		}
		ciphertextDigest, verifiedPlaintext, err := firstEncryptionPass(
			path, plaintextSize, key, artifactID, chunkCount,
		)
		if err != nil {
			return sendResult{}, fmt.Errorf("prepare artifact ciphertext: %w", err)
		}
		if verifiedPlaintext != plaintextDigest {
			return sendResult{}, fmt.Errorf("artifact file changed during encryption")
		}
		var recipientKey protocol.ArtifactRecipientKeyResult
		if err := client.call(ctx, hubapi.MethodReserveArtifact,
			protocol.ArtifactRecipientKeyParams{KeyOnly: true, Recipient: recipient},
			&recipientKey); err != nil {
			return sendResult{}, err
		}
		manifest := artifactformat.PrivateManifest{
			Filename: filepath.Base(path), MediaType: mediaType(path), Purpose: purpose,
			PlaintextSize: plaintextSize, PlaintextSHA256: plaintextDigest,
		}
		encryptedManifest, err := artifactformat.EncryptManifest(key, artifactID, manifest)
		if err != nil {
			return sendResult{}, fmt.Errorf("encrypt artifact manifest: %w", err)
		}
		keyEnvelope, err := artifactformat.WrapContentKey(
			recipientKey.PublicKey, artifactID, recipient, key,
		)
		if err != nil {
			return sendResult{}, fmt.Errorf("wrap artifact content key: %w", err)
		}
		reservedAt := (clock.System{}).Now().UTC()
		params := protocol.ReserveArtifactParams{
			ClientMsgID: clientMsgID, Recipient: recipient,
			EncryptionProfile: protocol.ArtifactEncryptionProfile,
			CiphertextSize:    ciphertextSize, ChunkCount: chunkCount,
			CiphertextSHA256: ciphertextDigest, EncryptedManifest: encryptedManifest,
			RecipientKeyID: recipientKey.KeyID, KeyEnvelope: keyEnvelope,
			SenderKeyID: protocol.SigningKeyID("ed25519:" + string(client.agent) + ":v1"),
			ReservedAt:  reservedAt, ExpiresAt: reservedAt.Add(24 * time.Hour),
		}
		private, err := endpoint.LoadAgentPrivate(client.dirs.Data, client.agent)
		if err != nil {
			return sendResult{}, fmt.Errorf("load artifact signing key: %w", err)
		}
		params.Signature, err = artifactformat.SignEnvelope(private, artifactformat.EnvelopeMetadata{
			ArtifactID: artifactID, ClientMsgID: clientMsgID, Sender: client.agent,
			Recipient: recipient, CiphertextSize: ciphertextSize, ChunkCount: chunkCount,
			CiphertextSHA256: ciphertextDigest, EncryptedManifest: encryptedManifest,
			KeyEnvelope: keyEnvelope, EncryptionProfile: params.EncryptionProfile,
			ExpiresAt: params.ExpiresAt,
		})
		if err != nil {
			return sendResult{}, fmt.Errorf("sign artifact envelope: %w", err)
		}
		record = outgoingRecord{
			Version: outgoingRecordVersion, ArtifactID: artifactID,
			Sender: client.agent, Reserve: params,
		}
		if err := saveOutgoingRecord(client.dirs.Data, record); err != nil {
			return sendResult{}, err
		}
	}

	if recorded && keyFound {
		ciphertextDigest, verifiedPlaintext, err := firstEncryptionPass(
			path, plaintextSize, key, artifactID, chunkCount,
		)
		if err != nil {
			return sendResult{}, err
		}
		if verifiedPlaintext != plaintextDigest || ciphertextDigest != record.Reserve.CiphertextSHA256 {
			return sendResult{}, fmt.Errorf("artifact file or durable content key changed")
		}
	}

	var reserved protocol.ReserveArtifactResult
	if err := client.call(ctx, hubapi.MethodReserveArtifact, record.Reserve, &reserved); err != nil {
		return sendResult{}, err
	}
	if reserved.ArtifactID != artifactID || reserved.NextChunk > chunkCount {
		return sendResult{}, fmt.Errorf("hub returned an invalid artifact reservation")
	}
	result := sendResult{
		ArtifactID: artifactID, State: reserved.State, ChunkCount: chunkCount,
		ResumedFrom: reserved.NextChunk,
	}
	// Upload, completion, and key deletion form one ordered resume transition.
	needsUpload, err := artifactNeedsUpload(reserved.State, keyFound)
	if err != nil {
		return sendResult{}, err
	}
	if needsUpload {
		state, chunksSent, err := uploadArtifact(ctx, client, path, plaintextSize, key,
			artifactID, chunkCount, reserved.NextChunk)
		if err != nil {
			return sendResult{}, err
		}
		result.State = state
		result.ChunksSent = chunksSent
		if _, err := artifactNeedsUpload(result.State, true); err != nil {
			return sendResult{}, err
		} else if !artifactUploadComplete(result.State) {
			return sendResult{}, fmt.Errorf("hub left artifact in incomplete state %s", result.State)
		}
	}
	remote, err := outgoingArtifactIsRemote(client.dirs.Data, artifactID)
	if err != nil {
		return sendResult{}, err
	}
	if artifactRemoteState(result.State) {
		if err := markOutgoingArtifactRemote(client.dirs.Data, artifactID); err != nil {
			return sendResult{}, err
		}
		remote = true
	}
	// A process may have died after complete committed but before removing the
	// durable resume key. An idempotent retry finishes that transition without
	// uploading ciphertext again.
	if err := finalizeContentKey(client.dirs.Data, artifactID, result.State, keyFound); err != nil {
		return sendResult{}, err
	}
	return finishArtifactSend(ctx, client, client.agent, recipient, plaintextSize,
		result, remote, artifactSendOptions{
			remoteWait: remoteArtifactWaitTimeout,
			pollEvery:  remoteArtifactPollEvery,
		})
}

func uploadArtifact(
	ctx context.Context,
	client artifactCallClient,
	path string,
	plaintextSize int64,
	key []byte,
	id protocol.OpaqueID,
	chunkCount, nextChunk int64,
) (protocol.ArtifactState, int64, error) {
	var chunksSent int64
	upload := func(index int64, ciphertext []byte) error {
		if err := uploadArtifactChunk(ctx, client, id, index, ciphertext); err != nil {
			return err
		}
		chunksSent++
		return nil
	}
	if err := encryptionPass(
		path, plaintextSize, key, id, chunkCount, nextChunk, upload,
	); err != nil {
		return "", 0, err
	}
	var completed protocol.ArtifactMutationResult
	if err := client.call(ctx, hubapi.MethodCompleteArtifactUpload,
		protocol.ArtifactIDParams{ArtifactID: id}, &completed); err != nil {
		return "", 0, err
	}
	if completed.ArtifactID != id {
		return "", 0, fmt.Errorf("hub returned an invalid completed artifact")
	}
	return completed.TransferState, chunksSent, nil
}

func uploadArtifactChunk(
	ctx context.Context,
	client artifactCallClient,
	id protocol.OpaqueID,
	index int64,
	ciphertext []byte,
) error {
	var uploaded protocol.UploadArtifactChunkResult
	if err := client.call(ctx, hubapi.MethodUploadArtifactChunk,
		protocol.UploadArtifactChunkParams{
			ArtifactID: id, ChunkIndex: index, Ciphertext: ciphertext,
			SHA256: artifactformat.Digest(ciphertext),
		}, &uploaded); err != nil {
		return err
	}
	if uploaded.ArtifactID != id || uploaded.ChunkIndex != index {
		return fmt.Errorf("hub returned an invalid chunk acknowledgement")
	}
	return nil
}

func artifactUploadComplete(state protocol.ArtifactState) bool {
	switch state {
	case protocol.ArtifactStateLocalAvailable, protocol.ArtifactStateQueuedRemote,
		protocol.ArtifactStateReplicating, protocol.ArtifactStateStoredRemote,
		protocol.ArtifactStateAcknowledged:
		return true
	default:
		return false
	}
}

func artifactRemoteState(state protocol.ArtifactState) bool {
	switch state {
	case protocol.ArtifactStateQueuedRemote, protocol.ArtifactStateReplicating,
		protocol.ArtifactStateStoredRemote:
		return true
	default:
		return false
	}
}

func artifactTerminalState(state protocol.ArtifactState) bool {
	switch state {
	case protocol.ArtifactStateFailedPermanent, protocol.ArtifactStateExpired,
		protocol.ArtifactStateCancelled, protocol.ArtifactStateRevoked:
		return true
	default:
		return false
	}
}

func artifactNeedsUpload(state protocol.ArtifactState, keyFound bool) (bool, error) {
	if artifactTerminalState(state) {
		return false, fmt.Errorf("artifact reached terminal state %s", state)
	}
	if artifactUploadComplete(state) {
		return false, nil
	}
	if !keyFound {
		return false, fmt.Errorf("cannot resume artifact: durable content key is missing")
	}
	return true, nil
}

func finishArtifactSend(
	ctx context.Context,
	client artifactSendClient,
	sender, recipient protocol.AgentID,
	plaintextSize int64,
	result sendResult,
	remote bool,
	options artifactSendOptions,
) (sendResult, error) {
	remote = remote || artifactRemoteState(result.State)
	if remote {
		state, err := waitForRemoteArtifact(ctx, client, result.ArtifactID, result.State, options)
		if err != nil {
			return sendResult{}, err
		}
		result.State = state
	}
	if artifactTerminalState(result.State) {
		return sendResult{}, fmt.Errorf("artifact reached terminal state %s", result.State)
	}
	if result.State != protocol.ArtifactStateLocalAvailable &&
		result.State != protocol.ArtifactStateStoredRemote &&
		result.State != protocol.ArtifactStateAcknowledged {
		return sendResult{}, fmt.Errorf("artifact is not ready for notification in state %s", result.State)
	}
	body := fmt.Sprintf(
		"Encrypted file %s from %s, %d bytes. To receive it, run: gifford artifact receive %s",
		result.ArtifactID, sender, plaintextSize, result.ArtifactID,
	)
	noticeErr := client.notify(ctx, endpoint.NotificationRequest{
		To: recipient, Subject: "FILE_READY", Body: body,
		ClientMsgID: protocol.OpaqueID("notice-" + string(result.ArtifactID)),
	})
	if noticeErr != nil && remote {
		return sendResult{}, fmt.Errorf("send remote FILE_READY notification: %w", noticeErr)
	}
	result.NoticeSent = noticeErr == nil
	return result, nil
}

func waitForRemoteArtifact(
	ctx context.Context,
	client artifactSendClient,
	id protocol.OpaqueID,
	initial protocol.ArtifactState,
	options artifactSendOptions,
) (protocol.ArtifactState, error) {
	if options.remoteWait <= 0 || options.pollEvery <= 0 {
		return "", fmt.Errorf("wait for remote artifact: invalid timing")
	}
	timeout := time.NewTimer(options.remoteWait)
	defer timeout.Stop()
	ticker := time.NewTicker(options.pollEvery)
	defer ticker.Stop()
	state := initial
	for {
		switch state {
		case protocol.ArtifactStateStoredRemote, protocol.ArtifactStateAcknowledged:
			return state, nil
		case protocol.ArtifactStateQueuedRemote, protocol.ArtifactStateReplicating:
		case protocol.ArtifactStateFailedPermanent, protocol.ArtifactStateExpired,
			protocol.ArtifactStateCancelled, protocol.ArtifactStateRevoked:
			return "", fmt.Errorf("artifact reached terminal state %s", state)
		default:
			return "", fmt.Errorf("artifact entered unexpected remote state %s", state)
		}
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("wait for remote artifact: %w", ctx.Err())
		case <-timeout.C:
			return "", fmt.Errorf(
				"artifact %s is stored at the origin and remains in replication (state %s); rerun the same command to finish the FILE_READY notification",
				id, state,
			)
		case <-ticker.C:
			var status protocol.ArtifactStatusResult
			if err := client.call(ctx, hubapi.MethodGetArtifactStatus,
				protocol.ArtifactIDParams{ArtifactID: id}, &status); err != nil {
				return "", err
			}
			if status.ArtifactID != id {
				return "", fmt.Errorf("hub returned status for a different artifact")
			}
			state = status.State
		}
	}
}

func finalizeContentKey(
	dataDir string,
	id protocol.OpaqueID,
	state protocol.ArtifactState,
	keyFound bool,
) error {
	if !keyFound || !artifactUploadComplete(state) {
		return nil
	}
	return removeContentKey(dataDir, id)
}

func scanPlaintext(path string) (int64, protocol.SHA256Hex, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, "", fmt.Errorf("open artifact file: %w", err)
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return 0, "", fmt.Errorf("artifact source must be a regular file")
	}
	hash := sha256.New()
	read, err := io.Copy(hash, file)
	if err != nil {
		return 0, "", fmt.Errorf("hash artifact file: %w", err)
	}
	if read != info.Size() {
		return 0, "", fmt.Errorf("artifact file changed while hashing")
	}
	return read, protocol.SHA256Hex(hex.EncodeToString(hash.Sum(nil))), nil
}

func firstEncryptionPass(
	path string,
	plaintextSize int64,
	key []byte,
	id protocol.OpaqueID,
	chunkCount int64,
) (protocol.SHA256Hex, protocol.SHA256Hex, error) {
	ciphertextHash := sha256.New()
	plaintextHash := sha256.New()
	err := encryptionPass(path, plaintextSize, key, id, chunkCount, 0,
		func(_ int64, ciphertext []byte) error {
			_, _ = ciphertextHash.Write(ciphertext)
			return nil
		}, plaintextHash)
	return protocol.SHA256Hex(hex.EncodeToString(ciphertextHash.Sum(nil))),
		protocol.SHA256Hex(hex.EncodeToString(plaintextHash.Sum(nil))), err
}

func encryptionPass(
	path string,
	plaintextSize int64,
	key []byte,
	id protocol.OpaqueID,
	chunkCount, start int64,
	callback func(int64, []byte) error,
	extraHash ...io.Writer,
) error {
	if start < 0 || start > chunkCount {
		return fmt.Errorf("invalid artifact resume point")
	}
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open artifact file: %w", err)
	}
	defer func() { _ = file.Close() }()
	if start > 0 {
		if _, err := file.Seek(start*protocol.ArtifactChunkSize, io.SeekStart); err != nil {
			return fmt.Errorf("seek artifact file: %w", err)
		}
	}
	for index := start; index < chunkCount; index++ {
		plainSize := protocol.ArtifactChunkSize
		if index == chunkCount-1 {
			plainSize = plaintextSize - (chunkCount-1)*protocol.ArtifactChunkSize
		}
		plaintext := make([]byte, int(plainSize))
		if _, err := io.ReadFull(file, plaintext); err != nil {
			return fmt.Errorf("read artifact chunk: %w", err)
		}
		for _, hash := range extraHash {
			_, _ = hash.Write(plaintext)
		}
		ciphertext, err := artifactformat.EncryptChunk(key, id, index, chunkCount, plaintext)
		if err != nil {
			return fmt.Errorf("encrypt artifact chunk: %w", err)
		}
		if err := callback(index, ciphertext); err != nil {
			return err
		}
	}
	if start == 0 {
		var extra [1]byte
		if n, err := file.Read(extra[:]); n != 0 || err != io.EOF {
			return fmt.Errorf("artifact file changed during encryption")
		}
	}
	return nil
}

func mediaType(path string) string {
	value := mime.TypeByExtension(filepath.Ext(path))
	if value == "" {
		return "application/octet-stream"
	}
	return value
}
