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

type sendResult struct {
	ArtifactID  protocol.OpaqueID      `json:"artifact_id"`
	State       protocol.ArtifactState `json:"state"`
	ChunkCount  int64                  `json:"chunk_count"`
	ChunksSent  int64                  `json:"chunks_sent"`
	ResumedFrom int64                  `json:"resumed_from"`
	NoticeSent  bool                   `json:"notice_sent"`
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
	if !artifactUploadComplete(reserved.State) {
		if !keyFound {
			return sendResult{}, fmt.Errorf("cannot resume artifact: durable content key is missing")
		}
		if err := encryptionPass(path, plaintextSize, key, artifactID, chunkCount,
			reserved.NextChunk, func(index int64, ciphertext []byte) error {
				var uploaded protocol.UploadArtifactChunkResult
				err := client.call(ctx, hubapi.MethodUploadArtifactChunk,
					protocol.UploadArtifactChunkParams{
						ArtifactID: artifactID, ChunkIndex: index, Ciphertext: ciphertext,
						SHA256: artifactformat.Digest(ciphertext),
					}, &uploaded)
				if err != nil {
					return err
				}
				if uploaded.ArtifactID != artifactID || uploaded.ChunkIndex != index {
					return fmt.Errorf("hub returned an invalid chunk acknowledgement")
				}
				result.ChunksSent++
				return nil
			}); err != nil {
			return sendResult{}, err
		}
		var completed protocol.ArtifactMutationResult
		if err := client.call(ctx, hubapi.MethodCompleteArtifactUpload,
			protocol.ArtifactIDParams{ArtifactID: artifactID}, &completed); err != nil {
			return sendResult{}, err
		}
		if completed.ArtifactID != artifactID ||
			completed.TransferState != protocol.ArtifactStateLocalAvailable {
			return sendResult{}, fmt.Errorf("hub did not publish the artifact")
		}
		result.State = completed.TransferState
	}
	// A process may have died after complete committed but before removing the
	// durable resume key. An idempotent retry must finish that local transition.
	if err := finalizeContentKey(client.dirs.Data, artifactID, result.State, keyFound); err != nil {
		return sendResult{}, err
	}

	if result.State == protocol.ArtifactStateLocalAvailable ||
		result.State == protocol.ArtifactStateAcknowledged {
		body := fmt.Sprintf(
			"Encrypted file %s from %s, %d bytes. To receive it, run: gifford artifact receive %s",
			artifactID, client.agent, plaintextSize, artifactID,
		)
		_, noticeErr := endpoint.SendNotification(ctx, client.hub, client.dirs.Data,
			endpoint.NotificationRequest{
				To: recipient, Subject: "FILE_READY", Body: body,
				ClientMsgID: protocol.OpaqueID("notice-" + string(artifactID)),
			})
		result.NoticeSent = noticeErr == nil
	}
	return result, nil
}

func artifactUploadComplete(state protocol.ArtifactState) bool {
	switch state {
	case protocol.ArtifactStateLocalAvailable, protocol.ArtifactStateAcknowledged:
		return true
	default:
		return false
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
