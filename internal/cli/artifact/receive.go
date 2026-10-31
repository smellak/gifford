package artifact

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	artifactformat "github.com/smellak/gifford/internal/artifact"
	"github.com/smellak/gifford/internal/endpoint"
	hubapi "github.com/smellak/gifford/internal/hub"
	"github.com/smellak/gifford/internal/protocol"
)

type receiveResult struct {
	Path   string             `json:"path"`
	Size   int64              `json:"size"`
	SHA256 protocol.SHA256Hex `json:"sha256"`
}

func receiveArtifact(
	ctx context.Context,
	id protocol.OpaqueID,
	outputDirectory string,
) (receiveResult, error) {
	if !id.Valid() {
		return receiveResult{}, fmt.Errorf("invalid artifact id")
	}
	client, err := openArtifactClient(ctx)
	if err != nil {
		return receiveResult{}, err
	}
	defer client.close()
	var status protocol.ArtifactStatusResult
	if err := client.call(ctx, hubapi.MethodGetArtifactStatus,
		protocol.ArtifactIDParams{ArtifactID: id}, &status); err != nil {
		return receiveResult{}, err
	}
	if status.ArtifactID != id || len(status.Recipients) != 1 ||
		status.Recipients[0].Agent != client.agent {
		return receiveResult{}, fmt.Errorf("artifact is not addressed to this agent")
	}
	result, err := receiveVerified(ctx, client, status, outputDirectory)
	if err == nil {
		var acknowledged protocol.ArtifactMutationResult
		if err := client.call(ctx, hubapi.MethodAckArtifact,
			protocol.ArtifactIDParams{ArtifactID: id}, &acknowledged); err != nil {
			return receiveResult{}, err
		}
		if acknowledged.ArtifactID != id ||
			acknowledged.RecipientState != protocol.ArtifactStateAcknowledged {
			return receiveResult{}, fmt.Errorf("hub did not acknowledge artifact")
		}
		return result, nil
	}
	if errors.Is(err, artifactformat.ErrVerification) {
		var rejected protocol.ArtifactMutationResult
		_ = client.call(ctx, hubapi.MethodRejectArtifact, protocol.RejectArtifactParams{
			ArtifactID: id, Reason: protocol.ArtifactRejectVerifyFailed,
		}, &rejected)
		return receiveResult{}, artifactformat.ErrVerification
	}
	return receiveResult{}, err
}

func receiveVerified(
	ctx context.Context,
	client *artifactClient,
	status protocol.ArtifactStatusResult,
	outputDirectory string,
) (receiveResult, error) {
	recipient := status.Recipients[0]
	metadata := artifactformat.EnvelopeMetadata{
		ArtifactID: status.ArtifactID, ClientMsgID: status.ClientMsgID,
		Sender: status.Sender, Recipient: recipient.Agent,
		CiphertextSize: status.CiphertextSize, ChunkCount: status.ChunkCount,
		CiphertextSHA256:  status.CiphertextSHA256,
		EncryptedManifest: status.EncryptedManifest, KeyEnvelope: recipient.KeyEnvelope,
		EncryptionProfile: status.EncryptionProfile, ExpiresAt: status.ExpiresAt,
	}
	if err := artifactformat.VerifyEnvelope(
		ed25519.PublicKey(status.SenderPublicKey), metadata, status.Signature,
	); err != nil {
		return receiveResult{}, artifactformat.ErrVerification
	}
	private, err := endpoint.LoadAgentEncryptionPrivate(client.dirs.Data, client.agent)
	if err != nil || recipient.KeyID != protocol.EncryptionKeyID("x25519:"+string(client.agent)+":v1") {
		return receiveResult{}, artifactformat.ErrVerification
	}
	key, err := artifactformat.UnwrapContentKey(
		private, status.ArtifactID, client.agent, recipient.KeyEnvelope,
	)
	if err != nil {
		return receiveResult{}, artifactformat.ErrVerification
	}
	manifest, err := artifactformat.DecryptManifest(key, status.ArtifactID, status.EncryptedManifest)
	if err != nil {
		return receiveResult{}, artifactformat.ErrVerification
	}
	directory, err := filepath.Abs(outputDirectory)
	if err != nil {
		return receiveResult{}, fmt.Errorf("resolve artifact output directory: %w", err)
	}
	info, err := os.Stat(directory)
	if err != nil || !info.IsDir() {
		return receiveResult{}, fmt.Errorf("artifact output is not a directory")
	}
	temporaryPath := filepath.Join(directory, ".gifford-"+string(status.ArtifactID)+".part")
	if err := removeStaleTemporary(temporaryPath); err != nil {
		return receiveResult{}, err
	}
	temporary, err := os.OpenFile(temporaryPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return receiveResult{}, fmt.Errorf("create artifact temporary: %w", err)
	}
	removeTemporary := true
	defer func() {
		_ = temporary.Close()
		if removeTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()
	plaintextHash := sha256.New()
	ciphertextHash := sha256.New()
	var plaintextSize, ciphertextSize int64
	for index := range status.ChunkCount {
		var downloaded protocol.DownloadArtifactChunkResult
		if err := client.call(ctx, hubapi.MethodDownloadArtifactChunk,
			protocol.DownloadArtifactChunkParams{
				ArtifactID: status.ArtifactID, ChunkIndex: index,
			}, &downloaded); err != nil {
			return receiveResult{}, err
		}
		if downloaded.ArtifactID != status.ArtifactID || downloaded.ChunkIndex != index ||
			artifactformat.Digest(downloaded.Ciphertext) != downloaded.SHA256 {
			return receiveResult{}, artifactformat.ErrVerification
		}
		wantSize, err := artifactformat.ExpectedChunkSize(
			status.CiphertextSize, status.ChunkCount, index,
		)
		if err != nil || int64(len(downloaded.Ciphertext)) != wantSize {
			return receiveResult{}, artifactformat.ErrVerification
		}
		_, _ = ciphertextHash.Write(downloaded.Ciphertext)
		ciphertextSize += int64(len(downloaded.Ciphertext))
		plaintext, err := artifactformat.DecryptChunk(
			key, status.ArtifactID, index, status.ChunkCount, downloaded.Ciphertext,
		)
		if err != nil {
			return receiveResult{}, artifactformat.ErrVerification
		}
		if err := writeFull(temporary, plaintext); err != nil {
			return receiveResult{}, err
		}
		_, _ = plaintextHash.Write(plaintext)
		plaintextSize += int64(len(plaintext))
	}
	if ciphertextSize != status.CiphertextSize ||
		protocol.SHA256Hex(hex.EncodeToString(ciphertextHash.Sum(nil))) != status.CiphertextSHA256 ||
		plaintextSize != manifest.PlaintextSize ||
		protocol.SHA256Hex(hex.EncodeToString(plaintextHash.Sum(nil))) != manifest.PlaintextSHA256 {
		return receiveResult{}, artifactformat.ErrVerification
	}
	if err := temporary.Sync(); err != nil {
		return receiveResult{}, fmt.Errorf("sync artifact plaintext: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return receiveResult{}, fmt.Errorf("close artifact plaintext: %w", err)
	}
	name := artifactformat.SanitizeFilename(manifest.Filename, status.ArtifactID)
	finalPath, err := linkWithoutOverwrite(temporaryPath, directory, name)
	if err != nil {
		return receiveResult{}, err
	}
	if err := os.Remove(temporaryPath); err != nil {
		return receiveResult{}, fmt.Errorf("remove artifact temporary: %w", err)
	}
	removeTemporary = false
	if err := syncDirectory(directory); err != nil {
		return receiveResult{}, err
	}
	return receiveResult{Path: finalPath, Size: plaintextSize, SHA256: manifest.PlaintextSHA256}, nil
}

func removeStaleTemporary(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect artifact temporary: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("artifact temporary path is not a regular file")
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("remove stale artifact temporary: %w", err)
	}
	return nil
}

func linkWithoutOverwrite(temporaryPath, directory, name string) (string, error) {
	for suffix := range 100_000 {
		candidateName := name
		if suffix > 0 {
			candidateName = fmt.Sprintf("%s (%d)", name, suffix)
		}
		candidate := filepath.Join(directory, candidateName)
		if err := os.Link(temporaryPath, candidate); err == nil {
			return candidate, nil
		} else if !errors.Is(err, fs.ErrExist) {
			return "", fmt.Errorf("publish artifact without overwrite: %w", err)
		}
	}
	return "", fmt.Errorf("no available artifact output name")
}

func writeFull(writer io.Writer, value []byte) error {
	for written := 0; written < len(value); {
		n, err := writer.Write(value[written:])
		if err != nil || n == 0 {
			if err == nil {
				err = io.ErrShortWrite
			}
			return fmt.Errorf("write artifact plaintext: %w", err)
		}
		written += n
	}
	return nil
}
