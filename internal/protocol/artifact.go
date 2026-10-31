package protocol

import (
	"fmt"
	"time"
)

// ArtifactChunkSize is the fixed plaintext chunk size used by H24.
const ArtifactChunkSize int64 = 1 << 20

// ArtifactEncryptionProfile is the only local artifact profile supported by H24.
const ArtifactEncryptionProfile = "gifford-artifact-e2e-v1"

// ArtifactRejectVerifyFailed is the fixed permanent verification failure reason.
const ArtifactRejectVerifyFailed ReasonCode = "VERIFY_FAILED"

const (
	pathChunkIndex     = "chunk_index"
	pathCiphertext     = "ciphertext"
	reasonInvalidSize  = "invalid size"
	reasonInvalidValue = "invalid value"
	reasonNonNegative  = "must be non-negative"
)

// ArtifactRecipientKeyParams is the key-discovery form of reserve_artifact.
// It stores nothing and exposes public material only to a bound agent.
type ArtifactRecipientKeyParams struct {
	KeyOnly   bool    `json:"key_only"`
	Recipient AgentID `json:"recipient"`
}

// Validate checks a recipient-key discovery request.
func (p ArtifactRecipientKeyParams) Validate() error {
	if err := checkRoot(&p); err != nil {
		return err
	}
	if !p.KeyOnly {
		return &ValidationError{Path: "key_only", Reason: "must be true"}
	}
	return nil
}

// ArtifactRecipientKeyResult is active local X25519 public material.
type ArtifactRecipientKeyResult struct {
	KeyID     EncryptionKeyID `json:"key_id"`
	PublicKey []byte          `json:"public_key"`
}

// Validate checks active recipient public material.
func (r ArtifactRecipientKeyResult) Validate() error {
	if err := checkRoot(&r); err != nil {
		return err
	}
	if len(r.PublicKey) != 32 {
		return &ValidationError{Path: "public_key", Reason: "must be 32 bytes"}
	}
	return nil
}

// ReserveArtifactParams is the encrypted metadata supplied by an authenticated
// sender. ArtifactID is derived from the authenticated sender and ClientMsgID.
type ReserveArtifactParams struct {
	ClientMsgID       OpaqueID        `json:"client_msg_id"`
	Recipient         AgentID         `json:"recipient"`
	EncryptionProfile string          `json:"encryption_profile"`
	CiphertextSize    int64           `json:"ciphertext_size"`
	ChunkCount        int64           `json:"chunk_count"`
	CiphertextSHA256  SHA256Hex       `json:"ciphertext_sha256"`
	EncryptedManifest []byte          `json:"encrypted_manifest"`
	RecipientKeyID    EncryptionKeyID `json:"recipient_key_id"`
	KeyEnvelope       []byte          `json:"key_envelope"`
	SenderKeyID       SigningKeyID    `json:"sender_key_id"`
	Signature         []byte          `json:"signature"`
	ReservedAt        time.Time       `json:"reserved_at"`
	ExpiresAt         time.Time       `json:"expires_at"`
}

// Validate checks the reservation wire shape without trusting its sender.
func (p ReserveArtifactParams) Validate() error {
	if err := checkRoot(&p); err != nil {
		return err
	}
	if p.EncryptionProfile != ArtifactEncryptionProfile {
		return &ValidationError{Path: "encryption_profile", Reason: "unsupported profile"}
	}
	if p.ChunkCount < 1 || p.ChunkCount > 1<<31 {
		return &ValidationError{Path: "chunk_count", Reason: "out of range"}
	}
	minimum := p.ChunkCount * 16
	maximum := p.ChunkCount * (ArtifactChunkSize + 16)
	if p.CiphertextSize < minimum || p.CiphertextSize > maximum {
		return &ValidationError{Path: "ciphertext_size", Reason: "inconsistent with chunk_count"}
	}
	if p.ChunkCount > 1 && p.CiphertextSize <= (p.ChunkCount-1)*(ArtifactChunkSize+16)+16 {
		return &ValidationError{Path: "ciphertext_size", Reason: "last chunk is empty"}
	}
	if len(p.EncryptedManifest) < 24+16 || len(p.EncryptedManifest) > 1<<20 {
		return &ValidationError{Path: "encrypted_manifest", Reason: reasonInvalidSize}
	}
	if len(p.KeyEnvelope) < 1 || len(p.KeyEnvelope) > 1<<20 {
		return &ValidationError{Path: "key_envelope", Reason: reasonInvalidSize}
	}
	if len(p.Signature) != 64 {
		return &ValidationError{Path: "signature", Reason: "must be 64 bytes"}
	}
	if p.ReservedAt.IsZero() || !p.ExpiresAt.Equal(p.ReservedAt.Add(24*time.Hour)) {
		return &ValidationError{Path: pathExpiresAt, Reason: "must be reserved_at plus 24h"}
	}
	return nil
}

// ArtifactAdmissionResult records the channel decision applied at reserve.
type ArtifactAdmissionResult struct {
	DecisionID     ChannelDecisionID `json:"decision_id"`
	Outcome        DecisionOutcome   `json:"outcome"`
	ControlReason  DecisionReason    `json:"control_reason"`
	ControlVersion int64             `json:"control_version"`
}

// Validate checks the artifact gate result.
func (r ArtifactAdmissionResult) Validate() error {
	if err := checkRoot(&r); err != nil {
		return err
	}
	if r.ControlVersion < 0 {
		return &ValidationError{Path: "control_version", Reason: reasonNonNegative}
	}
	return nil
}

// ReserveArtifactResult identifies the idempotent transfer and resume point.
type ReserveArtifactResult struct {
	ArtifactID OpaqueID                `json:"artifact_id"`
	State      ArtifactState           `json:"state"`
	ChunkSize  int64                   `json:"chunk_size"`
	NextChunk  int64                   `json:"next_chunk"`
	Admission  ArtifactAdmissionResult `json:"admission"`
}

// Validate checks the reservation result.
func (r ReserveArtifactResult) Validate() error {
	if err := checkRoot(&r); err != nil {
		return err
	}
	if r.ChunkSize != ArtifactChunkSize || r.NextChunk < 0 {
		return &ValidationError{Path: "next_chunk", Reason: "invalid resume point"}
	}
	return r.Admission.Validate()
}

// UploadArtifactChunkParams carries one authenticated ciphertext chunk.
type UploadArtifactChunkParams struct {
	ArtifactID OpaqueID  `json:"artifact_id"`
	ChunkIndex int64     `json:"chunk_index"`
	Ciphertext []byte    `json:"ciphertext"`
	SHA256     SHA256Hex `json:"sha256"`
}

// Validate checks an upload chunk before storage.
func (p UploadArtifactChunkParams) Validate() error {
	if !p.ArtifactID.Valid() {
		return &ValidationError{Path: "artifact_id", Reason: reasonInvalidValue}
	}
	if p.ChunkIndex < 0 {
		return &ValidationError{Path: pathChunkIndex, Reason: reasonNonNegative}
	}
	if len(p.Ciphertext) < 16 || len(p.Ciphertext) > int(ArtifactChunkSize+16) {
		return &ValidationError{Path: pathCiphertext, Reason: "invalid chunk size"}
	}
	if !p.SHA256.Valid() {
		return &ValidationError{Path: "sha256", Reason: reasonInvalidValue}
	}
	return nil
}

// UploadArtifactChunkResult reports whether the chunk advanced the transfer.
type UploadArtifactChunkResult struct {
	ArtifactID OpaqueID      `json:"artifact_id"`
	State      ArtifactState `json:"state"`
	ChunkIndex int64         `json:"chunk_index"`
	NextChunk  int64         `json:"next_chunk"`
	Stored     bool          `json:"stored"`
}

// Validate checks the upload result.
func (r UploadArtifactChunkResult) Validate() error {
	if err := checkRoot(&r); err != nil {
		return err
	}
	if r.ChunkIndex < 0 || r.NextChunk < 0 {
		return &ValidationError{Path: pathChunkIndex, Reason: reasonNonNegative}
	}
	return nil
}

// ArtifactIDParams addresses one artifact.
type ArtifactIDParams struct {
	ArtifactID OpaqueID `json:"artifact_id"`
}

// Validate checks the artifact id.
func (p ArtifactIDParams) Validate() error { return checkRoot(&p) }

// ArtifactRecipientStatus is the encrypted recipient projection of a transfer.
type ArtifactRecipientStatus struct {
	Agent          AgentID         `json:"agent"`
	KeyID          EncryptionKeyID `json:"key_id"`
	KeyEnvelope    []byte          `json:"key_envelope"`
	State          ArtifactState   `json:"state"`
	AcknowledgedAt *time.Time      `json:"acknowledged_at,omitempty"`
}

// Validate checks one recipient projection.
func (r ArtifactRecipientStatus) Validate() error {
	if err := checkRoot(&r); err != nil {
		return err
	}
	if len(r.KeyEnvelope) < 1 || len(r.KeyEnvelope) > 1<<20 {
		return &ValidationError{Path: "key_envelope", Reason: reasonInvalidSize}
	}
	if (r.AcknowledgedAt != nil) != (r.State == ArtifactStateAcknowledged) {
		return &ValidationError{Path: "acknowledged_at", Reason: "present iff acknowledged"}
	}
	return nil
}

// ArtifactStatusResult is the ciphertext-only transfer projection. The active
// sender public key lets a recipient verify before decrypting anything.
type ArtifactStatusResult struct {
	ArtifactID        OpaqueID                  `json:"artifact_id"`
	ClientMsgID       OpaqueID                  `json:"client_msg_id"`
	Sender            AgentID                   `json:"sender"`
	State             ArtifactState             `json:"state"`
	EncryptionProfile string                    `json:"encryption_profile"`
	CiphertextSize    int64                     `json:"ciphertext_size"`
	ChunkCount        int64                     `json:"chunk_count"`
	CiphertextSHA256  SHA256Hex                 `json:"ciphertext_sha256"`
	EncryptedManifest []byte                    `json:"encrypted_manifest"`
	SenderKeyID       SigningKeyID              `json:"sender_key_id"`
	SenderPublicKey   []byte                    `json:"sender_public_key"`
	Signature         []byte                    `json:"signature"`
	ExpiresAt         time.Time                 `json:"expires_at"`
	NextChunk         int64                     `json:"next_chunk"`
	Recipients        []ArtifactRecipientStatus `json:"recipients"`
}

// Validate checks a status projection.
func (r ArtifactStatusResult) Validate() error {
	if err := checkRoot(&r); err != nil {
		return err
	}
	if r.EncryptionProfile != ArtifactEncryptionProfile || r.CiphertextSize < 16 ||
		r.ChunkCount < 1 || r.NextChunk < 0 || r.NextChunk > r.ChunkCount || r.ExpiresAt.IsZero() {
		return &ValidationError{Path: string(PlaneArtifact), Reason: "invalid transfer metadata"}
	}
	if len(r.EncryptedManifest) < 40 || len(r.SenderPublicKey) != 32 || len(r.Signature) != 64 || len(r.Recipients) != 1 {
		return &ValidationError{Path: string(PlaneArtifact), Reason: "invalid cryptographic metadata"}
	}
	return r.Recipients[0].Validate()
}

// ListPendingArtifactsParams is intentionally empty.
type ListPendingArtifactsParams struct{}

// Validate checks the empty params object.
func (p ListPendingArtifactsParams) Validate() error { return checkRoot(&p) }

// ArtifactSummary is the ciphertext-only list projection for a recipient.
type ArtifactSummary struct {
	ArtifactID     OpaqueID      `json:"artifact_id"`
	Sender         AgentID       `json:"sender"`
	State          ArtifactState `json:"state"`
	CiphertextSize int64         `json:"ciphertext_size"`
	ChunkCount     int64         `json:"chunk_count"`
	ExpiresAt      time.Time     `json:"expires_at"`
}

// Validate checks a pending summary.
func (s ArtifactSummary) Validate() error {
	if err := checkRoot(&s); err != nil {
		return err
	}
	if s.CiphertextSize < 16 || s.ChunkCount < 1 || s.ExpiresAt.IsZero() {
		return &ValidationError{Path: string(PlaneArtifact), Reason: "invalid summary"}
	}
	return nil
}

// ListPendingArtifactsResult lists locally available artifacts for the caller.
type ListPendingArtifactsResult struct {
	Artifacts []ArtifactSummary `json:"artifacts"`
}

// Validate checks every summary.
func (r ListPendingArtifactsResult) Validate() error {
	if err := checkRoot(&r); err != nil {
		return err
	}
	for i := range r.Artifacts {
		if err := r.Artifacts[i].Validate(); err != nil {
			return fmt.Errorf("artifacts[%d]: %w", i, err)
		}
	}
	return nil
}

// DownloadArtifactChunkParams addresses one ciphertext chunk.
type DownloadArtifactChunkParams struct {
	ArtifactID OpaqueID `json:"artifact_id"`
	ChunkIndex int64    `json:"chunk_index"`
}

// Validate checks the requested index.
func (p DownloadArtifactChunkParams) Validate() error {
	if err := checkRoot(&p); err != nil {
		return err
	}
	if p.ChunkIndex < 0 {
		return &ValidationError{Path: pathChunkIndex, Reason: reasonNonNegative}
	}
	return nil
}

// DownloadArtifactChunkResult carries one stored ciphertext chunk.
type DownloadArtifactChunkResult struct {
	ArtifactID OpaqueID  `json:"artifact_id"`
	ChunkIndex int64     `json:"chunk_index"`
	Ciphertext []byte    `json:"ciphertext"`
	SHA256     SHA256Hex `json:"sha256"`
}

// Validate checks the downloaded chunk.
func (r DownloadArtifactChunkResult) Validate() error {
	if !r.ArtifactID.Valid() {
		return &ValidationError{Path: "artifact_id", Reason: reasonInvalidValue}
	}
	if r.ChunkIndex < 0 || len(r.Ciphertext) < 16 || len(r.Ciphertext) > int(ArtifactChunkSize+16) {
		return &ValidationError{Path: pathCiphertext, Reason: "invalid chunk"}
	}
	if !r.SHA256.Valid() {
		return &ValidationError{Path: "sha256", Reason: reasonInvalidValue}
	}
	return nil
}

// RejectArtifactParams marks verification failure for the bound recipient.
type RejectArtifactParams struct {
	ArtifactID OpaqueID   `json:"artifact_id"`
	Reason     ReasonCode `json:"reason"`
}

// Validate checks the fixed H24 rejection reason.
func (p RejectArtifactParams) Validate() error {
	if err := checkRoot(&p); err != nil {
		return err
	}
	if p.Reason != ArtifactRejectVerifyFailed {
		return &ValidationError{Path: "reason", Reason: "must be VERIFY_FAILED"}
	}
	return nil
}

// ArtifactMutationResult reports the resulting transfer and recipient states.
type ArtifactMutationResult struct {
	ArtifactID     OpaqueID      `json:"artifact_id"`
	TransferState  ArtifactState `json:"transfer_state"`
	RecipientState ArtifactState `json:"recipient_state"`
}

// Validate checks a mutation response.
func (r ArtifactMutationResult) Validate() error { return checkRoot(&r) }
