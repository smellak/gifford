package federation

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

// ArtifactReceiver owns the receiver side of the three-stage artifact
// replication protocol after mTLS has authenticated the calling node.
type ArtifactReceiver interface {
	ReceiveArtifactManifest(context.Context, protocol.NodeID, ArtifactManifestRequest) ([]byte, error)
	ReceiveArtifactChunk(context.Context, protocol.NodeID, ArtifactChunkRequest) ([]byte, error)
	CompleteArtifact(context.Context, protocol.NodeID, ArtifactCompleteRequest) ([]byte, error)
}

// ArtifactManifestRequest carries immutable ciphertext metadata and the
// origin's signed sender directory record. Content keys never cross this API.
type ArtifactManifestRequest struct {
	ArtifactID      protocol.OpaqueID              `json:"artifact_id"`
	OriginNode      protocol.NodeID                `json:"origin_node"`
	DestinationNode protocol.NodeID                `json:"destination_node"`
	Sender          protocol.AgentID               `json:"sender"`
	Reservation     protocol.ReserveArtifactParams `json:"reservation"`
	SenderRecord    registry.DirectoryRecord       `json:"sender_record"`
	OriginControl   protocol.ObjectControlState    `json:"origin_control"`
}

// Validate checks the manifest's bounded wire shape. Stateful ownership,
// directory signatures, envelope signatures, and routing are receiver checks.
func (r ArtifactManifestRequest) Validate() error {
	if !r.ArtifactID.Valid() || !r.OriginNode.Valid() || !r.DestinationNode.Valid() ||
		r.OriginNode == r.DestinationNode || !r.Sender.Valid() ||
		r.Reservation.Recipient == r.Sender {
		return fmt.Errorf("artifact manifest has invalid routing metadata")
	}
	if err := r.Reservation.Validate(); err != nil {
		return fmt.Errorf("artifact manifest reservation: %w", err)
	}
	if err := r.OriginControl.Validate(); err != nil {
		return fmt.Errorf("artifact manifest origin control: %w", err)
	}
	if r.OriginControl.ObjectType != protocol.DecisionObjectTypeArtifact ||
		r.OriginControl.ObjectID != r.ArtifactID ||
		r.OriginControl.ControlState != protocol.ControlStateAllowed ||
		!allowedControlReason(r.OriginControl.ControlReason) || r.OriginControl.Cancelled {
		return fmt.Errorf("artifact manifest has invalid origin control")
	}
	if r.SenderRecord.Agent != r.Sender || r.SenderRecord.Node != r.OriginNode ||
		r.SenderRecord.KeyID != string(r.Reservation.SenderKeyID) ||
		r.SenderRecord.Purpose != registry.KeyPurposeSigning ||
		r.SenderRecord.Algorithm != registry.KeyAlgorithmEd25519 ||
		r.SenderRecord.Status != activeDirectoryStatus || len(r.SenderRecord.PublicKey) != ed25519.PublicKeySize ||
		r.SenderRecord.ValidFrom == "" || r.SenderRecord.Issuer != "node:"+string(r.OriginNode) ||
		len(r.SenderRecord.RecordSignature) != ed25519.SignatureSize ||
		(r.SenderRecord.ValidUntil != nil && *r.SenderRecord.ValidUntil == "") {
		return fmt.Errorf("artifact manifest has invalid sender record")
	}
	return nil
}

// CanonicalBytes returns the exact JSON representation persisted and retried
// by the sender.
func (r ArtifactManifestRequest) CanonicalBytes() ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("encode artifact manifest: %w", err)
	}
	return raw, nil
}

// RequestSHA256 binds receipts and replay identity to the canonical request.
func (r ArtifactManifestRequest) RequestSHA256() (protocol.SHA256Hex, error) {
	raw, err := r.CanonicalBytes()
	if err != nil {
		return "", err
	}
	return artifactRequestDigest(raw), nil
}

// DecodeArtifactManifestRequest strictly decodes one canonical manifest.
func DecodeArtifactManifestRequest(raw []byte) (ArtifactManifestRequest, error) {
	var request ArtifactManifestRequest
	if err := decodeArtifactRequest(raw, &request); err != nil {
		return ArtifactManifestRequest{}, fmt.Errorf("decode artifact manifest: %w", err)
	}
	if err := request.Validate(); err != nil {
		return ArtifactManifestRequest{}, err
	}
	return request, nil
}

// ArtifactChunkRequest transfers exactly one ciphertext chunk.
type ArtifactChunkRequest struct {
	ArtifactID      protocol.OpaqueID  `json:"artifact_id"`
	OriginNode      protocol.NodeID    `json:"origin_node"`
	DestinationNode protocol.NodeID    `json:"destination_node"`
	ChunkIndex      int64              `json:"chunk_index"`
	Ciphertext      []byte             `json:"ciphertext"`
	SHA256          protocol.SHA256Hex `json:"sha256"`
}

// Validate checks a chunk frame before stateful cursor validation.
func (r ArtifactChunkRequest) Validate() error {
	if !r.ArtifactID.Valid() || !r.OriginNode.Valid() || !r.DestinationNode.Valid() ||
		r.OriginNode == r.DestinationNode || r.ChunkIndex < 0 || len(r.Ciphertext) < 16 ||
		len(r.Ciphertext) > int(protocol.ArtifactChunkSize+16) || !r.SHA256.Valid() {
		return fmt.Errorf("artifact chunk has invalid metadata")
	}
	return nil
}

// CanonicalBytes returns the exact JSON request representation.
func (r ArtifactChunkRequest) CanonicalBytes() ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("encode artifact chunk: %w", err)
	}
	return raw, nil
}

// RequestSHA256 binds a cursor receipt to this exact chunk.
func (r ArtifactChunkRequest) RequestSHA256() (protocol.SHA256Hex, error) {
	raw, err := r.CanonicalBytes()
	if err != nil {
		return "", err
	}
	return artifactRequestDigest(raw), nil
}

// DecodeArtifactChunkRequest strictly decodes one chunk request.
func DecodeArtifactChunkRequest(raw []byte) (ArtifactChunkRequest, error) {
	var request ArtifactChunkRequest
	if err := decodeArtifactRequest(raw, &request); err != nil {
		return ArtifactChunkRequest{}, fmt.Errorf("decode artifact chunk: %w", err)
	}
	if err := request.Validate(); err != nil {
		return ArtifactChunkRequest{}, err
	}
	return request, nil
}

// ArtifactCompleteRequest asks the receiver to validate and publish the
// already durable ciphertext.
type ArtifactCompleteRequest struct {
	ArtifactID      protocol.OpaqueID `json:"artifact_id"`
	OriginNode      protocol.NodeID   `json:"origin_node"`
	DestinationNode protocol.NodeID   `json:"destination_node"`
}

// Validate checks completion routing metadata.
func (r ArtifactCompleteRequest) Validate() error {
	if !r.ArtifactID.Valid() || !r.OriginNode.Valid() || !r.DestinationNode.Valid() ||
		r.OriginNode == r.DestinationNode {
		return fmt.Errorf("artifact completion has invalid metadata")
	}
	return nil
}

// CanonicalBytes returns the exact JSON request representation.
func (r ArtifactCompleteRequest) CanonicalBytes() ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("encode artifact completion: %w", err)
	}
	return raw, nil
}

// RequestSHA256 binds the final receipt to this completion request.
func (r ArtifactCompleteRequest) RequestSHA256() (protocol.SHA256Hex, error) {
	raw, err := r.CanonicalBytes()
	if err != nil {
		return "", err
	}
	return artifactRequestDigest(raw), nil
}

// DecodeArtifactCompleteRequest strictly decodes one completion request.
func DecodeArtifactCompleteRequest(raw []byte) (ArtifactCompleteRequest, error) {
	var request ArtifactCompleteRequest
	if err := decodeArtifactRequest(raw, &request); err != nil {
		return ArtifactCompleteRequest{}, fmt.Errorf("decode artifact completion: %w", err)
	}
	if err := request.Validate(); err != nil {
		return ArtifactCompleteRequest{}, err
	}
	return request, nil
}

// ArtifactReceiptPhase identifies which request produced a cursor receipt.
type ArtifactReceiptPhase string

const (
	// ArtifactReceiptManifest identifies a manifest cursor response.
	ArtifactReceiptManifest ArtifactReceiptPhase = "MANIFEST"
	// ArtifactReceiptChunk identifies a stored-chunk cursor response.
	ArtifactReceiptChunk ArtifactReceiptPhase = "CHUNK"
	// ArtifactReceiptComplete identifies the terminal validation response.
	ArtifactReceiptComplete ArtifactReceiptPhase = "COMPLETE"
)

func (p ArtifactReceiptPhase) valid() bool {
	return p == ArtifactReceiptManifest || p == ArtifactReceiptChunk || p == ArtifactReceiptComplete
}

// ArtifactReceiptStatus is either progress or the final durable outcome.
type ArtifactReceiptStatus string

const (
	// ArtifactReceiptReplicating reports receiver progress.
	ArtifactReceiptReplicating ArtifactReceiptStatus = "REPLICATING"
	// ArtifactReceiptStoredRemote proves the receiver published the artifact.
	ArtifactReceiptStoredRemote ArtifactReceiptStatus = "STORED_REMOTE"
)

func (s ArtifactReceiptStatus) valid() bool {
	return s == ArtifactReceiptReplicating || s == ArtifactReceiptStoredRemote
}

// ArtifactReceipt is the receiver-signed authoritative cursor. Only a valid
// STORED_REMOTE completion receipt may finalize the sender's transfer.
type ArtifactReceipt struct {
	ArtifactID      protocol.OpaqueID     `json:"artifact_id"`
	OriginNode      protocol.NodeID       `json:"origin_node"`
	DestinationNode protocol.NodeID       `json:"destination_node"`
	Phase           ArtifactReceiptPhase  `json:"phase"`
	RequestSHA256   protocol.SHA256Hex    `json:"request_sha256"`
	NextChunk       int64                 `json:"next_chunk"`
	ChunkCount      int64                 `json:"chunk_count"`
	Status          ArtifactReceiptStatus `json:"status"`
	ReceivedAt      time.Time             `json:"received_at"`
	Signature       []byte                `json:"signature,omitempty"`
}

// Validate checks a fully signed receipt.
func (r ArtifactReceipt) Validate() error {
	if err := r.validatePayload(); err != nil {
		return err
	}
	if len(r.Signature) != ed25519.SignatureSize {
		return fmt.Errorf("artifact receipt signature must be %d bytes", ed25519.SignatureSize)
	}
	return nil
}

func (r ArtifactReceipt) validatePayload() error {
	if !r.ArtifactID.Valid() || !r.OriginNode.Valid() || !r.DestinationNode.Valid() ||
		r.OriginNode == r.DestinationNode || !r.Phase.valid() || !r.RequestSHA256.Valid() ||
		r.ChunkCount < 1 || r.NextChunk < 0 || r.NextChunk > r.ChunkCount ||
		!r.Status.valid() || r.ReceivedAt.IsZero() {
		return fmt.Errorf("artifact receipt has invalid metadata")
	}
	if (r.Status == ArtifactReceiptStoredRemote) != (r.Phase == ArtifactReceiptComplete) ||
		(r.Status == ArtifactReceiptStoredRemote && r.NextChunk != r.ChunkCount) {
		return fmt.Errorf("artifact receipt has invalid terminal state")
	}
	return nil
}

func (r ArtifactReceipt) signingPayload() ([]byte, error) {
	if err := r.validatePayload(); err != nil {
		return nil, err
	}
	r.Signature = nil
	payload, err := json.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("encode artifact receipt signing payload: %w", err)
	}
	return payload, nil
}

// Sign authenticates the receipt with the receiver node key.
func (r *ArtifactReceipt) Sign(reg *registry.Registry) error {
	if r == nil || reg == nil {
		return fmt.Errorf("sign artifact receipt: invalid input")
	}
	payload, err := r.signingPayload()
	if err != nil {
		return err
	}
	signature, err := reg.SignFederationArtifactReceipt(payload)
	if err != nil {
		return fmt.Errorf("sign artifact receipt: %w", err)
	}
	r.Signature = signature
	return nil
}

// Verify authenticates the receipt with the pinned destination node key.
func (r ArtifactReceipt) Verify(publicKey ed25519.PublicKey) error {
	if err := r.Validate(); err != nil {
		return err
	}
	payload, err := r.signingPayload()
	if err != nil {
		return err
	}
	if !registry.VerifyFederationArtifactReceipt(publicKey, payload, r.Signature) {
		return fmt.Errorf("verify artifact receipt: invalid signature")
	}
	return nil
}

// DecodeArtifactReceipt strictly decodes one signed receipt.
func DecodeArtifactReceipt(raw []byte) (ArtifactReceipt, error) {
	var receipt ArtifactReceipt
	if err := decodeArtifactRequest(raw, &receipt); err != nil {
		return ArtifactReceipt{}, fmt.Errorf("decode artifact receipt: %w", err)
	}
	if err := receipt.Validate(); err != nil {
		return ArtifactReceipt{}, err
	}
	return receipt, nil
}

func decodeArtifactRequest(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode artifact JSON: %w", err)
	}
	return requireJSONEOF(decoder)
}

func artifactRequestDigest(raw []byte) protocol.SHA256Hex {
	digest := sha256.Sum256(raw)
	return protocol.SHA256Hex(hex.EncodeToString(digest[:]))
}
