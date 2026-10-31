// Package artifact implements the end-to-end cryptographic format shared by
// the artifact CLI and Hub tests. The Hub itself never receives a content key.
package artifact

import (
	"bytes"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/hpke"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	"golang.org/x/crypto/chacha20poly1305"

	"github.com/smellak/gifford/internal/protocol"
)

const (
	chunkDomain    = "gifford-artifact-chunk-v1"
	manifestDomain = "gifford-artifact-manifest-v1"
	keyDomain      = "gifford-artifact-key-v1"
	envelopeDomain = "gifford-artifact-envelope-v1"
	contentKeySize = chacha20poly1305.KeySize
)

// ErrVerification is returned for cryptographic or plaintext metadata failure.
var ErrVerification = errors.New("artifact verification failed")

// PrivateManifest is encrypted end to end and is never stored in plaintext by
// the Hub.
type PrivateManifest struct {
	Filename        string             `json:"filename"`
	MediaType       string             `json:"media_type"`
	Purpose         string             `json:"purpose"`
	PlaintextSize   int64              `json:"plaintext_size"`
	PlaintextSHA256 protocol.SHA256Hex `json:"plaintext_sha256"`
}

// Validate applies bounded private-manifest rules.
func (m PrivateManifest) Validate() error {
	switch {
	case !utf8.ValidString(m.Filename) || len(m.Filename) > 4096:
		return fmt.Errorf("filename: %w", ErrVerification)
	case !utf8.ValidString(m.MediaType) || len(m.MediaType) > 200:
		return fmt.Errorf("media type: %w", ErrVerification)
	case !utf8.ValidString(m.Purpose) || utf8.RuneCountInString(m.Purpose) > 200:
		return fmt.Errorf("purpose: %w", ErrVerification)
	case m.PlaintextSize < 0:
		return fmt.Errorf("plaintext size: %w", ErrVerification)
	case !m.PlaintextSHA256.Valid():
		return fmt.Errorf("plaintext digest: %w", ErrVerification)
	default:
		return nil
	}
}

// EnvelopeMetadata is exactly the signed, ciphertext-only artifact metadata.
type EnvelopeMetadata struct {
	ArtifactID        protocol.OpaqueID
	ClientMsgID       protocol.OpaqueID
	Sender            protocol.AgentID
	Recipient         protocol.AgentID
	CiphertextSize    int64
	ChunkCount        int64
	CiphertextSHA256  protocol.SHA256Hex
	EncryptedManifest []byte
	KeyEnvelope       []byte
	EncryptionProfile string
	ExpiresAt         time.Time
}

// NewContentKey returns a fresh 256-bit content key.
func NewContentKey() ([]byte, error) {
	key := make([]byte, contentKeySize)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return nil, fmt.Errorf("generate artifact content key: %w", err)
	}
	return key, nil
}

// ChunkCount returns max(1, ceil(size/1MiB)).
func ChunkCount(size int64) (int64, error) {
	if size < 0 {
		return 0, fmt.Errorf("negative plaintext size")
	}
	if size == 0 {
		return 1, nil
	}
	return (size + protocol.ArtifactChunkSize - 1) / protocol.ArtifactChunkSize, nil
}

// CiphertextSize adds one Poly1305 tag per chunk.
func CiphertextSize(plaintextSize, chunkCount int64) (int64, error) {
	if plaintextSize < 0 || chunkCount < 1 || plaintextSize > int64(^uint64(0)>>1)-16*chunkCount {
		return 0, fmt.Errorf("invalid artifact size")
	}
	return plaintextSize + 16*chunkCount, nil
}

// ExpectedChunkSize returns the exact ciphertext size for an index.
func ExpectedChunkSize(ciphertextSize, chunkCount, index int64) (int64, error) {
	if chunkCount < 1 || index < 0 || index >= chunkCount {
		return 0, fmt.Errorf("chunk index out of range")
	}
	if index < chunkCount-1 {
		return protocol.ArtifactChunkSize + 16, nil
	}
	last := ciphertextSize - (chunkCount-1)*(protocol.ArtifactChunkSize+16)
	if last < 16 || last > protocol.ArtifactChunkSize+16 || (chunkCount > 1 && last == 16) {
		return 0, fmt.Errorf("invalid final chunk size")
	}
	return last, nil
}

// DeriveArtifactID lets the sender build AAD before the idempotent reserve
// call. The Hub independently derives and returns the same opaque id.
func DeriveArtifactID(sender protocol.AgentID, clientMsgID protocol.OpaqueID) (protocol.OpaqueID, error) {
	if !sender.Valid() || !clientMsgID.Valid() {
		return "", fmt.Errorf("invalid artifact identity")
	}
	sum := sha256.Sum256([]byte(string(sender) + "\n" + string(clientMsgID)))
	return protocol.OpaqueID("art-" + hex.EncodeToString(sum[:16])), nil
}

// DefaultClientMessageID derives the stable default id from content and intent.
func DefaultClientMessageID(
	recipient protocol.AgentID,
	plaintextSHA256 protocol.SHA256Hex,
	purpose string,
) (protocol.OpaqueID, error) {
	if !recipient.Valid() || !plaintextSHA256.Valid() || !utf8.ValidString(purpose) ||
		utf8.RuneCountInString(purpose) > 200 {
		return "", fmt.Errorf("invalid artifact client id input")
	}
	sum := sha256.Sum256([]byte(string(recipient) + "\n" + string(plaintextSHA256) + "\n" + purpose))
	return protocol.OpaqueID("file-" + hex.EncodeToString(sum[:16])), nil
}

// EncryptChunk deterministically encrypts one plaintext chunk.
func EncryptChunk(
	key []byte,
	artifactID protocol.OpaqueID,
	index, count int64,
	plaintext []byte,
) ([]byte, error) {
	aead, nonce, aad, err := chunkInputs(key, artifactID, index, count)
	if err != nil {
		return nil, err
	}
	return aead.Seal(nil, nonce, plaintext, aad), nil
}

// DecryptChunk authenticates and decrypts one ciphertext chunk.
func DecryptChunk(
	key []byte,
	artifactID protocol.OpaqueID,
	index, count int64,
	ciphertext []byte,
) ([]byte, error) {
	aead, nonce, aad, err := chunkInputs(key, artifactID, index, count)
	if err != nil {
		return nil, fmt.Errorf("chunk inputs: %w", ErrVerification)
	}
	plaintext, err := aead.Open(nil, nonce, ciphertext, aad)
	if err != nil {
		return nil, fmt.Errorf("chunk authentication: %w", ErrVerification)
	}
	return plaintext, nil
}

func chunkInputs(
	key []byte,
	artifactID protocol.OpaqueID,
	index, count int64,
) (cipherAEAD, []byte, []byte, error) {
	if len(key) != contentKeySize || !artifactID.Valid() || count < 1 || index < 0 || index >= count {
		return nil, nil, nil, fmt.Errorf("invalid chunk inputs")
	}
	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("create artifact cipher: %w", err)
	}
	nonce := make([]byte, chacha20poly1305.NonceSizeX)
	binary.BigEndian.PutUint64(nonce[16:], uint64(index))
	aad := make([]byte, 0, len(chunkDomain)+len(artifactID)+16)
	aad = append(aad, chunkDomain...)
	aad = append(aad, artifactID...)
	var integer [8]byte
	binary.BigEndian.PutUint64(integer[:], uint64(index))
	aad = append(aad, integer[:]...)
	binary.BigEndian.PutUint64(integer[:], uint64(count))
	aad = append(aad, integer[:]...)
	return aead, nonce, aad, nil
}

// cipherAEAD is the subset used from cipher.AEAD, kept local to avoid
// exporting implementation details.
type cipherAEAD interface {
	Seal(dst, nonce, plaintext, additionalData []byte) []byte
	Open(dst, nonce, ciphertext, additionalData []byte) ([]byte, error)
}

// EncryptManifest encodes and encrypts a private manifest with a random nonce
// prefixed to the ciphertext.
func EncryptManifest(key []byte, artifactID protocol.OpaqueID, manifest PrivateManifest) ([]byte, error) {
	if err := manifest.Validate(); err != nil {
		return nil, err
	}
	if len(key) != contentKeySize || !artifactID.Valid() {
		return nil, fmt.Errorf("invalid manifest inputs")
	}
	plaintext, err := json.Marshal(manifest)
	if err != nil {
		return nil, fmt.Errorf("encode artifact manifest: %w", err)
	}
	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return nil, fmt.Errorf("create manifest cipher: %w", err)
	}
	nonce := make([]byte, chacha20poly1305.NonceSizeX)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("generate manifest nonce: %w", err)
	}
	aad := append([]byte(manifestDomain), artifactID...)
	out := append([]byte(nil), nonce...)
	return aead.Seal(out, nonce, plaintext, aad), nil
}

// DecryptManifest authenticates and strictly decodes a private manifest.
func DecryptManifest(key []byte, artifactID protocol.OpaqueID, encrypted []byte) (PrivateManifest, error) {
	if len(key) != contentKeySize || !artifactID.Valid() || len(encrypted) < chacha20poly1305.NonceSizeX+16 {
		return PrivateManifest{}, ErrVerification
	}
	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return PrivateManifest{}, ErrVerification
	}
	nonce := encrypted[:chacha20poly1305.NonceSizeX]
	aad := append([]byte(manifestDomain), artifactID...)
	plaintext, err := aead.Open(nil, nonce, encrypted[chacha20poly1305.NonceSizeX:], aad)
	if err != nil {
		return PrivateManifest{}, ErrVerification
	}
	decoder := json.NewDecoder(bytes.NewReader(plaintext))
	decoder.DisallowUnknownFields()
	var manifest PrivateManifest
	if err := decoder.Decode(&manifest); err != nil {
		return PrivateManifest{}, ErrVerification
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return PrivateManifest{}, ErrVerification
	}
	if err := manifest.Validate(); err != nil {
		return PrivateManifest{}, ErrVerification
	}
	return manifest, nil
}

// WrapContentKey seals a CEK to one X25519 recipient with standard HPKE.
func WrapContentKey(
	recipientPublic []byte,
	artifactID protocol.OpaqueID,
	recipient protocol.AgentID,
	key []byte,
) ([]byte, error) {
	if len(key) != contentKeySize || !artifactID.Valid() || !recipient.Valid() {
		return nil, fmt.Errorf("invalid key-envelope inputs")
	}
	public, err := ecdh.X25519().NewPublicKey(recipientPublic)
	if err != nil {
		return nil, fmt.Errorf("parse recipient encryption key: %w", err)
	}
	hpkePublic, err := hpke.NewDHKEMPublicKey(public)
	if err != nil {
		return nil, fmt.Errorf("build recipient HPKE key: %w", err)
	}
	envelope, err := hpke.Seal(hpkePublic, hpke.HKDFSHA256(), hpke.ChaCha20Poly1305(),
		keyInfo(artifactID, recipient), key)
	if err != nil {
		return nil, fmt.Errorf("seal recipient content key: %w", err)
	}
	return envelope, nil
}

// UnwrapContentKey opens a CEK with the recipient's X25519 private key.
func UnwrapContentKey(
	recipientPrivate *ecdh.PrivateKey,
	artifactID protocol.OpaqueID,
	recipient protocol.AgentID,
	envelope []byte,
) ([]byte, error) {
	if recipientPrivate == nil || !artifactID.Valid() || !recipient.Valid() || len(envelope) == 0 {
		return nil, ErrVerification
	}
	hpkePrivate, err := hpke.NewDHKEMPrivateKey(recipientPrivate)
	if err != nil {
		return nil, ErrVerification
	}
	key, err := hpke.Open(hpkePrivate, hpke.HKDFSHA256(), hpke.ChaCha20Poly1305(),
		keyInfo(artifactID, recipient), envelope)
	if err != nil || len(key) != contentKeySize {
		return nil, ErrVerification
	}
	return key, nil
}

func keyInfo(artifactID protocol.OpaqueID, recipient protocol.AgentID) []byte {
	info := make([]byte, 0, len(keyDomain)+len(artifactID)+len(recipient))
	info = append(info, keyDomain...)
	info = append(info, artifactID...)
	return append(info, recipient...)
}

// SignEnvelope signs the canonical ciphertext-only envelope metadata.
func SignEnvelope(private ed25519.PrivateKey, metadata EnvelopeMetadata) ([]byte, error) {
	canonical, err := CanonicalEnvelope(metadata)
	if err != nil {
		return nil, err
	}
	return ed25519.Sign(private, canonical), nil
}

// VerifyEnvelope verifies canonical metadata with the active sender key.
func VerifyEnvelope(public ed25519.PublicKey, metadata EnvelopeMetadata, signature []byte) error {
	canonical, err := CanonicalEnvelope(metadata)
	if err != nil || len(public) != ed25519.PublicKeySize || !ed25519.Verify(public, canonical, signature) {
		return ErrVerification
	}
	return nil
}

// CanonicalEnvelope renders the exact length-delimited signature input.
func CanonicalEnvelope(metadata EnvelopeMetadata) ([]byte, error) {
	if !metadata.ArtifactID.Valid() || !metadata.ClientMsgID.Valid() || !metadata.Sender.Valid() ||
		!metadata.Recipient.Valid() || metadata.Sender == metadata.Recipient || metadata.CiphertextSize < 16 ||
		metadata.ChunkCount < 1 || !metadata.CiphertextSHA256.Valid() || len(metadata.EncryptedManifest) < 40 ||
		len(metadata.KeyEnvelope) == 0 || metadata.EncryptionProfile != protocol.ArtifactEncryptionProfile ||
		metadata.ExpiresAt.IsZero() {
		return nil, fmt.Errorf("invalid artifact envelope metadata")
	}
	manifestHash := sha256.Sum256(metadata.EncryptedManifest)
	envelopeHash := sha256.Sum256(metadata.KeyEnvelope)
	var out bytes.Buffer
	out.WriteString(envelopeDomain)
	for _, value := range [][]byte{
		[]byte(metadata.ArtifactID), []byte(metadata.ClientMsgID), []byte(metadata.Sender),
		[]byte(metadata.Recipient),
	} {
		writeLengthPrefixed(&out, value)
	}
	writeUint64(&out, uint64(metadata.CiphertextSize))
	writeUint64(&out, uint64(metadata.ChunkCount))
	writeLengthPrefixed(&out, []byte(metadata.CiphertextSHA256))
	writeLengthPrefixed(&out, manifestHash[:])
	writeLengthPrefixed(&out, envelopeHash[:])
	writeLengthPrefixed(&out, []byte(metadata.EncryptionProfile))
	writeLengthPrefixed(&out, []byte(metadata.ExpiresAt.UTC().Format(time.RFC3339Nano)))
	return out.Bytes(), nil
}

func writeLengthPrefixed(out *bytes.Buffer, value []byte) {
	writeUint64(out, uint64(len(value)))
	_, _ = out.Write(value)
}

func writeUint64(out *bytes.Buffer, value uint64) {
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], value)
	_, _ = out.Write(raw[:])
}

// Digest returns the lowercase SHA-256 of bytes.
func Digest(value []byte) protocol.SHA256Hex {
	sum := sha256.Sum256(value)
	return protocol.SHA256Hex(hex.EncodeToString(sum[:]))
}

// SanitizeFilename returns a safe single-component output name.
func SanitizeFilename(name string, artifactID protocol.OpaqueID) string {
	name = strings.ReplaceAll(name, "\\", "/")
	parts := strings.Split(name, "/")
	name = parts[len(parts)-1]
	var clean strings.Builder
	for _, r := range name {
		if r < 0x20 || r == 0x7f || r == '/' || r == '\\' {
			continue
		}
		clean.WriteRune(r)
	}
	name = strings.TrimSpace(clean.String())
	if name == "" || name == "." || name == ".." || strings.Contains(name, "..") {
		return "artifact-" + string(artifactID)
	}
	return name
}
