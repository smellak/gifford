package registry

import (
	"crypto/ed25519"
	"encoding/binary"
	"fmt"
)

// This file holds the canonical record encoding: what the node key signs
// and what every read verifies. The encoding covers every stored column of
// an agent_keys row except record_signature itself, including status, so a
// status change without a fresh signature never verifies.

// recordDomain separates these signatures from every other use of the node
// key.
const recordDomain = "gifford-agent-key-record-v1"

// keyStatus is the lifecycle of one agent_keys row. The alert reads these
// values; no new event type exists.
type keyStatus string

// Agent key states.
const (
	statusActive    keyStatus = "ACTIVE"
	statusSuspended keyStatus = "SUSPENDED"
	statusRejected  keyStatus = "REJECTED"
	statusRevoked   keyStatus = "REVOKED"
	statusRotated   keyStatus = "ROTATED"
)

// keyRecord is one agent_keys row as the package reads and writes it.
type keyRecord struct {
	keyID     string
	agentID   string
	nodeID    string
	purpose   string
	algorithm string
	publicKey []byte
	status    keyStatus
	validFrom string
	// validUntil is nil for a NULL column (no expiry).
	validUntil *string
	issuer     string
	signature  []byte
}

// encodeRecord returns the canonical bytes of rec: the domain followed by
// every signed field in fixed order, each length-prefixed so boundaries
// are unambiguous. A nil validUntil encodes as absent, distinct from any
// stored text.
func encodeRecord(rec keyRecord) []byte {
	var out []byte
	out = appendField(out, []byte(recordDomain))
	out = appendField(out, []byte(rec.keyID))
	out = appendField(out, []byte(rec.agentID))
	out = appendField(out, []byte(rec.nodeID))
	out = appendField(out, []byte(rec.purpose))
	out = appendField(out, []byte(rec.algorithm))
	out = appendField(out, rec.publicKey)
	out = appendField(out, []byte(rec.status))
	out = appendField(out, []byte(rec.validFrom))
	if rec.validUntil == nil {
		out = append(out, 0)
	} else {
		out = append(out, 1)
		out = appendField(out, []byte(*rec.validUntil))
	}
	out = appendField(out, []byte(rec.issuer))
	return out
}

// appendField appends a 4-byte big-endian length and the raw bytes.
func appendField(out, field []byte) []byte {
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(field)))
	out = append(out, length[:]...)
	return append(out, field...)
}

// signRecord signs the canonical encoding of rec with the node key.
func signRecord(priv ed25519.PrivateKey, rec keyRecord) []byte {
	return ed25519.Sign(priv, encodeRecord(rec))
}

// verifyRecord reports whether signature is the node key's signature over
// the canonical encoding of rec. Malformed keys or signatures never verify.
func verifyRecord(pub ed25519.PublicKey, rec keyRecord, signature []byte) bool {
	if len(pub) != ed25519.PublicKeySize || len(signature) != ed25519.SignatureSize {
		return false
	}
	return ed25519.Verify(pub, encodeRecord(rec), signature)
}

// parseStatus reads a stored status; any value the writer could never have
// produced is an error.
func parseStatus(raw string) (keyStatus, error) {
	switch status := keyStatus(raw); status {
	case statusActive, statusSuspended, statusRejected, statusRevoked, statusRotated:
		return status, nil
	default:
		return "", fmt.Errorf("agent key holds status %q: %w", raw, ErrInvalidArgument)
	}
}
