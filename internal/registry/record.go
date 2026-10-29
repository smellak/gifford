package registry

import (
	"crypto/ed25519"
	"encoding/binary"
	"fmt"
	"math"
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
// stored text. Fields are small identifiers by construction; anything past
// 4 GiB is refused before the conversion instead of wrapping.
func encodeRecord(rec keyRecord) ([]byte, error) {
	var out []byte
	var err error
	for _, field := range [][]byte{
		[]byte(recordDomain),
		[]byte(rec.keyID),
		[]byte(rec.agentID),
		[]byte(rec.nodeID),
		[]byte(rec.purpose),
		[]byte(rec.algorithm),
		rec.publicKey,
		[]byte(rec.status),
		[]byte(rec.validFrom),
	} {
		if out, err = appendField(out, field); err != nil {
			return nil, err
		}
	}
	if rec.validUntil == nil {
		out = append(out, 0)
	} else {
		out = append(out, 1)
		if out, err = appendField(out, []byte(*rec.validUntil)); err != nil {
			return nil, err
		}
	}
	if out, err = appendField(out, []byte(rec.issuer)); err != nil {
		return nil, err
	}
	return out, nil
}

// appendField appends a 4-byte big-endian length and the raw bytes. The
// length is checked before narrowing to uint32.
func appendField(out, field []byte) ([]byte, error) {
	if len(field) > math.MaxUint32 {
		return nil, fmt.Errorf("record field holds %d bytes: %w", len(field), ErrInvalidArgument)
	}
	var length [4]byte
	//nolint:gosec // length is checked against math.MaxUint32 above.
	binary.BigEndian.PutUint32(length[:], uint32(len(field)))
	out = append(out, length[:]...)
	return append(out, field...), nil
}

// signRecord signs the canonical encoding of rec with the node key.
func signRecord(priv ed25519.PrivateKey, rec keyRecord) ([]byte, error) {
	msg, err := encodeRecord(rec)
	if err != nil {
		return nil, err
	}
	return ed25519.Sign(priv, msg), nil
}

// verifyRecord reports whether signature is the node key's signature over
// the canonical encoding of rec. Malformed keys, signatures, or records
// never verify.
func verifyRecord(pub ed25519.PublicKey, rec keyRecord, signature []byte) bool {
	if len(pub) != ed25519.PublicKeySize || len(signature) != ed25519.SignatureSize {
		return false
	}
	msg, err := encodeRecord(rec)
	if err != nil {
		return false
	}
	return ed25519.Verify(pub, msg, signature)
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
