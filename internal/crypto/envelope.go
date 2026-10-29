package crypto

import (
	"crypto/ed25519"
	"encoding/json"
	"fmt"

	"github.com/smellak/gifford/internal/protocol"
)

const wireEnvelopeDomain = "gifford:wire-envelope:v1\x00"

// WireEnvelopeSigningBytes returns the deterministic, domain-separated bytes
// shared by Endpoint signing and Hub verification. JSON is produced directly
// from the frozen struct after clearing Signature; no map participates.
func WireEnvelopeSigningBytes(envelope protocol.WireEnvelope) ([]byte, error) {
	envelope.Signature = nil
	payload, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("marshal wire envelope signing input: %w", err)
	}
	answer := make([]byte, 0, len(wireEnvelopeDomain)+len(payload))
	answer = append(answer, wireEnvelopeDomain...)
	answer = append(answer, payload...)
	return answer, nil
}

// SignWireEnvelope signs envelope in place using the v1 signing bytes.
func SignWireEnvelope(privateKey ed25519.PrivateKey, envelope *protocol.WireEnvelope) error {
	if len(privateKey) != ed25519.PrivateKeySize || envelope == nil {
		return fmt.Errorf("sign wire envelope: invalid input")
	}
	input, err := WireEnvelopeSigningBytes(*envelope)
	if err != nil {
		return err
	}
	envelope.Signature = ed25519.Sign(privateKey, input)
	return nil
}

// VerifyWireEnvelope verifies envelope using the same v1 signing bytes.
func VerifyWireEnvelope(publicKey ed25519.PublicKey, envelope protocol.WireEnvelope) error {
	if len(publicKey) != ed25519.PublicKeySize || len(envelope.Signature) != ed25519.SignatureSize {
		return fmt.Errorf("verify wire envelope: invalid key or signature size")
	}
	input, err := WireEnvelopeSigningBytes(envelope)
	if err != nil {
		return err
	}
	if !ed25519.Verify(publicKey, input, envelope.Signature) {
		return fmt.Errorf("verify wire envelope: invalid signature")
	}
	return nil
}
