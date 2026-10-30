package registry

import (
	"crypto/ed25519"
	"fmt"
)

const (
	federationReceiptDomain    = "gifford-federation-receipt-v1\x00"
	federationAckReceiptDomain = "gifford-federation-ack-receipt-v1\x00"
)

// SignFederationReceipt signs canonical receipt payload bytes under the fixed
// federation receipt domain. The node private key never leaves Registry.
func (r *Registry) SignFederationReceipt(payload []byte) ([]byte, error) {
	if len(payload) == 0 {
		return nil, fmt.Errorf("sign federation receipt: %w", ErrInvalidArgument)
	}
	privateKey, err := r.nodePrivate()
	if err != nil {
		return nil, err
	}
	return ed25519.Sign(privateKey, federationReceiptBytes(payload)), nil
}

// VerifyFederationReceipt verifies canonical receipt payload bytes under the
// same fixed domain used by SignFederationReceipt.
func VerifyFederationReceipt(publicKey ed25519.PublicKey, payload, signature []byte) bool {
	if len(publicKey) != ed25519.PublicKeySize || len(payload) == 0 || len(signature) != ed25519.SignatureSize {
		return false
	}
	return ed25519.Verify(publicKey, federationReceiptBytes(payload), signature)
}

// SignFederationAckReceipt signs canonical acknowledgement receipt payload
// bytes without exposing the node private key.
func (r *Registry) SignFederationAckReceipt(payload []byte) ([]byte, error) {
	if len(payload) == 0 {
		return nil, fmt.Errorf("sign federation ack receipt: %w", ErrInvalidArgument)
	}
	privateKey, err := r.nodePrivate()
	if err != nil {
		return nil, err
	}
	return ed25519.Sign(privateKey, federationAckReceiptBytes(payload)), nil
}

// VerifyFederationAckReceipt verifies canonical acknowledgement receipt
// payload bytes under their separate fixed domain.
func VerifyFederationAckReceipt(publicKey ed25519.PublicKey, payload, signature []byte) bool {
	if len(publicKey) != ed25519.PublicKeySize || len(payload) == 0 || len(signature) != ed25519.SignatureSize {
		return false
	}
	return ed25519.Verify(publicKey, federationAckReceiptBytes(payload), signature)
}

func federationReceiptBytes(payload []byte) []byte {
	answer := make([]byte, 0, len(federationReceiptDomain)+len(payload))
	answer = append(answer, federationReceiptDomain...)
	return append(answer, payload...)
}

func federationAckReceiptBytes(payload []byte) []byte {
	answer := make([]byte, 0, len(federationAckReceiptDomain)+len(payload))
	answer = append(answer, federationAckReceiptDomain...)
	return append(answer, payload...)
}
