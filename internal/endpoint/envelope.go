package endpoint

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/smellak/gifford/internal/protocol"
)

// This file holds the submission envelope: what the Endpoint signs with
// the agent key before admission. The signature covers the canonical
// encoding — the envelope without its signature field — so the Hub can
// verify sender and integrity against the registered key. The final
// envelope shape belongs to the admission subphases; this is the part-one
// submission.

// Envelope is one outgoing message as the Endpoint submits it. From is
// always the verified connection identity, never a client argument, so a
// caller cannot spoof its sender.
type Envelope struct {
	From              protocol.AgentID `json:"from"`
	To                string           `json:"to"`
	Subject           string           `json:"subject"`
	Body              string           `json:"body"`
	ThreadID          string           `json:"thread_id,omitempty"`
	Priority          string           `json:"priority,omitempty"`
	TTL               string           `json:"ttl,omitempty"`
	ClientMsgID       string           `json:"client_msg_id,omitempty"`
	RequestedAutonomy string           `json:"requested_autonomy,omitempty"`
	RequestedBudget   string           `json:"requested_budget,omitempty"`
	InReplyTo         string           `json:"in_reply_to,omitempty"`
	Signature         string           `json:"signature,omitempty"`
}

// canonicalBytes returns the signing encoding: the envelope without its
// signature, marshalled with fixed field order.
func (e Envelope) canonicalBytes() ([]byte, error) {
	unsigned := e
	unsigned.Signature = ""
	raw, err := json.Marshal(unsigned)
	if err != nil {
		return nil, fmt.Errorf("encode envelope: %w", err)
	}
	return raw, nil
}

// SignEnvelope signs env with the agent key, returning the envelope
// carrying its base64 signature.
func SignEnvelope(priv ed25519.PrivateKey, env Envelope) (Envelope, error) {
	msg, err := env.canonicalBytes()
	if err != nil {
		return Envelope{}, err
	}
	signed := env
	signed.Signature = base64.StdEncoding.EncodeToString(ed25519.Sign(priv, msg))
	return signed, nil
}

// VerifyEnvelope reports whether env carries pub's signature over its
// canonical encoding. It is the check the Hub repeats on receipt.
func VerifyEnvelope(pub ed25519.PublicKey, env Envelope) bool {
	if len(pub) != ed25519.PublicKeySize || env.Signature == "" {
		return false
	}
	signature, err := base64.StdEncoding.DecodeString(env.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize {
		return false
	}
	msg, err := env.canonicalBytes()
	if err != nil {
		return false
	}
	return ed25519.Verify(pub, msg, signature)
}
