package adapters

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/smellak/gifford/internal/protocol"
)

// MaxMessageBytes bounds one adapter message, matching the agent socket
// framing bound.
const MaxMessageBytes = 1 << 20

const messageLengthBytes = 4

var (
	// ErrMessageTooLarge means a length prefix exceeds MaxMessageBytes.
	ErrMessageTooLarge = errors.New("adapters: message too large")
	// ErrInvalidMessage means a message is malformed or breaks the wire rules.
	ErrInvalidMessage = errors.New("adapters: invalid message")
)

// Operation names one message on the adapter link.
type Operation string

// Adapter link operations.
const (
	OperationHandshake Operation = "handshake"
	OperationManifest  Operation = "manifest"
	OperationPing      Operation = "ping"
	OperationPong      Operation = "pong"
)

// Message is one length-delimited JSON message on the local adapter link.
// Handshake requests name the run; manifest replies carry the adapter's
// CapabilityManifest. Ping/pong messages keep the verified link live.
type Message struct {
	Operation Operation                    `json:"op"`
	ID        int64                        `json:"id"`
	ReplyTo   int64                        `json:"reply_to,omitempty"`
	AgentID   protocol.AgentID             `json:"agent_id,omitempty"`
	Harness   protocol.Harness             `json:"harness,omitempty"`
	Manifest  *protocol.CapabilityManifest `json:"manifest,omitempty"`
}

// Validate checks the adapter wire shape. A manifest's own semantic
// validation belongs to the client because invalid replies must still be
// readable and cause a generic-PTY fallback.
func (m Message) Validate() error {
	if m.ID < 1 {
		return fmt.Errorf("%w: id must be positive", ErrInvalidMessage)
	}
	switch m.Operation {
	case OperationHandshake:
		if m.ReplyTo != 0 || !m.AgentID.Valid() || !m.Harness.Valid() || m.Manifest != nil {
			return fmt.Errorf("%w: bad handshake", ErrInvalidMessage)
		}
	case OperationManifest:
		if m.ReplyTo < 1 || m.AgentID != "" || m.Harness != "" || m.Manifest == nil {
			return fmt.Errorf("%w: bad manifest reply", ErrInvalidMessage)
		}
	case OperationPing:
		if m.ReplyTo != 0 || m.AgentID != "" || m.Harness != "" || m.Manifest != nil {
			return fmt.Errorf("%w: bad ping", ErrInvalidMessage)
		}
	case OperationPong:
		if m.ReplyTo < 1 || m.AgentID != "" || m.Harness != "" || m.Manifest != nil {
			return fmt.Errorf("%w: bad pong", ErrInvalidMessage)
		}
	default:
		return fmt.Errorf("%w: unknown operation", ErrInvalidMessage)
	}
	return nil
}

// WriteMessage writes a four-byte big-endian JSON length followed by one
// validated message. It writes nothing when validation or encoding fails.
func WriteMessage(w io.Writer, message Message) error {
	if err := message.Validate(); err != nil {
		return err
	}
	body, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("%w: encode", ErrInvalidMessage)
	}
	if len(body) > MaxMessageBytes {
		return ErrMessageTooLarge
	}
	buf := make([]byte, messageLengthBytes, messageLengthBytes+len(body))
	binary.BigEndian.PutUint32(buf, uint32(len(body))) //nolint:gosec // bounded above
	buf = append(buf, body...)
	for len(buf) > 0 {
		n, err := w.Write(buf)
		if err != nil {
			return fmt.Errorf("adapters: write message: %w", err)
		}
		if n == 0 {
			return fmt.Errorf("adapters: write message: %w", io.ErrShortWrite)
		}
		buf = buf[n:]
	}
	return nil
}

// ReadMessage reads and validates one length-delimited adapter message.
// EOF between messages and UnexpectedEOF inside one are returned unchanged.
func ReadMessage(r io.Reader) (Message, error) {
	var prefix [messageLengthBytes]byte
	if _, err := io.ReadFull(r, prefix[:]); err != nil {
		return Message{}, err //nolint:wrapcheck // EOF shape is part of the contract
	}
	size := binary.BigEndian.Uint32(prefix[:])
	if size > MaxMessageBytes {
		return Message{}, ErrMessageTooLarge
	}
	body := make([]byte, size)
	if _, err := io.ReadFull(r, body); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return Message{}, err
	}
	var message Message
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&message); err != nil {
		return Message{}, fmt.Errorf("%w: decode", ErrInvalidMessage)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return Message{}, fmt.Errorf("%w: trailing JSON", ErrInvalidMessage)
	}
	if err := message.Validate(); err != nil {
		return Message{}, err
	}
	return message, nil
}
