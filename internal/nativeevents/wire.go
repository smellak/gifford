package nativeevents

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"unicode"
	"unicode/utf8"

	"github.com/smellak/gifford/internal/protocol"
)

// SchemaVersion is the native-event link version.
const SchemaVersion int64 = 2

// SocketEnvironment carries the run-created native-event entry to descendants.
const SocketEnvironment = "GIFFORD_ADAPTER_SOCKET"

// MaxMessageBytes bounds each JSON message independently.
const MaxMessageBytes = 4 << 10

const messageLengthBytes = 4

var (
	// ErrMessageTooLarge means a length prefix exceeds MaxMessageBytes.
	ErrMessageTooLarge = errors.New("nativeevents: message too large")
	// ErrInvalidMessage means a message is malformed or violates the closed wire contract.
	ErrInvalidMessage = errors.New("nativeevents: invalid message")
)

// Operation identifies one native-event link message.
type Operation string

// Native-event link operations.
const (
	OperationHello       Operation = "hello"
	OperationHelloResult Operation = "hello_result"
	OperationEvent       Operation = "event"
	OperationAck         Operation = "ack"
)

// EmitterType names the trusted integration surface that emitted an event.
type EmitterType string

// Emitter types.
const (
	EmitterTypePlugin EmitterType = "plugin"
	EmitterTypeHook   EmitterType = "hook"
	EmitterTypeMod    EmitterType = "mod"
)

// Valid reports whether e is a known emitter type.
func (e EmitterType) Valid() bool {
	switch e {
	case EmitterTypePlugin, EmitterTypeHook, EmitterTypeMod:
		return true
	default:
		return false
	}
}

// Hello is the first message on every connection.
type Hello struct {
	Operation      Operation        `json:"op"`
	Harness        protocol.Harness `json:"harness"`
	HarnessVersion string           `json:"harness_version"`
	EmitterType    EmitterType      `json:"emitter_type"`
	SchemaVersion  int64            `json:"schema_version"`
}

// Validate checks a hello independently of the run it will be matched to.
func (h Hello) Validate() error {
	if h.Operation != OperationHello || !h.Harness.Valid() ||
		!validHarnessVersion(h.HarnessVersion) || !h.EmitterType.Valid() ||
		h.SchemaVersion < 1 {
		return ErrInvalidMessage
	}
	return nil
}

// HelloRejectReason is the complete rejection vocabulary.
type HelloRejectReason string

// Hello rejection reasons.
const (
	HelloRejectUnsupportedSchema     HelloRejectReason = "unsupported_schema"
	HelloRejectHarnessMismatch       HelloRejectReason = "harness_mismatch"
	HelloRejectPeerNotInTree         HelloRejectReason = "peer_not_in_tree"
	HelloRejectObservationIncomplete HelloRejectReason = "observation_incomplete"
)

// Valid reports whether r is a known rejection reason.
func (r HelloRejectReason) Valid() bool {
	switch r {
	case HelloRejectUnsupportedSchema, HelloRejectHarnessMismatch,
		HelloRejectPeerNotInTree, HelloRejectObservationIncomplete:
		return true
	default:
		return false
	}
}

// HelloResult accepts a bound emitter or rejects it with one closed reason.
type HelloResult struct {
	Operation Operation         `json:"op"`
	Accepted  bool              `json:"accepted"`
	Reason    HelloRejectReason `json:"reason,omitempty"`
}

// Validate checks the accepted/rejected shape.
func (r HelloResult) Validate() error {
	if r.Operation != OperationHelloResult || r.Accepted && r.Reason != "" ||
		!r.Accepted && !r.Reason.Valid() {
		return ErrInvalidMessage
	}
	return nil
}

// Event is one content-free lifecycle observation from an accepted emitter.
type Event struct {
	Operation      Operation                  `json:"op"`
	Sequence       int64                      `json:"seq"`
	EmitterEventID protocol.OpaqueID          `json:"emitter_event_id"`
	Type           protocol.EventType         `json:"type"`
	ObservedAt     int64                      `json:"observed_at"`
	SessionRef     protocol.OpaqueID          `json:"session_ref,omitempty"`
	TurnRef        protocol.OpaqueID          `json:"turn_ref,omitempty"`
	ToolRef        protocol.OpaqueID          `json:"tool_ref,omitempty"`
	RequestRef     protocol.OpaqueID          `json:"request_ref,omitempty"`
	Result         protocol.NativeEventResult `json:"result,omitempty"`
	ToolKind       protocol.NativeToolKind    `json:"tool_kind,omitempty"`
}

// Validate checks the closed event shape. Ordering and replay conflicts need
// connection/session state and are enforced by the server.
func (e Event) Validate() error {
	if e.Operation != OperationEvent || e.Sequence < 1 || e.ObservedAt < 1 ||
		!validRef(e.EmitterEventID, true) || !protocol.IsNativeEventType(e.Type) ||
		!validRef(e.SessionRef, false) || !validRef(e.TurnRef, false) ||
		!validRef(e.ToolRef, false) || !validRef(e.RequestRef, false) ||
		e.Result != "" && !e.Result.Valid() || e.ToolKind != "" && !e.ToolKind.Valid() {
		return ErrInvalidMessage
	}
	if protocol.NativeEventNeedsToolRef(e.Type) && e.ToolRef == "" ||
		protocol.NativeEventNeedsRequestRef(e.Type) && e.RequestRef == "" ||
		e.Type == protocol.EventTypeQuestionResolved && e.Result == "" {
		return ErrInvalidMessage
	}
	return nil
}

// Ack identifies one event after its durable Core write completed.
type Ack struct {
	Operation      Operation         `json:"op"`
	Sequence       int64             `json:"seq"`
	EmitterEventID protocol.OpaqueID `json:"emitter_event_id"`
}

// Validate checks the ack identity.
func (a Ack) Validate() error {
	if a.Operation != OperationAck || a.Sequence < 1 || !validRef(a.EmitterEventID, true) {
		return ErrInvalidMessage
	}
	return nil
}

func validRef(value protocol.OpaqueID, required bool) bool {
	if value == "" {
		return !required
	}
	return len(value) <= 64 && value.Valid()
}

func validHarnessVersion(version string) bool {
	if !utf8.ValidString(version) {
		return false
	}
	n := 0
	for _, r := range version {
		if !unicode.IsPrint(r) || unicode.IsControl(r) {
			return false
		}
		n++
	}
	return n >= 1 && n <= 64
}

// WriteMessage writes a four-byte big-endian length and one validated JSON
// object. It writes nothing when validation or encoding fails.
func WriteMessage(w io.Writer, message any) error {
	if err := validateMessage(message); err != nil {
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
			return fmt.Errorf("nativeevents: write message: %w", err)
		}
		if n == 0 {
			return fmt.Errorf("nativeevents: write message: %w", io.ErrShortWrite)
		}
		buf = buf[n:]
	}
	return nil
}

// ReadMessage reads and strictly decodes one length-delimited message. The
// returned concrete type is Hello, HelloResult, Event or Ack.
func ReadMessage(r io.Reader) (any, error) {
	var prefix [messageLengthBytes]byte
	if _, err := io.ReadFull(r, prefix[:]); err != nil {
		return nil, err //nolint:wrapcheck // EOF shape is part of the contract
	}
	size := binary.BigEndian.Uint32(prefix[:])
	if size > MaxMessageBytes {
		return nil, ErrMessageTooLarge
	}
	body := make([]byte, size)
	if _, err := io.ReadFull(r, body); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}
	var header struct {
		Operation Operation `json:"op"`
	}
	if err := json.Unmarshal(body, &header); err != nil {
		return nil, fmt.Errorf("%w: decode operation", ErrInvalidMessage)
	}
	var message any
	switch header.Operation {
	case OperationHello:
		message = &Hello{}
	case OperationHelloResult:
		message = &HelloResult{}
	case OperationEvent:
		message = &Event{}
	case OperationAck:
		message = &Ack{}
	default:
		return nil, fmt.Errorf("%w: unknown operation", ErrInvalidMessage)
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(message); err != nil {
		return nil, fmt.Errorf("%w: decode", ErrInvalidMessage)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%w: trailing JSON", ErrInvalidMessage)
	}
	if err := validateMessage(message); err != nil {
		return nil, err
	}
	switch value := message.(type) {
	case *Hello:
		return *value, nil
	case *HelloResult:
		return *value, nil
	case *Event:
		return *value, nil
	case *Ack:
		return *value, nil
	default:
		panic("unreachable native event message type")
	}
}

func validateMessage(message any) error {
	var err error
	switch value := message.(type) {
	case Hello:
		err = value.Validate()
	case *Hello:
		err = value.Validate()
	case HelloResult:
		err = value.Validate()
	case *HelloResult:
		err = value.Validate()
	case Event:
		err = value.Validate()
	case *Event:
		err = value.Validate()
	case Ack:
		err = value.Validate()
	case *Ack:
		err = value.Validate()
	default:
		err = ErrInvalidMessage
	}
	if err != nil {
		return fmt.Errorf("%w: validate", err)
	}
	return nil
}
