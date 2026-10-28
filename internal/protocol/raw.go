package protocol

import (
	"fmt"
)

// Payload is raw JSON carried inside an envelope or frame. On the wire it
// is always a JSON object; it copies bytes like json.RawMessage.
type Payload []byte

// MarshalJSON encodes the raw bytes unchanged.
func (m Payload) MarshalJSON() ([]byte, error) {
	if len(m) == 0 {
		return []byte("{}"), nil
	}
	return []byte(m), nil
}

// UnmarshalJSON copies the raw bytes.
func (m *Payload) UnmarshalJSON(data []byte) error {
	if m == nil {
		return fmt.Errorf("unmarshal payload into nil receiver")
	}
	*m = append((*m)[:0], data...)
	return nil
}

// Value is raw JSON of any shape. It copies bytes like json.RawMessage.
type Value []byte

// MarshalJSON encodes the raw bytes unchanged.
func (m Value) MarshalJSON() ([]byte, error) {
	if len(m) == 0 {
		return []byte("{}"), nil
	}
	return []byte(m), nil
}

// UnmarshalJSON copies the raw bytes.
func (m *Value) UnmarshalJSON(data []byte) error {
	if m == nil {
		return fmt.Errorf("unmarshal value into nil receiver")
	}
	*m = append((*m)[:0], data...)
	return nil
}

// ControlVersions is the node control epoch vector: every scope the gate
// consulted, with 0 for scopes without a row. Opaque here: values are
// stored and echoed, never interpreted (key grammar in the v1.1 spec).
type ControlVersions map[string]int64
