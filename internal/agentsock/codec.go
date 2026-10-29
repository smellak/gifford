package agentsock

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/smellak/gifford/internal/protocol"
)

// MaxFrameBytes bounds the JSON of one frame, the same bound as the
// control API's.
const MaxFrameBytes = 1 << 20

// lengthBytes is the size of a frame's length prefix.
const lengthBytes = 4

var (
	// ErrFrameTooLarge means a length prefix past MaxFrameBytes. The
	// stream cannot be read past it: the caller closes the connection.
	ErrFrameTooLarge = errors.New("agentsock: frame too large")
	// ErrInvalidFrame means a whole frame was read that is not a valid
	// protocol.AgentFrame. The stream stays aligned on the next frame.
	ErrInvalidFrame = errors.New("agentsock: invalid frame")
)

// WriteFrame writes one frame: its JSON length as 4 bytes big-endian,
// then the JSON, in a single Write. It refuses a frame that does not
// validate or does not fit MaxFrameBytes, writing nothing.
func WriteFrame(w io.Writer, f protocol.AgentFrame) error {
	if err := f.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidFrame, err)
	}
	body, err := json.Marshal(f)
	if err != nil {
		return fmt.Errorf("%w: not encoded", ErrInvalidFrame)
	}
	if len(body) > MaxFrameBytes {
		return ErrFrameTooLarge
	}
	buf := make([]byte, lengthBytes, lengthBytes+len(body))
	binary.BigEndian.PutUint32(buf, uint32(len(body))) //nolint:gosec // bounded by MaxFrameBytes above
	if _, err := w.Write(append(buf, body...)); err != nil {
		return fmt.Errorf("agentsock: write frame: %w", err)
	}
	return nil
}

// ReadFrame reads one frame and validates it with
// protocol.AgentFrame.Validate. At the end of the stream, between
// frames, it returns io.EOF, and io.ErrUnexpectedEOF inside one. A
// length past MaxFrameBytes is ErrFrameTooLarge, read no further. A
// frame read whole but not valid is ErrInvalidFrame, with the frame's
// frame_id in the returned frame when it could be read, so the error
// can answer it; its reason never quotes the frame.
func ReadFrame(r io.Reader) (protocol.AgentFrame, error) {
	var prefix [lengthBytes]byte
	if _, err := io.ReadFull(r, prefix[:]); err != nil {
		return protocol.AgentFrame{}, err //nolint:wrapcheck // io.EOF and io.ErrUnexpectedEOF are the contract
	}
	n := binary.BigEndian.Uint32(prefix[:])
	if n > MaxFrameBytes {
		return protocol.AgentFrame{}, ErrFrameTooLarge
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return protocol.AgentFrame{}, err
	}
	var f protocol.AgentFrame
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil || dec.More() {
		return protocol.AgentFrame{FrameID: frameID(body)}, fmt.Errorf("%w: not a frame", ErrInvalidFrame)
	}
	if err := f.Validate(); err != nil {
		return protocol.AgentFrame{FrameID: frameID(body)}, fmt.Errorf("%w: does not validate", ErrInvalidFrame)
	}
	return f, nil
}

// frameID reads only the frame_id of a frame that did not validate, or
// 0 when there is none to answer.
func frameID(body []byte) int64 {
	var id struct {
		FrameID int64 `json:"frame_id"`
	}
	if json.Unmarshal(body, &id) != nil || id.FrameID < 1 {
		return 0
	}
	return id.FrameID
}
