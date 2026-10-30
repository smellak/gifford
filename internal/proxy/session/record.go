// Package session wraps one harness run with a reconstructed screen, a
// raw recording and input metadata. It drives the proxy core with an
// intermediate PTY pair so the core stays untouched: the core copies
// between the child and the middle terminal, while this package pumps
// between the middle terminal and the real one, tapping output into
// internal/vt and the recording and input into the 3.4 parser and the 3.5
// guard. Typed content is never persisted, not even as a debug option:
// the recording holds outer output only, and input events carry counts
// and types.
//
// No delivery starts while the parser holds part of a human key
// sequence: a lone ESC, a cut escape sequence or paste marker, or a cut
// rune. Those bytes are already on the harness, and a line written after
// them would join them. Silence never proves a sequence ended, so no
// timer ends a hold: a lone ESC holds every delivery until the next
// human key resolves it. A hold is no guard transition and moves no
// input guard version, so the Core learns of it only when its prepare
// is refused.
package session

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

// chunkHeader is one recording record head: a JSON line followed by
// exactly length raw bytes. Wall time is RFC3339Nano UTC; monotonic_ns
// counts nanoseconds from the session start; sequence starts at zero.
type chunkHeader struct {
	WallTime    string `json:"wall_time"`
	MonotonicNS int64  `json:"monotonic_ns"`
	Sequence    int64  `json:"sequence"`
	Length      int64  `json:"length"`
	ANSIPresent bool   `json:"ansi_present"`
}

// Chunk is one decoded recording record: intact output bytes with their
// capture metadata.
type Chunk struct {
	WallTime    time.Time
	MonotonicNS int64
	Sequence    int64
	ANSIPresent bool
	Data        []byte
}

// Recorder appends length-delimited raw chunks to a recording stream.
type Recorder struct {
	w   io.Writer
	seq int64
}

// NewRecorder writes recording records to w.
func NewRecorder(w io.Writer) *Recorder { return &Recorder{w: w} }

// Append writes one record: the header line plus data untouched, no
// escape stripping, no recoding. It returns the record sequence.
func (r *Recorder) Append(wall time.Time, mono int64, data []byte) (int64, error) {
	head, err := json.Marshal(chunkHeader{
		WallTime:    wall.UTC().Format(time.RFC3339Nano),
		MonotonicNS: mono,
		Sequence:    r.seq,
		Length:      int64(len(data)),
		ANSIPresent: bytes.Contains(data, []byte{0x1b}),
	})
	if err != nil {
		return 0, fmt.Errorf("session: encode chunk header: %w", err)
	}
	if _, err := r.w.Write(append(head, '\n')); err != nil {
		return 0, fmt.Errorf("session: write chunk header: %w", err)
	}
	if _, err := r.w.Write(data); err != nil {
		return 0, fmt.Errorf("session: write chunk bytes: %w", err)
	}
	seq := r.seq
	r.seq++
	return seq, nil
}

// ReadChunk decodes one record from a recording stream: the header line
// plus exactly length raw bytes. It returns io.EOF on a clean stream end
// and io.ErrUnexpectedEOF on a torn record.
func ReadChunk(r *bufio.Reader) (Chunk, error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		if errors.Is(err, io.EOF) && len(line) == 0 {
			return Chunk{}, io.EOF
		}
		return Chunk{}, io.ErrUnexpectedEOF
	}
	var head chunkHeader
	if err := json.Unmarshal(line, &head); err != nil {
		return Chunk{}, fmt.Errorf("session: decode chunk header: %w", err)
	}
	if head.Length < 0 {
		return Chunk{}, fmt.Errorf("session: negative chunk length %d", head.Length)
	}
	wall, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(head.WallTime))
	if err != nil {
		return Chunk{}, fmt.Errorf("session: bad chunk wall_time: %w", err)
	}
	data := make([]byte, head.Length)
	if _, err := io.ReadFull(r, data); err != nil {
		return Chunk{}, io.ErrUnexpectedEOF
	}
	return Chunk{
		WallTime:    wall,
		MonotonicNS: head.MonotonicNS,
		Sequence:    head.Sequence,
		ANSIPresent: head.ANSIPresent,
		Data:        data,
	}, nil
}

// Replay feeds every record of a recording stream to apply in order. A
// screen rebuilt this way matches one rebuilt live from the same bytes.
func Replay(r io.Reader, apply func(Chunk) error) error {
	br, ok := r.(*bufio.Reader)
	if !ok {
		br = bufio.NewReader(r)
	}
	for {
		chunk, err := ReadChunk(br)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := apply(chunk); err != nil {
			return err
		}
	}
}
