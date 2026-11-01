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
// No delivery starts while outer input remains unclassified: a lone ESC,
// a cut escape sequence or paste marker, a cut rune, or a writer chunk the
// loop has not folded. Those bytes are already on the harness, and a line
// written after them would join them. Silence never proves a sequence ended,
// so no timer resolves it. Transport uncertainty suspends guard settlement
// but is not human evidence: it changes no draft state, version, generation,
// activity, or human timestamp. Once classified, human events move the guard;
// a proven passive terminal response changes nothing.
//
// Nor does a delivery start while the harness paints: prepare and commit
// both refuse until 300 ms of the session's clock have passed since the
// harness's last output, so a line never lands inside a redraw. The
// Proxy's own notices are not harness output. A harness that never
// pauses that long, such as a fast spinner, receives no delivery while
// it lasts: the Core defers and tries again.
//
// ANALYZE activity has a separate effective clock. A deliberately small,
// stateful classifier may prove repeated rendition or bracketed-paste
// assertions inert; everything else, including incomplete or uncovered
// control sequences, remains activity. This affects only observation output
// generation and byte/quiet features. Raw last-output timing, screen writes,
// pending output, recording and guard Output/Observe still receive every
// byte, so filtering never authorizes delivery or closes a draft.
//
// On a measured profile, one with an input line pattern, a delivery
// also needs positive evidence that the input line is empty: prepare
// and commit read the emulator's last snapshot as the consume check
// does, and refuse unless nothing is held back, no synchronized update
// is open, the cursor is visible and the input line under it reads
// empty. On a profile that declares an input placeholder, empty means
// the placeholder drawn dimmed with the cursor on it, never the prompt
// alone, and the placeholder's text undimmed is a draft. The draft
// guard's CLOSED never reads the screen again, so this is what keeps a
// delivery off a draft that came back after a torn frame. A measured
// profile without an input line pattern needs no such evidence.
//
// A profile not yet measured never delivers: prepare and commit refuse
// every delivery and write nothing to the harness, whatever the guard
// says, after a submit, a cancel or an unlock alike. Its keys and input
// line are xterm guesses, so nothing can prove the input line empty.
// Human keys and the recording are unaffected.
//
// On a measured profile a submit, a clear or a cancel closes the draft
// only once the harness has reacted to it, see package draft, so the
// guard's state follows the session's clock: the loop settles the guard
// on its ticks, and every reader settles it first. Nothing in the
// session unlocks a guard that went UNKNOWN: it refuses every delivery
// for the rest of the session, while human keys still flow.
//
// Nor does a classified key that may have left text on the input line keep
// the draft CLOSED, see package draft: while none is open, any key that is
// not text, a paste or a submit, a clear or a cancel, and any sequence the
// parser does not recognise, opens one at the key itself. If output crosses
// input that is still unclassified, its recording chunk is deferred. A human
// event is therefore recorded before that output; a passive terminal response
// releases it without inventing a draft event. Replay and the Analyst never
// project the crossed screen under a stale CLOSED state.
//
// Deferred output leaves the heap after 256 KiB for a private, unlinked spool
// capped at 16 MiB. If that quota fills, the session emits capture quota and
// gap events, stops capture, and returns ErrDeferredOutputLimit when the child
// ends. Screen tracking, the PTY, human input and the unresolved guard remain
// live; capacity never resolves input or fabricates human activity.
//
// The writer puts each outer-input chunk on the harness before the loop takes
// its copy, and the loop may take harness output first. Such output is treated
// conservatively as a re-read and kept unrecorded until classification; it
// cannot confirm an unseen key. Output read before a later chunk was sent is
// likewise no reaction to it. Only output whose captured sent count exactly
// matches the loop's classified chunk count is established as a reaction.
//
// A DurableQueue given as the sink, or beside it, keeps the session's events
// of the durable allowlist for the agent socket. Each is built once, with its
// own id, and carries counts, generations and fixed codes only: no text, no
// output, no screen and no content hash. The queue never makes the session
// wait: past MaxDurableBytes a new event is dropped and counted.
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
