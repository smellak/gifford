//go:build unix

package session

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/smellak/gifford/internal/protocol"
)

// deferredOutputMemoryLimit is the most unresolved harness output retained in
// the session buffer before it is spooled. The output pump reads at most 32 KiB
// at a time, so the live allocation can exceed this only by one pump read.
const deferredOutputMemoryLimit = 256 << 10

// deferredOutputSpoolLimit bounds the private on-disk copy as well as the
// heap. Reaching it disables capture observably but never the PTY or keyboard.
const deferredOutputSpoolLimit = 16 << 20

// deferredOutput keeps output that crossed unresolved input off the heap until
// its ordering can be established. Its file is opened inside recordings/ with
// private permissions and immediately unlinked, so a crash leaves no second
// recording behind.
type deferredOutput struct {
	file     *os.File
	count    int64
	bytes    int64
	maxPiece int64
	limit    int64
	broken   bool
}

type deferredOutputHeader struct {
	MonotonicNS     int64           `json:"monotonic_ns"`
	Length          int64           `json:"length"`
	ScreenHashAfter protocol.Digest `json:"screen_hash_after"`
}

func newDeferredOutput(dir, sessionID string, maxPiece int, limit int64) (*deferredOutput, error) {
	f, err := os.CreateTemp(dir, "."+sessionID+"-deferred-*")
	if err != nil {
		return nil, fmt.Errorf("session: create deferred output: %w", err)
	}
	path := f.Name()
	if err := os.Remove(path); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return nil, fmt.Errorf("session: unlink deferred output: %w", err)
	}
	return &deferredOutput{file: f, maxPiece: int64(maxPiece), limit: limit}, nil
}

func (d *deferredOutput) append(mono int64, piece pendingPiece) error {
	if d.broken {
		return errDeferredOutputBroken
	}
	head, err := json.Marshal(deferredOutputHeader{
		MonotonicNS:     mono,
		Length:          int64(len(piece.data)),
		ScreenHashAfter: piece.hash,
	})
	if err != nil {
		d.broken = true
		return fmt.Errorf("session: encode deferred output: %w", err)
	}
	written := int64(len(head) + 1 + len(piece.data))
	if d.bytes+written > d.limit {
		return &deferredOutputLimitError{bytes: d.bytes + written, limit: d.limit}
	}
	if err := writeFull(d.file, append(head, '\n')); err != nil {
		d.broken = true
		return fmt.Errorf("session: write deferred output header: %w", err)
	}
	if err := writeFull(d.file, piece.data); err != nil {
		d.broken = true
		return fmt.Errorf("session: write deferred output bytes: %w", err)
	}
	d.count++
	d.bytes += written
	return nil
}

// drain replays every deferred piece without loading more than one bounded
// piece at once. A failed drain is terminal: retrying could duplicate pieces
// already appended to the recording.
func (d *deferredOutput) drain(apply func(int64, pendingPiece) error) error {
	if d.broken {
		return errDeferredOutputBroken
	}
	if _, err := d.file.Seek(0, io.SeekStart); err != nil {
		d.broken = true
		return fmt.Errorf("session: seek deferred output: %w", err)
	}
	r := bufio.NewReader(d.file)
	for range d.count {
		line, err := r.ReadBytes('\n')
		if err != nil {
			d.broken = true
			return fmt.Errorf("session: read deferred output header: %w", err)
		}
		var head deferredOutputHeader
		if err := json.Unmarshal(line, &head); err != nil {
			d.broken = true
			return fmt.Errorf("session: decode deferred output header: %w", err)
		}
		if head.Length < 0 || head.Length > d.maxPiece {
			d.broken = true
			return fmt.Errorf("session: deferred output length %d exceeds %d", head.Length, d.maxPiece)
		}
		if !head.ScreenHashAfter.Valid() {
			d.broken = true
			return fmt.Errorf("session: invalid deferred output screen hash")
		}
		data := make([]byte, head.Length)
		if _, err := io.ReadFull(r, data); err != nil {
			d.broken = true
			return fmt.Errorf("session: read deferred output bytes: %w", err)
		}
		if err := apply(head.MonotonicNS, pendingPiece{data: data, hash: head.ScreenHashAfter}); err != nil {
			d.broken = true
			return err
		}
	}
	if err := d.file.Truncate(0); err != nil {
		d.broken = true
		return fmt.Errorf("session: truncate deferred output: %w", err)
	}
	if _, err := d.file.Seek(0, io.SeekStart); err != nil {
		d.broken = true
		return fmt.Errorf("session: reset deferred output: %w", err)
	}
	d.count = 0
	d.bytes = 0
	return nil
}

func (d *deferredOutput) close() error {
	if d == nil || d.file == nil {
		return nil
	}
	err := d.file.Close()
	d.file = nil
	if err != nil {
		return fmt.Errorf("session: close deferred output: %w", err)
	}
	return nil
}

var errDeferredOutputBroken = errors.New("session: deferred output is unusable")

type deferredOutputLimitError struct {
	bytes int64
	limit int64
}

func (e *deferredOutputLimitError) Error() string {
	return fmt.Sprintf("session: deferred output needs %d bytes, limit %d: %v", e.bytes, e.limit, ErrDeferredOutputLimit)
}

func (e *deferredOutputLimitError) Unwrap() error { return ErrDeferredOutputLimit }
