package capture

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/smellak/gifford/internal/clock"
)

// readChunkSize is the fixed read buffer: 32 KiB.
const readChunkSize = 32 * 1024

// readStamp is one pane read: its bytes with a single wall and
// monotonic stamp.
type readStamp struct {
	wall time.Time
	mono int64
	data []byte
}

// capLog is the ingest's bounded log: it appends text lines until
// log_max_bytes, writes one final limit line, then drops.
type capLog struct {
	mu    sync.Mutex
	file  *os.File
	limit int64
	used  int64
	done  bool
	clock clock.Clock
}

// openLog opens the session log for appends, measuring what is there.
func (r *ingestRun) openLog() error {
	f, err := r.root.OpenFile(r.opts.Agent+"/"+r.opts.Session+"/ingest.log",
		os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("%w: open log: %w", ErrUnsafePath, err)
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("%w: stat log: %w", ErrUnsafePath, err)
	}
	r.log = &capLog{file: f, limit: r.lim.LogMaxBytes, used: fi.Size(), clock: r.deps.Clock}
	return nil
}

// logLine appends one timestamped line within the bound.
func (r *ingestRun) logLine(msg string) {
	if r.log == nil {
		return
	}
	r.log.write(formatWall(r.deps.Clock.Now()) + " " + msg + "\n")
}

// write appends a line, the limit line once, then nothing.
func (l *capLog) write(line string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.done {
		return
	}
	if l.used+int64(len(line)) > l.limit {
		_, _ = fmt.Fprintln(l.file, formatWall(l.clock.Now())+" log limit reached")
		l.done = true
		return
	}
	if _, err := fmt.Fprint(l.file, line); err != nil {
		return
	}
	l.used += int64(len(line))
}

// queueCap bounds the read queue in 32 KiB reads.
func queueCap(queueMax int64) int {
	n := int(queueMax / readChunkSize)
	if n < 1 {
		n = 1
	}
	return n
}

// readLoop is the reader goroutine: it stamps each read once and
// queues it without ever blocking. On overflow it closes the stream
// itself, drops that read and everything after it, and records the
// stall. EOF and errors end the queue.
func (r *ingestRun) readLoop() {
	defer close(r.queue)
	defer close(r.readerDone)
	buf := make([]byte, readChunkSize)
	fd := r.stream.Fd()
	for {
		if r.streamDead.Load() || r.stopFlag.Load() {
			return
		}
		ready, err := pollReadable(fd, readPollMs)
		if err != nil {
			if pollInterrupted(err) {
				continue
			}
			r.readErr = fmt.Errorf("pane poll: %w", err)
			return
		}
		if !ready {
			continue
		}
		n, retry, err := readReady(int(fd), buf)
		if err != nil {
			r.readErr = err
			return
		}
		if retry {
			continue
		}
		if n == 0 {
			r.readEOF = true
			return
		}
		stamp := readStamp{
			wall: r.deps.Clock.Now(),
			mono: r.deps.Mono.MonotonicNS(),
			data: bytes.Clone(buf[:n]),
		}
		select {
		case r.queue <- stamp:
			r.queuedBytes.Add(int64(n))
		default:
			r.readStall = true
			r.streamClose()
			return
		}
	}
}
