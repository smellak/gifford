package fuzz

import (
	"context"
	"fmt"
	"io"
	"math"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/smellak/gifford/internal/vt"
)

// The outer terminal every scenario runs on.
const (
	cols = 80
	rows = 24
)

// inputLine reads the draft off the fake harness's input line.
var inputLine = regexp.MustCompile(InputLine)

// screen is what the human sees: the harness output through an
// emulator, so the human can wait for its keys to show before pressing
// Enter. Safe for concurrent use.
type screen struct {
	mu      sync.Mutex
	emu     *vt.Emulator
	changed chan struct{}
	ended   bool
}

// newScreen opens an empty screen the size of the outer terminal.
func newScreen() (*screen, error) {
	emu, err := vt.New(cols, rows)
	if err != nil {
		return nil, fmt.Errorf("fuzz: screen: %w", err)
	}
	return &screen{emu: emu, changed: make(chan struct{})}, nil
}

// read draws everything r gives until it fails, then ends the screen.
func (s *screen) read(r io.Reader) {
	buf := make([]byte, 32*1024)
	for {
		n, err := r.Read(buf)
		s.mu.Lock()
		if n > 0 {
			_, _ = s.emu.Write(buf[:n])
		}
		if err != nil {
			s.ended = true
			_ = s.emu.Close()
		}
		close(s.changed)
		s.changed = make(chan struct{})
		s.mu.Unlock()
		if err != nil {
			return
		}
	}
}

// showsDraft waits up to patience for the cursor row to show the prompt
// and then draft, as the screen keeps a line: without trailing spaces.
func (s *screen) showsDraft(ctx context.Context, draft string, patience time.Duration) bool {
	want := strings.TrimRight(draft, " ")
	timer := time.NewTimer(patience)
	defer timer.Stop()
	for {
		s.mu.Lock()
		ok := false
		if !s.ended {
			snap := s.emu.Snapshot()
			if snap.CursorRow >= 0 && snap.CursorRow < len(snap.Lines) {
				m := inputLine.FindStringSubmatch(snap.Lines[snap.CursorRow])
				ok = m != nil && m[1] == want
			}
		}
		changed, ended := s.changed, s.ended
		s.mu.Unlock()
		if ok {
			return true
		}
		if ended {
			return false
		}
		select {
		case <-changed:
		case <-timer.C:
			return false
		case <-ctx.Done():
			return false
		}
	}
}

// progress is how far the human got: 2i as it starts writing step i,
// 2i+1 once step i is written. Safe for concurrent use.
type progress struct {
	mu      sync.Mutex
	pos     int
	changed chan struct{}
}

// done is the position of a human that took its last step.
const done = math.MaxInt

func newProgress() *progress { return &progress{pos: -1, changed: make(chan struct{})} }

// set moves the human to pos.
func (p *progress) set(pos int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pos = pos
	close(p.changed)
	p.changed = make(chan struct{})
}

// at is where the human is now.
func (p *progress) at() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pos
}

// reach waits until the human gets to pos or stops; false when ctx
// ends first.
func (p *progress) reach(ctx context.Context, pos int) bool {
	for {
		p.mu.Lock()
		there, changed := p.pos >= pos, p.changed
		p.mu.Unlock()
		if there {
			return true
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return false
		}
	}
}

// position is where an anchor falls in the human's progress.
func position(a Anchor) int {
	if a.With {
		return 2 * a.Step
	}
	return 2*a.Step + 1
}

// human types a script on the outer terminal, step by step.
type human struct {
	keys   io.Writer
	screen *screen
	clock  *virtualClock
	at     *progress
	// patience bounds each look at the screen; past it the human
	// presses Enter anyway.
	patience time.Duration
	// blind counts the looks that ran out of patience.
	blind int
}

// run takes every step, then waits for an empty prompt and stops. It
// never waits for the injector.
func (h *human) run(ctx context.Context, steps []Step) error {
	for i, st := range steps {
		h.clock.advance(st.Wait)
		if !sleep(ctx, st.Real) {
			return stopped(ctx)
		}
		if st.Look && !h.screen.showsDraft(ctx, st.Draft, h.patience) {
			h.blind++
		}
		if err := stopped(ctx); err != nil {
			return err
		}
		h.at.set(2 * i)
		if st.Keys != "" {
			if _, err := io.WriteString(h.keys, st.Keys); err != nil {
				return fmt.Errorf("fuzz: human step %d: %w", i, err)
			}
		}
		h.at.set(2*i + 1)
	}
	if !h.screen.showsDraft(ctx, "", h.patience) {
		h.blind++
	}
	h.at.set(done)
	return stopped(ctx)
}

// stopped is why ctx ended, or nil while it runs.
func stopped(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("fuzz: %w", err)
	}
	return nil
}

// sleep waits d of real time; false when ctx ends first.
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
