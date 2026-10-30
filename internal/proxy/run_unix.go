//go:build unix

package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"runtime/debug"
	"sync"
	"syscall"

	"github.com/creack/pty"
	"golang.org/x/sys/unix"
	"golang.org/x/term"
)

var (
	// errChildClosed reports the child's end of the PTY hung up.
	errChildClosed = errors.New("proxy: child closed")
	// errOuterClosed reports the outer terminal hung up.
	errOuterClosed = errors.New("proxy: outer terminal closed")
)

// eventKind names the goroutine reporting an event.
type eventKind int

const (
	eventInput eventKind = iota
	eventOutput
	eventWaiter
)

// event is one goroutine's report to the loop.
type event struct {
	kind       eventKind
	err        error
	state      *os.ProcessState
	panicValue any
	stack      []byte
}

// session is one Run: the harness, both ends of the pipe and the three
// goroutines. Only the loop changes its state.
type session struct {
	cmd    *exec.Cmd
	in     *os.File
	inR    *fdReader
	out    io.Writer
	child  *childEnd
	ptmx   *os.File
	c      *canceler
	events chan event
	wg     sync.WaitGroup
}

// teardown is the loop's state: what ended, what was recorded, who is left.
type teardown struct {
	pid      int
	canceler *canceler
	stopped  bool
	hungUp   bool
	closed   bool
	pumps    int
	reported int
	exited   bool
	exit     Exit
	runErr   error
	panicked *goroutinePanic
}

// goroutinePanic carries a recovered goroutine panic back through Run.
type goroutinePanic struct {
	value any
	stack []byte
}

// Error describes the recovered panic and where it happened.
func (p *goroutinePanic) Error() string {
	return "proxy: goroutine panic: " + fmt.Sprint(p.value) + "\n\n" + string(p.stack)
}

// endOfStream reports a hung-up descriptor: EOF on Linux, EIO on ptmx once
// every holder of the child's end closed it.
func endOfStream(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, unix.EIO)
}

// writeAll writes all of b to the blocking outer terminal.
func writeAll(out io.Writer, b []byte) error {
	for len(b) > 0 {
		n, err := out.Write(b)
		b = b[n:]
		if err != nil {
			return fmt.Errorf("write outer: %w", err)
		}
	}
	return nil
}

// terminalFD reports whether f is a terminal.
func terminalFD(f *os.File, name string) error {
	if f == nil || !term.IsTerminal(int(f.Fd())) {
		return fmt.Errorf("%s: %w", name, ErrNotTerminal)
	}
	return nil
}

// run is Run on unix: validation, the harness command and the daemon probe
// (§6.1 steps 1-5), then runWithWriter for the raw terminal, the session,
// the loop and the teardown (steps 6-11, §6.2-§6.4).
func run(ctx context.Context, opts Options) (exit Exit, rerr error) {
	if len(opts.Command) == 0 {
		return Exit{}, ErrNoCommand
	}
	if err := ctx.Err(); err != nil {
		return Exit{}, fmt.Errorf("proxy: %w", err)
	}
	if err := terminalFD(opts.In, "stdin"); err != nil {
		return Exit{}, err
	}
	if err := terminalFD(opts.Out, "stdout"); err != nil {
		return Exit{}, err
	}
	// The harness command line is explicit user input, like a shell's.
	cmd := exec.CommandContext(context.WithoutCancel(ctx), opts.Command[0], opts.Command[1:]...) // #nosec G204
	if cmd.Err != nil {
		return Exit{}, fmt.Errorf("start %s: %w", opts.Command[0], cmd.Err)
	}
	cmd.Env = opts.Env
	if !daemonRunning(ctx, opts.DaemonSocket) && opts.Notice != nil {
		_, _ = opts.Notice.Write([]byte(DaemonNotice + "\n"))
	}
	sigs := opts.Signals
	if sigs == nil {
		ch := make(chan os.Signal, 8)
		signal.Notify(ch, handledSignals()...)
		defer signal.Stop(ch)
		sigs = ch
	}
	return runWithWriter(ctx, cmd, opts.In, opts.Out, sigs)
}

// runWithWriter starts cmd on a child PTY sized like in, puts in raw and
// copies between the child and out until the harness exits. in must be a
// terminal; out is the session's output, normally the same terminal. Every
// goroutine starts through spawn and has returned before this does; in is
// restored on every path out, panics included.
func runWithWriter(ctx context.Context, cmd *exec.Cmd, in *os.File, out io.Writer, sigs <-chan os.Signal) (exit Exit, rerr error) {
	size, err := pty.GetsizeFull(in)
	if err != nil {
		return Exit{}, fmt.Errorf("size: %w", err)
	}
	c, err := newCanceler()
	if err != nil {
		return Exit{}, err
	}
	defer func() {
		if cerr := c.close(); cerr != nil {
			rerr = errors.Join(rerr, cerr)
		}
	}()
	inR, err := c.reader(in)
	if err != nil {
		return Exit{}, err
	}
	inFD := int(in.Fd())
	old, err := term.MakeRaw(inFD)
	if err != nil {
		return Exit{}, fmt.Errorf("raw: %w", err)
	}
	defer func() {
		if serr := term.Restore(inFD, old); serr != nil && !errors.Is(serr, unix.EIO) {
			rerr = errors.Join(rerr, fmt.Errorf("restore: %w", serr))
		}
	}()
	ptmx, err := pty.StartWithSize(cmd, size)
	if err != nil {
		return Exit{}, fmt.Errorf("start %s: %w", cmd.Args[0], err)
	}
	child, err := c.child(ptmx)
	if err != nil {
		jerr := errors.Join(err, ptmx.Close())
		if cmd.Process != nil {
			jerr = errors.Join(jerr, signalGroup(cmd.Process.Pid, syscall.SIGHUP), cmd.Wait())
		}
		return Exit{}, jerr
	}
	s := &session{
		cmd:    cmd,
		in:     in,
		inR:    inR,
		out:    out,
		child:  child,
		ptmx:   ptmx,
		c:      c,
		events: make(chan event, 3),
	}
	s.spawn(eventInput, func() event { return event{err: pumpInput(s.inR, s.child)} })
	s.spawn(eventOutput, func() event { return event{err: pumpOutput(s.child, s.out)} })
	s.spawn(eventWaiter, func() event {
		err := s.cmd.Wait()
		state := s.cmd.ProcessState
		if state != nil {
			err = nil
		}
		return event{state: state, err: err}
	})
	exit, p, rerr := s.loop(ctx, sigs)
	s.wg.Wait()
	if p != nil {
		panic(p)
	}
	return exit, rerr
}

// spawn runs f on a new goroutine counted in s.wg. It sends exactly one
// event on s.events (capacity 3, so the send never blocks): f's, or, when
// f panics, one carrying the recovered value and debug.Stack(). f has
// returned before the send and nothing touches a descriptor after it, so a
// descriptor closed on the event is safe; wg.Done is only accounting.
func (s *session) spawn(kind eventKind, f func() event) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				s.events <- event{kind: kind, panicValue: r, stack: debug.Stack()}
			}
		}()
		ev := f()
		ev.kind = kind
		s.events <- ev
	}()
}

// pumpOutput copies the child PTY to the outer terminal until the harness
// hangs up or the session stops.
func pumpOutput(child *childEnd, out io.Writer) error {
	buf := make([]byte, 32*1024)
	for {
		n, rerr := child.Read(buf)
		if n > 0 {
			if werr := writeAll(out, buf[:n]); werr != nil {
				if endOfStream(werr) {
					return errOuterClosed
				}
				return werr
			}
		}
		if rerr != nil {
			if endOfStream(rerr) {
				return errChildClosed
			}
			return rerr
		}
	}
}

// drainLimit caps drainOutput: about 66 times the largest PTY capacity
// measured, so a grandchild that keeps writing cannot hold the drain open.
const drainLimit = 1 << 20

// drainOutput forwards what the child still holds after the harness exits,
// stopping at errNothingPending, end of stream, a closed outer end, or
// exactly drainLimit bytes.
func drainOutput(child *childEnd, out io.Writer) error {
	buf := make([]byte, 32*1024)
	forwarded := 0
	for forwarded < drainLimit {
		n, rerr := child.readNow(buf[:min(len(buf), drainLimit-forwarded)])
		if n > 0 {
			if werr := writeAll(out, buf[:n]); werr != nil {
				if endOfStream(werr) {
					return nil
				}
				return werr
			}
			forwarded += n
		}
		if rerr != nil {
			if errors.Is(rerr, errNothingPending) || endOfStream(rerr) {
				return nil
			}
			return rerr
		}
	}
	return nil
}

// loop runs the single select over the signals, ctx.Done and the events;
// only it changes the teardown state. It ends when both pumps and the
// waiter have reported, so the harness is always reaped.
func (s *session) loop(ctx context.Context, sigs <-chan os.Signal) (Exit, *goroutinePanic, error) {
	done := ctx.Done()
	td := teardown{pid: s.cmd.Process.Pid, canceler: s.c}
	for td.reported < 3 {
		select {
		case sig := <-sigs:
			td.onSignal(sig, s)
		case <-done:
			td.onDone(ctx.Err())
			done = nil
		case ev := <-s.events:
			td.onEvent(ev, s)
		}
	}
	return td.exit, td.panicked, td.runErr
}

// onSignal forwards a handled signal to the harness group, or resizes the
// child PTY on SIGWINCH; signals after the exit are ignored.
func (td *teardown) onSignal(sig os.Signal, s *session) {
	if td.exited {
		return
	}
	if sig == unix.SIGWINCH {
		if size, err := pty.GetsizeFull(s.in); err == nil {
			_ = s.child.setSize(size.Rows, size.Cols, size.X, size.Y)
		}
		return
	}
	if num, ok := sig.(syscall.Signal); ok {
		_ = signalGroup(td.pid, num)
	}
}

// onDone records the context error and hangs up; done is nilled by the
// caller so this runs once.
func (td *teardown) onDone(err error) {
	if td.runErr == nil {
		td.runErr = fmt.Errorf("proxy: %w", err)
	}
	td.hangUp()
}

// onEvent folds one goroutine report into the teardown state.
func (td *teardown) onEvent(ev event, s *session) {
	td.reported++
	if ev.panicValue != nil {
		td.recordPanic(ev)
		td.hangUp()
		// Every pump termination, including panic, counts once; waiter panics do not.
		if ev.kind == eventInput || ev.kind == eventOutput {
			td.countPump(s)
		}
		return
	}
	switch ev.kind {
	case eventWaiter:
		td.onWaiterEvent(ev)
	case eventInput, eventOutput:
		td.onPumpEvent(ev)
		td.countPump(s)
	}
}

// recordPanic keeps the first recovered pump panic.
func (td *teardown) recordPanic(ev event) {
	if td.panicked == nil {
		td.panicked = &goroutinePanic{value: ev.panicValue, stack: ev.stack}
	}
}

// countPump counts one pump termination and closes ptmx exactly once when
// both pumps have reported. A drain or close failure is kept as the run
// error when nothing earlier was recorded; it never hides the harness exit,
// which onWaiterEvent records separately.
func (td *teardown) countPump(s *session) {
	if td.closed {
		return
	}
	td.pumps++
	if td.pumps != 2 {
		return
	}
	td.closed = true
	if !td.hungUp {
		if derr := drainOutput(s.child, s.out); derr != nil && td.runErr == nil {
			td.runErr = derr
		}
	}
	if cerr := s.ptmx.Close(); cerr != nil && td.runErr == nil {
		td.runErr = cerr
	}
}

// onWaiterEvent folds the waiter report: the harness exit or its error.
func (td *teardown) onWaiterEvent(ev event) {
	if ev.state != nil {
		td.exit = exitOf(ev.state)
		td.exited = true
	} else if td.runErr == nil {
		td.runErr = ev.err
	}
	td.stop()
}

// onPumpEvent records a pump's error, if it carries one worth keeping.
func (td *teardown) onPumpEvent(ev event) {
	switch {
	case ev.err == nil || errors.Is(ev.err, errCanceled) || errors.Is(ev.err, errChildClosed):
		// Nothing to do.
	case errors.Is(ev.err, errOuterClosed):
		td.hangUp()
	default:
		if td.runErr == nil {
			td.runErr = ev.err
		}
		td.hangUp()
	}
}

// stop cancels every wait, once: both pumps return even with the child's
// input queue full.
func (td *teardown) stop() {
	if !td.stopped {
		td.stopped = true
		td.canceler.cancel()
	}
}

// hangUp is stop plus SIGHUP to the harness group, once.
func (td *teardown) hangUp() {
	td.stop()
	if !td.hungUp {
		td.hungUp = true
		_ = signalGroup(td.pid, syscall.SIGHUP)
	}
}
