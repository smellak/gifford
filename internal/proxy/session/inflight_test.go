//go:build unix

package session

import (
	"bytes"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/draft"
)

// harnessGate stands for the harness's end of the middle terminal: each
// write hands its bytes to the test and waits for it, so the harness can
// answer a chunk while the writer is still inside the write, before it
// counts the chunk written or hands the loop its copy.
type harnessGate struct {
	wrote   chan []byte
	release chan struct{}
}

func (g *harnessGate) Write(p []byte) (int, error) {
	g.wrote <- append([]byte(nil), p...)
	<-g.release
	return len(p), nil
}

// next is the write the harness has now, still in flight.
func (g *harnessGate) next(t *testing.T) string {
	t.Helper()
	select {
	case b := <-g.wrote:
		return string(b)
	case <-time.After(10 * time.Second):
		require.FailNow(t, "the writer never wrote")
		return ""
	}
}

// orderRun is a session no loop runs: the test is the loop, so it takes
// the harness's output before the copy of a human chunk the writer
// sent, an order Run's select may take. The writer is the real one, with
// a gate for the harness.
type orderRun struct {
	s      *Session
	sink   *chanSink
	clock  *stepClock
	gate   *harnessGate
	w      *sessionWriter
	queued chan []byte
}

// newOrderRun builds an order run on a measured profile with an input
// line. Every output is its own io.chunk at once, so the state the
// recording reports before a reply's chunk is the one a replay reads its
// screen under.
func newOrderRun(t *testing.T, id string) *orderRun {
	t.Helper()
	_, slave := testPTY(t)
	sink := &chanSink{}
	clk := &stepClock{now: time.Date(2026, 10, 30, 23, 0, 0, 0, time.UTC)}
	opts := testOptions(t.TempDir(), id, slave, slave, sink, []string{"unused"})
	measuredInput(&opts)
	opts.Clock = clk
	opts.RecordTo = &bytes.Buffer{}
	opts.ChunkMinBytes = 1
	opts.SubmitDelay = time.Millisecond
	s, err := NewSession(opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.emu.Close() })
	o := &orderRun{
		s: s, sink: sink, clock: clk,
		gate:   &harnessGate{wrote: make(chan []byte), release: make(chan struct{})},
		queued: make(chan []byte, 4),
	}
	o.w = &sessionWriter{
		s: s, dst: o.gate, in: make(chan []byte), loop: o.queued,
		done: make(chan struct{}), perr: make(chan error, 1),
	}
	return o
}

// arrived counts a human chunk read from the terminal, as pumpIn does
// before it hands the chunk to the writer.
func (o *orderRun) arrived() {
	o.s.mu.Lock()
	o.s.arrived++
	o.s.mu.Unlock()
}

// typed has the writer put keys on the harness and the loop fold them,
// with no output in between.
func (o *orderRun) typed(t *testing.T, keys string) {
	t.Helper()
	o.arrived()
	go o.w.writeHuman([]byte(keys))
	require.Equal(t, keys, o.gate.next(t))
	o.gate.release <- struct{}{}
	o.s.onInput(<-o.queued)
}

// answered has the writer put keys on the harness, and the harness draw
// reply over a cleared screen while that write is still in flight. It
// returns the copy the writer then queued for the loop, not folded yet.
func (o *orderRun) answered(t *testing.T, keys, reply string) []byte {
	t.Helper()
	o.arrived()
	go o.w.writeHuman([]byte(keys))
	require.Equal(t, keys, o.gate.next(t))
	o.s.onOutput([]byte(clearScreen + reply))
	o.gate.release <- struct{}{}
	return <-o.queued
}

// closed folds a warmup submit the screen confirmed: the guard is
// CLOSED by a genuine outcome, not by its initial state.
func (o *orderRun) closed(t *testing.T) {
	t.Helper()
	o.s.onOutput([]byte(clearScreen + emptyBox))
	o.typed(t, "warmup\r")
	o.clock.advance(time.Millisecond)
	o.s.onOutput([]byte(clearScreen + emptyBox))
	o.clock.advance(time.Second)
	require.Equal(t, protocol.DraftStateClosed, o.s.InputGuard().DraftState)
	require.NotEmpty(t, sinkPayloads(o.sink, protocol.EventTypeInputDraftSubmitted), "a genuine closed event")
}

// draftAtLastChunk is the draft state sink reports last before its last
// io.chunk, the state a replay reads that chunk's screen under.
func draftAtLastChunk(sink *chanSink) protocol.DraftState {
	state, at := protocol.DraftStateUnknown, protocol.DraftStateUnknown
	for _, e := range sink.all() {
		switch p := e.Payload.(type) {
		case InputDraft:
			state = draftState(draft.State(p.State))
		case IOChunkEvent:
			at = state
		}
	}
	return at
}

// TestOutputBeforeItsInputNeverReachesTheProjection has the harness
// answer a human chunk while the writer is still putting it on the
// middle terminal, on a draft the screen confirmed closed: the output
// comes before the writer counts the chunk written and before the loop
// folds its copy. A letter, a lone ESC, an arrow and a submit, each
// answered with text on the input line. The guard holds before the
// output lands: the recording reports a fresh draft open before the
// reply's io.chunk, a delivery is refused, and the projection holds none
// of the reply. Once the chunk is folded the reply closes nothing,
// however long quiet: only an empty line drawn after the fold closes the
// draft the hold opened, with the chunk's own outcome, and never a
// typed letter or a held ESC.
func TestOutputBeforeItsInputNeverReachesTheProjection(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		keys string
		text string
		// closes is the outcome an empty line drawn after the fold
		// closes the draft with, "" for none.
		closes protocol.EventType
	}{
		{name: "a letter", keys: "x", text: "x-echoed-before-its-fold"},
		{name: "a lone ESC", keys: "\x1b", text: "restored-before-input-callback"},
		{name: "arrow up", keys: arrowUp, text: "recalled-before-its-fold", closes: protocol.EventTypeInputDraftCleared},
		{name: "a submit", keys: "\r", text: "left-before-its-fold", closes: protocol.EventTypeInputDraftSubmitted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			o := newOrderRun(t, "order-reply")
			o.closed(t)
			gen := publishedSnapshot(t, o.s).Generation

			c := o.answered(t, tc.keys, "> "+tc.text+"\x1b[0m")
			held := publishedSnapshot(t, o.s)
			require.Equal(t, draft.StateOpen, held.State, "the hold opened a draft")
			require.Equal(t, gen+1, held.Generation, "a fresh draft")
			require.Equal(t, protocol.DraftStateOpen, draftAtLastChunk(o.sink), "the opening comes before the reply's io.chunk")
			require.NotContains(t, projectedUnder(t, o.s, lastDraft(o.sink)), tc.text)
			o.clock.advance(time.Hour)
			require.Equal(t, DeliveryAbortedHumanPriorityOutcome, prepareNow(t, o.s, "dlv-order-held"))

			o.s.onInput(c)
			o.clock.advance(time.Hour)
			require.NotEqual(t, protocol.DraftStateClosed, o.s.InputGuard().DraftState, "the reply from before the fold closed the draft")
			require.NotContains(t, projectedUnder(t, o.s, lastDraft(o.sink)), tc.text)

			o.clock.advance(time.Millisecond)
			o.s.onOutput([]byte(clearScreen + emptyBox))
			o.clock.advance(time.Second)
			if tc.closes == "" {
				require.Equal(t, protocol.DraftStateOpen, o.s.InputGuard().DraftState)
				return
			}
			require.Equal(t, protocol.DraftStateClosed, o.s.InputGuard().DraftState)
			outs := sinkPayloads(o.sink, tc.closes)
			require.Equal(t, gen+1, outs[len(outs)-1].(InputDraft).Generation, "the draft the hold opened")
			require.Equal(t, DeliveryPreparedOutcome, prepareNow(t, o.s, "dlv-order-closed"))
		})
	}
}

// TestOutputBeforeAFlushedKeyNeverReachesTheProjection has a human type
// a letter while a delivery holds the lease: the writer buffers it, puts
// it on the harness once the submit is out, and only then hands the
// loop its copy. The harness answers the letter while that write is
// still in flight. The delivery commits, and the recording reports a
// fresh draft open before the reply's io.chunk, so the projection holds
// none of the reply, before the fold and after it.
func TestOutputBeforeAFlushedKeyNeverReachesTheProjection(t *testing.T) {
	t.Parallel()
	o := newOrderRun(t, "order-lease")
	o.closed(t)
	gen := publishedSnapshot(t, o.s).Generation
	require.Equal(t, DeliveryPreparedOutcome, prepareNow(t, o.s, "dlv-order-lease"))
	type result struct {
		rep DeliveryReport
		err error
	}
	committed := make(chan result, 1)
	go func() {
		rep, err := o.s.CommitDelivery("dlv-order-lease", ambiguousNotice, protocol.InjectModeTyped)
		committed <- result{rep: rep, err: err}
	}()
	go o.w.handleCommit(<-o.s.opCh)

	require.Equal(t, ambiguousNotice, o.gate.next(t), "the delivered line")
	// The human types while the lease holds: the writer buffers it.
	o.arrived()
	o.w.handleChunk([]byte("x"))
	o.gate.release <- struct{}{}
	require.Equal(t, o.s.opts.Profile.Submit, o.gate.next(t), "the submit")
	o.gate.release <- struct{}{}
	require.Equal(t, "x", o.gate.next(t), "the letter, after the submit")
	o.s.onOutput([]byte(clearScreen + "> x-echoed-before-its-fold\x1b[0m"))
	o.gate.release <- struct{}{}
	res := <-committed
	require.NoError(t, res.err)
	require.Equal(t, DeliveryCommittedOutcome, res.rep.Outcome)

	held := publishedSnapshot(t, o.s)
	require.Equal(t, draft.StateOpen, held.State, "the hold opened a draft")
	require.Equal(t, gen+1, held.Generation, "a fresh draft")
	require.Equal(t, protocol.DraftStateOpen, draftAtLastChunk(o.sink), "the opening comes before the reply's io.chunk")
	require.NotContains(t, projectedUnder(t, o.s, lastDraft(o.sink)), "x-echoed-before-its-fold")

	o.s.onInput(<-o.queued)
	o.clock.advance(time.Hour)
	require.Equal(t, protocol.DraftStateOpen, o.s.InputGuard().DraftState, "the letter is a draft")
	require.NotContains(t, projectedUnder(t, o.s, lastDraft(o.sink)), "x-echoed-before-its-fold")
}

// TestOutputWithNothingInFlightHoldsNothing draws harness output once
// every human chunk the writer sent is folded: the guard does not move,
// so a draft the screen confirmed closed stays closed under the same
// version, and a delivery goes through.
func TestOutputWithNothingInFlightHoldsNothing(t *testing.T) {
	t.Parallel()
	o := newOrderRun(t, "order-none")
	o.closed(t)
	before := publishedSnapshot(t, o.s)
	o.clock.advance(time.Millisecond)
	o.s.onOutput([]byte(clearScreen + "agent output\r\n" + emptyBox))
	o.clock.advance(time.Second)
	after := publishedSnapshot(t, o.s)
	require.Equal(t, draft.StateClosed, after.State)
	require.Equal(t, before.Version, after.Version)
	require.Equal(t, protocol.DraftStateClosed, draftAtLastChunk(o.sink))
	require.Contains(t, projectedUnder(t, o.s, lastDraft(o.sink)), "agent output")
	require.Equal(t, DeliveryPreparedOutcome, prepareNow(t, o.s, "dlv-order-none"))
}

// pumped starts the real output pump on a pipe that stands for the
// harness's output, and returns the pipe's write end and the reads the
// pump hands the loop.
func (o *orderRun) pumped(t *testing.T) (*os.File, <-chan outputRead) {
	t.Helper()
	r, w, err := os.Pipe()
	require.NoError(t, err)
	reads := make(chan outputRead, 4)
	var wg sync.WaitGroup
	wg.Add(1)
	go o.s.pumpOut(r, reads, make(chan error, 1), &wg)
	t.Cleanup(func() {
		_ = w.Close()
		wg.Wait()
		_ = r.Close()
	})
	return w, reads
}

// harnessDraws has the harness draw raw and returns the pump's read of
// it, with the human chunks sent once it was read, not folded yet.
func harnessDraws(t *testing.T, w *os.File, reads <-chan outputRead, raw string) outputRead {
	t.Helper()
	_, err := w.WriteString(raw)
	require.NoError(t, err)
	select {
	case r := <-reads:
		return r
	case <-time.After(10 * time.Second):
		require.FailNow(t, "the pump never read the output")
		return outputRead{}
	}
}

// TestOutputReadBeforeAChunkHoldsNothing reads the harness's output
// through the real output pump, on a draft the screen confirmed closed.
// Output the pump read before the writer sent a chunk cannot answer it:
// folded after the chunk is on the harness, and before the loop folds
// the chunk's copy, it holds nothing, as the screen the human typed in
// reply to. Output the pump read while the writer was still inside the
// chunk's write may answer it, and holds the guard before it lands.
func TestOutputReadBeforeAChunkHoldsNothing(t *testing.T) {
	t.Parallel()
	o := newOrderRun(t, "order-pump")
	o.closed(t)
	before := publishedSnapshot(t, o.s)
	w, reads := o.pumped(t)

	seen := harnessDraws(t, w, reads, clearScreen+"agent output\r\n"+emptyBox)
	o.arrived()
	go o.w.writeHuman([]byte("x"))
	require.Equal(t, "x", o.gate.next(t))
	answer := harnessDraws(t, w, reads, clearScreen+"> x-echoed-before-its-fold\x1b[0m")
	o.gate.release <- struct{}{}
	c := <-o.queued

	o.clock.advance(time.Millisecond)
	o.s.onOutputRead(seen)
	after := publishedSnapshot(t, o.s)
	require.Equal(t, draft.StateClosed, after.State, "output read before the chunk was sent held the guard")
	require.Equal(t, before.Version, after.Version)

	o.clock.advance(time.Millisecond)
	o.s.onOutputRead(answer)
	held := publishedSnapshot(t, o.s)
	require.Equal(t, draft.StateOpen, held.State, "the hold opened a draft")
	require.Equal(t, before.Generation+1, held.Generation, "a fresh draft")
	require.Equal(t, protocol.DraftStateOpen, draftAtLastChunk(o.sink), "the opening comes before the reply's io.chunk")
	require.NotContains(t, projectedUnder(t, o.s, lastDraft(o.sink)), "x-echoed-before-its-fold")

	o.s.onInput(c)
	o.clock.advance(time.Hour)
	require.Equal(t, protocol.DraftStateOpen, o.s.InputGuard().DraftState, "the letter is a draft")
}

// TestOutputReadBeforeAKeyNeverConfirmsIt reads an empty input line
// through the real output pump before the human presses an arrow, on a
// draft the screen confirmed closed, and has the loop fold that read
// before the key or after it: Run's select may take the key's copy
// first. Either way the read came before the harness had the key, so
// however long quiet it is no reaction to it: the draft the key opened
// stays open, and recorded open, so a draft the harness then draws back
// stays out of the projection. An empty line read after the key still
// closes the draft, as cleared and once, under the key's generation.
func TestOutputReadBeforeAKeyNeverConfirmsIt(t *testing.T) {
	t.Parallel()
	const restored = "restored-after-stale-empty"
	for _, order := range []string{"folded before the key", "folded after the key"} {
		for _, then := range []string{"a draft drawn back", "an empty line read after the key"} {
			t.Run(order+", "+then, func(t *testing.T) {
				t.Parallel()
				o := newOrderRun(t, "order-stale")
				o.closed(t)
				w, reads := o.pumped(t)
				old := harnessDraws(t, w, reads, clearScreen+emptyBox)
				require.Equal(t, uint64(1), old.sent, "read before the key was sent")
				if order == "folded before the key" {
					o.clock.advance(time.Millisecond)
					o.s.onOutputRead(old)
				}
				o.clock.advance(time.Millisecond)
				o.typed(t, arrowUp)
				opened := publishedSnapshot(t, o.s)
				require.Equal(t, draft.StateOpen, opened.State, "the key opened a draft")
				require.Equal(t, uint64(2), opened.Chunks)
				if order == "folded after the key" {
					o.clock.advance(time.Millisecond)
					o.s.onOutputRead(old)
				}
				o.clock.advance(time.Second)
				require.Equal(t, protocol.DraftStateOpen, o.s.InputGuard().DraftState, "output read before the key confirmed it")
				require.Equal(t, protocol.DraftStateOpen, lastDraft(o.sink))
				require.Empty(t, sinkPayloads(o.sink, protocol.EventTypeInputDraftCleared))

				o.clock.advance(time.Millisecond)
				if then == "a draft drawn back" {
					o.s.onOutputRead(harnessDraws(t, w, reads, clearScreen+"> "+restored+"\x1b[0m"))
					o.clock.advance(time.Second)
					require.Equal(t, protocol.DraftStateOpen, o.s.InputGuard().DraftState)
					require.NotContains(t, projectedUnder(t, o.s, lastDraft(o.sink)), restored)
					return
				}
				o.s.onOutputRead(harnessDraws(t, w, reads, clearScreen+emptyBox))
				o.clock.advance(time.Second)
				require.Equal(t, protocol.DraftStateClosed, o.s.InputGuard().DraftState, "output read after the key never closed the draft")
				cleared := sinkPayloads(o.sink, protocol.EventTypeInputDraftCleared)
				require.Len(t, cleared, 1)
				require.Equal(t, opened.Generation, cleared[0].(InputDraft).Generation, "the draft the key opened")
			})
		}
	}
}
