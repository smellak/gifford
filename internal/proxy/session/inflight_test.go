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
	r, w, err := os.Pipe()
	require.NoError(t, err)
	reads := make(chan outputRead, 4)
	var wg sync.WaitGroup
	wg.Add(1)
	go o.s.pumpOut(r, reads, make(chan error, 1), &wg)
	defer func() {
		_ = w.Close()
		wg.Wait()
		_ = r.Close()
	}()

	_, err = w.WriteString(clearScreen + "agent output\r\n" + emptyBox)
	require.NoError(t, err)
	seen := <-reads
	o.arrived()
	go o.w.writeHuman([]byte("x"))
	require.Equal(t, "x", o.gate.next(t))
	_, err = w.WriteString(clearScreen + "> x-echoed-before-its-fold\x1b[0m")
	require.NoError(t, err)
	answer := <-reads
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
