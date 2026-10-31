//go:build unix

package session

import (
	"bytes"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

// receiveAfterWrite starts the loop receiver from inside the PTY write. With
// one scheduler P, it cannot run until writeHuman either blocks forwarding the
// copy or returns after dropping it.
type receiveAfterWrite struct {
	loop   <-chan []byte
	cancel <-chan struct{}
	got    chan<- []byte
}

func (w receiveAfterWrite) Write(p []byte) (int, error) {
	go func() {
		select {
		case data := <-w.loop:
			w.got <- data
		case <-w.cancel:
			w.got <- nil
		}
	}()
	return len(p), nil
}

func TestWriterForwardsWrittenInputBeforeStop(t *testing.T) {
	previous := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })
	runtime.LockOSThread()
	t.Cleanup(runtime.UnlockOSThread)

	loop := make(chan []byte)
	stop := make(chan struct{})
	close(stop)
	cancel := make(chan struct{})
	got := make(chan []byte, 1)
	w := &sessionWriter{
		s:    &Session{},
		dst:  receiveAfterWrite{loop: loop, cancel: cancel, got: got},
		loop: loop,
		done: stop,
		perr: make(chan error, 1),
	}
	want := []byte("final input\n")

	w.writeHuman(want)
	close(cancel)
	require.Equal(t, want, <-got, "a successful PTY write must reach the session loop before stop")
}

func TestTeardownMarksUnresolvedDeferredOutput(t *testing.T) {
	o := newOrderRun(t, "unresolved-teardown-gap")
	recording := o.s.opts.RecordTo.(*bytes.Buffer)
	const recordedPrefix = "recorded harness prefix"
	const privateInput = "private-human-input"
	const visibleOutput = "visible harness output"

	o.s.onOutput([]byte(recordedPrefix))
	chunksBefore := len(sinkPayloads(o.sink, protocol.EventTypeIOChunk))
	require.Positive(t, chunksBefore)
	// One input chunk reached the loop and another successful write did not.
	// The first chunk proves that human bytes remain metadata-only even when the
	// final transport boundary cannot be reconstructed.
	o.s.humanSent.Store(2)
	o.s.onInput([]byte(privateInput))
	o.s.onOutput([]byte(visibleOutput))
	o.s.mu.Lock()
	o.s.running = false
	o.s.flushInputLocked()
	deferred := o.s.deferred
	o.s.mu.Unlock()
	if deferred != nil {
		require.NoError(t, deferred.close())
	}

	require.Contains(t, recording.String(), recordedPrefix)
	require.NotContains(t, recording.String(), privateInput)
	require.NotContains(t, recording.String(), visibleOutput, "unresolved output stays out of the recording")
	require.Len(t, sinkPayloads(o.sink, protocol.EventTypeIOChunk), chunksBefore)
	gaps := sinkPayloads(o.sink, protocol.EventTypeCaptureGapDetected)
	require.Len(t, gaps, 1, "discarded output must be visible to replay and the dataset")
	require.Equal(t, protocol.CaptureGapCauseIngestDied, gaps[0].(protocol.CaptureGapDetected).Cause)
}
