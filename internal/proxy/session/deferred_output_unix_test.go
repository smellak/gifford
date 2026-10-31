//go:build unix

package session

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

func TestUnresolvedOutputSpoolsAtExplicitMemoryLimit(t *testing.T) {
	o := newOrderRun(t, "pending-output-spool-limit")
	o.closed(t)
	o.s.opts.ChunkMinBytes = deferredOutputMemoryLimit * 2
	o.typed(t, "\x1b")
	before := len(sinkPayloads(o.sink, protocol.EventTypeIOChunk))
	chunk := bytes.Repeat([]byte("x"), o.s.opts.ChunkMaxBytes)
	for range deferredOutputMemoryLimit/o.s.opts.ChunkMaxBytes + 1 {
		o.s.onOutput(chunk)
	}

	o.s.mu.Lock()
	buffered := o.s.buffered
	spooled := o.s.deferred != nil && o.s.deferred.count > 0
	o.s.mu.Unlock()
	require.Less(t, buffered, deferredOutputMemoryLimit)
	require.True(t, spooled)
	require.Len(t, sinkPayloads(o.sink, protocol.EventTypeIOChunk), before,
		"spooled output stays behind input classification")

	o.typed(t, "x")
	o.s.mu.Lock()
	buffered, spool := o.s.buffered, o.s.deferred
	o.s.mu.Unlock()
	require.Zero(t, buffered)
	require.Nil(t, spool)
	require.Greater(t, len(sinkPayloads(o.sink, protocol.EventTypeIOChunk)), before)
	require.Equal(t, protocol.DraftStateOpen, draftAtLastChunk(o.sink),
		"the resolved human event precedes every spooled chunk")
}

func TestDeferredOutputSpoolQuotaDisablesOnlyCapture(t *testing.T) {
	o := newOrderRun(t, "pending-output-spool-quota")
	o.closed(t)
	o.s.deferredLimit = int64(2 * o.s.opts.ChunkMaxBytes)
	o.typed(t, "\x1b")
	before := len(sinkPayloads(o.sink, protocol.EventTypeIOChunk))
	chunk := bytes.Repeat([]byte("x"), o.s.opts.ChunkMaxBytes)
	for range 4 {
		o.s.onOutput(chunk)
	}

	o.s.mu.Lock()
	buffered, pieces := o.s.buffered, len(o.s.pending)
	disabled, fatal := o.s.captureDisabled, o.s.fatalErr
	spoolBytes, limit := o.s.deferred.bytes, o.s.deferred.limit
	o.s.mu.Unlock()
	require.True(t, publishedSnapshot(t, o.s).Pending)
	require.True(t, disabled)
	require.ErrorIs(t, fatal, ErrDeferredOutputLimit)
	require.Zero(t, buffered)
	require.Zero(t, pieces)
	require.LessOrEqual(t, spoolBytes, limit)
	require.Len(t, sinkPayloads(o.sink, protocol.EventTypeIOChunk), before)
	quotas := sinkPayloads(o.sink, protocol.EventTypeCaptureQuotaReached)
	require.Len(t, quotas, 1)
	quota := quotas[0].(protocol.CaptureQuotaReached)
	require.Greater(t, quota.Bytes, quota.LimitBytes)
	require.Len(t, sinkPayloads(o.sink, protocol.EventTypeCaptureGapDetected), 1)

	// Continued output is still painted and never grows either retention tier.
	for range 128 {
		o.s.onOutput(chunk)
	}
	o.s.mu.Lock()
	buffered, spoolBytes = o.s.buffered, o.s.deferred.bytes
	o.s.mu.Unlock()
	require.Zero(t, buffered)
	require.LessOrEqual(t, spoolBytes, limit)
	require.Len(t, sinkPayloads(o.sink, protocol.EventTypeCaptureGapDetected), 1)

	// Input still reaches the harness. Resolution records the human event before
	// the portion that fit in the spool, without ever closing by timeout.
	o.typed(t, "x")
	require.False(t, publishedSnapshot(t, o.s).Pending)
	require.Greater(t, len(sinkPayloads(o.sink, protocol.EventTypeIOChunk)), before)
	require.Equal(t, protocol.DraftStateOpen, draftAtLastChunk(o.sink))
	require.ErrorIs(t, o.s.fatalErr, ErrDeferredOutputLimit)
}

func TestDeferredOutputSpoolFailureDisablesOnlyCapture(t *testing.T) {
	o := newOrderRun(t, "pending-output-spool-failure")
	o.closed(t)
	o.typed(t, "\x1b")
	before := len(sinkPayloads(o.sink, protocol.EventTypeIOChunk))
	chunk := bytes.Repeat([]byte("x"), o.s.opts.ChunkMaxBytes)
	o.s.onOutput(chunk)
	o.s.mu.Lock()
	require.NotNil(t, o.s.deferred)
	require.NoError(t, o.s.deferred.file.Close())
	o.s.mu.Unlock()

	o.s.onOutput(chunk)
	o.s.mu.Lock()
	buffered, pieces := o.s.buffered, len(o.s.pending)
	disabled, fatal, spool := o.s.captureDisabled, o.s.fatalErr, o.s.deferred
	o.s.mu.Unlock()
	require.True(t, disabled)
	require.Error(t, fatal)
	require.NotErrorIs(t, fatal, ErrDeferredOutputLimit)
	require.Zero(t, buffered)
	require.Zero(t, pieces)
	require.Nil(t, spool)
	require.Len(t, sinkPayloads(o.sink, protocol.EventTypeIOChunk), before)
	require.Empty(t, sinkPayloads(o.sink, protocol.EventTypeCaptureQuotaReached))
	gaps := sinkPayloads(o.sink, protocol.EventTypeCaptureGapDetected)
	require.Len(t, gaps, 1)
	require.Equal(t, protocol.CaptureGapCauseIngestDied, gaps[0].(protocol.CaptureGapDetected).Cause)

	for range 128 {
		o.s.onOutput(chunk)
	}
	o.s.mu.Lock()
	buffered = o.s.buffered
	o.s.mu.Unlock()
	require.Zero(t, buffered)
	require.Len(t, sinkPayloads(o.sink, protocol.EventTypeCaptureGapDetected), 1)

	// A persistence failure cannot consume or settle the unresolved ESC.
	require.True(t, publishedSnapshot(t, o.s).Pending)
	o.typed(t, "x")
	require.False(t, publishedSnapshot(t, o.s).Pending)
	require.Equal(t, protocol.DraftStateOpen, o.s.InputGuard().DraftState)
}
