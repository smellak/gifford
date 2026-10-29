package capture

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/smellak/gifford/internal/protocol"
)

// Funds outcomes for a reservation request.
const (
	fundsOK          = "ok"
	fundsQuota       = "quota"
	fundsUnavailable = "unavailable"
)

// segInfo is one segment's committed coverage: rows are reconciled
// from it at every flush.
type segInfo struct {
	firstSeq int64
	lastSeq  int64
	bytes    int64
	chunks   int
	lens     []int64
	started  time.Time
	lastWall time.Time
	ended    time.Time
	closed   bool
}

// segFiles holds the current segment's append descriptors.
type segFiles struct {
	raw *os.File
	idx *os.File
}

// indexLine renders one .idx JSON line.
func indexLine(seq, offset, length int64, wall time.Time, mono int64, ansi bool) []byte {
	line, _ := json.Marshal(map[string]any{
		"sequence": seq, "offset": offset, "length": length,
		"wall_time": formatWall(wall), "monotonic_ns": mono, "ansi_present": ansi,
	})
	return append(line, '\n')
}

// budgetOrStop runs the §8.2 step 7 budget: the footprint must not
// exceed the ledger, or the difference is reserved. A refused or
// unavailable budget stops as §8.6 before the writer starts.
func (r *ingestRun) budgetOrStop(ctx context.Context) (string, bool, error) {
	r.nextSeq = r.opts.StartSequence
	_, _, data, err := footprint(r.root, r.opts.Agent, r.opts.Session)
	if err != nil {
		return r.exhaustNew(ctx, fundsUnavailable)
	}
	ledger, err := readLedger(r.root, r.opts.Agent, r.opts.Session)
	if err != nil {
		return r.exhaustNew(ctx, fundsUnavailable)
	}
	r.granted = ledger
	r.baseData = data
	control, log, _, err := footprint(r.root, r.opts.Agent, r.opts.Session)
	if err != nil {
		return r.exhaustNew(ctx, fundsUnavailable)
	}
	if total := control + log + data; total > ledger {
		switch r.ensureFunds(total - ledger) {
		case fundsOK:
		case fundsQuota:
			return r.exhaustNew(ctx, fundsQuota)
		default:
			return r.exhaustNew(ctx, fundsUnavailable)
		}
	}
	return "", false, nil
}

// ledgersReadable reports whether every budget input reads strictly.
func (r *ingestRun) ledgersReadable() bool {
	if _, err := readLedger(r.root, r.opts.Agent, r.opts.Session); err != nil {
		return false
	}
	if _, err := agentUsage(r.root, r.opts.Agent); err != nil {
		return false
	}
	_, err := nodeUsage(r.root)
	return err == nil
}

// ensureFunds holds a reservation for n more bytes: the block first,
// then exactly what the next write needs. Only this run changes its
// ledger, so the cached grant stays exact. The last need is kept for
// the quota scope decision.
func (r *ingestRun) ensureFunds(n int64) string {
	r.lastNeed = n
	if r.granted-(r.baseData+r.written) >= n {
		return fundsOK
	}
	if ok, _ := reserve(r.root, r.opts.Agent, r.opts.Session, r.lim.ReserveBlockBytes, r.lim); ok {
		r.granted += r.lim.ReserveBlockBytes
		if r.granted-(r.baseData+r.written) >= n {
			return fundsOK
		}
	}
	if ok, _ := reserve(r.root, r.opts.Agent, r.opts.Session, n, r.lim); ok {
		r.granted += n
		return fundsOK
	}
	if !r.ledgersReadable() {
		return fundsUnavailable
	}
	return fundsQuota
}

// loop runs the writer: chunks, flushes, rotations and the resize
// poller, until the queue drains or funds or stop end it.
func (r *ingestRun) loop(ctx context.Context) (string, error) {
	rows, err := r.store.SegmentsOf(ctx, r.opts.Session)
	if err != nil {
		return r.exitError(fmt.Errorf("read segments: %w", err))
	}
	r.segRows = make(map[string]bool, len(rows))
	for _, seg := range rows {
		r.segRows[seg.SegmentID] = true
	}
	r.segs = make(map[string]*segInfo)
	r.queue = make(chan readStamp, queueCap(r.lim.QueueMaxBytes))
	if r.readerDone == nil {
		r.readerDone = make(chan struct{})
	}
	go r.readLoop()
	pollCtx, pollStop := context.WithCancel(ctx)
	defer pollStop()
	r.startPoller(pollCtx)
	cause, err := r.writeLoop(ctx)
	r.stopPoller()
	if err != nil {
		return r.exitError(err)
	}
	return r.finishRun(ctx, cause)
}

// absorb cuts one read into chunks: a chunk closes at chunk_max_bytes
// with the excess starting the next one, and sparse output stays open
// for the age check.
func (r *ingestRun) absorb(stamp readStamp) string {
	if !r.chunkOpen {
		r.chunkFirst = stamp
		r.chunkOpen = true
	}
	r.chunkBuf = append(r.chunkBuf, stamp.data...)
	for int64(len(r.chunkBuf)) >= r.lim.ChunkMaxBytes {
		full := append([]byte(nil), r.chunkBuf[:r.lim.ChunkMaxBytes]...)
		r.chunkBuf = append([]byte(nil), r.chunkBuf[r.lim.ChunkMaxBytes:]...)
		if sig := r.emitChunk(full, r.chunkFirst); sig != "" {
			r.chunkBuf = nil
			r.chunkOpen = false
			return sig
		}
		if len(r.chunkBuf) == 0 {
			r.chunkOpen = false
		}
	}
	return ""
}

// emitChunk reserves, rotates, writes and enqueues one chunk event.
// Unreserved bytes are never written and no event refers to them.
func (r *ingestRun) emitChunk(raw []byte, first readStamp) string {
	if len(raw) == 0 {
		r.chunkOpen = false
		return ""
	}
	line := indexLine(r.nextSeq, 0, int64(len(raw)), first.wall, first.mono, hasESC(raw))
	if funds := r.ensureFunds(int64(len(raw)) + int64(len(line))); funds != fundsOK {
		return funds
	}
	if r.curID == "" || r.segs[r.curID].bytes+int64(len(raw)) > r.lim.SegmentMaxBytes ||
		(r.segs[r.curID].chunks > 0 && first.mono-r.segStart >= int64(r.lim.SegmentMaxAge)) {
		if err := r.rotate(); err != nil {
			r.logLine("rotation failed: " + err.Error())
			return fundsUnavailable
		}
	}
	info := r.segs[r.curID]
	offset := info.bytes
	line = indexLine(r.nextSeq, offset, int64(len(raw)), first.wall, first.mono, hasESC(raw))
	if _, err := r.curFiles.raw.Write(raw); err != nil {
		return fundsUnavailable
	}
	if _, err := r.curFiles.idx.Write(line); err != nil {
		return fundsUnavailable
	}
	need := int64(len(raw)) + int64(len(line))
	r.written += need
	r.flushedRaw += int64(len(raw))
	info.bytes += int64(len(raw))
	info.lastSeq = r.nextSeq
	info.lastWall = first.wall
	info.lens = append(info.lens, int64(len(raw)))
	info.chunks++
	if info.chunks == 1 {
		info.firstSeq = r.nextSeq
		info.started = first.wall
	}
	r.batch = append(r.batch, r.chunkEvent(r.nextSeq, offset, int64(len(raw)), hasESC(raw), first))
	r.nextSeq++
	return ""
}

// hasESC reports an escape byte in the chunk.
func hasESC(raw []byte) bool {
	for _, b := range raw {
		if b == 0x1b {
			return true
		}
	}
	return false
}

// chunkEvent builds one capture.chunk envelope from its first stamp.
func (r *ingestRun) chunkEvent(seq, offset, nbytes int64, ansi bool, first readStamp) protocol.EventEnvelope {
	data, _ := jsonBytes(protocol.CaptureChunk{
		CaptureSessionID: protocol.OpaqueID(r.opts.Session),
		ByteCount:        nbytes,
		ANSIPresent:      ansi,
		RawBlobRef:       protocol.BlobRef("blob://capture/" + r.opts.Session + "/" + r.curID + "/" + strconv.FormatInt(offset, 10)),
	})
	ev := r.baseEnvelope(protocol.EventTypeCaptureChunk, seq, first.wall, first.mono)
	ev.Direction = protocol.DirectionOut
	ev.Origin = protocol.OriginHarness
	ev.Payload = data
	return ev
}

// baseEnvelope builds the §8.4 envelope fields shared by every capture
// event of this run.
func (r *ingestRun) baseEnvelope(evType protocol.EventType, seq int64, wall time.Time, mono int64) protocol.EventEnvelope {
	id, err := newULID(r.deps.Clock)
	if err != nil {
		id = "01ARZ3NDEKTSV4RRFFQ69G5FAV"
	}
	return protocol.EventEnvelope{
		EventID:       protocol.ULID(id),
		SchemaVersion: 1,
		NodeID:        protocol.NodeID(r.node),
		SessionID:     protocol.OpaqueID(r.opts.Session),
		AgentID:       protocol.AgentID(r.opts.Agent),
		Harness:       protocol.Harness(r.row.Harness),
		AdapterType:   protocol.AdapterTypeNone,
		SourceQuality: protocol.SourceQualityBootstrapPipePane,
		EventType:     evType,
		Stream:        protocol.StreamPTY,
		WallTime:      wall,
		MonotonicNS:   mono,
		Sequence:      seq,
		Sensitivity:   protocol.SensitivityNormal,
	}
}

// rotate closes the current segment and opens the next one with a new
// id. The rotated event is emitted as the next segment opens; the
// first segment opens silently.
func (r *ingestRun) rotate() error {
	prev := r.curID
	id, err := newSegmentID(r.deps.Clock)
	if err != nil {
		return fmt.Errorf("segment id: %w", err)
	}
	dir := r.opts.Agent + "/" + r.opts.Session
	rawFd, err := r.root.OpenFile(dir+"/"+id+".raw", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("%w: segment raw: %w", ErrUnsafePath, err)
	}
	idxFd, err := r.root.OpenFile(dir+"/"+id+".idx", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		_ = rawFd.Close()
		return fmt.Errorf("%w: segment idx: %w", ErrUnsafePath, err)
	}
	if prev != "" {
		info := r.segs[prev]
		info.closed = true
		info.ended = info.lastWall
		data, _ := jsonBytes(protocol.CaptureRotated{
			CaptureSessionID: protocol.OpaqueID(r.opts.Session),
			ClosedSegmentID:  protocol.OpaqueID(prev),
			NewSegmentID:     protocol.OpaqueID(id),
			Bytes:            info.bytes,
		})
		ev := r.baseEnvelope(protocol.EventTypeCaptureRotated, r.nextSeq, r.deps.Clock.Now(), r.deps.Mono.MonotonicNS())
		ev.Payload = data
		r.batch = append(r.batch, ev)
		r.nextSeq++
		r.dirtySegs = append(r.dirtySegs, prev)
		_ = r.curFiles.raw.Close()
		_ = r.curFiles.idx.Close()
	}
	r.segs[id] = &segInfo{}
	r.dirtySegs = append(r.dirtySegs, id)
	r.curID = id
	r.curFiles = &segFiles{raw: rawFd, idx: idxFd}
	r.newSegFiles = true
	r.segStart = r.deps.Mono.MonotonicNS()
	return nil
}

// writeLoop drains the queue into chunks and flushes on time, count
// and close. It returns the ending cause: eof, stall, stop, quota or
// unavailable.
func (r *ingestRun) writeLoop(ctx context.Context) (string, error) {
	tick := time.NewTicker(time.Duration(r.lim.FlushInterval))
	defer tick.Stop()
	queueOpen := true
	end := ""
	for {
		ticked := false
		select {
		case stamp, ok := <-r.queue:
			if !ok {
				queueOpen = false
			} else if sig := r.absorb(stamp); sig != "" && end == "" {
				end = sig
			}
		case <-tick.C:
			ticked = true
		}
		if r.chunkOpen && r.deps.Mono.MonotonicNS()-r.chunkFirst.mono >= int64(r.lim.ChunkMaxAge) {
			if sig := r.emitChunk(r.chunkBuf, r.chunkFirst); sig != "" && end == "" {
				end = sig
			}
			r.chunkBuf = nil
			r.chunkOpen = false
		}
		if end == fundsQuota || end == fundsUnavailable {
			r.stopFlag.Store(true)
			r.chunkBuf = nil
			r.chunkOpen = false
			if sig, err := r.flush(ctx); err != nil {
				return ExitError, err
			} else if sig != "" && end == "" {
				end = sig
			}
			return end, nil
		}
		if len(r.batch) >= flushBatchCap || ticked || !queueOpen {
			sig, err := r.flush(ctx)
			if err != nil {
				return ExitError, err
			}
			if sig != "" && end == "" {
				end = sig
			}
		}
		if !queueOpen && r.chunkOpen {
			if sig := r.emitChunk(r.chunkBuf, r.chunkFirst); sig != "" && end == "" {
				end = sig
			}
			r.chunkBuf = nil
			r.chunkOpen = false
			if sig, err := r.flush(ctx); err != nil {
				return ExitError, err
			} else if sig != "" && end == "" {
				end = sig
			}
		}
		if !queueOpen && !r.chunkOpen {
			if r.readErr != nil {
				return ExitError, r.readErr
			}
			if r.readStall {
				return ExitStall, nil
			}
			if end != "" {
				return end, nil
			}
			return ExitEOF, nil
		}
	}
}

// flush commits the pending batch and the rows it implies, then reads
// stop.request: present means stop as §8.7.
func (r *ingestRun) flush(ctx context.Context) (string, error) {
	r.takeResize()
	if len(r.batch) > 0 {
		if err := r.commitBatch(ctx); err != nil {
			return "", err
		}
	}
	return r.checkStop()
}

// takeResize sequences a pending pane resize into the batch.
func (r *ingestRun) takeResize() {
	r.resizeMu.Lock()
	defer r.resizeMu.Unlock()
	if !r.resizeHit {
		return
	}
	ev := r.baseEnvelope(protocol.EventTypeCaptureResize, r.nextSeq, r.deps.Clock.Now(), r.deps.Mono.MonotonicNS())
	data, _ := jsonBytes(protocol.CaptureResize{
		CaptureSessionID: protocol.OpaqueID(r.opts.Session),
		Cols:             r.resizeCol, Rows: r.resizeRow,
	})
	ev.Payload = data
	r.batch = append(r.batch, ev)
	r.nextSeq++
	r.row.PaneCols, r.row.PaneRows = r.resizeCol, r.resizeRow
	r.resizeHit = false
}

// commitHook stalls the commit in tests: the writer blocks until the
// test releases it, proving the shutdown grace bounds a stuck drain.
var commitHook atomic.Pointer[func()]

// commitBatch fsyncs, appends and records the rows the batch implies:
// new segment rows after their first chunk, seq and byte updates, and
// bytes_captured.
func (r *ingestRun) commitBatch(ctx context.Context) error {
	if hook := commitHook.Load(); hook != nil {
		(*hook)()
	}
	if r.curFiles != nil {
		if err := r.curFiles.raw.Sync(); err != nil {
			return fmt.Errorf("sync raw: %w", err)
		}
		if err := r.curFiles.idx.Sync(); err != nil {
			return fmt.Errorf("sync idx: %w", err)
		}
	}
	if r.newSegFiles {
		if err := syncDirAt(r.root, r.opts.Agent+"/"+r.opts.Session); err != nil {
			return err
		}
		r.newSegFiles = false
	}
	if err := r.store.AppendEvents(ctx, r.batch); err != nil {
		return fmt.Errorf("append batch: %w", err)
	}
	r.batch = nil
	if err := r.syncSegmentRows(ctx); err != nil {
		return err
	}
	r.row.BytesCaptured += r.flushedRaw
	r.flushedRaw = 0
	if err := r.store.UpdateSession(ctx, r.row); err != nil {
		return fmt.Errorf("row bytes: %w", err)
	}
	return nil
}

// checkStop reads stop.request; present means stop as §8.7.
func (r *ingestRun) checkStop() (string, error) {
	req, err := readStopRequest(r.root, r.opts.Agent, r.opts.Session)
	if err != nil {
		return "", fmt.Errorf("stop request: %w", err)
	}
	if req != nil {
		r.stopReq = req
		r.stopFlag.Store(true)
		return ExitStop, nil
	}
	return "", nil
}

// syncSegmentRows inserts rows for segments with committed chunks and
// updates seq, bytes and ends for the rest.
func (r *ingestRun) syncSegmentRows(ctx context.Context) error {
	for _, id := range r.dirtySegs {
		info := r.segs[id]
		if info.chunks == 0 {
			continue
		}
		row := SegmentRow{
			SegmentID: id, CaptureSessionID: r.opts.Session,
			SeqFrom: info.firstSeq, SeqTo: info.lastSeq,
			StartedAt:   formatWall(info.started),
			StoragePath: id + ".raw", IndexPath: id + ".idx",
			BytesRaw: info.bytes,
		}
		if info.closed {
			row.EndedAt = formatWall(info.ended)
		}
		if r.segRows[id] {
			if err := r.store.UpdateSegment(ctx, row); err != nil {
				return fmt.Errorf("update segment: %w", err)
			}
			continue
		}
		if err := r.store.InsertSegment(ctx, row); err != nil {
			return fmt.Errorf("insert segment: %w", err)
		}
		r.segRows[id] = true
	}
	r.dirtySegs = nil
	return nil
}

// finishRun closes the stream, the segment, the terminal event, the
// exit record and the lock, in the §8.7 order.
func (r *ingestRun) finishRun(ctx context.Context, cause string) (string, error) {
	r.streamClose()
	if sig, err := r.flush(ctx); err != nil {
		return r.exitError(err)
	} else if sig == ExitStop && (cause == ExitEOF || cause == ExitStall) {
		cause = ExitStop
	}
	if r.curID != "" {
		if err := r.closeSegmentRow(ctx); err != nil {
			return r.exitError(err)
		}
		if (cause == ExitEOF || cause == ExitStop) && len(r.segs[r.curID].lens) > 0 {
			r.compressFinal(ctx, cause)
		}
		_ = r.curFiles.raw.Close()
		_ = r.curFiles.idx.Close()
		r.curFiles = nil
	}
	switch cause {
	case ExitStop, ExitQuota, fundsUnavailable:
		return r.finishTerminal(ctx, cause)
	default:
		r.writeExit(cause)
		r.settleExit(cause == ExitEOF || cause == ExitStall)
		r.unlock()
		return cause, nil
	}
}

// settleExit lowers D at exit: the data footprint plus at most one
// block while live, the data footprint once terminal.
func (r *ingestRun) settleExit(live bool) {
	if err := settle(r.root, r.opts.Agent, r.opts.Session, r.lim, !live); err != nil {
		r.logLine("settle skipped: " + err.Error())
	}
}

// finishTerminal appends the terminal events, sets the row, writes the
// exit record, settles terminally and releases the lock.
func (r *ingestRun) finishTerminal(ctx context.Context, cause string) (string, error) {
	var terminal []protocol.EventEnvelope
	var state, reason string
	switch cause {
	case ExitStop:
		req := r.stopReq
		ev, err := stoppedEnvelopeFor(r.deps.Clock, r.node, r.row, r.nextSeq, req.Reason, req.Detail, r.deps.Mono.MonotonicNS())
		if err != nil {
			return r.exitError(err)
		}
		terminal = []protocol.EventEnvelope{ev}
		state, reason = StoppedRowState(protocol.CaptureStopReason(req.Reason)), req.Reason
		r.nextSeq++
	case ExitQuota:
		scope, used, limit := r.quotaScope()
		terminal = r.quotaEvents(scope, used, limit)
		state, reason = "QUOTA", "quota"
	default:
		ev, err := stoppedEnvelopeFor(r.deps.Clock, r.node, r.row, r.nextSeq, "error", "budget unavailable", r.deps.Mono.MonotonicNS())
		if err != nil {
			return r.exitError(err)
		}
		terminal = []protocol.EventEnvelope{ev}
		state, reason = stateError, reasonError
		r.nextSeq++
		cause = ExitError
	}
	if err := r.store.AppendEvents(ctx, terminal); err != nil {
		return r.exitError(fmt.Errorf("append terminal: %w", err))
	}
	r.row.State = state
	r.row.StoppedAt = formatWall(terminal[len(terminal)-1].WallTime)
	r.row.StopReason = reason
	if err := r.store.UpdateSession(ctx, r.row); err != nil {
		return r.exitError(fmt.Errorf("terminal row: %w", err))
	}
	exitCause := cause
	if cause == fundsUnavailable {
		exitCause = ExitError
	}
	r.writeExit(exitCause)
	r.settleExit(false)
	r.unlock()
	return cause, nil
}

// exhaustNew stops before the writer starts: no batch and no segment
// exist, so only the terminal order runs.
func (r *ingestRun) exhaustNew(ctx context.Context, kind string) (string, bool, error) {
	r.streamClose()
	cause := ExitQuota
	if kind == fundsUnavailable {
		cause = fundsUnavailable
	}
	if _, err := r.finishTerminal(ctx, cause); err != nil {
		return ExitError, true, err
	}
	if cause == fundsUnavailable {
		return ExitError, true, nil
	}
	return ExitQuota, true, nil
}

// quotaScope names the refused scope with its usage and limit: the
// agent scope when the last need refused it, else the node scope.
func (r *ingestRun) quotaScope() (protocol.QuotaScope, int64, int64) {
	if au, err := agentUsage(r.root, r.opts.Agent); err == nil && au+r.lastNeed > r.lim.AgentQuotaBytes {
		return protocol.QuotaScopeAgent, au, r.lim.AgentQuotaBytes
	}
	nu, err := nodeUsage(r.root)
	if err != nil {
		nu = 0
	}
	return protocol.QuotaScopeNode, nu, r.lim.NodeQuotaBytes
}

// quotaEvents builds the §8.6 exhaustion triple: quota_reached, the
// quota gap, and the quota stop last.
func (r *ingestRun) quotaEvents(scope protocol.QuotaScope, used, limit int64) []protocol.EventEnvelope {
	now := r.deps.Clock.Now()
	mono := r.deps.Mono.MonotonicNS()
	reachedData, _ := jsonBytes(protocol.CaptureQuotaReached{
		CaptureSessionID: protocol.OpaqueID(r.opts.Session),
		QuotaScope:       scope, Bytes: used, LimitBytes: limit,
	})
	reached := r.baseEnvelope(protocol.EventTypeCaptureQuotaReached, r.nextSeq, now, mono)
	reached.Payload = reachedData
	gapData, _ := jsonBytes(protocol.CaptureGapDetected{
		CaptureSessionID: protocol.OpaqueID(r.opts.Session),
		Cause:            protocol.CaptureGapCauseQuota,
	})
	gap := r.baseEnvelope(protocol.EventTypeCaptureGapDetected, r.nextSeq+1, now, mono)
	gap.Payload = gapData
	stoppedData, _ := jsonBytes(protocol.CaptureStopped{
		CaptureSessionID: protocol.OpaqueID(r.opts.Session),
		Reason:           protocol.CaptureStopReasonQuota,
	})
	stopped := r.baseEnvelope(protocol.EventTypeCaptureStopped, r.nextSeq+2, now, mono)
	stopped.Payload = stoppedData
	r.nextSeq += 3
	return []protocol.EventEnvelope{reached, gap, stopped}
}

// closeSegmentRow records the run's final segment state.
func (r *ingestRun) closeSegmentRow(ctx context.Context) error {
	info := r.segs[r.curID]
	info.closed = true
	if info.ended.IsZero() {
		info.ended = info.lastWall
	}
	row := SegmentRow{
		SegmentID: r.curID, CaptureSessionID: r.opts.Session,
		SeqFrom: info.firstSeq, SeqTo: info.lastSeq,
		StartedAt:   formatWall(info.started),
		EndedAt:     formatWall(info.ended),
		StoragePath: r.curID + ".raw", IndexPath: r.curID + ".idx",
		BytesRaw: info.bytes,
	}
	if r.segRows[r.curID] {
		return r.store.UpdateSegment(ctx, row)
	}
	if info.chunks == 0 {
		return nil
	}
	return r.store.InsertSegment(ctx, row)
}

// compressFinal closes the final segment with verification after eof
// or stop: headroom, frame-at-a-time encode, verify, publish, row,
// raw removal. Failures keep the raw segment and log a line: recovery
// is forward, never a rewrite.
func (r *ingestRun) compressFinal(ctx context.Context, cause string) {
	id := r.curID
	info := r.segs[id]
	total := info.bytes
	headroom := total + total/64 + 1048576
	if ok, _ := reserve(r.root, r.opts.Agent, r.opts.Session, headroom, r.lim); !ok {
		r.logLine("skip compression: headroom refused")
		return
	}
	r.granted += headroom
	codec, err := newFrameCodec(r.lim.FrameMaxBytes)
	if err != nil {
		r.logLine("skip compression: " + err.Error())
		return
	}
	defer func() { _ = codec.Close() }()
	runs, err := groupFrameRuns(info.lens, r.lim.FrameMaxBytes)
	if err != nil {
		r.logLine("skip compression: " + err.Error())
		return
	}
	dir := r.opts.Agent + "/" + r.opts.Session
	rawFd, err := r.root.Open(dir + "/" + id + ".raw")
	if err != nil {
		r.logLine("skip compression: " + err.Error())
		return
	}
	defer func() { _ = rawFd.Close() }()
	zstFd, zstTmp, err := createTemp(r.root, dir, id+".zst")
	if err != nil {
		r.logLine("skip compression: " + err.Error())
		return
	}
	framesFd, framesTmp, err := createTemp(r.root, dir, id+".frames")
	if err != nil {
		_ = zstFd.Close()
		_ = removeTemp(r.root, dir, zstTmp)
		r.logLine("skip compression: " + err.Error())
		return
	}
	ok := r.encodeFrames(codec, rawFd, zstFd, framesFd, id, total, runs)
	zstLen := fileSize(zstFd)
	_ = zstFd.Close()
	_ = framesFd.Close()
	if !ok {
		_ = removeTemp(r.root, dir, zstTmp)
		_ = removeTemp(r.root, dir, framesTmp)
		return
	}
	rawSHA := r.segmentSHA(id, total)
	r.publishCompressed(ctx, id, zstTmp, framesTmp, zstLen, total, rawSHA)
	_ = cause
}

// encodeFrames writes the temp .zst and .frames frame by frame,
// verifying each decoded frame against its raw range and hashing the
// decoded bytes incrementally. Only one frame's raw bytes are held.
func (r *ingestRun) encodeFrames(codec *frameCodec, rawFd, zstFd, framesFd *os.File, id string, total int64, runs [][]int) bool {
	var entries []frameEntry
	var compOff int64
	rawHash := sha256.New()
	decHash := sha256.New()
	var rawOff int64
	for _, run := range runs {
		var rawLen int64
		for _, ci := range run {
			rawLen += r.segs[id].lens[ci]
		}
		raw, err := readAt(rawFd, rawOff, rawLen)
		if err != nil {
			r.logLine("skip compression: " + err.Error())
			return false
		}
		frag, err := codec.compressFrame(raw)
		if err != nil {
			r.logLine("skip compression: " + err.Error())
			return false
		}
		back, err := codec.decompressFrame(frag, rawLen)
		if err != nil {
			r.logLine("skip compression: " + err.Error())
			return false
		}
		if !bytes.Equal(back, raw) {
			r.logLine("skip compression: verify mismatch")
			return false
		}
		rawHash.Write(raw)
		decHash.Write(back)
		entries = append(entries, frameEntry{RawOffset: rawOff, RawLength: rawLen, Offset: compOff, Length: int64(len(frag))})
		if err := writeFull(zstFd, frag); err != nil {
			r.logLine("skip compression: " + err.Error())
			return false
		}
		compOff += int64(len(frag))
		rawOff += rawLen
	}
	rawSHA := hex.EncodeToString(rawHash.Sum(nil))
	if rawOff != total || rawSHA != hex.EncodeToString(decHash.Sum(nil)) {
		r.logLine("skip compression: coverage mismatch")
		return false
	}
	doc, err := json.Marshal(framesDoc{
		Schema: "gifford.capture.frames.v1", SegmentID: id,
		RawBytes: total, RawSHA256: rawSHA, Frames: entries,
	})
	if err != nil {
		r.logLine("skip compression: " + err.Error())
		return false
	}
	if err := writeFull(framesFd, doc); err != nil {
		r.logLine("skip compression: " + err.Error())
		return false
	}
	if err := zstFd.Sync(); err != nil {
		r.logLine("skip compression: " + err.Error())
		return false
	}
	if err := framesFd.Sync(); err != nil {
		r.logLine("skip compression: " + err.Error())
		return false
	}
	return true
}

// publishCompressed publishes .frames then .zst without replacing,
// reusing verified finals, then records the row and removes the .raw.
// Finals that already exist are reused only when they verify against
// the same raw SHA; otherwise everything is kept and the segment
// stays raw.
func (r *ingestRun) publishCompressed(ctx context.Context, id, zstTmp, framesTmp string, zstLen, total int64, rawSHA string) {
	dir := r.opts.Agent + "/" + r.opts.Session
	done := func(compressed bool, storage string, size int64) {
		row := SegmentRow{
			SegmentID: id, CaptureSessionID: r.opts.Session,
			SeqFrom: r.segs[id].firstSeq, SeqTo: r.segs[id].lastSeq,
			StartedAt:   formatWall(r.segs[id].started),
			EndedAt:     formatWall(r.segs[id].ended),
			StoragePath: storage, IndexPath: id + ".idx",
			BytesRaw: total, Compressed: compressed, SHA256: rawSHA,
		}
		if compressed {
			n := size
			row.BytesCompressed = &n
		}
		if err := r.store.UpdateSegment(ctx, row); err != nil {
			r.logLine("compressed row skipped: " + err.Error())
			return
		}
		if compressed {
			_ = r.root.Remove(dir + "/" + id + ".raw")
			_ = syncDirAt(r.root, dir)
		}
	}
	if r.verifyFinals(id, total, rawSHA) {
		fi, err := r.root.Stat(dir + "/" + id + ".zst")
		if err != nil {
			r.logLine("reuse skipped: " + err.Error())
			return
		}
		done(true, id+".zst", fi.Size())
		return
	}
	if err := renamePublished(r.root, dir, framesTmp, id+".frames", true); err != nil {
		r.logLine("keep raw: " + err.Error())
		return
	}
	if err := renamePublished(r.root, dir, zstTmp, id+".zst", true); err != nil {
		r.logLine("keep raw: " + err.Error())
		return
	}
	if err := syncDirAt(r.root, dir); err != nil {
		r.logLine("keep raw: " + err.Error())
		return
	}
	done(true, id+".zst", zstLen)
}

// verifyFinals checks an existing compressed pair against the raw SHA
// and contiguous coverage, without trusting the row.
func (r *ingestRun) verifyFinals(id string, total int64, rawSHA string) bool {
	dir := r.opts.Agent + "/" + r.opts.Session
	var doc framesDoc
	if err := readControlAt(r.root, dir+"/"+id+".frames", 1<<24, &doc); err != nil {
		return false
	}
	if doc.RawSHA256 != rawSHA || doc.RawBytes != total || doc.SegmentID != id {
		return false
	}
	var at int64
	for _, f := range doc.Frames {
		if f.RawOffset != at {
			return false
		}
		at += f.RawLength
	}
	return at == total
}

// readAt reads exactly n bytes at off.
func readAt(f *os.File, off, n int64) ([]byte, error) {
	buf := make([]byte, n)
	if _, err := io.ReadFull(io.NewSectionReader(f, off, n), buf); err != nil {
		return nil, fmt.Errorf("read segment: %w", err)
	}
	return buf, nil
}

// fileSize stats an open file's length, or -1.
func fileSize(f *os.File) int64 {
	fi, err := f.Stat()
	if err != nil {
		return -1
	}
	return fi.Size()
}

// segmentSHA recomputes the raw file's SHA-256, streamed.
func (r *ingestRun) segmentSHA(id string, total int64) string {
	dir := r.opts.Agent + "/" + r.opts.Session
	f, err := r.root.Open(dir + "/" + id + ".raw")
	if err != nil {
		return ""
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err := io.CopyBuffer(h, io.LimitReader(f, total+1), make([]byte, 1<<20)); err != nil {
		return ""
	}
	return hex.EncodeToString(h.Sum(nil))
}

// createTemp creates an O_EXCL temp file for a final name.
func createTemp(root *os.Root, dir, name string) (*os.File, string, error) {
	tmp, err := makeTempName(name)
	if err != nil {
		return nil, "", err
	}
	f, err := root.OpenFile(dir+"/"+tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, "", fmt.Errorf("%w: temp file: %w", ErrUnsafePath, err)
	}
	return f, tmp, nil
}

// startPoller reads the pane size every resize interval; a missing
// pane is ignored since EOF follows.
func (r *ingestRun) startPoller(ctx context.Context) {
	pollCtx, stop := context.WithCancel(ctx)
	r.mu.Lock()
	r.pollStop = stop
	r.mu.Unlock()
	seenCols, seenRows := r.row.PaneCols, r.row.PaneRows
	r.pollerWg.Add(1)
	go func() {
		defer r.pollerWg.Done()
		tick := time.NewTicker(time.Duration(r.lim.ResizeInterval))
		defer tick.Stop()
		for {
			select {
			case <-pollCtx.Done():
				return
			case <-tick.C:
				pane, err := r.deps.Runner.pane(pollCtx, r.row.TmuxPane)
				if err != nil {
					continue
				}
				if int64(pane.cols) != seenCols || int64(pane.rows) != seenRows {
					seenCols, seenRows = int64(pane.cols), int64(pane.rows)
					r.resizeMu.Lock()
					r.resizeCol, r.resizeRow, r.resizeHit = seenCols, seenRows, true
					r.resizeMu.Unlock()
				}
			}
		}
	}()
}

// stopPoller ends the resize poller and waits for it: no resize is
// produced past this point.
func (r *ingestRun) stopPoller() {
	r.mu.Lock()
	stop := r.pollStop
	r.mu.Unlock()
	if stop != nil {
		stop()
		r.pollerWg.Wait()
	}
}

// writeExit publishes the bounded exit record, replacing. A failure,
// the remnant cap included, is logged and skipped: a missing record
// maps to ingest_died.
func (r *ingestRun) writeExit(cause string) {
	data, err := jsonBytes(ingestExit{
		Schema: "gifford.capture.exit.v1", Cause: cause,
		At:  formatWall(r.deps.Clock.Now()),
		PID: r.deps.PID, Attempt: r.opts.Attempt,
	})
	if err != nil {
		r.logLine("exit record skipped: " + err.Error())
		return
	}
	if err := publishFile(r.root, r.opts.Agent+"/"+r.opts.Session, "ingest.exit", []byte(data), false); err != nil {
		r.logLine("exit record skipped: " + err.Error())
	}
}
