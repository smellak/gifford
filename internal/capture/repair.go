package capture

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

// RepairSession repairs one session's rows from its events. The caller
// holds the session's ingest.lock: every decision re-reads under it.
// It repairs the cached state, sizes, panes and segment rows, and the
// NORMAL-loss restore of a stop intent from stop.request. Quota and
// error re-evaluation after power loss belongs to the exhaustion
// module (§8.6/§8.7), which owns those stop paths.
func (s *Store) RepairSession(ctx context.Context, root *os.Root, _, session string, clk clock.Clock) error {
	row, found, err := s.SessionRow(ctx, session)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	// One bounded streaming pass: the fold carries the repair and
	// the lifecycle, so the history is never materialized.
	fold := newRepairFold(row)
	if err := streamSession(ctx, s, session, fold.add); err != nil {
		return err
	}
	switch fold.life() {
	case LifeTerminal:
		return s.repairFolded(ctx, row, fold)
	case LifeIncomplete:
		// The caller holds the session lock by contract, so no
		// liveness probe runs: a held lock here is always ours,
		// and probing it would mistake it for a live attach.
		return finishIncompleteHeld(ctx, s, row, clk)
	default:
		return s.repairLiveFolded(ctx, root, row, fold, clk)
	}
}

// readAllEvents pages through the whole session stream. No store
// path retains histories anymore; it stays for the tagged synthetic
// review probes, which feed slices to the slice-based checks.
//
//nolint:unused // review-harness support, not dead code.
func readAllEvents(ctx context.Context, s *Store, session string) ([]protocol.EventEnvelope, error) {
	var out []protocol.EventEnvelope
	var from int64
	for {
		page, err := s.SessionEvents(ctx, session, from, eventstore.MaxBatchEvents)
		if err != nil {
			return nil, err
		}
		if len(page) == 0 {
			return out, nil
		}
		out = append(out, page...)
		from = page[len(page)-1].Sequence + 1
		if len(page) < eventstore.MaxBatchEvents {
			return out, nil
		}
	}
}

// finishIncomplete finishes a row whose attach never committed, but
// only when its lock is free: a held lock means attach still owns it.
// This is a row-only finish, because no event stream exists.
func (s *Store) finishIncomplete(ctx context.Context, root *os.Root, agent, session string, row SessionRow, clk clock.Clock) error {
	live, err := lockLive(root, agent+"/"+session+"/ingest.lock")
	if err != nil {
		return err
	}
	if live {
		return nil
	}
	row.State = stateError
	row.StopReason = reasonError
	row.StoppedAt = formatWall(clk.Now())
	return s.UpdateSession(ctx, row)
}

// repairFolded applies a committed stop to the row, over the event
// repair every terminal session gets. The stopped envelope comes from
// the fold's last stopped event.
func (s *Store) repairFolded(ctx context.Context, row SessionRow, fold *repairFold) error {
	reason, err := StoppedReason(&fold.stopped)
	if err != nil {
		return err
	}
	row.State = StoppedRowState(reason)
	row.StoppedAt = formatWall(fold.stopped.WallTime)
	row.StopReason = string(reason)
	rep, err := fold.finish()
	if err != nil {
		return err
	}
	return s.applyRepair(ctx, row, rep)
}

// repairLiveFolded restores a lost stop intent from stop.request,
// then repairs the event cache of a live session.
func (s *Store) repairLiveFolded(ctx context.Context, root *os.Root, row SessionRow, fold *repairFold, clk clock.Clock) error {
	req, err := readStopRequest(root, row.AgentID, row.CaptureSessionID)
	if err != nil {
		return err
	}
	if req != nil {
		return s.restoreStopFolded(ctx, root, row, fold, req, clk, 0)
	}
	rep, err := fold.finish()
	if err != nil {
		return err
	}
	return s.applyRepair(ctx, row, rep)
}

// restoreStopFolded re-appends a stop the power failure lost, then
// repairs terminally. The restored event folds into the running
// scan: no second history read. The request stays: once terminal it
// is inert. monoNS timestamps the new event: zero only for the
// power-loss restore, where no clock survived; live callers pass
// their current reading.
func (s *Store) restoreStopFolded(ctx context.Context, root *os.Root, row SessionRow, fold *repairFold, req *stopRequest, clk clock.Clock, monoNS int64) error {
	node, err := readNodeID(root)
	if err != nil {
		return err
	}
	ev, err := stoppedEnvelopeFor(clk, node, row, fold.maxSeq+1, req.Reason, req.Detail, monoNS)
	if err != nil {
		return err
	}
	if err := s.AppendEvents(ctx, []protocol.EventEnvelope{ev}); err != nil {
		return err
	}
	if err := fold.add(&ev); err != nil {
		return err
	}
	return s.repairFolded(ctx, row, fold)
}

// stoppedEnvelopeFor builds a stopped envelope: the §8.4 fields from
// the row, the wall time from the clock, and the given monotonic
// reading. Restores pass zero, since they follow a power loss the
// clock did not survive.
func stoppedEnvelopeFor(clk clock.Clock, node string, row SessionRow, seq int64, reason, detail string, monoNS int64) (protocol.EventEnvelope, error) {
	id, err := newULID(clk)
	if err != nil {
		return protocol.EventEnvelope{}, fmt.Errorf("stop restore ulid: %w", err)
	}
	data, err := jsonBytes(protocol.CaptureStopped{
		CaptureSessionID: protocol.OpaqueID(row.CaptureSessionID),
		Reason:           protocol.CaptureStopReason(reason),
		Detail:           detail,
	})
	if err != nil {
		return protocol.EventEnvelope{}, err
	}
	return protocol.EventEnvelope{
		EventID:       protocol.ULID(id),
		SchemaVersion: 1,
		NodeID:        protocol.NodeID(node),
		SessionID:     protocol.OpaqueID(row.CaptureSessionID),
		AgentID:       protocol.AgentID(row.AgentID),
		Harness:       protocol.Harness(row.Harness),
		AdapterType:   protocol.AdapterTypeNone,
		SourceQuality: protocol.SourceQualityBootstrapPipePane,
		EventType:     protocol.EventTypeCaptureStopped,
		Stream:        protocol.StreamPTY,
		WallTime:      clk.Now(),
		MonotonicNS:   monoNS,
		Sequence:      seq,
		Payload:       data,
		Sensitivity:   protocol.SensitivityNormal,
	}, nil
}

// jsonBytes encodes one capture payload for an envelope.
func jsonBytes(v any) (protocol.Payload, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("%w: encode capture payload", ErrUnsafePath)
	}
	return protocol.Payload(data), nil
}

// segAcc accumulates one segment's chunks in stream order. The
// identity and the raw offsets come from each chunk's raw_blob_ref;
// the ref is the only identity source, so runs never borrow each
// other's segments and the disk is never scanned for names.
type segAcc struct {
	id      string
	from    int64
	to      int64
	bytes   int64
	started time.Time
	ended   time.Time
	nextOff int64
}

// segSpan is one segment derived from the events: closed by a rotated
// event naming it, or closed as crash leftover at a run boundary or
// the stream end. The id always comes from a chunk ref.
type segSpan struct {
	id      string
	from    int64
	to      int64
	bytes   int64
	started time.Time
	ended   time.Time
	closed  bool
}

// parseChunkRef splits a chunk's raw_blob_ref into its segment id and
// raw offset: blob://capture/<session>/<seg-id>/<offset>[-<len>]. The
// ref must name this session; a foreign or malformed ref fails closed
// instead of attributing bytes elsewhere.
func parseChunkRef(session, ref string) (string, int64, error) {
	rest, ok := strings.CutPrefix(ref, "blob://capture/"+session+"/")
	if !ok {
		return "", 0, fmt.Errorf("%w: chunk ref outside session: %q", ErrUnsafePath, ref)
	}
	seg, tail, ok := strings.Cut(rest, "/")
	if !ok || tail == "" {
		return "", 0, fmt.Errorf("%w: malformed chunk ref: %q", ErrUnsafePath, ref)
	}
	if err := validateSegmentID(seg); err != nil {
		return "", 0, fmt.Errorf("%w: malformed chunk ref: %q", ErrUnsafePath, ref)
	}
	if i := strings.Index(tail, "-"); i >= 0 {
		tail = tail[:i]
	}
	off, err := strconv.ParseInt(tail, 10, 64)
	if err != nil || off < 0 {
		return "", 0, fmt.Errorf("%w: malformed chunk offset: %q", ErrUnsafePath, ref)
	}
	return seg, off, nil
}

// eventRepair is the row cache recomputed from the events.
type eventRepair struct {
	pane       string
	cols       int64
	rows       int64
	blobRef    string
	reattaches int64
	spans      []segSpan
}

// errScanDone ends a streamSession scan early: the caller already
// decided. It maps to nil, never to a failure.
var errScanDone = errors.New("capture scan decided")

// streamSession pages one session's stream in sequence order through
// the session-scoped point query, feeding one page at a time. Only
// one page is retained; callers fold O(1) facts out of the stream
// instead of materializing the whole history.
func streamSession(ctx context.Context, s *Store, session string, f func(ev *protocol.EventEnvelope) error) error {
	var from int64
	for {
		page, err := s.SessionEvents(ctx, session, from, eventstore.MaxBatchEvents)
		if err != nil {
			return err
		}
		if len(page) == 0 {
			return nil
		}
		for i := range page {
			if err := f(&page[i]); err != nil {
				if errors.Is(err, errScanDone) {
					return nil
				}
				return err
			}
		}
		from = page[len(page)-1].Sequence + 1
		if len(page) < eventstore.MaxBatchEvents {
			return nil
		}
	}
}

// scanLife derives one session's lifecycle with a single bounded
// streaming pass: only O(1) facts are kept, and the scan ends at the
// first stopped event. Terminal rows skip the scan entirely through
// terminalRowState.
func scanLife(ctx context.Context, s *Store, session string) (Life, error) {
	stopped := false
	attached0 := false
	err := streamSession(ctx, s, session, func(ev *protocol.EventEnvelope) error {
		switch ev.EventType {
		case protocol.EventTypeCaptureStopped:
			stopped = true
			return errScanDone
		case protocol.EventTypeCaptureAttached:
			if ev.Sequence == 0 {
				attached0 = true
			}
		default:
			// Only capture events feed the lifecycle; anything
			// else stays out of it.
		}
		return nil
	})
	if err != nil {
		return LifeNoRow, err
	}
	if stopped {
		return LifeTerminal, nil
	}
	if !attached0 {
		return LifeIncomplete, nil
	}
	return LifeLive, nil
}

// repairFold folds one session stream in order into the row repair:
// per-segment chunk coverage from each chunk's raw_blob_ref, the
// rotated boundaries, the last pane recreation, resize and snapshot
// sizes, the reattach count, and the lifecycle facts the start token
// and the repair branches need. One fold replaces two full-history
// materializations: the stream is read once, in bounded pages.
type repairFold struct {
	rep       eventRepair
	session   string
	accs      map[string]*segAcc
	order     []string
	screenSeq int64
	resizeSeq int64
	seq1ref   string
	seq1seen  bool
	// lastScreenRef is the latest committed initial-screen ref and
	// its sequence: the start boundary the supervisor gap decision
	// validates exit attempts against, exactly.
	lastScreenRef string
	lastScreenSeq int64
	maxSeq        int64
	lastMono      int64
	stopped       protocol.EventEnvelope
	hasStopped    bool
	attached0     bool
}

// newRepairFold starts the fold from the cached row.
func newRepairFold(row SessionRow) *repairFold {
	return &repairFold{
		rep: eventRepair{
			pane: row.TmuxPane, cols: row.PaneCols, rows: row.PaneRows,
			blobRef: row.InitialScreenBlobRef,
		},
		session:   row.CaptureSessionID,
		accs:      make(map[string]*segAcc),
		screenSeq: -1, resizeSeq: -1,
		maxSeq: -1,
	}
}

// closeAcc closes one open span: at the rotation wall for a clean
// close, at its last committed chunk for crash leftover.
func (f *repairFold) closeAcc(id string, ended time.Time, crash bool) {
	acc, ok := f.accs[id]
	if !ok {
		return
	}
	if !crash {
		acc.ended = ended
	}
	f.rep.spans = append(f.rep.spans, segSpan{
		id: acc.id, from: acc.from, to: acc.to, bytes: acc.bytes,
		started: acc.started, ended: acc.ended, closed: true,
	})
	delete(f.accs, id)
}

// add folds one envelope in stream order.
func (f *repairFold) add(ev *protocol.EventEnvelope) error {
	if ev.Sequence > f.maxSeq {
		f.maxSeq = ev.Sequence
	}
	f.lastMono = ev.MonotonicNS
	switch ev.EventType {
	case protocol.EventTypeCaptureStopped:
		f.stopped = *ev
		f.hasStopped = true
	case protocol.EventTypeCaptureAttached:
		if ev.Sequence == 0 {
			f.attached0 = true
		}
	case protocol.EventTypeCaptureChunk:
		var chunk protocol.CaptureChunk
		if err := decodePayload(ev, &chunk); err != nil {
			return err
		}
		seg, off, err := parseChunkRef(f.session, string(chunk.RawBlobRef))
		if err != nil {
			return err
		}
		acc, ok := f.accs[seg]
		if !ok {
			acc = &segAcc{id: seg, from: ev.Sequence, started: ev.WallTime}
			f.accs[seg] = acc
			f.order = append(f.order, seg)
		}
		if off != acc.nextOff {
			return fmt.Errorf("%w: segment %s offset %d, want %d", ErrUnsafePath, seg, off, acc.nextOff)
		}
		acc.nextOff = off + chunk.ByteCount
		acc.to = ev.Sequence
		acc.bytes += chunk.ByteCount
		acc.ended = ev.WallTime
	case protocol.EventTypeCaptureRotated:
		var rotated protocol.CaptureRotated
		if err := decodePayload(ev, &rotated); err != nil {
			return err
		}
		// The rotation's closed id ends the ref-derived span;
		// an id with no chunks is an empty segment with nothing
		// to record.
		f.closeAcc(string(rotated.ClosedSegmentID), ev.WallTime, false)
	case protocol.EventTypeCapturePaneRecreated:
		var recreated protocol.CapturePaneRecreated
		if err := decodePayload(ev, &recreated); err != nil {
			return err
		}
		f.rep.pane = string(recreated.NewPane)
	case protocol.EventTypeCaptureResize:
		var resize protocol.CaptureResize
		if err := decodePayload(ev, &resize); err != nil {
			return err
		}
		if ev.Sequence >= f.resizeSeq {
			f.resizeSeq = ev.Sequence
			f.rep.cols, f.rep.rows = resize.Cols, resize.Rows
		}
	case protocol.EventTypeCaptureInitialScreen:
		var screen protocol.CaptureInitialScreen
		if err := decodePayload(ev, &screen); err != nil {
			return err
		}
		if ev.Sequence == 1 && !f.seq1seen {
			f.seq1seen = true
			f.seq1ref = string(screen.SnapshotBlobRef)
		}
		f.lastScreenRef = string(screen.SnapshotBlobRef)
		f.lastScreenSeq = ev.Sequence
		if ev.Sequence >= f.screenSeq {
			f.screenSeq = ev.Sequence
			if f.resizeSeq < f.screenSeq {
				f.rep.cols, f.rep.rows = screen.Cols, screen.Rows
			}
		}
	case protocol.EventTypeCaptureReattached:
		f.rep.reattaches++
		// A run boundary: every open span belongs to the dead run
		// and closes at its last committed chunk, in
		// first-appearance order.
		for _, id := range f.order {
			f.closeAcc(id, time.Time{}, true)
		}
		f.order = nil
	default:
		// Only capture events repair rows; anything else stays.
	}
	return nil
}

// finish closes the trailing spans and returns the repair: no run
// ever appends to another run's segment, so a writer-less tail is
// crash leftover with its end at the last committed chunk.
func (f *repairFold) finish() (eventRepair, error) {
	for _, id := range f.order {
		f.closeAcc(id, time.Time{}, true)
	}
	f.order = nil
	// CAP-A2: the row's initial ref is repaired only from the
	// sequence-1 attach snapshot; a conflicting non-null ref is
	// refused instead of silently kept.
	switch {
	case f.seq1seen && f.rep.blobRef == "":
		f.rep.blobRef = f.seq1ref
	case f.seq1seen && f.rep.blobRef != f.seq1ref:
		return f.rep, fmt.Errorf("%w: conflicting initial_screen_blob_ref", ErrUnsafePath)
	}
	return f.rep, nil
}

// life reports the folded lifecycle, the DeriveLife truth over the
// scanned stream: terminal iff a stopped event exists, incomplete
// iff no attached sits at zero.
func (f *repairFold) life() Life {
	if f.hasStopped {
		return LifeTerminal
	}
	if !f.attached0 {
		return LifeIncomplete
	}
	return LifeLive
}

// repairFromEvents walks a retained slice through the fold. It keeps
// the slice-based repair for callers that already hold events; the
// store paths stream through streamSession instead.
func repairFromEvents(row SessionRow, events []protocol.EventEnvelope) (eventRepair, error) {
	fold := newRepairFold(row)
	for i := range events {
		if err := fold.add(&events[i]); err != nil {
			return fold.rep, err
		}
	}
	return fold.finish()
}

// tokenCheck folds the §6.4 start-token facts out of the stream:
// contiguity at n, a real boundary at n-1, and the latest boundary
// screen naming exactly this boundary and this attempt.
type tokenCheck struct {
	n         int64
	maxSeq    int64
	stopped   bool
	attached0 bool
	edge      protocol.EventEnvelope
	screen    protocol.EventEnvelope
	hasEdge   bool
	hasScreen bool
}

// newTokenCheck starts the token fold for a resume at n.
func newTokenCheck(n int64) *tokenCheck {
	return &tokenCheck{n: n, maxSeq: -1}
}

// add folds one envelope: lifecycle bits, the n-1 edge, and the
// latest boundary screen below n.
func (c *tokenCheck) add(ev *protocol.EventEnvelope) {
	if ev.Sequence > c.maxSeq {
		c.maxSeq = ev.Sequence
	}
	switch ev.EventType {
	case protocol.EventTypeCaptureStopped:
		c.stopped = true
	case protocol.EventTypeCaptureAttached:
		if ev.Sequence == 0 {
			c.attached0 = true
		}
	case protocol.EventTypeCaptureInitialScreen:
		if ev.Sequence < c.n {
			c.screen = *ev
			c.hasScreen = true
		}
	default:
		// Only capture events feed the token; anything else
		// stays out of it.
	}
	if ev.Sequence == c.n-1 {
		c.edge = *ev
		c.hasEdge = true
	}
}

// ok reports the token verdict for this attempt: the row exists with
// a committed attach, the session is live, the next sequence matches,
// n-1 is a real boundary, and the latest boundary screen names
// exactly this boundary and this attempt (§4.8 initial-<seq>-<nonce>).
func (c *tokenCheck) ok(attempt string) bool {
	if c.stopped || !c.attached0 {
		return false
	}
	if c.maxSeq+1 != c.n {
		return false
	}
	if !c.hasEdge || !c.hasScreen {
		return false
	}
	switch c.edge.EventType {
	case protocol.EventTypeCaptureAttached,
		protocol.EventTypeCaptureInitialScreen,
		protocol.EventTypeCaptureReattached:
	default:
		return false
	}
	var initial protocol.CaptureInitialScreen
	if err := decodePayload(&c.screen, &initial); err != nil {
		return false
	}
	// The boundary batch is the authority for its own session: the
	// events come from this session's store, so the screen carries
	// the session the ref must name.
	want := "blob://capture/" + string(c.screen.SessionID) + "/initial-" +
		strconv.FormatInt(c.screen.Sequence, 10) + "-" + attempt
	return string(initial.SnapshotBlobRef) == want
}

// applyRepair reconciles the segment rows with the folded spans, sums
// bytes_captured over them, and writes the session row.
func (s *Store) applyRepair(ctx context.Context, row SessionRow, rep eventRepair) error {
	existing, err := s.SegmentsOf(ctx, row.CaptureSessionID)
	if err != nil {
		return err
	}
	byID := make(map[string]SegmentRow, len(existing))
	for _, seg := range existing {
		byID[seg.SegmentID] = seg
	}
	for _, span := range rep.spans {
		id := span.id
		if prev, ok := byID[id]; ok {
			if err := s.reconcileSegment(ctx, prev, span); err != nil {
				return err
			}
			continue
		}
		ended := ""
		if span.closed {
			ended = formatWall(span.ended)
		}
		next := SegmentRow{
			SegmentID: id, CaptureSessionID: row.CaptureSessionID,
			SeqFrom: span.from, SeqTo: span.to,
			StartedAt: formatWall(span.started), EndedAt: ended,
			StoragePath: id + ".raw", IndexPath: id + ".idx",
			BytesRaw: span.bytes,
		}
		if err := s.InsertSegment(ctx, next); err != nil {
			return err
		}
		byID[id] = next
	}
	segs, err := s.SegmentsOf(ctx, row.CaptureSessionID)
	if err != nil {
		return err
	}
	var total int64
	for _, seg := range segs {
		total += seg.BytesRaw
	}
	row.TmuxPane = rep.pane
	row.PaneCols = rep.cols
	row.PaneRows = rep.rows
	row.InitialScreenBlobRef = rep.blobRef
	row.ReattachCount = rep.reattaches
	row.BytesCaptured = total
	return s.UpdateSession(ctx, row)
}

// reconcileSegment brings an existing row to its derived span: an open
// row is updated or closed from the events. A closed row stays.
func (s *Store) reconcileSegment(ctx context.Context, prev SegmentRow, span segSpan) error {
	if prev.EndedAt != "" {
		return nil
	}
	prev.SeqTo = span.to
	prev.BytesRaw = span.bytes
	if span.closed {
		prev.EndedAt = formatWall(span.ended)
	}
	return s.UpdateSegment(ctx, prev)
}

// NOTE: span identity used to fall back to sequence ranges and a
// disk scan for row-less raw files. Since every span id comes from a
// committed chunk ref, those fallbacks are gone: no guessing.
