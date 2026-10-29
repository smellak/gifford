package capture

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
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
func (s *Store) RepairSession(ctx context.Context, root *os.Root, agent, session string, clk clock.Clock) error {
	row, found, err := s.SessionRow(ctx, session)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	events, err := readAllEvents(ctx, s, session)
	if err != nil {
		return err
	}
	switch DeriveLife(true, events) {
	case LifeTerminal:
		return s.repairTerminal(ctx, root, agent, row, events)
	case LifeIncomplete:
		return s.finishIncomplete(ctx, root, agent, session, row, clk)
	default:
		return s.repairLive(ctx, root, agent, session, row, events, clk)
	}
}

// readAllEvents pages through the whole session stream.
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

// maxSequence returns the highest committed sequence, or -1 on none.
func maxSequence(events []protocol.EventEnvelope) int64 {
	highest := int64(-1)
	for _, ev := range events {
		if ev.Sequence > highest {
			highest = ev.Sequence
		}
	}
	return highest
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
	row.State = "ERROR"
	row.StopReason = "error"
	row.StoppedAt = formatWall(clk.Now())
	return s.UpdateSession(ctx, row)
}

// repairTerminal applies a committed stop to the row, over the event
// repair every terminal session gets.
func (s *Store) repairTerminal(ctx context.Context, root *os.Root, agent string, row SessionRow, events []protocol.EventEnvelope) error {
	stopped := FindStopped(events)
	reason, err := StoppedReason(stopped)
	if err != nil {
		return err
	}
	row.State = StoppedRowState(reason)
	row.StoppedAt = formatWall(stopped.WallTime)
	row.StopReason = string(reason)
	return s.applyRepair(ctx, root, agent, row, events)
}

// repairLive restores a lost stop intent from stop.request, then
// repairs the event cache of a live session.
func (s *Store) repairLive(ctx context.Context, root *os.Root, agent, session string, row SessionRow, events []protocol.EventEnvelope, clk clock.Clock) error {
	req, err := readStopRequest(root, agent, session)
	if err != nil {
		return err
	}
	if req != nil {
		return s.restoreStop(ctx, root, row, events, req, clk)
	}
	return s.applyRepair(ctx, root, agent, row, events)
}

// restoreStop re-appends a stop the power failure lost, then repairs
// terminally. The request stays: once terminal it is inert.
func (s *Store) restoreStop(ctx context.Context, root *os.Root, row SessionRow, events []protocol.EventEnvelope, req *stopRequest, clk clock.Clock) error {
	node, err := readNodeID(root)
	if err != nil {
		return err
	}
	ev, err := stoppedEnvelopeFor(clk, node, row, maxSequence(events)+1, req.Reason, req.Detail)
	if err != nil {
		return err
	}
	if err := s.AppendEvents(ctx, []protocol.EventEnvelope{ev}); err != nil {
		return err
	}
	return s.repairTerminal(ctx, root, row.AgentID, row, append(events, ev))
}

// stoppedEnvelopeFor builds the restore envelope: the §8.4 fields from
// the row, the wall time from the clock, and monotonic zero, since a
// restored stop follows a power loss the clock did not survive.
func stoppedEnvelopeFor(clk clock.Clock, node string, row SessionRow, seq int64, reason, detail string) (protocol.EventEnvelope, error) {
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
		MonotonicNS:   0,
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

// chunkFacts is one committed chunk: its sequence, byte count and wall
// time.
type chunkFacts struct {
	seq   int64
	bytes int64
	wall  time.Time
}

// segSpan is one segment derived from the events: closed by a rotated
// event naming it, or open when chunks follow the last boundary. An
// empty id means no rotated event named the open segment: its id comes
// from the single orphan file on disk, or the repair fails closed.
type segSpan struct {
	id      string
	from    int64
	to      int64
	bytes   int64
	started time.Time
	ended   time.Time
	closed  bool
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

// repairFromEvents walks the stream in order: chunk coverage, the
// rotated boundaries, the last pane recreation, resize and snapshot
// sizes, and the reattach count.
func repairFromEvents(row SessionRow, events []protocol.EventEnvelope) (eventRepair, error) {
	rep := eventRepair{pane: row.TmuxPane, cols: row.PaneCols, rows: row.PaneRows, blobRef: row.InitialScreenBlobRef}
	var pending []chunkFacts
	var openID string
	var screenSeq, resizeSeq int64 = -1, -1
	for i := range events {
		ev := &events[i]
		switch ev.EventType {
		case protocol.EventTypeCaptureChunk:
			var chunk protocol.CaptureChunk
			if err := decodePayload(ev, &chunk); err != nil {
				return rep, err
			}
			pending = append(pending, chunkFacts{seq: ev.Sequence, bytes: chunk.ByteCount, wall: ev.WallTime})
		case protocol.EventTypeCaptureRotated:
			var rotated protocol.CaptureRotated
			if err := decodePayload(ev, &rotated); err != nil {
				return rep, err
			}
			if len(pending) > 0 {
				rep.spans = append(rep.spans, segSpan{
					id:      string(rotated.ClosedSegmentID),
					from:    pending[0].seq,
					to:      pending[len(pending)-1].seq,
					bytes:   sumChunks(pending),
					started: pending[0].wall,
					ended:   ev.WallTime,
					closed:  true,
				})
				pending = nil
			}
			openID = string(rotated.NewSegmentID)
		case protocol.EventTypeCapturePaneRecreated:
			var recreated protocol.CapturePaneRecreated
			if err := decodePayload(ev, &recreated); err != nil {
				return rep, err
			}
			rep.pane = string(recreated.NewPane)
		case protocol.EventTypeCaptureResize:
			var resize protocol.CaptureResize
			if err := decodePayload(ev, &resize); err != nil {
				return rep, err
			}
			if ev.Sequence >= resizeSeq {
				resizeSeq = ev.Sequence
				rep.cols, rep.rows = resize.Cols, resize.Rows
			}
		case protocol.EventTypeCaptureInitialScreen:
			var screen protocol.CaptureInitialScreen
			if err := decodePayload(ev, &screen); err != nil {
				return rep, err
			}
			if rep.blobRef == "" {
				rep.blobRef = string(screen.SnapshotBlobRef)
			}
			if ev.Sequence >= screenSeq {
				screenSeq = ev.Sequence
				if resizeSeq < screenSeq {
					rep.cols, rep.rows = screen.Cols, screen.Rows
				}
			}
		case protocol.EventTypeCaptureReattached:
			rep.reattaches++
		default:
			// Only capture events repair rows; anything else stays.
		}
	}
	if len(pending) > 0 {
		rep.spans = append(rep.spans, segSpan{
			id:      openID,
			from:    pending[0].seq,
			to:      pending[len(pending)-1].seq,
			bytes:   sumChunks(pending),
			started: pending[0].wall,
			closed:  false,
		})
	}
	return rep, nil
}

// sumChunks totals one span's committed bytes.
func sumChunks(chunks []chunkFacts) int64 {
	var total int64
	for _, c := range chunks {
		total += c.bytes
	}
	return total
}

// applyRepair reconciles the segment rows with the derived spans, sums
// bytes_captured over them, and writes the session row.
func (s *Store) applyRepair(ctx context.Context, root *os.Root, agent string, row SessionRow, events []protocol.EventEnvelope) error {
	rep, err := repairFromEvents(row, events)
	if err != nil {
		return err
	}
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
		if id == "" {
			id, err = orphanSegmentID(root, agent, row.CaptureSessionID, byID)
			if err != nil {
				return err
			}
		}
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

// orphanSegmentID names the open segment from disk: the single seg-
// raw file with no row. Zero or several is ambiguous, and the repair
// fails closed instead of guessing.
func orphanSegmentID(root *os.Root, agent, session string, have map[string]SegmentRow) (string, error) {
	d, err := root.Open(agent + "/" + session)
	if err != nil {
		return "", fmt.Errorf("%w: list session files: %w", ErrUnsafePath, err)
	}
	names, err := d.Readdirnames(-1)
	_ = d.Close()
	if err != nil {
		return "", fmt.Errorf("%w: list session files: %w", ErrUnsafePath, err)
	}
	var orphans []string
	for _, name := range names {
		stem, ok := strings.CutSuffix(name, ".raw")
		if !ok {
			continue
		}
		if err := validateSegmentID(stem); err != nil {
			continue
		}
		if _, ok := have[stem]; !ok {
			orphans = append(orphans, stem)
		}
	}
	if len(orphans) != 1 {
		return "", fmt.Errorf("%w: %d orphan segments", ErrUnsafePath, len(orphans))
	}
	return orphans[0], nil
}
