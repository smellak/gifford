package capture

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/protocol"
)

// SuperviseDeps is supervise's injectable environment: a per-server
// runner factory, the ingest executable, identity and clocks, and the
// data directory resolver. Zero ReadyTimeout and ReadyPoll take the
// §7 handover bounds; tests pass short ones.
type SuperviseDeps struct {
	RunnerFor    func(sock string) tmuxRunner
	Exe          func() (string, error)
	PID          int
	Clock        clock.Clock
	Mono         Mono
	DataDir      func() string
	ReadyTimeout time.Duration
	ReadyPoll    time.Duration
}

// Supervise bounds for one session's claims share and the pass loop.
const (
	superviseClaimsBound = time.Second
	superviseEvery       = 5 * time.Second
)

// Supervise walks every ATTACHED, REATTACHING and incomplete session
// once (Once) or every 5 s, repairing rows and reattaching live
// panes under each session's ingest.lock. It creates the capture
// root and opens the store mutating. A second supervisor prints one
// line and returns nil; one session's failure is logged and never
// stops the others. A failed reattach never ends a capture.
func Supervise(ctx context.Context, deps SuperviseDeps, opts SuperviseOptions) error {
	data := opts.Data
	if data == "" {
		data = deps.DataDir()
	}
	root, err := openCaptureRoot(data, true)
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	store, err := OpenStore(ctx, filepath.Join(data, "gifford.db"))
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()
	if err := ensureLock(root, "supervise.lock"); err != nil {
		return err
	}
	held, err := tryLock(root, "supervise.lock", true)
	if err != nil {
		if errors.Is(err, ErrBusy) {
			log.Print("capture supervise: another supervisor holds supervise.lock")
			return nil
		}
		return err
	}
	defer func() { _ = releaseLock(held) }()
	if err := supervisePass(ctx, deps, store, root, data); err != nil {
		return err
	}
	if opts.Once {
		return nil
	}
	tick := time.NewTicker(superviseEvery)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("supervise: %w", ctx.Err())
		case <-tick.C:
			if err := supervisePass(ctx, deps, store, root, data); err != nil {
				return err
			}
		}
	}
}

// supervisePass runs one pass over the repairable sessions: every
// live row plus every incomplete one.
func supervisePass(ctx context.Context, deps SuperviseDeps, store *Store, root *os.Root, data string) error {
	rows, err := store.AllSessions(ctx)
	if err != nil {
		return err
	}
	lives := map[string]Life{}
	claimed := map[string]bool{}
	for _, row := range rows {
		// Terminal rows never go live again: their streams stay
		// unread. Other rows get one bounded streaming pass, so a
		// pass never materializes any history.
		if terminalRowState(row.State) {
			lives[row.CaptureSessionID] = LifeTerminal
			continue
		}
		life, err := scanLife(ctx, store, row.CaptureSessionID)
		if err != nil {
			return err
		}
		lives[row.CaptureSessionID] = life
		if life == LifeLive {
			claimed[row.TmuxServer+"\x00"+row.TmuxPane] = true
		}
	}
	for _, row := range rows {
		// Terminal rows never go live again. Every other row
		// reaches the pass under its locks: a terminal stream
		// under a live row repairs its cache (§9 step 3) instead
		// of staying stale forever. The pass itself ends
		// terminal sessions with no further event or reattach.
		if terminalRowState(row.State) {
			continue
		}
		var perr error
		err := withLock(root, "claims.lock", true, superviseClaimsBound, func() error {
			perr = passSession(ctx, deps, store, root, data, row, claimed)
			return nil
		})
		if err != nil {
			if errors.Is(err, ErrBusy) {
				continue
			}
			log.Printf("capture supervise: session %s: %v", row.CaptureSessionID, err)
			continue
		}
		if perr != nil {
			log.Printf("capture supervise: session %s: %v", row.CaptureSessionID, perr)
		}
	}
	return nil
}

// passSession repairs and possibly reattaches one session, holding
// its ingest.lock for every row and ledger change on every outcome.
// A held lock means an ingest owns the session: healthy or
// transient, the pass moves on without writing.
func passSession(ctx context.Context, deps SuperviseDeps, store *Store, root *os.Root, data string, row SessionRow, claimed map[string]bool) error {
	dir := row.AgentID + "/" + row.CaptureSessionID
	lockPath := dir + "/ingest.lock"
	held, err := tryLock(root, lockPath, true)
	if err != nil {
		if errors.Is(err, ErrBusy) {
			return nil
		}
		return err
	}
	released := false
	release := func() {
		if !released {
			released = true
			_ = releaseLock(held)
		}
	}
	defer release()
	if err := writeLockLine(held, fmt.Sprintf("supervise %d", deps.PID)); err != nil {
		return err
	}
	// One bounded streaming pass for the repair and the lifecycle:
	// the history is never materialized, and the repair branches run
	// on the fold instead of re-reading. Downstream work takes only
	// the folded tail facts.
	fold := newRepairFold(row)
	if err := streamSession(ctx, store, row.CaptureSessionID, fold.add); err != nil {
		return err
	}
	switch fold.life() {
	case LifeIncomplete:
		if err := finishIncompleteHeld(ctx, store, row, deps.Clock); err != nil {
			return err
		}
		return nil
	case LifeTerminal:
		if err := store.repairFolded(ctx, row, fold); err != nil {
			return err
		}
		return nil
	default:
	}
	if req, err := readStopRequest(root, row.AgentID, row.CaptureSessionID); err != nil {
		return err
	} else if req != nil {
		// A lost stop intent restores terminally, as the repair
		// would: nothing downstream runs on a stopping session.
		// This is a live supervisor honoring the request, so the
		// new event carries its current monotonic reading; zero
		// stays reserved for the power-loss restore.
		return store.restoreStopFolded(ctx, root, row, fold, req, deps.Clock, deps.Mono.MonotonicNS())
	}
	rep, err := fold.finish()
	if err != nil {
		return err
	}
	if err := store.applyRepair(ctx, row, rep); err != nil {
		return err
	}
	row, _, err = store.SessionRow(ctx, row.CaptureSessionID)
	if err != nil {
		return err
	}
	// CAP-A8, §9 steps 3-6: terminal, stop, server and pane
	// decisions already resolved or still ahead; the exit and
	// boundary facts travel with the tail and gate the reattach
	// itself, immediately before reserve/open. Branches that only
	// repair, end or park never meet this preflight.
	exit := preflightExit(root, row.AgentID, row.CaptureSessionID)
	// The reattach boundary is the complete committed reference:
	// this session, this screen sequence, a valid nonce. The
	// boundary is REQUIRED, not the optional exit file: only a
	// missing ingest.exit maps to ingest_died with a valid
	// boundary. Enforcement happens at step 6 inside reattach,
	// after the server and pane decisions, immediately before
	// reserve/open.
	nonce, boundaryOK := "", false
	if fold.lastScreenRef != "" {
		nonce, boundaryOK = parseBoundaryRef(row.CaptureSessionID, fold.lastScreenSeq, fold.lastScreenRef)
	}
	tail := streamTail{
		nextSeq:    fold.maxSeq + 1,
		lastMono:   fold.lastMono,
		cause:      gapCauseFor(exit, nonce),
		exit:       exit,
		boundaryOK: boundaryOK,
	}
	runner := deps.RunnerFor(row.TmuxServer)
	var meta SessionMeta
	if err := readControlAt(root, dir+"/session.json", 16384, &meta); err != nil {
		return err
	}
	server, err := runner.server(ctx)
	if err != nil {
		return setReattaching(ctx, store, row)
	}
	if server.pid != meta.Tmux.ServerPID || server.startTime != meta.Tmux.ServerStartTime {
		return stopReplaced(ctx, store, root, row, tail.nextSeq, deps)
	}
	pane, err := runner.pane(ctx, row.TmuxPane)
	if err != nil {
		return passGone(ctx, deps, store, root, data, row, tail, meta, runner, server, claimed, release, &released)
	}
	if pane.dead {
		return setReattaching(ctx, store, row)
	}
	if pane.pipe {
		// A pipe no running ingest of ours holds: ours dead on
		// an idle pane, or foreign. Never opened, never
		// replaced: look again next pass.
		return setReattaching(ctx, store, row)
	}
	return reattach(ctx, deps, store, root, data, row, tail, meta, runner, server, row.TmuxPane, pane, false, release, &released)
}

// setReattaching parks a live session for the next pass. The caller
// holds ingest.lock; release happens at the pass end.
func setReattaching(ctx context.Context, store *Store, row SessionRow) error {
	row.State = stateReattaching
	return store.UpdateSession(ctx, row)
}

// stopReplaced ends a session whose server instance was replaced:
// its recorded panes can never return.
func stopReplaced(ctx context.Context, store *Store, root *os.Root, row SessionRow, nextSeq int64, deps SuperviseDeps) error {
	return stopLiveSession(ctx, store, root, row, nextSeq,
		&stopRequest{Reason: "error", Detail: "tmux server replaced"},
		StopDeps{PID: deps.PID, Clock: deps.Clock, Mono: deps.Mono})
}

// passGone looks for authorized recreation after the %N is gone:
// exactly one pane at the recorded slot, with a new %N, alive,
// without a pipe, on the same server instance, claimed by no other
// live session. Anything else parks the session.
func passGone(ctx context.Context, deps SuperviseDeps, store *Store, root *os.Root, data string, row SessionRow, tail streamTail, meta SessionMeta, runner tmuxRunner, server serverFacts, claimed map[string]bool, release func(), released *bool) error {
	panes, err := runner.panes(ctx)
	if err != nil {
		return setReattaching(ctx, store, row)
	}
	var match *paneFacts
	count := 0
	for i, pane := range panes {
		if pane.id == row.TmuxPane || pane.dead || pane.pipe {
			continue
		}
		if pane.sessionID != meta.Tmux.SessionID ||
			pane.windowIndex != meta.Tmux.WindowIndex ||
			pane.paneIndex != meta.Tmux.PaneIndex {
			continue
		}
		if claimed[row.TmuxServer+"\x00"+pane.id] {
			continue
		}
		match = &panes[i]
		count++
	}
	if count != 1 {
		return setReattaching(ctx, store, row)
	}
	return reattach(ctx, deps, store, root, data, row, tail, meta, runner, server, match.id, *match, true, release, released)
}

// reattach reserves, opens, snapshots, commits the reattach batch
// and hands over to the new ingest, every step holding ingest.lock
// until the handover. Every failure parks the session as
// REATTACHING; a failed reattach never ends a capture.
// streamTail carries the folded stream end downstream: the first free
// sequence and the last committed monotonic reading, without the
// history behind them.
type streamTail struct {
	nextSeq  int64
	lastMono int64
	// cause is the preflighted exit evidence mapped against the
	// validated boundary nonce.
	cause protocol.CaptureGapCause
	// exit is the preflighted evidence; boundaryOK reports a
	// confirmed valid boundary. Both gate the step-6 reattach.
	exit       exitEvidence
	boundaryOK bool
}

func reattach(ctx context.Context, deps SuperviseDeps, store *Store, root *os.Root, data string, row SessionRow, tail streamTail, meta SessionMeta, runner tmuxRunner, server serverFacts, paneID string, pane paneFacts, recreated bool, release func(), released *bool) error {
	// CAP-A8 step 6: the exit and boundary preflight gates the
	// reattach immediately before reserve/open, after the life,
	// stop, server and pane decisions, holding every lock. Without
	// a confirmed valid boundary, or with malformed evidence, the
	// session parks with no grant, open or publish.
	if tail.exit.malformed {
		log.Printf("capture supervise: session %s: malformed ingest.exit, parking without open", row.CaptureSessionID)
		return setReattaching(ctx, store, row)
	}
	if !tail.boundaryOK {
		log.Printf("capture supervise: session %s: no confirmed boundary, parking without open", row.CaptureSessionID)
		return setReattaching(ctx, store, row)
	}
	lim := meta.Limits.resolve()
	agent, session := row.AgentID, row.CaptureSessionID
	granted, err := reserve(root, agent, session, lim.SnapshotMaxBytes, lim)
	if err != nil {
		return err
	}
	if !granted {
		if !budgetReadable(root, agent, session) {
			return stopLiveSession(ctx, store, root, row, tail.nextSeq,
				&stopRequest{Reason: "error", Detail: "budget unavailable"},
				StopDeps{PID: deps.PID, Clock: deps.Clock, Mono: deps.Mono})
		}
		return quotaStop(ctx, deps, store, root, row, tail.nextSeq, lim)
	}
	releaseSnap := func(size int64) {
		if current, err := readLedger(root, agent, session); err == nil {
			lowerLedger(root, agent, session, current-lim.SnapshotMaxBytes+size)
		}
	}
	nonce, err := newNonce()
	if err != nil {
		releaseSnap(0)
		return setReattaching(ctx, store, row)
	}
	exe, err := deps.Exe()
	if err != nil {
		releaseSnap(0)
		return setReattaching(ctx, store, row)
	}
	// The batch length is fixed before the open: pane_recreated only
	// after a recreation, then gap, screen and reattached. n is the
	// first chunk sequence just after the batch.
	base := tail.nextSeq
	batchLen := int64(3)
	if recreated {
		batchLen = 4
	}
	screenSeq := base + batchLen - 2
	n := base + batchLen
	argv := []string{
		exe, "capture", ingestCommand, "--session", session, "--agent", agent,
		"--data-dir", data, "--start-sequence", strconv.FormatInt(n, 10), "--attempt", nonce,
	}
	for _, word := range argv {
		if strings.ContainsAny(word, "\x00\n") {
			releaseSnap(0)
			return setReattaching(ctx, store, row)
		}
	}
	out, err := runner.open(ctx, openRequest{
		target: paneID, mode: modeNormal,
		server: server,
		slot:   paneSlot{sessionID: meta.Tmux.SessionID, windowIndex: meta.Tmux.WindowIndex, paneIndex: meta.Tmux.PaneIndex},
		nonce:  nonce, argv: argv, snapshotMax: lim.SnapshotMaxBytes,
	})
	if err != nil {
		releaseSnap(0)
		return setReattaching(ctx, store, row)
	}
	if out.refused {
		releaseSnap(0)
		if out.reason == refuseServerReplaced {
			return stopReplaced(ctx, store, root, row, tail.nextSeq, deps)
		}
		return setReattaching(ctx, store, row)
	}
	if out.nothing {
		releaseSnap(0)
		return setReattaching(ctx, store, row)
	}
	if int64(len(out.snapshot)) > lim.SnapshotMaxBytes {
		releaseSnap(0)
		return setReattaching(ctx, store, row)
	}
	snapName := "initial-" + strconv.FormatInt(screenSeq, 10) + "-" + nonce + ".ansi"
	if err := publishFile(root, agent+"/"+session, snapName, out.snapshot, true); err != nil {
		releaseSnap(0)
		return setReattaching(ctx, store, row)
	}
	releaseSnap(int64(len(out.snapshot)))
	batch, err := reattachBatch(meta, row, tail.lastMono, tail.cause, pane, paneID, recreated, base, screenSeq, nonce, out.snapshot, deps)
	if err != nil {
		return setReattaching(ctx, store, row)
	}
	if err := store.AppendEvents(ctx, batch); err != nil {
		// The snapshot stays as a charged orphan; the started
		// ingest refuses on its nonce.
		return setReattaching(ctx, store, row)
	}
	row.ReattachCount++
	row.State = stateAttached
	row.PaneCols = int64(pane.cols)
	row.PaneRows = int64(pane.rows)
	row.TmuxPane = paneID
	if err := store.UpdateSession(ctx, row); err != nil {
		return err
	}
	release()
	*released = true
	return handover(ctx, deps, store, root, agent, session, nonce, paneID, runner)
}

// reattachBatch builds the §11 reattach batch: pane_recreated only
// after a recreation, then gap_detected, initial_screen and
// reattached, at base and the following sequences. The screen
// carries the published snapshot's bytes and reference.
func reattachBatch(meta SessionMeta, row SessionRow, lastMono int64, cause protocol.CaptureGapCause, pane paneFacts, paneID string, recreated bool, base, screenSeq int64, nonce string, snapshot []byte, deps SuperviseDeps) ([]protocol.EventEnvelope, error) {
	seq := base
	var batch []protocol.EventEnvelope
	add := func(evType protocol.EventType, payload protocol.Payload, mono int64) error {
		id, err := newULID(deps.Clock)
		if err != nil {
			return fmt.Errorf("reattach ulid: %w", err)
		}
		batch = append(batch, protocol.EventEnvelope{
			EventID:       protocol.ULID(id),
			SchemaVersion: 1,
			NodeID:        protocol.NodeID(meta.NodeID),
			SessionID:     protocol.OpaqueID(row.CaptureSessionID),
			AgentID:       protocol.AgentID(row.AgentID),
			Harness:       protocol.Harness(row.Harness),
			AdapterType:   protocol.AdapterTypeNone,
			SourceQuality: protocol.SourceQualityBootstrapPipePane,
			EventType:     evType,
			Stream:        protocol.StreamPTY,
			WallTime:      deps.Clock.Now(),
			MonotonicNS:   mono,
			Sequence:      seq,
			Payload:       payload,
			Sensitivity:   protocol.SensitivityNormal,
		})
		seq++
		return nil
	}
	mono := deps.Mono.MonotonicNS()
	if recreated {
		data, err := jsonBytes(protocol.CapturePaneRecreated{
			CaptureSessionID: protocol.OpaqueID(row.CaptureSessionID),
			OldPane:          protocol.TmuxPaneID(row.TmuxPane),
			NewPane:          protocol.TmuxPaneID(paneID),
		})
		if err != nil {
			return nil, err
		}
		if err := add(protocol.EventTypeCapturePaneRecreated, data, mono); err != nil {
			return nil, err
		}
	}
	gapData, err := jsonBytes(protocol.CaptureGapDetected{
		CaptureSessionID: protocol.OpaqueID(row.CaptureSessionID),
		Cause:            cause,
	})
	if err != nil {
		return nil, err
	}
	if err := add(protocol.EventTypeCaptureGapDetected, gapData, mono); err != nil {
		return nil, err
	}
	screenData, err := jsonBytes(protocol.CaptureInitialScreen{
		CaptureSessionID: protocol.OpaqueID(row.CaptureSessionID),
		SnapshotBlobRef:  protocol.BlobRef("blob://capture/" + row.CaptureSessionID + "/initial-" + strconv.FormatInt(screenSeq, 10) + "-" + nonce),
		ScreenHash:       protocol.Digest("sha256:" + sha256Hex(snapshot)),
		Cols:             int64(pane.cols),
		Rows:             int64(pane.rows),
	})
	if err != nil {
		return nil, err
	}
	if err := add(protocol.EventTypeCaptureInitialScreen, screenData, mono); err != nil {
		return nil, err
	}
	gapMS := gapMillis(mono, lastMono)
	reData, err := jsonBytes(protocol.CaptureReattached{
		CaptureSessionID: protocol.OpaqueID(row.CaptureSessionID),
		GapFromSequence:  base - 1,
		GapToSequence:    seq,
		GapMS:            gapMS,
	})
	if err != nil {
		return nil, err
	}
	if err := add(protocol.EventTypeCaptureReattached, reData, mono); err != nil {
		return nil, err
	}
	return batch, nil
}

// gapMillis converts the nanosecond monotonic gap to the frozen
// integer-millisecond gap_ms field. A backward clock clamps to zero;
// a sub-millisecond gap truncates to zero.
func gapMillis(nowMono, lastMono int64) int64 {
	if nowMono < lastMono {
		return 0
	}
	return (nowMono - lastMono) / int64(time.Millisecond)
}

// exitEvidence is the preflighted ingest.exit record: absent, valid,
// or malformed. The strict bounded parser is never weakened: any
// decode or schema failure is malformed, never a cause.
type exitEvidence struct {
	present   bool
	malformed bool
	exit      ingestExit
}

// preflightExit reads and strictly validates the ingest.exit record
// before any reservation, open, snapshot or batch. A missing record
// is absent evidence; anything unreadable or failing the control
// schema is malformed.
func preflightExit(root *os.Root, agent, session string) exitEvidence {
	var exit ingestExit
	if err := readControlAt(root, agent+"/"+session+"/ingest.exit", 4096, &exit); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return exitEvidence{}
		}
		return exitEvidence{present: true, malformed: true}
	}
	return exitEvidence{present: true, exit: exit}
}

// parseBoundaryRef validates a committed screen ref as the complete
// start boundary: blob://capture/<session>/initial-<seq>-<nonce>
// with this session, this screen sequence, and a valid lowercase
// hex nonce. Anything else is not a boundary: foreign namespaces,
// other sessions, shifted sequences and suffixed names all fail.
func parseBoundaryRef(session string, seq int64, ref string) (string, bool) {
	rest, ok := strings.CutPrefix(ref, "blob://capture/"+session+"/")
	if !ok {
		return "", false
	}
	rest, ok = strings.CutPrefix(rest, "initial-")
	if !ok {
		return "", false
	}
	seqStr, nonce, ok := strings.Cut(rest, "-")
	if !ok || seqStr != strconv.FormatInt(seq, 10) {
		return "", false
	}
	if len(nonce) != 32 {
		return "", false
	}
	for i := range len(nonce) {
		c := nonce[i]
		if c < '0' || c > '9' && c < 'a' || c > 'f' {
			return "", false
		}
	}
	return nonce, true
}

// gapCauseFor maps validated exit evidence plus the validated
// boundary nonce to the gap cause: pipe_closed only for a valid EOF
// record whose attempt exactly matches the boundary nonce. Anything
// else is ingest_died.
func gapCauseFor(ev exitEvidence, nonce string) protocol.CaptureGapCause {
	if ev.present && !ev.malformed && ev.exit.Cause == ExitEOF &&
		nonce != "" && ev.exit.Attempt == nonce {
		return protocol.CaptureGapCausePipeClosed
	}
	return protocol.CaptureGapCauseIngestDied
}

// handover releases nothing: the lock is already free. It observes
// the started ingest as §7 step 13, with the supervisor timeout rule:
// on timeout a free lock parks the session, a held one is left to
// its holder.
func handover(ctx context.Context, deps SuperviseDeps, store *Store, root *os.Root, agent, session, nonce, paneID string, runner tmuxRunner) error {
	timeout, poll := stopPollTimeout, stopPollEvery
	if deps.ReadyTimeout > 0 {
		timeout = deps.ReadyTimeout
	}
	if deps.ReadyPoll > 0 {
		poll = deps.ReadyPoll
	}
	lockPath := agent + "/" + session + "/ingest.lock"
	if observeHandover(ctx, deps.Clock, runner, root, lockPath, nonce, paneID, timeout, poll) {
		return nil
	}
	held, err := tryLock(root, lockPath, true)
	if err != nil {
		if errors.Is(err, ErrBusy) {
			return nil
		}
		return err
	}
	defer func() { _ = releaseLock(held) }()
	row, found, err := store.SessionRow(ctx, session)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	// Bounded lifecycle scan: a live session parks for the next
	// pass, anything else stays as the stream left it.
	life, err := scanLife(ctx, store, session)
	if err != nil {
		return err
	}
	if life == LifeLive {
		row.State = stateReattaching
		return store.UpdateSession(ctx, row)
	}
	return nil
}

// observeHandover polls one full readiness observation within the
// bound: this attempt's marker, a live holder, the same line again,
// then the pane's pipe.
func observeHandover(ctx context.Context, clk clock.Clock, runner tmuxRunner, root *os.Root, lockPath, nonce, target string, timeout, poll time.Duration) bool {
	deadline := clk.Now().Add(timeout)
	for {
		if handoverProbe(ctx, runner, root, lockPath, nonce, target) {
			return true
		}
		if !clk.Now().Before(deadline) {
			return false
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(poll):
		}
	}
}

// handoverProbe runs one observation: the marker, a live holder, the
// same line again, then the pane's pipe.
func handoverProbe(ctx context.Context, runner tmuxRunner, root *os.Root, lockPath, nonce, target string) bool {
	line, err := readLockLine(root, lockPath)
	if err != nil || !readyMarker(line, nonce) {
		return false
	}
	live, err := lockLive(root, lockPath)
	if err != nil || !live {
		return false
	}
	again, err := readLockLine(root, lockPath)
	if err != nil || again != line {
		return false
	}
	pane, err := runner.pane(ctx, target)
	if err != nil {
		return false
	}
	return pane.pipe
}

// quotaStop ends a session the budget refuses: quota_reached, the
// quota gap and the quota stop last, with the QUOTA row.
func quotaStop(ctx context.Context, deps SuperviseDeps, store *Store, root *os.Root, row SessionRow, nextSeq int64, lim Limits) error {
	node, err := readNodeID(root)
	if err != nil {
		return err
	}
	scope, used, limit := quotaScopeFor(root, row.AgentID, lim.SnapshotMaxBytes, lim)
	mono := deps.Mono.MonotonicNS()
	seq := nextSeq
	mk := func(evType protocol.EventType, payload protocol.Payload, seq int64) (protocol.EventEnvelope, error) {
		return sessionEnvelope(deps.Clock, node, row, evType, seq, mono, payload)
	}
	reachedData, err := jsonBytes(protocol.CaptureQuotaReached{
		CaptureSessionID: protocol.OpaqueID(row.CaptureSessionID),
		QuotaScope:       scope, Bytes: used, LimitBytes: limit,
	})
	if err != nil {
		return err
	}
	gapData, err := jsonBytes(protocol.CaptureGapDetected{
		CaptureSessionID: protocol.OpaqueID(row.CaptureSessionID),
		Cause:            protocol.CaptureGapCauseQuota,
	})
	if err != nil {
		return err
	}
	stoppedData, err := jsonBytes(protocol.CaptureStopped{
		CaptureSessionID: protocol.OpaqueID(row.CaptureSessionID),
		Reason:           protocol.CaptureStopReasonQuota,
	})
	if err != nil {
		return err
	}
	batch := make([]protocol.EventEnvelope, 0, 3)
	for i, item := range []struct {
		evType  protocol.EventType
		payload protocol.Payload
	}{
		{protocol.EventTypeCaptureQuotaReached, reachedData},
		{protocol.EventTypeCaptureGapDetected, gapData},
		{protocol.EventTypeCaptureStopped, stoppedData},
	} {
		ev, err := mk(item.evType, item.payload, seq+int64(i))
		if err != nil {
			return err
		}
		batch = append(batch, ev)
	}
	if err := store.AppendEvents(ctx, batch); err != nil {
		return err
	}
	row.State = stateQuota
	row.StoppedAt = formatWall(batch[2].WallTime)
	row.StopReason = "quota"
	if err := store.UpdateSession(ctx, row); err != nil {
		return err
	}
	_ = settle(root, row.AgentID, row.CaptureSessionID, lim, true)
	return nil
}

// quotaScopeFor decides the exhaustion scope: the agent when its
// usage plus the need passes its quota, else the node.
func quotaScopeFor(root *os.Root, agent string, need int64, lim Limits) (protocol.QuotaScope, int64, int64) {
	if au, err := agentUsage(root, agent); err == nil && au+need > lim.AgentQuotaBytes {
		return protocol.QuotaScopeAgent, au, lim.AgentQuotaBytes
	}
	nu, err := nodeUsage(root)
	if err != nil {
		nu = 0
	}
	return protocol.QuotaScopeNode, nu, lim.NodeQuotaBytes
}

// budgetReadable reports whether every budget input reads strictly:
// an unreadable ledger is unavailable, not a refusal.
func budgetReadable(root *os.Root, agent, session string) bool {
	if _, err := readLedger(root, agent, session); err != nil {
		return false
	}
	if _, err := agentUsage(root, agent); err != nil {
		return false
	}
	_, err := nodeUsage(root)
	return err == nil
}
