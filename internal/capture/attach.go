package capture

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/protocol"
)

// Attach bounds: claims.lock acquisition, and the ingest handover
// observation window with its poll step.
const (
	claimsLockBound  = 3 * time.Second
	readinessTimeout = 5 * time.Second
	readinessPoll    = 50 * time.Millisecond
)

// AttachDeps is attach's injectable environment: the tmux runner, the
// socket discovery, the executable path, the process id, the clocks
// and the data directory resolver. Zero readiness bounds take the §7
// values; tests pass short ones.
type AttachDeps struct {
	Runner       tmuxRunner
	Discover     func(ctx context.Context) (string, error)
	Exe          func() (string, error)
	PID          int
	Clock        clock.Clock
	Mono         Mono
	DataDir      func() string
	ReadyTimeout time.Duration
	ReadyPoll    time.Duration
}

// readyBounds resolves the handover observation bounds.
func (d AttachDeps) readyBounds() (timeout, poll time.Duration) {
	timeout, poll = readinessTimeout, readinessPoll
	if d.ReadyTimeout > 0 {
		timeout = d.ReadyTimeout
	}
	if d.ReadyPoll > 0 {
		poll = d.ReadyPoll
	}
	return timeout, poll
}

// AttachResult is the §7 step 14 line: agent, capture session, pane,
// server path, and whether a pipe was replaced.
type AttachResult struct {
	Agent    string
	Session  string
	Pane     string
	Server   string
	Replaced bool
}

// Attach runs the §7 steps in order with an overall caller deadline:
// validate, open, claims, create, attempt, open, snapshot, row,
// commit, abort paths, handover and readiness. The CLI owns the
// attach_timeout and prints the result line.
func Attach(ctx context.Context, deps AttachDeps, opts AttachOptions) (AttachResult, error) {
	lim, sock, err := attachValidate(ctx, deps, opts)
	if err != nil {
		return AttachResult{}, err
	}
	data := opts.Data
	if data == "" {
		data = deps.DataDir()
	}
	root, err := openCaptureRoot(data, true)
	if err != nil {
		return AttachResult{}, err
	}
	defer func() { _ = root.Close() }()
	store, err := OpenStore(ctx, filepath.Join(data, "gifford.db"))
	if err != nil {
		return AttachResult{}, err
	}
	defer func() { _ = store.Close() }()
	if err := checkNoReplace(root); err != nil {
		return AttachResult{}, err
	}
	if err := ensureLock(root, "claims.lock"); err != nil {
		return AttachResult{}, err
	}
	var result AttachResult
	err = withLock(root, "claims.lock", true, claimsLockBound, func() error {
		r, err := attachUnderClaims(ctx, deps, store, root, opts, lim, sock, data)
		if err != nil {
			return err
		}
		result = r
		return nil
	})
	if err != nil {
		return AttachResult{}, err
	}
	return result, nil
}

// attachValidate checks agent, harness, target, limits and socket
// before anything is created or changed.
func attachValidate(ctx context.Context, deps AttachDeps, opts AttachOptions) (Limits, string, error) {
	fail := func(err error) (Limits, string, error) {
		return Limits{}, "", err
	}
	if !protocol.AgentID(opts.Agent).Valid() {
		return fail(fmt.Errorf("%w: bad agent id", ErrUsage))
	}
	if !opts.Harness.Valid() {
		return fail(fmt.Errorf("%w: bad harness", ErrUsage))
	}
	if !protocol.TmuxPaneID(opts.Target).Valid() {
		return fail(fmt.Errorf("%w: bad pane target", ErrUsage))
	}
	lim := opts.Limits.resolve()
	if err := lim.Validate(); err != nil {
		return fail(fmt.Errorf("%w: bad limits: %w", ErrUsage, err))
	}
	sock := opts.Socket
	if sock == "" {
		var err error
		sock, err = deps.Discover(ctx)
		if err != nil {
			return fail(err)
		}
	}
	if sock == "" || len(sock) > 128 || !strings.HasPrefix(sock, "/") ||
		strings.IndexFunc(sock, unicode.IsControl) >= 0 {
		return fail(fmt.Errorf("%w: bad socket", ErrUnsafePath))
	}
	return lim, sock, nil
}

// attachUnderClaims runs steps 4 to 14 holding claims.lock.
func attachUnderClaims(ctx context.Context, deps AttachDeps, store *Store, root *os.Root, opts AttachOptions, lim Limits, sock, data string) (AttachResult, error) {
	node, err := ensureNodeID(root)
	if err != nil {
		return AttachResult{}, err
	}
	runner := deps.Runner
	server, err := runner.server(ctx)
	if err != nil {
		return AttachResult{}, err
	}
	if server.socket != sock {
		return AttachResult{}, fmt.Errorf("%w: server socket mismatch", ErrTargetChanged)
	}
	pane, err := runner.pane(ctx, opts.Target)
	if err != nil {
		return AttachResult{}, err
	}
	if pane.dead {
		return AttachResult{}, fmt.Errorf("%w: pane has exited", ErrPaneNotFound)
	}
	if err := attachCheckClaims(ctx, deps, store, root, opts, sock, server, pane); err != nil {
		return AttachResult{}, err
	}
	session, err := createSessionDir(root, opts.Agent, deps.Clock)
	if err != nil {
		return AttachResult{}, err
	}
	lockPath := opts.Agent + "/" + session + "/ingest.lock"
	if err := ensureLock(root, lockPath); err != nil {
		return AttachResult{}, err
	}
	held, err := tryLock(root, lockPath, true)
	if err != nil {
		return AttachResult{}, fmt.Errorf("%w: new session lock", ErrUnsafePath)
	}
	released := false
	release := func() {
		if !released {
			released = true
			_ = releaseLock(held)
		}
	}
	defer release()
	allowance := controlRetain + lim.LogMaxBytes + lim.SnapshotMaxBytes
	granted, err := reserveNew(root, opts.Agent, session, allowance, lim)
	if err != nil {
		return AttachResult{}, err
	}
	if !granted {
		return AttachResult{}, fmt.Errorf("%w: session allowances", ErrQuota)
	}
	// CAP-A1: the attach marker is written only after the budget
	// granted the allowances and quota.json is published. A refused
	// allocation leaves an empty lock, no row and no pipe.
	if err := writeLockLine(held, fmt.Sprintf("attach %d", deps.PID)); err != nil {
		return AttachResult{}, err
	}
	if err := ensureLock(root, opts.Agent+"/"+session+"/stop.lock"); err != nil {
		return AttachResult{}, err
	}
	if err := publishSessionJSON(root, opts, session, node, server, pane, lim); err != nil {
		return AttachResult{}, err
	}
	nonce, err := newNonce()
	if err != nil {
		return AttachResult{}, fmt.Errorf("attempt nonce: %w", err)
	}
	exe, err := deps.Exe()
	if err != nil {
		return AttachResult{}, fmt.Errorf("%w: executable: %w", ErrUnsafePath, err)
	}
	argv := []string{exe, "capture", "ingest", "--session", session, "--agent", opts.Agent, "--data-dir", data, "--start-sequence", "2", "--attempt", nonce}
	for _, word := range argv {
		if strings.ContainsAny(word, "\x00\n") {
			return AttachResult{}, fmt.Errorf("%w: bad ingest word", ErrUnsafePath)
		}
	}
	return attachOpen(ctx, deps, store, root, opts, lim, sock, node, server, pane, session, nonce, argv, release)
}

// ensureNodeID reads the node identity, creating it once holding
// claims.lock: an existing file is read and never regenerated.
func ensureNodeID(root *os.Root) (string, error) {
	id, err := readNodeID(root)
	if err == nil {
		return id, nil
	}
	made, err := newNodeID()
	if err != nil {
		return "", err
	}
	if err := publishFile(root, ".", "node-id", []byte(made+"\n"), true); err != nil {
		return "", err
	}
	return made, nil
}

// createSessionDir makes the agent and session directories, fsyncing
// each parent, and checks them after creation.
func createSessionDir(root *os.Root, agent string, clk clock.Clock) (string, error) {
	if err := root.Mkdir(agent, 0o700); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return "", fmt.Errorf("%w: agent dir: %w", ErrUnsafePath, err)
		}
	}
	if _, err := openChecked(root, agent, true); err != nil {
		return "", err
	}
	if err := syncDirAt(root, "."); err != nil {
		return "", err
	}
	session, err := newCaptureSessionID(clk)
	if err != nil {
		return "", err
	}
	if err := root.Mkdir(agent+"/"+session, 0o700); err != nil {
		return "", fmt.Errorf("%w: session dir: %w", ErrUnsafePath, err)
	}
	if _, err := openChecked(root, agent+"/"+session, true); err != nil {
		return "", err
	}
	if err := syncDirAt(root, agent); err != nil {
		return "", err
	}
	return session, nil
}

// attachCheckClaims finishes this agent's or pane's incomplete rows
// whose lock is free, then evaluates §6.3 over the live sessions.
func attachCheckClaims(ctx context.Context, deps AttachDeps, store *Store, root *os.Root, opts AttachOptions, sock string, server serverFacts, pane paneFacts) error {
	rows, err := store.AllSessions(ctx)
	if err != nil {
		return err
	}
	var live []LiveClaim
	for _, row := range rows {
		events, err := readAllEvents(ctx, store, row.CaptureSessionID)
		if err != nil {
			return err
		}
		switch DeriveLife(true, events) {
		case LifeTerminal:
			continue
		case LifeIncomplete:
			if row.AgentID != opts.Agent && (row.TmuxServer != sock || row.TmuxPane != opts.Target) {
				continue
			}
			if err := store.finishIncomplete(ctx, root, row.AgentID, row.CaptureSessionID, row, deps.Clock); err != nil {
				return err
			}
		case LifeNoRow:
			continue
		default:
			claim, err := liveClaimOf(root, row)
			if err != nil {
				return err
			}
			live = append(live, claim)
		}
	}
	return CheckClaims(live, ClaimWant{
		AgentID: opts.Agent,
		Server:  sock,
		Pane:    opts.Target,
		Slot: Slot{
			Socket:          sock,
			ServerPID:       int64(server.pid),
			ServerStartTime: server.startTime,
			SessionID:       pane.sessionID,
			WindowIndex:     int64(pane.windowIndex),
			PaneIndex:       int64(pane.paneIndex),
		},
		HasPipe: pane.pipe,
		Replace: opts.Replace,
	})
}

// liveClaimOf reads one live row's server, pane and logical slot: the
// slot comes from the immutable session.json.
func liveClaimOf(root *os.Root, row SessionRow) (LiveClaim, error) {
	var meta SessionMeta
	if err := readControlAt(root, row.AgentID+"/"+row.CaptureSessionID+"/session.json", 16384, &meta); err != nil {
		return LiveClaim{}, err
	}
	return LiveClaim{
		AgentID:          row.AgentID,
		CaptureSessionID: row.CaptureSessionID,
		Server:           row.TmuxServer,
		Pane:             row.TmuxPane,
		Slot:             SlotOf(meta.Tmux),
	}, nil
}

// publishSessionJSON writes the immutable session metadata once,
// without replacing.
func publishSessionJSON(root *os.Root, opts AttachOptions, session, node string, server serverFacts, pane paneFacts, lim Limits) error {
	meta := SessionMeta{
		Schema:           sessionSchema,
		CaptureSessionID: session,
		AgentID:          opts.Agent,
		Harness:          string(opts.Harness),
		NodeID:           node,
		Tmux: SessionTmux{
			Socket:          server.socket,
			ServerPID:       server.pid,
			ServerStartTime: server.startTime,
			SessionID:       pane.sessionID,
			SessionName:     pane.sessionName,
			WindowIndex:     pane.windowIndex,
			PaneIndex:       pane.paneIndex,
		},
		Limits: lim,
	}
	if err := meta.validate(); err != nil {
		return err
	}
	data, err := jsonBytes(meta)
	if err != nil {
		return err
	}
	return publishFile(root, opts.Agent+"/"+session, "session.json", []byte(data), true)
}

// attachOpen runs steps 8 to 14: the guarded open, the snapshot, the
// row, the commit, the abort paths and the handover. release drops
// the session ingest.lock at step 13.
func attachOpen(ctx context.Context, deps AttachDeps, store *Store, root *os.Root, opts AttachOptions, lim Limits, sock, node string, server serverFacts, pane paneFacts, session, nonce string, argv []string, release func()) (AttachResult, error) {
	dir := opts.Agent + "/" + session
	mode := modeNormal
	if opts.Replace {
		mode = modeReplace
	}
	out, err := deps.Runner.open(ctx, openRequest{
		target: opts.Target, mode: mode, server: server,
		slot:  paneSlot{sessionID: pane.sessionID, windowIndex: pane.windowIndex, paneIndex: pane.paneIndex},
		nonce: nonce, argv: argv, snapshotMax: lim.SnapshotMaxBytes,
	})
	if err != nil {
		return attachAbort(root, dir, lim, 0, release, ErrAttach)
	}
	if out.nothing {
		if total, err := footprintTotal(root, opts.Agent, session); err == nil {
			lowerLedger(root, opts.Agent, session, total)
		}
		release()
		return AttachResult{}, fmt.Errorf("%w: pane is gone", ErrPaneNotFound)
	}
	if out.refused {
		if total, err := footprintTotal(root, opts.Agent, session); err == nil {
			lowerLedger(root, opts.Agent, session, total)
		}
		release()
		return AttachResult{}, refusedError(out.reason)
	}
	if len(out.snapshot) > int(lim.SnapshotMaxBytes) {
		return attachAbort(root, dir, lim, 0, release, ErrAttach)
	}
	snapName := "initial-1-" + nonce + ".ansi"
	if err := publishFile(root, dir, snapName, out.snapshot, true); err != nil {
		return attachAbort(root, dir, lim, 0, release, ErrAttach)
	}
	snapSize := int64(len(out.snapshot))
	lowerLedger(root, opts.Agent, session, controlRetain+lim.LogMaxBytes+snapSize)
	row := SessionRow{
		CaptureSessionID: session,
		AgentID:          opts.Agent,
		Harness:          string(opts.Harness),
		TmuxServer:       sock,
		TmuxSession:      pane.sessionName,
		TmuxWindow:       strconv.Itoa(pane.windowIndex),
		TmuxPane:         opts.Target,
		PaneCols:         int64(pane.cols),
		PaneRows:         int64(pane.rows),
		State:            stateAttached,
		StartedAt:        formatWall(deps.Clock.Now()),
		SourceQuality:    string(protocol.SourceQualityBootstrapPipePane),
	}
	if err := store.InsertSession(ctx, row); err != nil {
		return attachAbort(root, dir, lim, snapSize, release, ErrAttach)
	}
	if err := attachCommit(ctx, deps, store, row, node, out.prePipe, snapName, out.snapshot); err != nil {
		return attachAbort(root, dir, lim, snapSize, release, ErrAttach)
	}
	release()
	return attachReady(ctx, deps, root, dir, opts, sock, session, nonce, out.prePipe)
}

// refusedError maps a decision-instant refusal to its sentinel: a
// replaced server or a moved pane changed the target, a dead pane is
// gone, and a present pipe refuses the open.
func refusedError(reason refuseReason) error {
	switch reason {
	case refuseServerReplaced, refusePaneMoved:
		return fmt.Errorf("%w: target changed during open", ErrTargetChanged)
	case refuseDead:
		return fmt.Errorf("%w: pane is dead", ErrPaneNotFound)
	default:
		return fmt.Errorf("%w: pipe present", ErrPipeExists)
	}
}

// footprintTotal sums a session directory for the no-pipe abort: no
// late writer is possible, so the ledger drops to the raw footprint.
// On error the ledger stays, which fails closed.
func footprintTotal(root *os.Root, agent, session string) (int64, error) {
	control, log, data, err := footprint(root, agent, session)
	if err != nil {
		return 0, err
	}
	return control + log + data, nil
}

// attachAbort releases only the unused snapshot reservation, keeping
// the retained allowance for a possible late writer, drops the
// session lock, and returns the mapping error. snapSize is the
// published snapshot's real size, or zero when nothing was published.
func attachAbort(root *os.Root, dir string, lim Limits, snapSize int64, release func(), mapping error) (AttachResult, error) {
	agent, session := splitDir(dir)
	if current, err := readLedger(root, agent, session); err == nil {
		unused := lim.SnapshotMaxBytes - snapSize
		if unused < 0 {
			unused = 0
		}
		lowerLedger(root, agent, session, current-unused)
	}
	release()
	return AttachResult{}, mapping
}

// attachCommit appends the attach batch and records the snapshot ref:
// capture.attached at 0, then capture.initial_screen at 1. This is
// the commit point. Afterwards the row carries the blob ref.
func attachCommit(ctx context.Context, deps AttachDeps, store *Store, row SessionRow, node string, replaced bool, snapName string, snapshot []byte) error {
	attachedID, err := newULID(deps.Clock)
	if err != nil {
		return fmt.Errorf("attach ulid: %w", err)
	}
	screenID, err := newULID(deps.Clock)
	if err != nil {
		return fmt.Errorf("attach ulid: %w", err)
	}
	attachedData, err := jsonBytes(protocol.CaptureAttached{
		CaptureSessionID:     protocol.OpaqueID(row.CaptureSessionID),
		AgentID:              protocol.AgentID(row.AgentID),
		Harness:              protocol.Harness(row.Harness),
		TmuxServer:           row.TmuxServer,
		TmuxSession:          row.TmuxSession,
		TmuxWindow:           row.TmuxWindow,
		TmuxPane:             protocol.TmuxPaneID(row.TmuxPane),
		PaneCols:             row.PaneCols,
		PaneRows:             row.PaneRows,
		ReplacedExistingPipe: replaced,
	})
	if err != nil {
		return err
	}
	blobRef := "blob://capture/" + row.CaptureSessionID + "/" + snapName
	screenData, err := jsonBytes(protocol.CaptureInitialScreen{
		CaptureSessionID: protocol.OpaqueID(row.CaptureSessionID),
		SnapshotBlobRef:  protocol.BlobRef(blobRef),
		ScreenHash:       protocol.Digest("sha256:" + sha256Hex(snapshot)),
		Cols:             row.PaneCols,
		Rows:             row.PaneRows,
	})
	if err != nil {
		return err
	}
	base := protocol.EventEnvelope{
		SchemaVersion: 1,
		NodeID:        protocol.NodeID(node),
		SessionID:     protocol.OpaqueID(row.CaptureSessionID),
		AgentID:       protocol.AgentID(row.AgentID),
		Harness:       protocol.Harness(row.Harness),
		AdapterType:   protocol.AdapterTypeNone,
		SourceQuality: protocol.SourceQualityBootstrapPipePane,
		Stream:        protocol.StreamPTY,
		WallTime:      deps.Clock.Now(),
		MonotonicNS:   deps.Mono.MonotonicNS(),
		Sensitivity:   protocol.SensitivityNormal,
	}
	attached := base
	attached.EventID = protocol.ULID(attachedID)
	attached.EventType = protocol.EventTypeCaptureAttached
	attached.Sequence = 0
	attached.Payload = attachedData
	screen := base
	screen.EventID = protocol.ULID(screenID)
	screen.EventType = protocol.EventTypeCaptureInitialScreen
	screen.Sequence = 1
	screen.Payload = screenData
	if err := store.AppendEvents(ctx, []protocol.EventEnvelope{attached, screen}); err != nil {
		return err
	}
	row.InitialScreenBlobRef = blobRef
	return store.UpdateSession(ctx, row)
}

// attachReady observes the handover: the lock reads this attempt's
// marker, a holder exists, the marker repeats, and the pane shows its
// pipe. On timeout it returns ErrAttach stating the capture was
// committed and its readiness is unconfirmed. That path is read-only:
// it never tries the session lock to build the error and updates no
// state. A slow matching ingest still starts; otherwise the supervisor
// recovers the session.
func attachReady(ctx context.Context, deps AttachDeps, root *os.Root, dir string, opts AttachOptions, sock, session, nonce string, replaced bool) (AttachResult, error) {
	readyResult := AttachResult{
		Agent: opts.Agent, Session: session, Pane: opts.Target, Server: sock, Replaced: replaced,
	}
	lockPath := dir + "/ingest.lock"
	timeout, poll := deps.readyBounds()
	if ready(ctx, deps, root, lockPath, nonce, opts.Target, timeout, poll) {
		return readyResult, nil
	}
	return AttachResult{}, fmt.Errorf("%w: capture committed; readiness unconfirmed", ErrAttach)
}

// ready reports one full observation pass within the bound.
func ready(ctx context.Context, deps AttachDeps, root *os.Root, lockPath, nonce, target string, timeout, poll time.Duration) bool {
	deadline := deps.Clock.Now().Add(timeout)
	for {
		if readyProbe(ctx, deps, root, lockPath, nonce, target) {
			return true
		}
		if !deps.Clock.Now().Before(deadline) {
			return false
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(poll):
		}
	}
}

// readyMarker parses an ingest readiness line: "ingest <pid>
// <nonce>" with a positive pid and this attempt's nonce. The pid is
// the ingest's own: it runs in another process, so the opener's pid
// is never expected here.
func readyMarker(line, nonce string) bool {
	parts := strings.Split(line, " ")
	if len(parts) != 3 || parts[0] != "ingest" || parts[2] != nonce {
		return false
	}
	pid, err := strconv.Atoi(parts[1])
	return err == nil && pid > 0
}

// readyProbe runs one observation in the §7 step 13 order: this
// attempt's marker, a live holder, the same line again, then the
// pane's pipe. The repeated line proves the holder did not change
// across the probe.
func readyProbe(ctx context.Context, deps AttachDeps, root *os.Root, lockPath, nonce, target string) bool {
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
	pane, err := deps.Runner.pane(ctx, target)
	if err != nil {
		return false
	}
	return pane.pipe
}
