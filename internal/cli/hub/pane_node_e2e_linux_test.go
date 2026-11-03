//go:build linux

package hub

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/config"
	"github.com/smellak/gifford/internal/controlapi"
	"github.com/smellak/gifford/internal/daemon"
	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/detect"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/orchestrator"
	"github.com/smellak/gifford/internal/procguard"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/tmuxattach"
	"github.com/smellak/gifford/internal/tmuxinstance"
	"github.com/smellak/gifford/internal/tmuxlock"
	"github.com/smellak/gifford/internal/tmuxlock/writerlog"
)

const (
	paneNodeAgent   protocol.AgentID    = "pane-node-agent"
	paneNodeSender  protocol.AgentID    = "pane-node-sender"
	paneNodeSession protocol.OpaqueID   = "session-pane-node"
	paneNodeID      protocol.NodeID     = "pane-node"
	paneNodePane    protocol.TmuxPaneID = "%7"
	paneNodeHost                        = "node-a"
	paneNodePeer                        = "peer_writer"
)

const paneNodeTmuxCommand = `#!/bin/sh
set -eu
command=
literal=no
last=
for arg do
  case "$arg" in
    capture-pane|display-message|list-clients|send-keys) command="$arg" ;;
    -l) literal=yes ;;
  esac
  last="$arg"
done
case "$command" in
  capture-pane)
    cat "$GIFFORD_PANE_NODE_DIR/screen"
    ;;
  display-message)
    printf '%%7\t%s\t80\t24\n' "$GIFFORD_PANE_NODE_PID"
    ;;
  list-clients) : ;;
  send-keys)
    printf '%s\n' "$literal" >> "$GIFFORD_PANE_NODE_DIR/writes"
    if test "$literal" = yes; then
      printf '$ %s' "$last" > "$GIFFORD_PANE_NODE_DIR/screen"
    elif test "$last" = Enter; then
      printf '$ ' > "$GIFFORD_PANE_NODE_DIR/screen"
    else
      exit 2
    fi
    ;;
  *) exit 2 ;;
esac
`

type paneNodeClock struct {
	mu    sync.Mutex
	now   time.Time
	ticks chan time.Time
}

func (c *paneNodeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *paneNodeClock) After(time.Duration) <-chan time.Time { return c.ticks }

func (c *paneNodeClock) set(now time.Time) {
	c.mu.Lock()
	c.now = now
	c.mu.Unlock()
}

func (c *paneNodeClock) tick() { c.ticks <- c.Now() }

type paneNodeAuthorizer struct {
	uid uint32
	gid uint32

	mu    sync.Mutex
	calls int
}

func (a *paneNodeAuthorizer) Authorize(peer controlapi.Peer) error {
	a.mu.Lock()
	a.calls++
	a.mu.Unlock()
	if !peer.Verified || peer.UID != a.uid || peer.GID != a.gid || peer.PID <= 0 {
		return controlapi.ErrPaneOperationUnauthorized
	}
	return nil
}

type paneNodeRegistry struct {
	instance tmuxinstance.Instance

	mu       sync.Mutex
	verified int
}

func (r *paneNodeRegistry) Lookup(
	_ context.Context,
	agent protocol.AgentID,
) (tmuxinstance.Instance, error) {
	if agent != r.instance.AgentID {
		return tmuxinstance.Instance{}, tmuxinstance.ErrNotRegistered
	}
	return r.instance, nil
}

func (r *paneNodeRegistry) Verify(
	_ context.Context,
	instance tmuxinstance.Instance,
	observation tmuxinstance.PaneObservation,
) error {
	if instance.ID != r.instance.ID || observation.PaneID != r.instance.PaneID ||
		observation.PanePID != r.instance.PanePID {
		return tmuxinstance.ErrInvalidated
	}
	r.mu.Lock()
	r.verified++
	r.mu.Unlock()
	return nil
}

func (r *paneNodeRegistry) ListActive(context.Context) ([]tmuxinstance.Instance, error) {
	return []tmuxinstance.Instance{r.instance}, nil
}

type paneNodeSessions struct{ invalidated int }

func (s *paneNodeSessions) Invalidate(
	context.Context,
	protocol.AgentID,
	protocol.OpaqueID,
) error {
	s.invalidated++
	return nil
}

type paneNodeLinks struct{}

func (paneNodeLinks) Snapshot(protocol.AgentID) (agentlink.Snapshot, bool) {
	return agentlink.Snapshot{}, false
}

func (paneNodeLinks) Client(protocol.AgentID) (*agentlink.Client, bool) { return nil, false }

type paneNodeWorkerResult struct {
	processed int
	err       error
}

type paneNodeObservedWorker struct {
	worker *daemon.Worker
	runs   chan paneNodeWorkerResult
}

func (w paneNodeObservedWorker) RunOnce(ctx context.Context) (int, error) {
	processed, err := w.worker.RunOnce(ctx)
	w.runs <- paneNodeWorkerResult{processed: processed, err: err}
	return processed, err
}

type paneNodeRPCError struct{ value protocol.RPCError }

func (e *paneNodeRPCError) Error() string { return e.value.Message }

type paneNodeState struct {
	Attempt   string `json:"attempt"`
	Epoch     uint32 `json:"epoch"`
	Phase     string `json:"phase"`
	Result    string `json:"result"`
	Step      int    `json:"step"`
	Timestamp string `json:"ts"`
}

type paneNodeWriterEvent struct {
	Event       string  `json:"event"`
	OperationID string  `json:"operation_id"`
	Outcome     *string `json:"outcome"`
	Profile     string  `json:"profile"`
	Version     int     `json:"v"`
	WriteID     *string `json:"write_id"`
}

type paneNodeMediatorResult struct {
	events []paneNodeWriterEvent
	err    error
}

// TestPaneNodeControlAPIProvisionDeliveryRecoveryAndRepair is the node-level
// contract: administrative mutations enter through the live Control API,
// delivery enters through the recurring worker, and both converge on the
// same pane state journal and common writer log.
func TestPaneNodeControlAPIProvisionDeliveryRecoveryAndRepair(t *testing.T) {
	root := filepath.Join(t.TempDir(), "pane-locks")
	require.NoError(t, os.Mkdir(root, 0o711))
	require.NoError(t, os.Chmod(root, 0o711))
	modePath := filepath.Join(t.TempDir(), "pane-lock-mode")
	require.NoError(t, os.WriteFile(modePath, []byte("required\n"), 0o644))
	require.NoError(t, os.Chmod(modePath, 0o644))

	commandDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(commandDir, "sudo"), []byte(paneNodeTmuxCommand), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(commandDir, "screen"), []byte("completed fixture task\n$ "), 0o600))
	t.Setenv("GIFFORD_PANE_NODE_DIR", commandDir)
	t.Setenv("PATH", commandDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	uid := uint32(os.Getuid())
	gid := uint32(os.Getgid())
	authorizer := &paneNodeAuthorizer{uid: uid, gid: gid}
	previousRuntime := paneLockRuntime
	paneLockRuntime = paneLockRuntimeAuthority{
		root: root, modeUID: uid, modeGID: gid,
		authorizer: func(string) (controlapi.PaneRequestAuthorizer, error) {
			return authorizer, nil
		},
	}
	t.Cleanup(func() { paneLockRuntime = previousRuntime })

	clock := &paneNodeClock{
		now:   time.Date(2026, time.November, 3, 11, 0, 0, 0, time.UTC),
		ticks: make(chan time.Time, 4),
	}
	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "node.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	engine, err := channel.New(channel.Options{Clock: clock})
	require.NoError(t, err)
	controller, err := channel.NewController(channel.ControllerOptions{
		DB: store.DB(), Engine: engine, Clock: clock, NodeID: paneNodeID,
		Entropy: bytes.NewReader(make([]byte, 4096)),
	})
	require.NoError(t, err)
	require.NoError(t, controller.Reload(t.Context()))

	listener, writerSocket, mediatorDone := startPaneNodeMediator(t)
	settings := config.TmuxLocks{
		Enabled: true, PeerOwner: paneNodePeer, PeerWriters: []string{paneNodePeer},
		Host: paneNodeHost, ModePath: modePath,
		AdminUIDsPath:   filepath.Join(t.TempDir(), "pane-admin-uids"),
		OperationSchema: "gifford-pane-op/1", InitialTakeDrain: time.Second,
		WriterLogSocket: writerSocket,
	}
	telemetry := controlapi.NewPaneOperationTelemetry(nil)
	locker, paneAuth, paneWriter, paneCreate, paneOwner, err := newCooperativePaneLock(
		store.DB(), clock, settings, paneLockRuntime, &tmuxPaneLockReporter{}, telemetry,
	)
	require.NoError(t, err)
	journal, err := tmuxattach.NewPaneJournalAt(settings.OperationSchema,
		tmuxlock.RecordPolicy{PeerOwner: settings.PeerOwner, PeerWriters: settings.PeerWriters}, root)
	require.NoError(t, err)
	commonLog, err := writerlog.New(settings.WriterLogSocket)
	require.NoError(t, err)
	lifecycle, err := tmuxattach.NewWriterLoggedLifecycle(locker, journal, commonLog)
	require.NoError(t, err)
	transport, err := tmuxattach.New(tmuxattach.Options{
		Locker: locker, Lifecycle: lifecycle, SettleTimeout: 500 * time.Millisecond,
	})
	require.NoError(t, err)

	panePID := int64(os.Getpid())
	paneStart, err := tmuxlock.ProcessStart(panePID)
	require.NoError(t, err)
	bootID, err := tmuxlock.BootID()
	require.NoError(t, err)
	t.Setenv("GIFFORD_PANE_NODE_PID", strconv.FormatInt(panePID, 10))
	ownerUID := uint32(60_001)
	if ownerUID == uid {
		ownerUID++
	}
	target := protocol.PaneOperationTarget{
		OwnerUID: ownerUID, Socket: "/tmp/gifford-p9-pane.sock", PaneID: paneNodePane,
		Host: paneNodeHost, BootID: bootID, PanePID: panePID, PaneStart: int64(paneStart),
	}
	require.NoError(t, target.Validate())
	harnessPID := panePID + 1_000_000
	harnessStart := int64(paneStart) + 1
	instance := tmuxinstance.Instance{
		ID: "01JB0000000000000000000042", AgentID: paneNodeAgent,
		BootID: target.BootID, OwnerUser: "agent_owner", OwnerUID: target.OwnerUID,
		Socket: target.Socket, PaneID: target.PaneID, PanePID: target.PanePID,
		PaneStart: target.PaneStart, PaneComm: paneRecoveryTestShell, Harness: protocol.HarnessCodex,
		HarnessVersion: "1.0.0", HarnessPID: harnessPID, HarnessStart: harnessStart,
		HarnessComm: paneRecoveryTestCodex, HarnessArgv0: paneRecoveryTestCodex, AgentUID: uid, AgentGID: gid,
		SessionID: paneNodeSession, State: tmuxinstance.StateActive,
	}
	registry := &paneNodeRegistry{instance: instance}
	entries := []procguard.Entry{
		{
			PID: int(panePID), StartTime: paneStart, Comm: paneRecoveryTestShell, Program: paneRecoveryTestShell,
			Pgrp: int(panePID), TPGID: int(harnessPID), State: "S",
		},
		{
			PID: int(harnessPID), PPID: int(panePID), StartTime: uint64(harnessStart),
			Comm: paneRecoveryTestCodex, Program: paneRecoveryTestCodex, Argv0: "/opt/gifford-test/codex",
			Pgrp: int(harnessPID), TPGID: int(harnessPID), State: "S",
		},
	}
	detectorTable := detect.Table{detect.HarnessCodex: {
		InputExpr: `[$#] ?(.*)$`, CLIBinaries: []string{paneRecoveryTestCodex},
	}}
	paneRecovery, err := newTmuxPaneRecoveryService(paneRecoveryServiceOptions{
		Writer: paneWriter, Registry: registry, Transport: transport,
		Clock: clock, Drain: settings.InitialTakeDrain, Telemetry: telemetry,
		DetectorTable: detectorTable, Notices: []protocol.NoticeLine{tmuxattach.FixedNoticeLine},
		SampleTree: func(int) ([]procguard.Entry, error) {
			return append([]procguard.Entry(nil), entries...), nil
		},
		SampleProcess: func(int) (procguard.Entry, error) {
			return procguard.Entry{}, procguard.ErrNoRoot
		},
		BootID: func() (string, error) { return target.BootID, nil },
	})
	require.NoError(t, err)

	seedPaneNodeSession(t, store, clock, instance)
	producer, err := orchestrator.NewStateProducer(orchestrator.StateProducerOptions{
		DB: store.DB(), Clock: clock, Channel: controller,
		Entropy: bytes.NewReader(make([]byte, 4096)),
	})
	require.NoError(t, err)
	stateLoop := startStateProducerLoop(t.Context(), producer)
	require.Eventually(t, func() bool {
		var snapshots int64
		queryErr := store.DB().QueryRowContext(t.Context(),
			"SELECT count(*) FROM state_snapshots WHERE agent_id = ?", paneNodeAgent).Scan(&snapshots)
		return queryErr == nil && snapshots == 1
	}, 5*time.Second, 5*time.Millisecond)
	stateLoop.stop()
	inputGuards, err := agentlink.NewInputGuardStore(store.DB(), clock)
	require.NoError(t, err)
	sessions := &paneNodeSessions{}
	links := paneNodeLinks{}
	executor, err := orchestrator.NewExecutor(orchestrator.ExecutorOptions{
		DB: store.DB(), Channel: engine, Approvals: controller, Links: links,
		Clock: clock, Entropy: bytes.NewReader(make([]byte, 4096)),
		NoticeLine: tmuxattach.FixedNoticeLine,
		TmuxAttach: &orchestrator.TmuxAttachOptions{
			Host: target.Host, Registry: registry, Transport: transport, Sessions: sessions,
			InputGuards: inputGuards, ProcessGuard: &procguard.Guard{},
			SampleTree: func(int) ([]procguard.Entry, error) {
				return append([]procguard.Entry(nil), entries...), nil
			},
			DetectorTable: detectorTable, HumanActivityWindow: 15 * time.Second,
		},
	})
	require.NoError(t, err)
	worker, err := daemon.NewWorker(daemon.WorkerOptions{
		DB: store.DB(), Processor: executor, Approvals: executor, Prepared: executor,
		Proxies: links, Clock: clock, Concurrency: 1,
	})
	require.NoError(t, err)

	controlServer, err := controlapi.New(controlapi.Options{
		RuntimeDir: filepath.Join(t.TempDir(), "runtime"), DB: store.DB(), Controller: controller,
		PaneAuthorizer: paneAuth, PaneProvision: paneCreate,
		PaneOwnership: paneOwner, PaneRecovery: paneRecovery, PaneTelemetry: telemetry,
	})
	require.NoError(t, err)
	serveCtx, cancelServe := context.WithCancel(t.Context())
	serveDone := make(chan error, 1)
	go func() { serveDone <- controlServer.Serve(serveCtx) }()
	require.Eventually(t, func() bool {
		_, statErr := os.Stat(controlServer.Path())
		return statErr == nil
	}, 5*time.Second, 5*time.Millisecond)
	t.Cleanup(func() {
		cancelServe()
		require.NoError(t, <-serveDone)
	})

	params := protocol.PaneOperationParams{Target: target}
	var operation protocol.PaneOperationResult
	require.NoError(t, paneNodeCall(t.Context(), controlServer.Path(),
		protocol.ControlMethodPaneProvision, params, &operation))
	require.True(t, operation.Changed)
	var publications int64
	require.NoError(t, store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM pane_index_publications").Scan(&publications))
	require.Equal(t, int64(1), publications)

	clock.set(clock.Now().Add(2 * time.Second))
	require.NoError(t, paneNodeCall(t.Context(), controlServer.Path(),
		protocol.ControlMethodPaneTake, params, &operation))
	require.True(t, operation.Changed)
	policy := tmuxlock.RecordPolicy{PeerOwner: settings.PeerOwner, PeerWriters: settings.PeerWriters}
	live := paneLiveBinding(target)
	receipt, err := tmuxlock.ReadPanelReceipt(root, live, policy)
	require.NoError(t, err)
	require.Equal(t, "gifford", receipt.Owner)
	require.Equal(t, "IDLE", receipt.Phase)

	first := seedPaneNodeDelivery(t, store, clock, 41)
	runs := make(chan paneNodeWorkerResult, 4)
	var workerOutput bytes.Buffer
	workerLoop := startWorkerLoop(t.Context(), &workerOutput,
		paneNodeObservedWorker{worker: worker, runs: runs}, clock)
	t.Cleanup(workerLoop.stop)
	firstPass := waitPaneNodeWorker(t, runs)
	require.NoError(t, firstPass.err)
	require.Equal(t, 1, firstPass.processed)
	clock.tick()
	commitPass := waitPaneNodeWorker(t, runs)
	require.NoError(t, commitPass.err)
	require.Equal(t, 1, commitPass.processed)

	mediated := <-mediatorDone
	require.NoError(t, mediated.err)
	require.NoError(t, listener.Close())
	requirePaneNodeWriterEvents(t, mediated.events)
	require.Equal(t, "DELIVERED", paneNodeQueueState(t, store, first))
	receipt, err = tmuxlock.ReadPanelReceipt(root, live, policy)
	require.NoError(t, err)
	require.Equal(t, "gifford", receipt.Owner)
	require.Equal(t, "HECHO", receipt.Phase)

	require.NoError(t, paneNodeCall(t.Context(), controlServer.Path(),
		protocol.ControlMethodPaneReturn, params, &operation))
	require.True(t, operation.Changed)
	receipt, err = tmuxlock.ReadPanelReceipt(root, live, policy)
	require.NoError(t, err)
	require.Equal(t, paneNodePeer, receipt.Owner)
	require.Equal(t, "IDLE", receipt.Phase)

	require.NoError(t, paneNodeCall(t.Context(), controlServer.Path(),
		protocol.ControlMethodPaneTake, params, &operation))
	require.True(t, operation.Changed)
	second := seedPaneNodeDelivery(t, store, clock, 42)
	clock.tick()
	deferredPass := waitPaneNodeWorker(t, runs)
	require.NoError(t, deferredPass.err)
	require.Equal(t, 1, deferredPass.processed)
	require.Equal(t, "QUEUED", paneNodeQueueState(t, store, second))
	var attemptState, terminalReason string
	require.NoError(t, store.DB().QueryRowContext(t.Context(), `SELECT state, terminal_reason
		FROM delivery_attempts WHERE message_id = ? AND agent_id = ?
		ORDER BY created_at DESC, delivery_id DESC LIMIT 1`,
		second.MessageID, second.AgentID).Scan(&attemptState, &terminalReason))
	require.Equal(t, "CANCELLED", attemptState)
	require.Equal(t, "TMUX_WRITER_LOG_UNAVAILABLE", terminalReason)

	statePath := paneNodeStatePath(t, root, live.PanelBinding)
	deferred := readPaneNodeState(t, statePath)
	require.Equal(t, "APLAZADO", deferred.Phase)
	require.Equal(t, "-", deferred.Result)
	require.Equal(t, 0, deferred.Step)
	require.Len(t, deferred.Attempt, 32)
	reconcile := protocol.PaneAttemptParams{
		Target: target, ExpectedAttempt: protocol.PaneAttemptID(deferred.Attempt),
	}

	require.NoError(t, paneNodeCall(t.Context(), controlServer.Path(),
		protocol.ControlMethodPaneReturn, params, &operation))
	require.True(t, operation.Changed)
	receipt, err = tmuxlock.ReadPanelReceipt(root, live, policy)
	require.NoError(t, err)
	require.Equal(t, paneNodePeer, receipt.Owner)
	beforeRepairEpoch := receipt.Epoch

	raw, err := os.ReadFile(statePath)
	require.NoError(t, err)
	raw[0] ^= 0xff
	require.NoError(t, os.WriteFile(statePath, raw, 0o660))
	require.NoError(t, paneNodeCall(t.Context(), controlServer.Path(),
		protocol.ControlMethodPaneRepair, params, &operation))
	require.True(t, operation.Changed)
	repaired := readPaneNodeState(t, statePath)
	require.Equal(t, beforeRepairEpoch+1, repaired.Epoch)
	require.Equal(t, "INCIERTO", repaired.Phase)
	require.Equal(t, "REPARADO", repaired.Result)
	require.Equal(t, "-", repaired.Attempt)
	require.Equal(t, -1, repaired.Step)

	var rpcErr *paneNodeRPCError
	reconcileAfterRepair := paneNodeCall(t.Context(), controlServer.Path(),
		protocol.ControlMethodPaneReconcile, reconcile, &operation)
	require.ErrorAs(t, reconcileAfterRepair, &rpcErr,
		"repair deliberately installs attempt '-' so an old attempt cannot clear inhibition")
	require.Equal(t, repaired, readPaneNodeState(t, statePath))
	dash := protocol.PaneAttemptParams{Target: target, ExpectedAttempt: "-"}
	operation = protocol.PaneOperationResult{}
	require.NoError(t, paneNodeCall(t.Context(), controlServer.Path(),
		protocol.ControlMethodPaneReconcile, dash, &operation))
	require.False(t, operation.Changed, "a repaired record stays parked until the drain wait elapses")
	require.Equal(t, repaired, readPaneNodeState(t, statePath))
	// The writer stamps records with the system clock, so the drain wait is
	// measured from the repaired record's own timestamp.
	repairedAt, err := time.Parse("2006-01-02T15:04:05.000000000Z", repaired.Timestamp)
	require.NoError(t, err)
	clock.set(repairedAt.Add(settings.InitialTakeDrain))
	require.NoError(t, paneNodeCall(t.Context(), controlServer.Path(),
		protocol.ControlMethodPaneReconcile, dash, &operation),
		"a repaired record names attempt '-', and fresh evidence frees it")
	require.True(t, operation.Changed)
	freed := readPaneNodeState(t, statePath)
	require.Equal(t, "IDLE", freed.Phase)
	require.Equal(t, "SIN_EFECTO", freed.Result)
	require.NoError(t, paneNodeCall(t.Context(), controlServer.Path(),
		protocol.ControlMethodPaneTake, params, &operation))
	require.True(t, operation.Changed)
	require.Zero(t, sessions.invalidated)
	require.Empty(t, workerOutput.String(),
		"a durable pre-effect cancellation is handled, not a recurring-loop failure")
}

func seedPaneNodeSession(
	t *testing.T,
	store *eventstore.Store,
	clock *paneNodeClock,
	instance tmuxinstance.Instance,
) {
	t.Helper()
	manifest := protocol.CapabilityManifest{
		SchemaVersion: protocol.SchemaVersion, AgentID: instance.AgentID,
		Harness: instance.Harness, HarnessVersion: instance.HarnessVersion,
		AdapterType:        protocol.AdapterTypeGenericPTY,
		InputGuardProvider: protocol.InputGuardProviderTmuxAttach,
		CaptureMode:        protocol.CaptureModeTmuxAttach, Deliverable: true,
		Capabilities: protocol.Capabilities{PTYCapture: true},
	}
	require.NoError(t, manifest.Validate())
	manifestJSON, err := json.Marshal(manifest)
	require.NoError(t, err)
	processJSON, err := json.Marshal(instance.Process())
	require.NoError(t, err)
	stamp := eventstore.FormatTime(clock.Now())
	_, err = store.DB().ExecContext(t.Context(), `INSERT INTO agents
		(agent_id, node_id, harness, created_at, updated_at) VALUES (?, ?, ?, ?, ?)`,
		instance.AgentID, paneNodeID, instance.Harness, stamp, stamp)
	require.NoError(t, err)
	_, err = store.DB().ExecContext(t.Context(), `INSERT INTO sessions
		(session_id, agent_id, state, capture_mode, deliverable, manifest_json,
		 process_json, started_at, ended_at, updated_at)
		VALUES (?, ?, 'CONNECTED', ?, 1, ?, ?, ?, NULL, ?)`,
		instance.SessionID, instance.AgentID, protocol.CaptureModeTmuxAttach,
		string(manifestJSON), string(processJSON), stamp, stamp)
	require.NoError(t, err)
}

func seedPaneNodeDelivery(
	t *testing.T,
	store *eventstore.Store,
	clock *paneNodeClock,
	sequence int,
) daemon.QueueDelivery {
	t.Helper()
	messageID := protocol.ULID(fmt.Sprintf("01ARZ3NDEKTSV4RRFFQ69G%04d", sequence))
	created := eventstore.FormatTime(clock.Now())
	expires := eventstore.FormatTime(clock.Now().Add(time.Hour))
	threadRef := fmt.Sprintf("thr_01ARZ3NDEKTSV4RRFFQ69G%04d", sequence)
	_, err := store.DB().ExecContext(t.Context(), `INSERT INTO messages
		(message_id, client_msg_id, origin_node, from_agent, destination_node,
		 thread_ref, thread_id, priority, state, created_at, expires_at, updated_at,
		 hop_count, route_digest)
		VALUES (?, ?, ?, ?, ?, ?, ?, 'NORMAL', 'PENDING', ?, ?, ?, 0, ?)`,
		messageID, fmt.Sprintf("client-%d", sequence), paneNodeID, paneNodeSender,
		paneNodeID, threadRef, fmt.Sprintf("thread-%d", sequence), created, expires,
		created, fmt.Sprintf("sha256:route-%d", sequence))
	require.NoError(t, err)
	_, err = store.DB().ExecContext(t.Context(), `INSERT INTO delivery_queue
		(message_id, agent_id, delivery_generation, from_agent, thread_ref,
		 state, priority, created_at, expires_at, updated_at, hop_count, route_digest)
		VALUES (?, ?, 1, ?, ?, 'QUEUED', 2, ?, ?, ?, 0, ?)`,
		messageID, paneNodeAgent, paneNodeSender, threadRef, created, expires,
		created, fmt.Sprintf("sha256:route-%d", sequence))
	require.NoError(t, err)
	return daemon.QueueDelivery{
		MessageID: messageID, AgentID: paneNodeAgent, DeliveryGeneration: 1,
	}
}

func paneNodeQueueState(
	t *testing.T,
	store *eventstore.Store,
	delivery daemon.QueueDelivery,
) string {
	t.Helper()
	var state string
	require.NoError(t, store.DB().QueryRowContext(t.Context(), `SELECT state
		FROM delivery_queue WHERE message_id = ? AND agent_id = ? AND delivery_generation = ?`,
		delivery.MessageID, delivery.AgentID, delivery.DeliveryGeneration).Scan(&state))
	return state
}

func waitPaneNodeWorker(
	t *testing.T,
	runs <-chan paneNodeWorkerResult,
) paneNodeWorkerResult {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case result := <-runs:
		return result
	case <-timer.C:
		t.Fatal("timed out waiting for recurring pane-node worker")
		return paneNodeWorkerResult{}
	}
}

func paneNodeCall(
	ctx context.Context,
	socketPath string,
	method protocol.ControlMethod,
	params any,
	result any,
) error {
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
	if err != nil {
		return fmt.Errorf("dial pane-node control api: %w", err)
	}
	defer func() { _ = conn.Close() }()
	raw, err := json.Marshal(params)
	if err != nil {
		return err
	}
	request := protocol.RPCRequest{
		JSONRPC: protocol.JSONRPCVersion, ID: 1, Method: method, Params: protocol.Payload(raw),
	}
	if err := json.NewEncoder(conn).Encode(request); err != nil {
		return err
	}
	line, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		return err
	}
	var response protocol.RPCResponse
	if err := json.Unmarshal(line, &response); err != nil {
		return err
	}
	if err := response.Validate(); err != nil {
		return err
	}
	if response.Error != nil {
		return &paneNodeRPCError{value: *response.Error}
	}
	return json.Unmarshal(response.Result, result)
}

func paneNodeStatePath(
	t *testing.T,
	root string,
	binding tmuxlock.PanelBinding,
) string {
	t.Helper()
	paths, err := tmuxlock.DerivePanelPaths(binding)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(paths.State, tmuxlock.DefaultRoot+"/"))
	return root + strings.TrimPrefix(paths.State, tmuxlock.DefaultRoot)
}

func readPaneNodeState(t *testing.T, path string) paneNodeState {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Len(t, raw, 1024)
	require.Equal(t, "PSTATE03", string(raw[:8]))
	var state paneNodeState
	require.NoError(t, json.Unmarshal(bytes.TrimRight(raw[8:952], " "), &state))
	return state
}

func startPaneNodeMediator(
	t *testing.T,
) (*net.UnixListener, string, <-chan paneNodeMediatorResult) {
	t.Helper()
	socketPath := filepath.Join(t.TempDir(), "writer.sock")
	listener, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: socketPath, Net: "unixpacket"})
	require.NoError(t, err)
	require.NoError(t, paneNodeEnableCredentials(listener))
	done := make(chan paneNodeMediatorResult, 1)
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		var events []paneNodeWriterEvent
		for index := range 6 {
			connection, acceptErr := listener.AcceptUnix()
			if acceptErr != nil {
				done <- paneNodeMediatorResult{events: events, err: acceptErr}
				return
			}
			event, receiveErr := receivePaneNodeWriterEvent(connection)
			if receiveErr == nil {
				events = append(events, event)
				_, receiveErr = fmt.Fprintf(connection, `{"ok":true,"seq":%d,"v":1}`, index+1)
			}
			closeErr := connection.Close()
			if receiveErr != nil || closeErr != nil {
				done <- paneNodeMediatorResult{events: events, err: errors.Join(receiveErr, closeErr)}
				return
			}
		}
		done <- paneNodeMediatorResult{events: events}
	}()
	return listener, socketPath, done
}

func receivePaneNodeWriterEvent(connection *net.UnixConn) (paneNodeWriterEvent, error) {
	if err := paneNodeEnableCredentials(connection); err != nil {
		return paneNodeWriterEvent{}, err
	}
	body := make([]byte, 4097)
	control := make([]byte, unix.CmsgSpace(unix.SizeofUcred)+unix.CmsgSpace(4))
	read, controlRead, flags, _, err := connection.ReadMsgUnix(body, control)
	if err != nil {
		return paneNodeWriterEvent{}, err
	}
	if read == 0 || read > 4096 || flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 {
		return paneNodeWriterEvent{}, errors.New("truncated writer-log request")
	}
	messages, err := unix.ParseSocketControlMessage(control[:controlRead])
	if err != nil {
		return paneNodeWriterEvent{}, err
	}
	var credentials *unix.Ucred
	descriptor := -1
	defer func() {
		if descriptor >= 0 {
			_ = unix.Close(descriptor)
		}
	}()
	for index := range messages {
		message := &messages[index]
		switch {
		case message.Header.Level == unix.SOL_SOCKET && message.Header.Type == unix.SCM_CREDENTIALS:
			credentials, err = unix.ParseUnixCredentials(message)
		case message.Header.Level == unix.SOL_SOCKET && message.Header.Type == unix.SCM_RIGHTS:
			var descriptors []int
			descriptors, err = unix.ParseUnixRights(message)
			if len(descriptors) == 1 {
				descriptor = descriptors[0]
			} else {
				for _, extra := range descriptors {
					_ = unix.Close(extra)
				}
				err = errors.New("unexpected writer-log descriptor count")
			}
		default:
			err = errors.New("unexpected writer-log authority")
		}
		if err != nil {
			return paneNodeWriterEvent{}, err
		}
	}
	if credentials == nil || descriptor < 0 || credentials.Pid != int32(os.Getpid()) ||
		credentials.Uid != uint32(os.Getuid()) || credentials.Gid != uint32(os.Getgid()) {
		return paneNodeWriterEvent{}, errors.New("invalid writer-log authority")
	}
	if err := unix.Flock(descriptor, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return paneNodeWriterEvent{}, fmt.Errorf("writer-log descriptor lost pane claim: %w", err)
	}
	var event paneNodeWriterEvent
	if err := json.Unmarshal(body[:read], &event); err != nil {
		return paneNodeWriterEvent{}, err
	}
	return event, nil
}

func paneNodeEnableCredentials(connection syscall.Conn) error {
	raw, err := connection.SyscallConn()
	if err != nil {
		return err
	}
	var controlErr error
	if err := raw.Control(func(descriptor uintptr) {
		controlErr = unix.SetsockoptInt(int(descriptor), unix.SOL_SOCKET, unix.SO_PASSCRED, 1)
	}); err != nil {
		return err
	}
	return controlErr
}

func requirePaneNodeWriterEvents(t *testing.T, events []paneNodeWriterEvent) {
	t.Helper()
	require.Len(t, events, 6)
	wantEvents := []string{"acquire", "key_begin", "key_end", "key_begin", "key_end", "release"}
	wantWrites := []string{"", "1", "1", "3", "3", ""}
	wantOutcomes := []string{"", "", "ok", "", "ok", ""}
	attempt := events[0].OperationID
	require.Len(t, attempt, 32)
	for index, event := range events {
		require.Equal(t, wantEvents[index], event.Event)
		require.Equal(t, attempt, event.OperationID)
		require.Equal(t, "gifford", event.Profile)
		require.Equal(t, 1, event.Version)
		require.Equal(t, wantWrites[index], paneNodePointer(event.WriteID))
		require.Equal(t, wantOutcomes[index], paneNodePointer(event.Outcome))
	}
}

func paneNodePointer(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
