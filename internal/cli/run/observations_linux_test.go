//go:build linux

package run

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/creack/pty"
	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/adapters"
	"github.com/smellak/gifford/internal/analyst"
	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/config"
	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/nativeevents"
	"github.com/smellak/gifford/internal/orchestrator"
	"github.com/smellak/gifford/internal/protocol"
)

// nodeID is the in-process node's id.
const nodeID protocol.NodeID = "node-test"

type observingNode struct {
	db       *sql.DB
	store    *eventstore.Store
	producer *orchestrator.StateProducer
}

// startObservingNode starts the part of a node that takes a run's state
// observations, on a store of its own: the state producer, with no
// analyzer, and the link to the run's agent socket under runtimeDir,
// handing the producer what the socket sends. The session the fake Hub
// gave the run is recorded first, with registration, as the Hub records a
// registration. It returns the store's database.
func startObservingNode(t *testing.T, runtimeDir string, registration protocol.RegisterAgent) *sql.DB {
	return startObservingNodeWithAnalyzer(
		t, runtimeDir, registration, nil, orchestrator.StateAnalysisProfile{},
	).db
}

func startObservingNodeWithAnalyzer(
	t *testing.T,
	runtimeDir string,
	registration protocol.RegisterAgent,
	analyzer orchestrator.StateAnalyzer,
	profile orchestrator.StateAnalysisProfile,
) observingNode {
	t.Helper()
	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "gifford.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	db := store.DB()
	manifest, err := json.Marshal(registration.Manifest)
	require.NoError(t, err)
	process, err := json.Marshal(registration.Process)
	require.NoError(t, err)
	clk := agentlink.SystemClock{}
	stamp := eventstore.FormatTime(clk.Now())
	_, err = db.ExecContext(t.Context(), `INSERT INTO agents
		(agent_id, node_id, harness, created_at, updated_at) VALUES (?, ?, ?, ?, ?)`,
		socketAgent, nodeID, registration.Manifest.Harness, stamp, stamp)
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), `INSERT INTO sessions
		(session_id, agent_id, state, capture_mode, deliverable, manifest_json,
		 process_json, started_at, ended_at, updated_at)
		VALUES (?, ?, 'CONNECTED', 'pty_proxy', 1, ?, ?, ?, NULL, ?)`,
		socketSession, socketAgent, string(manifest), string(process), stamp, stamp)
	require.NoError(t, err)

	engine, err := channel.New(channel.Options{Clock: clk})
	require.NoError(t, err)
	controller, err := channel.NewController(channel.ControllerOptions{
		DB: db, Engine: engine, Clock: clk, NodeID: nodeID,
	})
	require.NoError(t, err)
	require.NoError(t, controller.Reload(t.Context()))
	producer, err := orchestrator.NewStateProducer(orchestrator.StateProducerOptions{
		DB: db, Clock: clk, Channel: controller,
		Analyzer: analyzer, Profile: profile,
		ErrorSink: func(err error) { t.Logf("state producer: %v", err) },
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	produced := make(chan error, 1)
	go func() { produced <- producer.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		require.NoError(t, <-produced)
	})

	guards, err := agentlink.NewInputGuardStore(db, clk)
	require.NoError(t, err)
	link, err := agentlink.Start(t.Context(), agentlink.Options{
		RuntimeDir: runtimeDir, DB: db, AgentID: socketAgent, SessionID: socketSession,
		Writer: guards, Observations: producer, Clock: clk,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, link.Close()) })
	return observingNode{db: db, store: store, producer: producer}
}

type nativeWindowAnalyzer struct{}

func (nativeWindowAnalyzer) Analyze(
	_ context.Context,
	observation analyst.Observation,
) (analyst.Analysis, error) {
	reduced := observation.ReduceNative()
	return analyst.Analysis{
		AgentID: observation.AgentID, Route: analyst.RouteNative,
		Phase: reduced.Phase, Confidence: reduced.Confidence,
		PreviousStale:   reduced.Stale,
		NativeEvidence:  append([]protocol.ULID(nil), reduced.Evidence...),
		NativeExpiresAt: reduced.ExpiresAt,
	}, nil
}

// TestRunObservationsReachTheProducer runs --agent with state analysis
// enabled against an in-process node: the real link, wired to the real
// state producer. The run's observations reach the producer, which commits
// a snapshot under the run's own observation id. The producer has no
// analyzer, so nothing calls a model.
func TestRunObservationsReachTheProducer(t *testing.T) {
	runtimeHome := t.TempDir()
	configDir := filepath.Join(t.TempDir(), "gifford")
	t.Setenv("XDG_RUNTIME_DIR", runtimeHome)
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", filepath.Dir(configDir))
	t.Setenv("GORACE", "atexit_sleep_ms=0")
	require.NoError(t, os.MkdirAll(configDir, 0o700))
	require.NoError(t, os.WriteFile(config.DefaultPath(configDir), []byte(stateAnalysisYAML), 0o600))
	fake := startFakeHub(t, filepath.Join(runtimeHome, "gifford", "hub.sock"))
	script := filepath.Join(t.TempDir(), "fakeharness")
	writeTestExecutable(t, script, []byte("#!/bin/sh\n"+
		"if [ \"$1\" = \"--version\" ]; then echo \"fakeharness 1.2.3\"; exit 0; fi\n"+
		"stty -echo\necho ready\nIFS= read -r line\n"))

	outer, slave, err := pty.Open()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = outer.Close()
		_ = slave.Close()
	})
	require.NoError(t, pty.Setsize(outer, &pty.Winsize{Rows: 24, Cols: 80}))
	cmd := NewCmd(nil)
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	cmd.SetIn(slave)
	cmd.SetOut(slave)
	cmd.SetErr(&syncBuffer{})
	cmd.SetArgs([]string{agentFlag, socketAgent, harnessFlag, codexHarness, "--", script})
	done := make(chan error, 1)
	go func() { done <- cmd.Execute() }()
	readUntil(t, outer, "ready")
	require.Eventually(t, func() bool {
		return len(fake.registrations()) == 1
	}, 10*time.Second, 10*time.Millisecond, "hub never saw register_agent")

	db := startObservingNode(t, filepath.Join(runtimeHome, "gifford"), fake.registrations()[0])
	var id string
	require.Eventually(t, func() bool {
		return db.QueryRowContext(t.Context(), `SELECT observation_id FROM state_snapshots
			WHERE agent_id = ? AND session_id = ? AND producer_version = ?
			AND observation_id LIKE 'o-%' LIMIT 1`,
			socketAgent, socketSession, orchestrator.StateProducerVersion).Scan(&id) == nil
	}, 15*time.Second, 10*time.Millisecond, "no snapshot of the run's observations")
	require.Regexp(t, observationID, id)
	var calls int64
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT count(*) FROM glimmer_calls").Scan(&calls))
	require.Zero(t, calls, "nothing calls a model")

	_, err = outer.Write([]byte("\n"))
	require.NoError(t, err)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("run never ended after releasing the child")
	}
}

func TestRunNativeWindowReachesReducerWithDurablePrefix(t *testing.T) {
	runtimeHome := t.TempDir()
	manifest := adapters.GenericManifest(socketAgent, protocol.HarnessCodex, "1.2.3")
	registration := protocol.RegisterAgent{
		Manifest: manifest,
		Process: &protocol.ProcessIdentity{
			PID: int64(os.Getpid()), ProcessStartTime: 1,
			UID: int64(os.Getuid()), GID: int64(os.Getgid()),
		},
	}
	digest := protocol.SHA256Hex(
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	)
	profile := orchestrator.StateAnalysisProfile{
		Revision: "profile-window-v1", SHA256: digest,
		CalibrationRevision: "calibration-window-v1", CalibrationSHA256: digest,
		Temperature: 1, IdleThreshold: .9,
	}
	node := startObservingNodeWithAnalyzer(
		t, runtimeHome, registration, nativeWindowAnalyzer{}, profile,
	)
	queue, err := nativeevents.NewQueue(nativeevents.Lineage{
		NodeID: nodeID, SessionID: socketSession, AgentID: socketAgent,
		Harness: manifest.Harness, HarnessVersion: manifest.HarnessVersion,
		AdapterType: manifest.AdapterType,
	})
	require.NoError(t, err)
	const total = nativeevents.MaxQueuedEvents + 56
	var last protocol.EventEnvelope
	for sequence := 1; sequence <= total; sequence++ {
		typeID := protocol.EventTypeNotificationReceived
		toolRef := protocol.OpaqueID("")
		if sequence == total {
			typeID = protocol.EventTypeToolStarted
			toolRef = "tool-window"
		}
		receipt, submitErr := queue.Submit(nativeevents.EmitterTypePlugin, nativeevents.Event{
			Operation: nativeevents.OperationEvent, Sequence: int64(sequence),
			EmitterEventID: protocol.OpaqueID(fmt.Sprintf("event-window-%d", sequence)),
			Type:           typeID, ObservedAt: time.Now().UnixMilli(),
			TurnRef: "turn-window", ToolRef: toolRef,
		})
		require.NoError(t, submitErr)
		require.True(t, receipt.Accepted())
		head, ok := queue.Head()
		require.True(t, ok)
		require.NoError(t, node.store.AppendEvents(
			t.Context(), []protocol.EventEnvelope{head},
		))
		queue.Settle(protocol.EmitEventResult{
			EventID: head.EventID, Status: protocol.EmitEventStatusStored,
		})
		last = head
	}

	base := &nativeObservationBase{
		report: protocol.StateObservationReport{
			AgentID: socketAgent, SessionID: socketSession,
			ObservationID: "observation-native-window", ObservedAt: time.Now().UTC(),
			Harness: manifest.Harness, HarnessVersion: manifest.HarnessVersion,
			ObservationGeneration: 1,
			ScreenState:           protocol.StateObservationScreenWithheld,
			ScreenRows:            []string{}, NativeEvents: []protocol.StateObservationEvent{},
		},
		changed: make(chan struct{}, 1),
	}
	source := newNativeObservationSession(t.Context(), base, queue)
	report, ok := source.Observe()
	require.True(t, ok)
	require.Equal(t, int64(total), report.EventGeneration)
	require.Len(t, report.NativeEvents, nativeevents.MaxQueuedEvents)
	require.Equal(t, last.EventID, report.NativeEvents[len(report.NativeEvents)-1].EventID)
	require.NoError(t, node.producer.Demand(socketAgent))
	require.NoError(t, node.producer.Submit(report))

	require.Eventually(t, func() bool {
		var phase string
		return node.db.QueryRowContext(t.Context(), `SELECT agent_phase FROM state_snapshots
			WHERE observation_id = ? AND producer_version = ?`,
			report.ObservationID, orchestrator.StateProducerVersion,
		).Scan(&phase) == nil && protocol.AgentPhase(phase) == protocol.AgentPhaseRunningTool
	}, 15*time.Second, 10*time.Millisecond,
		"the fresh operation in the bounded run window never reached the reducer")
}
