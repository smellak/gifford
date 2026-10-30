//go:build unix

package run

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/creack/pty"
	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/adapters"
	"github.com/smellak/gifford/internal/agentsock"
	"github.com/smellak/gifford/internal/protocol"
)

type runTestAdapter struct {
	listener net.Listener
	mu       sync.Mutex
	conn     net.Conn
	once     sync.Once
}

func startRunTestAdapter(t *testing.T, path string, manifest protocol.CapabilityManifest) *runTestAdapter {
	t.Helper()
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "unix", path)
	require.NoError(t, err)
	require.NoError(t, os.Chmod(path, 0o600))
	fake := &runTestAdapter{listener: listener}
	t.Cleanup(fake.kill)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		fake.mu.Lock()
		fake.conn = conn
		fake.mu.Unlock()
		request, err := adapters.ReadMessage(conn)
		if err != nil || request.Operation != adapters.OperationHandshake {
			_ = conn.Close()
			return
		}
		if err := adapters.WriteMessage(conn, adapters.Message{
			Operation: adapters.OperationManifest,
			ID:        1,
			ReplyTo:   request.ID,
			Manifest:  &manifest,
		}); err != nil {
			_ = conn.Close()
			return
		}
		for {
			message, err := adapters.ReadMessage(conn)
			if err != nil {
				return
			}
			if message.Operation != adapters.OperationPing {
				return
			}
			if err := adapters.WriteMessage(conn, adapters.Message{
				Operation: adapters.OperationPong,
				ID:        message.ID,
				ReplyTo:   message.ID,
			}); err != nil {
				return
			}
		}
	}()
	return fake
}

func (f *runTestAdapter) kill() {
	f.once.Do(func() {
		f.mu.Lock()
		conn := f.conn
		f.mu.Unlock()
		if conn != nil {
			_ = conn.Close()
		}
		_ = f.listener.Close()
	})
}

func writeAdapterHarness(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fakeharness")
	body := []byte("#!/bin/sh\n" +
		"if [ \"$1\" = \"--version\" ]; then echo \"fakeharness 1.2.3\"; exit 0; fi\n" +
		"stty -echo\necho ready\nIFS= read -r line\necho \"got:$line\"\nread -r gate\n")
	syscall.ForkLock.RLock()
	err := os.WriteFile(path, body, 0o700)
	syscall.ForkLock.RUnlock()
	require.NoError(t, err)
	return path
}

func nativeRunManifest() protocol.CapabilityManifest {
	return protocol.CapabilityManifest{
		SchemaVersion:      protocol.SchemaVersion,
		AgentID:            socketAgent,
		Harness:            protocol.HarnessCodex,
		HarnessVersion:     "fakeadapter 9.9.9",
		AdapterType:        protocol.AdapterTypeNativeAPIThinPTY,
		InputGuardProvider: protocol.InputGuardProviderPTYProxy,
		CaptureMode:        protocol.CaptureModePTYProxy,
		Deliverable:        true,
		Capabilities: protocol.Capabilities{
			StructuredEvents: true,
			NativeCancel:     true,
			PTYCapture:       true,
			MCP:              true,
		},
	}
}

func configuredRunAdapterPath(t *testing.T, runtimeHome string) string {
	t.Helper()
	path := adapterSocketPath(filepath.Join(runtimeHome, "gifford"), socketAgent)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
	return path
}

func TestAdapterRunConsumesLocalDegradation(t *testing.T) {
	runtimeHome := t.TempDir()
	path := configuredRunAdapterPath(t, runtimeHome)
	fake := startRunTestAdapter(t, path, nativeRunManifest())
	fallback := adapters.GenericManifest(socketAgent, protocol.HarnessCodex, "1.2.3")
	client, _, err := adapters.Connect(t.Context(), adapters.Options{
		SocketPath: path, AgentID: socketAgent, Harness: protocol.HarnessCodex,
		Fallback: fallback, HeartbeatEvery: 10 * time.Millisecond, HeartbeatTimeout: 20 * time.Millisecond,
	})
	require.NoError(t, err)
	run := &adapterRun{client: client}
	done := run.start()
	fake.kill()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("adapter degradation was not consumed")
	}
	require.Equal(t, protocol.AdapterTypeGenericPTY, client.Manifest().AdapterType)
	run.stop(done)
}

// TestAdapterHandshakeFailureRegistersGeneric keeps a configured but
// unavailable adapter from changing startup: the harness still runs and the
// Hub sees only the valid generic PTY fallback.
func TestAdapterHandshakeFailureRegistersGeneric(t *testing.T) {
	runtimeHome := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", runtimeHome)
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	t.Setenv("GORACE", "atexit_sleep_ms=0")
	fakeHub := startFakeHub(t, filepath.Join(runtimeHome, "gifford", "hub.sock"))
	adapterPath := configuredRunAdapterPath(t, runtimeHome)
	require.NoError(t, os.WriteFile(adapterPath, []byte("not a socket"), 0o600))

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
	var stderr syncBuffer
	cmd.SetErr(&stderr)
	cmd.SetArgs(append([]string{
		agentFlag, socketAgent,
		harnessFlag, codexHarness,
		"--",
	}, cliHelperArgv("say")...))
	require.NoError(t, cmd.ExecuteContext(t.Context()))
	readUntil(t, outer, "hello-agent-say")

	registrations := fakeHub.registrations()
	require.Len(t, registrations, 1)
	manifest := registrations[0].Manifest
	require.Equal(t, protocol.AdapterTypeGenericPTY, manifest.AdapterType)
	require.False(t, manifest.Capabilities.NativeCancel)
	require.Contains(t, stderr.String(), "native adapter unavailable")
	require.Contains(t, stderr.String(), "using generic_pty")
	require.Equal(t, 1, strings.Count(stderr.String(), "native adapter unavailable"))
}

// TestAdapterDeathKeepsGuardAndPTYDelivery verifies the whole run boundary:
// native startup followed by local-only degradation, with the same Draft
// Guard state and the same agent-socket PTY delivery path.
func TestAdapterDeathKeepsGuardAndPTYDelivery(t *testing.T) {
	runtimeHome := t.TempDir()
	dataHome := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", runtimeHome)
	t.Setenv("XDG_DATA_HOME", dataHome)
	t.Setenv("GORACE", "atexit_sleep_ms=0")
	fakeHub := startFakeHub(t, filepath.Join(runtimeHome, "gifford", "hub.sock"))
	adapterPath := configuredRunAdapterPath(t, runtimeHome)
	fakeAdapter := startRunTestAdapter(t, adapterPath, nativeRunManifest())
	harness := writeAdapterHarness(t)

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
	var stderr syncBuffer
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		agentFlag, socketAgent,
		harnessFlag, codexHarness,
		"--", harness,
	})
	done := make(chan error, 1)
	go func() { done <- cmd.ExecuteContext(t.Context()) }()
	readUntil(t, outer, "ready")
	require.Eventually(t, func() bool {
		return len(fakeHub.registrations()) == 1
	}, 10*time.Second, 10*time.Millisecond, "native manifest was not registered")
	require.Equal(t, protocol.AdapterTypeNativeAPIThinPTY, fakeHub.registrations()[0].Manifest.AdapterType)

	path := filepath.Join(runtimeHome, "gifford", "agents", socketAgent+".sock")
	conn, err := (&net.Dialer{}).DialContext(context.Background(), "unix", path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(20*time.Second)))
	hello, err := agentsock.ReadFrame(conn)
	require.NoError(t, err)
	require.Equal(t, protocol.AgentOpRegisterAgent, hello.Op)
	var socketRegistration protocol.RegisterAgent
	require.NoError(t, json.Unmarshal(hello.Body, &socketRegistration))
	require.Equal(t, protocol.AdapterTypeNativeAPIThinPTY, socketRegistration.Manifest.AdapterType)

	sentAt := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	next := int64(1)
	send := func(op protocol.AgentOp, replyTo int64, body any) int64 {
		t.Helper()
		raw, err := json.Marshal(body)
		require.NoError(t, err)
		frame := protocol.AgentFrame{Op: op, FrameID: next, SentAt: sentAt, Body: raw}
		if replyTo > 0 {
			frame.ReplyTo = &replyTo
		}
		require.NoError(t, agentsock.WriteFrame(conn, frame))
		next++
		return frame.FrameID
	}
	send(protocol.AgentOpRegisterAgentResult, hello.FrameID,
		protocol.RegisterAgentResult{AgentID: socketAgent, SessionID: socketSession})
	first, err := agentsock.ReadFrame(conn)
	require.NoError(t, err)
	var before protocol.InputGuardReport
	require.NoError(t, json.Unmarshal(first.Body, &before))
	require.Equal(t, protocol.DraftStateClosed, before.DraftState)

	noticesBeforeDeath := stderr.String()
	fakeAdapter.kill()
	// Allow the default heartbeat to observe the closed adapter. This phase
	// degrades locally only: it must neither re-register nor print over the
	// harness screen while the propagation contract is still pending.
	require.Never(t, func() bool {
		return len(fakeHub.registrations()) != 1
	}, 4*time.Second, 25*time.Millisecond)
	require.Equal(t, noticesBeforeDeath, stderr.String())

	resyncID := send(protocol.AgentOpResync, 0, protocol.AgentTarget{AgentID: socketAgent})
	resync := readAnswer(t, conn, resyncID)
	require.Equal(t, protocol.AgentOpReportInputGuard, resync.Op)
	var after protocol.InputGuardReport
	require.NoError(t, json.Unmarshal(resync.Body, &after))
	require.Equal(t, before.DraftState, after.DraftState)
	require.Equal(t, before.DraftGeneration, after.DraftGeneration)
	require.Equal(t, before.InputGuardVersion, after.InputGuardVersion)

	const notice = "[GIFFORD] adapter fallback still delivers"
	result := func(id int64) protocol.DeliveryResult {
		t.Helper()
		frame := readAnswer(t, conn, id)
		require.Equal(t, protocol.AgentOpReportDeliveryResult, frame.Op)
		var result protocol.DeliveryResult
		require.NoError(t, json.Unmarshal(frame.Body, &result))
		return result
	}
	prepared := result(send(protocol.AgentOpPrepareDelivery, 0, protocol.PrepareDelivery{
		DeliveryID: "dlv-adapter", AgentID: socketAgent,
		MessageRef: protocol.MessageRef{
			MessageID: "01JB0000000000000000000021", FromAgent: "agent-2",
			Priority: protocol.PriorityNormal, ThreadID: "thread-adapter",
		},
		NoticeLine:             notice,
		OrchestratorDecisionID: "01JB0000000000000000000022",
		ExpectedStateVersion:   1, ExpectedInputGuardVersion: after.InputGuardVersion,
		ExpectedDraftGeneration: after.DraftGeneration,
		ControlVersions:         protocol.ControlVersions{"global": 19},
	}))
	require.Equal(t, protocol.DeliveryResultKindPrepared, prepared.Result)
	committed := result(send(protocol.AgentOpCommitDelivery, 0,
		protocol.CommitDelivery{DeliveryID: "dlv-adapter", WriterLeaseID: "lease-adapter"}))
	require.Equal(t, protocol.DeliveryResultKindCommitted, committed.Result)
	readUntil(t, outer, "got:"+notice)

	_, err = outer.Write([]byte("\n"))
	require.NoError(t, err)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("run never ended after PTY delivery")
	}
}
