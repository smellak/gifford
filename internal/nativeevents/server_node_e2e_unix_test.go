//go:build unix

package nativeevents

import (
	"context"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/orchestrator"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/tmuxattach"
	"github.com/smellak/gifford/internal/registry"
	"github.com/smellak/gifford/internal/tmuxinstance"
)

const nodeE2ETestNodeID = "node-dbe2e"

type tmuxTestPaneLister struct {
	socket string
}

func (l tmuxTestPaneLister) ListPanes(_ context.Context, _, _ string) ([]tmuxinstance.PaneRow, error) {
	out, err := exec.CommandContext(context.Background(), "tmux", "-S", l.socket, "list-panes", "-F", "#{pane_id} #{pane_pid}").Output()
	if err != nil {
		return nil, err
	}
	var rows []tmuxinstance.PaneRow
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		pid, err := strconv.Atoi(fields[1])
		if err != nil {
			continue
		}
		rows = append(rows, tmuxinstance.PaneRow{
			PaneID: protocol.TmuxPaneID(fields[0]), PanePID: int64(pid),
		})
	}
	return rows, nil
}

func tmuxRun(t *testing.T, socket string, args ...string) string {
	t.Helper()
	cmd := exec.CommandContext(context.Background(), "tmux", append([]string{"-S", socket}, args...)...)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "tmux %v: %s", args, out)
	return strings.TrimSpace(string(out))
}

// TestNodeServerStoresHookEventDurablyBeforeAck runs the
// acceptance end to end: a hook of a registered tmux instance emits
// through the node entry, the event lands in the database, and only then
// the ack leaves the entry. Tree, registry row, store and ack are all
// real; the harness is the pane process itself.
func TestNodeServerStoresHookEventDurablyBeforeAck(t *testing.T) {
	if _, err := exec.LookPath("tmux"); err != nil {
		t.Skip("tmux is unavailable")
	}
	ctx := context.Background()
	self, err := os.Executable()
	require.NoError(t, err)
	me, err := user.Current()
	require.NoError(t, err)

	//nolint:usetesting // keep the Unix socket paths below their platform limit
	base, err := os.MkdirTemp("", "node-dbe2e-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(base) })
	sockDir := filepath.Join(base, "tmux")
	nodeDir := filepath.Join(base, "node")
	for _, dir := range []string{sockDir, nodeDir} {
		require.NoError(t, os.MkdirAll(dir, 0o700))
		require.NoError(t, os.Chmod(dir, 0o700))
	}
	tmuxSocket := filepath.Join(sockDir, "tmux.sock")
	goFile := filepath.Join(base, "go")
	statusFile := filepath.Join(base, "harness-status")

	store, err := eventstore.Open(ctx, filepath.Join(base, "events.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	concrete, err := tmuxinstance.New(tmuxinstance.Options{
		DB: store.DB(), Clock: clock.System{}, Panes: tmuxTestPaneLister{socket: tmuxSocket},
	})
	require.NoError(t, err)
	instances := InstanceRegistry{Registry: concrete}
	nodeSocketPath := filepath.Join(nodeDir, NodeSocketName)
	server, err := ListenNode(ctx, NodeOptions{
		SocketDir: nodeDir,
		Resolve: func(ctx context.Context, pid int) (protocol.AgentID, protocol.OpaqueID, error) {
			return registry.ResolveAgentSessionPID(ctx, store.DB(), pid)
		},
		Instances: instances,
		Queues:    &SessionQueueProvider{NodeID: nodeE2ETestNodeID},
		Store:     store,
		Inspector: orchestrator.NewPeerProcessInspector(""),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.Close() })

	tmuxRun(t, tmuxSocket, "new-session", "-d", "-s", "node-e2e", "-x", "200", "-y", "50",
		"-e", "GO_TEST_NODE_HARNESS=1",
		"-e", "GO_TEST_NODE_GO="+goFile,
		"-e", "GO_TEST_NODE_STATUS="+statusFile,
		"-e", "GO_TEST_NODE_SOCKET="+nodeSocketPath,
		self, "-test.run=TestNodeHarnessChild$")
	t.Cleanup(func() {
		_ = exec.CommandContext(context.Background(), "tmux", "-S", tmuxSocket, "kill-server").Run()
	})

	panePID := waitTmuxPane(t, tmuxSocket)
	comm := readProcComm(t, panePID)
	paneStart, err := readProcessStartTime(panePID)
	require.NoError(t, err)

	inst, err := concrete.Register(ctx, tmuxinstance.Registration{
		AgentID: nodeTestAgentID, AgentUser: me.Username, OwnerUser: me.Username,
		Socket: tmuxSocket, PaneID: waitTmuxPaneID(t, tmuxSocket),
		PaneComm: comm, HarnessComm: comm, HarnessArgv0: filepath.Base(self),
		Harness: protocol.HarnessCodex, HarnessVersion: testHarnessVersion,
		Source: nodeE2ETestNodeID, RegisteredBy: uint32(os.Getuid()),
	})
	require.NoError(t, err)

	sessions, err := tmuxattach.NewSessionStore(tmuxattach.SessionStoreOptions{
		DB: store.DB(), NodeID: nodeE2ETestNodeID, Clock: clock.System{},
	})
	require.NoError(t, err)
	sessionID, err := sessions.Bind(ctx, tmuxattach.SessionSpec{
		InstanceID: inst.ID, AgentID: nodeTestAgentID,
		Harness: protocol.HarnessCodex, HarnessVersion: testHarnessVersion,
		Process: protocol.ProcessIdentity{
			PID: panePID, ProcessStartTime: paneStart, UID: int64(os.Getuid()),
		},
		BootID: "node-dbe2e-boot",
	})
	require.NoError(t, err)
	_ = sessionID

	require.NoError(t, os.WriteFile(goFile, []byte("go"), 0o600))

	deadline := time.Now().Add(60 * time.Second)
	for {
		if raw, err := os.ReadFile(statusFile); err == nil {
			require.Equal(t, "emitter-ok", strings.TrimSpace(string(raw)), "the hook must reach its durable ack")
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the harness never reported the emitter outcome")
		}
		time.Sleep(200 * time.Millisecond)
	}

	envelopes, err := store.SessionEvents(ctx, sessionID, 0, 10)
	require.NoError(t, err)
	require.Len(t, envelopes, 1, "the hook event must be stored before the ack")
	require.Equal(t, sessionID, envelopes[0].SessionID)
	require.Equal(t, protocol.AgentID(nodeTestAgentID), envelopes[0].AgentID)
	require.Equal(t, protocol.HarnessCodex, envelopes[0].Harness)
	payload, err := protocol.DecodeNativeDurableEventPayload(envelopes[0].Payload)
	require.NoError(t, err)
	require.Equal(t, protocol.OpaqueID("event-1"), payload.EmitterEventID)
}

func waitTmuxPane(t *testing.T, tmuxSocket string) int64 {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		lister := tmuxTestPaneLister{socket: tmuxSocket}
		rows, err := lister.ListPanes(context.Background(), "", "")
		if err == nil && len(rows) == 1 {
			return rows[0].PanePID
		}
		if time.Now().After(deadline) {
			t.Fatalf("tmux pane never appeared: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func waitTmuxPaneID(t *testing.T, tmuxSocket string) protocol.TmuxPaneID {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		lister := tmuxTestPaneLister{socket: tmuxSocket}
		rows, err := lister.ListPanes(context.Background(), "", "")
		if err == nil && len(rows) == 1 {
			return rows[0].PaneID
		}
		if time.Now().After(deadline) {
			t.Fatalf("tmux pane never appeared: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func readProcComm(t *testing.T, pid int64) string {
	t.Helper()
	raw, err := os.ReadFile("/proc/" + strconv.FormatInt(pid, 10) + "/comm")
	require.NoError(t, err)
	return strings.TrimSpace(string(raw))
}
