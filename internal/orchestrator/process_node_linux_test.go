//go:build linux

package orchestrator

import (
	"bufio"
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

// nodeTestTree spawns a CLI-shaped process tree with real pids:
//
//	P (cli) -> A (hook child), M (mid child) -> H (hook grandchild).
//
// It returns the four pids keyed by role. The tree stays alive until the
// test ends; cleanup kills every role and reaps the root.
func nodeTestTree(t *testing.T, roleLines ...string) (map[string]int64, func()) {
	t.Helper()
	cmd := exec.CommandContext(context.Background(), "sh", "-c",
		`echo CLI=$$; sleep 120 & echo A=$!; sh -c 'sleep 120 & echo H=$!; wait' & echo M=$!; wait`)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	pids := make(map[string]int64, 4)
	reader := bufio.NewReader(stdout)
	for range roleLines {
		line, rerr := reader.ReadString('\n')
		require.NoError(t, rerr)
		kv := strings.SplitN(strings.TrimSpace(line), "=", 2)
		require.Len(t, kv, 2, "line %q", line)
		n, cerr := strconv.ParseInt(kv[1], 10, 64)
		require.NoError(t, cerr)
		pids[kv[0]] = n
	}
	return pids, func() {
		for _, role := range roleLines {
			_ = exec.CommandContext(context.Background(), "kill", fmt.Sprint(pids[role])).Run()
		}
		_ = cmd.Wait()
	}
}

func nodeTestStart(t *testing.T, pid int64) int64 {
	t.Helper()
	start, _, err := newProcInspector("/proc").readStat(pid)
	require.NoError(t, err)
	return start
}

// TestNodePeerInspectorAcceptsCliShape is the 80277 focal test: a harness
// CLI with several hook and MCP children authenticates the CLI itself (an
// in-process plugin) and any descendant (a hook child), with the root token
// staying the CLI. The current peer guard rejects all three with
// "expected one direct harness child".
func TestNodePeerInspectorAcceptsCliShape(t *testing.T) {
	t.Parallel()
	pids, stop := nodeTestTree(t, "CLI", "A", "M", "H")
	t.Cleanup(stop)

	cliStart := nodeTestStart(t, pids["CLI"])
	proc := protocol.ProcessIdentity{PID: pids["CLI"], ProcessStartTime: cliStart}
	root := ProcessToken{PID: pids["CLI"], StartTime: cliStart}
	inspector := NewNodePeerProcessInspector("")

	t.Run("cli itself", func(t *testing.T) {
		t.Parallel()
		lineage, err := inspector.InspectNodePeer(proc, root)
		require.NoError(t, err)
		require.Equal(t, PeerLineage{Root: root, Harness: root, Peer: root}, lineage)
	})

	t.Run("hook child", func(t *testing.T) {
		t.Parallel()
		peer := ProcessToken{PID: pids["A"], StartTime: nodeTestStart(t, pids["A"])}
		lineage, err := inspector.InspectNodePeer(proc, peer)
		require.NoError(t, err)
		require.Equal(t, root, lineage.Root)
		require.Equal(t, peer, lineage.Peer)
		require.Equal(t, peer, lineage.Harness)
	})

	t.Run("hook grandchild", func(t *testing.T) {
		t.Parallel()
		peer := ProcessToken{PID: pids["H"], StartTime: nodeTestStart(t, pids["H"])}
		mid := ProcessToken{PID: pids["M"], StartTime: nodeTestStart(t, pids["M"])}
		lineage, err := inspector.InspectNodePeer(proc, peer)
		require.NoError(t, err)
		require.Equal(t, root, lineage.Root)
		require.Equal(t, peer, lineage.Peer)
		require.Equal(t, mid, lineage.Harness)
	})
}

// TestNodePeerInspectorFailsClosed covers the negative contract with real
// processes: an unrelated peer of the same uid never reaches the harness,
// and a reused pid with a different start time is rejected.
func TestNodePeerInspectorFailsClosed(t *testing.T) {
	t.Parallel()

	t.Run("outsider same uid", func(t *testing.T) {
		t.Parallel()
		pids, stop := nodeTestTree(t, "CLI", "A", "M", "H")
		t.Cleanup(stop)
		outside := exec.CommandContext(context.Background(), "sleep", "120")
		require.NoError(t, outside.Start())
		t.Cleanup(func() { _ = outside.Process.Kill() })
		outsider := ProcessToken{
			PID:       int64(outside.Process.Pid),
			StartTime: nodeTestStart(t, int64(outside.Process.Pid)),
		}
		proc := protocol.ProcessIdentity{
			PID:              pids["CLI"],
			ProcessStartTime: nodeTestStart(t, pids["CLI"]),
		}
		_, err := NewNodePeerProcessInspector("").InspectNodePeer(proc, outsider)
		require.ErrorContains(t, err, "node peer is not below harness")
		require.False(t, IsProcessObservationIncomplete(err))
	})

	t.Run("reused pid identity", func(t *testing.T) {
		t.Parallel()
		pids, stop := nodeTestTree(t, "CLI", "A", "M", "H")
		t.Cleanup(stop)
		proc := protocol.ProcessIdentity{
			PID:              pids["CLI"],
			ProcessStartTime: nodeTestStart(t, pids["CLI"]),
		}
		stale := ProcessToken{PID: pids["A"], StartTime: nodeTestStart(t, pids["A"]) + 1}
		_, err := NewNodePeerProcessInspector("").InspectNodePeer(proc, stale)
		require.ErrorContains(t, err, "identity changed during peer observation")
		require.False(t, IsProcessObservationIncomplete(err))
	})

	t.Run("reused harness identity", func(t *testing.T) {
		t.Parallel()
		pids, stop := nodeTestTree(t, "CLI", "A", "M", "H")
		t.Cleanup(stop)
		staleRoot := protocol.ProcessIdentity{
			PID:              pids["CLI"],
			ProcessStartTime: nodeTestStart(t, pids["CLI"]) + 1,
		}
		peer := ProcessToken{PID: pids["A"], StartTime: nodeTestStart(t, pids["A"])}
		_, err := NewNodePeerProcessInspector("").InspectNodePeer(staleRoot, peer)
		require.ErrorContains(t, err, "session pid identity changed")
		require.False(t, IsProcessObservationIncomplete(err))
	})
}
