package orchestrator

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

// The neutral bridge default stays a valid single line without commands.
func TestDefaultBridgeNoticeLine(t *testing.T) {
	t.Parallel()

	line := protocol.NoticeLine(defaultBridgeNoticeLine)
	require.True(t, line.Valid(), "default must be a valid NoticeLine")
	require.NotContains(t, defaultBridgeNoticeLine, ";", "tmux would chain commands on a semicolon")
	require.NotContains(t, defaultBridgeNoticeLine, "\n", "notice stays a single line")
	require.True(t, strings.HasPrefix(defaultBridgeNoticeLine, ": [GIFFORD] "),
		"notice starts with the shell no-op, then the product prefix")
}

func bridgeOptionExecutor(t *testing.T, line protocol.NoticeLine) (*Executor, error) {
	t.Helper()
	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "bridge.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	clk := &executorClock{now: executorTestTime}
	engine, err := channel.New(channel.Options{Clock: clk})
	require.NoError(t, err)
	controller, err := channel.NewController(channel.ControllerOptions{
		DB: store.DB(), Engine: engine, Clock: clk, NodeID: executorTestNode,
		Entropy: bytes.NewReader(make([]byte, 4096)),
	})
	require.NoError(t, err)
	return NewExecutor(ExecutorOptions{
		DB: store.DB(), Channel: engine, Approvals: controller,
		Links: &executorLinks{}, Clock: clk, BridgeNoticeLine: line,
	})
}

func TestBridgeNoticeLineOption(t *testing.T) {
	t.Parallel()

	executor, err := bridgeOptionExecutor(t, "")
	require.NoError(t, err)
	require.Equal(t, protocol.NoticeLine(defaultBridgeNoticeLine), executor.bridgeNotice,
		"empty option means the neutral default")

	custom := protocol.NoticeLine("[GIFFORD] 1 pending message: deploy line")
	executor, err = bridgeOptionExecutor(t, custom)
	require.NoError(t, err)
	require.Equal(t, custom, executor.bridgeNotice, "configured value is kept")

	for _, bad := range []protocol.NoticeLine{
		"[GIFFORD] pending; rm",
		"[GIFFORD] pending\nmessage",
	} {
		if _, err := bridgeOptionExecutor(t, bad); err == nil {
			t.Fatalf("%q must be rejected", string(bad))
		}
	}
}
