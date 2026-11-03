//go:build unix

package hub

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/config"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/nativeevents"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/tmuxinstance"
	"github.com/smellak/gifford/internal/xdg"
)

type unusedNativeInstanceRegistry struct{}

func (unusedNativeInstanceRegistry) Lookup(context.Context, protocol.AgentID) (tmuxinstance.Instance, error) {
	return tmuxinstance.Instance{}, tmuxinstance.ErrNotRegistered
}

func (unusedNativeInstanceRegistry) Verify(context.Context, tmuxinstance.Instance, tmuxinstance.PaneObservation) error {
	return tmuxinstance.ErrNotRegistered
}

func TestStartNativeEventServerChoosesTheNodeSocketDirectory(t *testing.T) {
	root, err := os.MkdirTemp("", "gifford-native-hub-") //nolint:usetesting // Unix socket paths must stay below sun_path.
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(root)) })

	for _, test := range []struct {
		name       string
		configured bool
	}{
		{name: "beside configured node socket", configured: true},
		{name: "private runtime fallback", configured: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			caseRoot := filepath.Join(root, test.name)
			runtimeDir := filepath.Join(caseRoot, "runtime")
			sharedDir := filepath.Join(caseRoot, "shared")
			require.NoError(t, os.MkdirAll(runtimeDir, 0o700))
			require.NoError(t, os.MkdirAll(sharedDir, 0o755))

			cfg := config.Config{NodeID: "node-test"}
			wantDir := runtimeDir
			if test.configured {
				cfg.NodeSocket = &config.NodeSocket{
					Path: filepath.Join(sharedDir, "node.sock"), BridgeUID: 1001,
				}
				wantDir = sharedDir
			}
			store, err := eventstore.Open(t.Context(), filepath.Join(caseRoot, "gifford.db"))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })

			server, err := startNativeEventServer(
				t.Context(), cfg, xdg.Dirs{Runtime: runtimeDir}, store, unusedNativeInstanceRegistry{},
			)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, server.Close()) })

			path := filepath.Join(wantDir, nativeevents.NodeSocketName)
			info, err := os.Lstat(path)
			require.NoError(t, err)
			require.Equal(t, os.ModeSocket, info.Mode().Type())
			require.Equal(t, os.FileMode(0o666), info.Mode().Perm())

			conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(t.Context(), "unix", path)
			require.NoError(t, err)
			defer func() { _ = conn.Close() }()
			require.NoError(t, conn.SetDeadline(time.Now().Add(time.Second)))
			require.NoError(t, nativeevents.WriteMessage(conn, nativeevents.StatusRequest{
				Operation: nativeevents.OperationStatus,
			}))
			message, err := nativeevents.ReadMessage(conn)
			require.NoError(t, err)
			result, ok := message.(nativeevents.StatusResult)
			require.True(t, ok)
			require.True(t, result.Serving)
		})
	}
}
