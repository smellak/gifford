//go:build unix

package hub

import (
	"context"
	"fmt"
	"io"
	"path/filepath"

	"github.com/smellak/gifford/internal/config"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/nativeevents"
	"github.com/smellak/gifford/internal/orchestrator"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
	"github.com/smellak/gifford/internal/xdg"
)

// startNativeEventServer serves the node-shared native-event entry next
// to the hub sockets. Hook and plugin emitters of resumed tmux sessions,
// which never learn a per-run socket path, deliver here; every hello is
// bound to the CONNECTED session of a registered ACTIVE instance, and
// every head is committed to the event store before its ack leaves.
func startNativeEventServer(
	ctx context.Context,
	cfg config.Config,
	dirs xdg.Dirs,
	store *eventstore.Store,
	instances orchestrator.TmuxInstanceRegistry,
) (io.Closer, error) {
	socketDir := dirs.Runtime
	if cfg.NodeSocket != nil {
		socketDir = filepath.Dir(cfg.NodeSocket.Path)
	}
	queues := &nativeevents.SessionQueueProvider{NodeID: cfg.NodeID}
	server, err := nativeevents.ListenNode(ctx, nativeevents.NodeOptions{
		SocketDir: socketDir,
		Resolve: func(ctx context.Context, pid int) (protocol.AgentID, protocol.OpaqueID, error) {
			return registry.ResolveAgentSessionPID(ctx, store.DB(), pid)
		},
		Instances: nativeevents.InstanceRegistry{Registry: instances},
		Queues:    queues,
		Store:     store,
		Inspector: orchestrator.NewNodePeerProcessInspector(""),
	})
	if err != nil {
		return nil, fmt.Errorf("listen node native entry: %w", err)
	}
	return server, nil
}
