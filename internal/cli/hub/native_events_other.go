//go:build !unix

package hub

import (
	"context"
	"io"

	"github.com/smellak/gifford/internal/config"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/orchestrator"
	"github.com/smellak/gifford/internal/xdg"
)

// startNativeEventServer is unavailable where SO_PEERCRED cannot
// attribute a peer: the node entry stays down and hook emitters keep
// their per-run socket only.
func startNativeEventServer(
	_ context.Context,
	_ config.Config,
	_ xdg.Dirs,
	_ *eventstore.Store,
	_ orchestrator.TmuxInstanceRegistry,
) (io.Closer, error) {
	return nil, nil
}
