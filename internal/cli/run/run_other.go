//go:build !unix

package run

import (
	"context"

	"github.com/smellak/gifford/internal/proxy"
)

// runSession runs the harness straight through the proxy core: without
// unix PTYs there is no session wrapper, so the core reports the
// platform as unsupported.
func runSession(ctx context.Context, p sessionParams) (proxy.Exit, error) {
	return proxy.Run(ctx, proxy.Options{
		Command:      p.Command,
		In:           p.In,
		Out:          p.Out,
		Notice:       p.Notice,
		DaemonSocket: daemonSocket(),
	})
}
