//go:build !unix

package run

import (
	"context"

	"github.com/smellak/gifford/internal/proxy"
	"github.com/smellak/gifford/internal/xdg"
)

// agentRuntimeDir resolves the XDG runtime dir, or "" when it does
// not resolve.
func agentRuntimeDir() string {
	dirs, err := xdg.Resolve()
	if err != nil {
		return ""
	}
	return dirs.Runtime
}

// runSession runs the harness straight through the proxy core: without
// unix PTYs there is no session wrapper, so the core reports the
// platform as unsupported. --agent still registers when asked, held
// until the command ends.
func runSession(ctx context.Context, p sessionParams) (proxy.Exit, error) {
	if p.Agent != "" {
		harness := p.Harness
		if harness == "" {
			harness = "generic"
		}
		if hub := openAgentSession(ctx, p.Notice, p.Agent, harness, p.Command, agentRuntimeDir()); hub != nil {
			defer func() { _ = hub.Close() }()
		}
	}
	return proxy.Run(ctx, proxy.Options{
		Command:      p.Command,
		In:           p.In,
		Out:          p.Out,
		Notice:       p.Notice,
		DaemonSocket: daemonSocket(),
	})
}
