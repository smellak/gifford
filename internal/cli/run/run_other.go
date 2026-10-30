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
			harness = genericHarness
		}
		version := probeHarnessVersion(ctx, p.Command)
		reg, err := agentRegistration(p.Agent, harness, version)
		if err != nil {
			agentNotice(p.Notice, err)
		} else if registered := openRegisteredAgentSession(ctx, p.Notice, reg, agentRuntimeDir()); registered != nil {
			defer func() { _ = registered.Close() }()
		}
	}
	return proxy.Run(ctx, proxy.Options{
		Command:      p.Command,
		Env:          p.Env,
		In:           p.In,
		Out:          p.Out,
		Notice:       p.Notice,
		DaemonSocket: daemonSocket(),
	})
}
