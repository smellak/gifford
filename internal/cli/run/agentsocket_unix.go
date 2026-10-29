//go:build unix

package run

import (
	"context"
	"fmt"
	"io"

	"github.com/smellak/gifford/internal/agentsock"
)

// agentSocketRun is what the agent socket of a --agent run needs: the
// agent, its harness and measured version, and the runtime directory.
type agentSocketRun struct {
	agent, harness, version string
	runtimeDir              string
}

// agentSession is the session an agent socket serves: the guard it
// reports and the deliveries it runs.
type agentSession interface {
	agentsock.Guard
	SocketDelivery() agentsock.Delivery
}

// openAgentSocket listens on the agent socket of a --agent run, the
// Core's link to this session, at <runtime>/agents/<agent>.sock. It
// greets with the same register_agent as the hub registration, reports
// the session's input guard and runs the Core's deliveries on it. Any
// failure prints one notice line and returns nil: the harness always
// runs, transparent, without a socket.
func openAgentSocket(ctx context.Context, notice io.Writer, a agentSocketRun, s agentSession) *agentsock.Server {
	if a.runtimeDir == "" {
		socketNotice(notice, fmt.Errorf("resolve runtime dir: %w", errNoRuntimeDir))
		return nil
	}
	reg, err := agentRegistration(a.agent, a.harness, a.version)
	if err != nil {
		socketNotice(notice, err)
		return nil
	}
	srv, err := agentsock.Listen(ctx, agentsock.Options{
		RuntimeDir: a.runtimeDir, Register: reg, Guard: s, Delivery: s.SocketDelivery(),
	})
	if err != nil {
		socketNotice(notice, err)
		return nil
	}
	return srv
}

// socketNotice prints the one stderr line saying the run continues
// without an agent socket, in the style of the hub notice.
func socketNotice(notice io.Writer, err error) {
	_, _ = fmt.Fprintf(notice, "gifford: no agent socket for this run: %v\n", err)
}
