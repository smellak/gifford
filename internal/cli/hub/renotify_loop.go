package hub

import (
	"context"
	"io"

	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/renotify"
)

// nodeOperator returns the operator agent escalation notices go to. It
// connects to the shared config field once the incident patch lands;
// until then the loop renotices without escalating, quietly.
func nodeOperator() protocol.AgentID {
	return protocol.AgentID("")
}

type ownedRenotifyLoop struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// startRenotifyLoop runs the backup loop in the node until stop. The
// runner carries the loop with real facts and effects; the operator
// binds when its config value arrives.
func startRenotifyLoop(
	parent context.Context,
	errW io.Writer,
	runner renotify.Turner,
	loader renotify.LoadFunc,
	clk agentlink.Clock,
) *ownedRenotifyLoop {
	ctx, cancel := context.WithCancel(parent)
	loop := &ownedRenotifyLoop{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(loop.done)
		(&renotify.Driver{Turner: runner, Loader: loader, Clock: clk, ErrW: errW}).Run(ctx)
	}()
	return loop
}

func (l *ownedRenotifyLoop) stop() {
	l.cancel()
	<-l.done
}
