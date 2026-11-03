package hub

import (
	"context"
	"io"

	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/renotify"
)

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
