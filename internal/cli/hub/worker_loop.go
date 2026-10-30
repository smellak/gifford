package hub

import (
	"context"
	"io"
	"time"

	"github.com/smellak/gifford/internal/daemon/agentlink"
)

const workerEvery = 500 * time.Millisecond

type queueWorker interface {
	RunOnce(context.Context) (int, error)
}

type ownedWorkerLoop struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func startWorkerLoop(
	parent context.Context,
	errW io.Writer,
	worker queueWorker,
	clk agentlink.Clock,
) *ownedWorkerLoop {
	ctx, cancel := context.WithCancel(parent)
	loop := &ownedWorkerLoop{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(loop.done)
		runWorkerLoop(ctx, errW, worker, clk)
	}()
	return loop
}

func (l *ownedWorkerLoop) stop() {
	l.cancel()
	<-l.done
}

func runWorkerLoop(
	ctx context.Context,
	errW io.Writer,
	worker queueWorker,
	clk agentlink.Clock,
) {
	for ctx.Err() == nil {
		if _, err := worker.RunOnce(ctx); err != nil && ctx.Err() == nil {
			sayf(errW, "run orchestrator worker: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-clk.After(workerEvery):
		}
	}
}
