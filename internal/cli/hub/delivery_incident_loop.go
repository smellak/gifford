package hub

import (
	"context"
	"io"
	"time"

	"github.com/smellak/gifford/internal/daemon/agentlink"
)

const deliveryIncidentEvery = time.Minute

type deliveryIncidentRunner interface {
	RunOnce(context.Context) (int, error)
}

type ownedDeliveryIncidentLoop struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func startDeliveryIncidentLoop(
	parent context.Context,
	errW io.Writer,
	worker deliveryIncidentRunner,
	clk agentlink.Clock,
) *ownedDeliveryIncidentLoop {
	ctx, cancel := context.WithCancel(parent)
	loop := &ownedDeliveryIncidentLoop{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(loop.done)
		for ctx.Err() == nil {
			if _, err := worker.RunOnce(ctx); err != nil && ctx.Err() == nil {
				sayf(errW, "delivery incident worker: %v", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-clk.After(deliveryIncidentEvery):
			}
		}
	}()
	return loop
}

func (l *ownedDeliveryIncidentLoop) stop() {
	l.cancel()
	<-l.done
}
