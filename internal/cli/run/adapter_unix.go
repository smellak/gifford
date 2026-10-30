//go:build unix

package run

import (
	"github.com/smellak/gifford/internal/adapters"
)

// adapterRun connects one verified native adapter to the run lifecycle. The
// PTY session remains the input guard and delivery path throughout. Changes
// are drained so liveness degradation is local and never blocks the client;
// propagation to the registered node is deliberately outside this phase.
type adapterRun struct {
	client *adapters.Client
}

func (a *adapterRun) start() <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range a.client.Changes() {
		}
	}()
	return done
}

func (a *adapterRun) stop(done <-chan struct{}) {
	_ = a.client.Close()
	<-done
}
