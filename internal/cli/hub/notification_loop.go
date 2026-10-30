package hub

import "context"

type notificationRunner interface {
	Run(context.Context)
}

type ownedNotificationLoop struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func startNotificationLoop(parent context.Context, service notificationRunner) *ownedNotificationLoop {
	ctx, cancel := context.WithCancel(parent)
	loop := &ownedNotificationLoop{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(loop.done)
		service.Run(ctx)
	}()
	return loop
}

func (l *ownedNotificationLoop) stop() {
	l.cancel()
	<-l.done
}
