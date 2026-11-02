//go:build linux

package adapter

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/nativeevents"
	"github.com/smellak/gifford/internal/orchestrator"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

type museBurstInspector struct{ tree orchestrator.ProcessTree }

func (i museBurstInspector) Inspect(protocol.ProcessIdentity) (orchestrator.ProcessTree, error) {
	return i.tree, nil
}

type museBurstIncompleteInspector struct {
	tree  orchestrator.ProcessTree
	total int

	mu      sync.Mutex
	calls   int
	started chan struct{}
}

func (i *museBurstIncompleteInspector) Inspect(
	protocol.ProcessIdentity,
) (orchestrator.ProcessTree, error) {
	i.mu.Lock()
	call := i.calls
	i.calls++
	if i.calls == i.total {
		close(i.started)
	}
	i.mu.Unlock()
	<-i.started
	if call < 3 {
		return orchestrator.ProcessTree{}, orchestrator.ErrProcessObservationIncomplete
	}
	return i.tree, nil
}

type museBurstLifecycle struct {
	activated atomic.Int64
	failed    atomic.Int64
}

func serveMuseObservationRetry(
	listener net.Listener,
	hello nativeevents.Hello,
	event nativeevents.Event,
) (<-chan nativeevents.Event, <-chan error) {
	received := make(chan nativeevents.Event, 1)
	finished := make(chan error, 1)
	go func() {
		for attempt := range 2 {
			conn, err := listener.Accept()
			if err != nil {
				finished <- err
				return
			}
			message, err := nativeevents.ReadMessage(conn)
			if err != nil {
				_ = conn.Close()
				finished <- err
				return
			}
			gotHello, ok := message.(nativeevents.Hello)
			if !ok || gotHello != hello {
				_ = conn.Close()
				finished <- fmt.Errorf("unexpected hello on attempt %d", attempt+1)
				return
			}
			if attempt == 0 {
				err = nativeevents.WriteMessage(conn, nativeevents.HelloResult{
					Operation: nativeevents.OperationHelloResult,
					Reason:    nativeevents.HelloRejectObservationIncomplete,
				})
				_ = conn.Close()
				if err != nil {
					finished <- err
					return
				}
				continue
			}
			if err = nativeevents.WriteMessage(conn, nativeevents.HelloResult{
				Operation: nativeevents.OperationHelloResult, Accepted: true,
			}); err != nil {
				_ = conn.Close()
				finished <- err
				return
			}
			message, err = nativeevents.ReadMessage(conn)
			if err != nil {
				_ = conn.Close()
				finished <- err
				return
			}
			gotEvent, ok := message.(nativeevents.Event)
			if !ok || gotEvent != event {
				_ = conn.Close()
				finished <- fmt.Errorf("retry changed the event")
				return
			}
			received <- gotEvent
			err = nativeevents.WriteMessage(conn, nativeevents.Ack{
				Operation: nativeevents.OperationAck,
				Sequence:  gotEvent.Sequence, EmitterEventID: gotEvent.EmitterEventID,
			})
			_ = conn.Close()
			finished <- err
			return
		}
	}()
	return received, finished
}

func TestEmitRetriesObservationIncompleteOnFreshConnection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "native.sock")
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "unix", path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	hello, event := museFixtureMessage(t)
	received, finished := serveMuseObservationRetry(listener, hello, event)
	ctx, cancel := context.WithTimeout(t.Context(), EmitTimeout)
	defer cancel()

	require.NoError(t, emit(ctx, path, hello, event))
	require.Equal(t, event, <-received)
	require.NoError(t, <-finished)
}

func TestMuseFallbackRetriesObservationIncompleteWithoutMarker(t *testing.T) {
	fixture := newMuseLinuxFixture(t)
	hello, event := museFixtureMessage(t)
	received, finished := serveMuseObservationRetry(fixture.nativeListener, hello, event)
	ctx, cancel := context.WithTimeout(t.Context(), EmitTimeout)
	defer cancel()

	require.NoError(t, fixture.resolver.emit(ctx, hello, event))
	require.Equal(t, event, <-received)
	require.NoError(t, <-finished)
	require.NoDirExists(t, filepath.Join(fixture.runtime, museDiagnosticDirectory))
}

func (l *museBurstLifecycle) NativeEmitterActivated(nativeevents.EmitterType) error {
	l.activated.Add(1)
	return nil
}

func (l *museBurstLifecycle) NativeEmitterFailed(nativeevents.EmitterType) {
	l.failed.Add(1)
}

func TestMuseConcurrentOneShotHooksReachDurableAck(t *testing.T) {
	t.Parallel()
	const total = 25

	process, err := registry.CurrentProcess()
	require.NoError(t, err)
	root := protocol.ProcessIdentity{
		PID: process.PID + 1_000_000, ProcessStartTime: 10,
		UID: process.UID, GID: process.GID,
	}
	rootToken := orchestrator.ProcessToken{PID: root.PID, StartTime: root.ProcessStartTime}
	harnessToken := orchestrator.ProcessToken{PID: root.PID + 1, StartTime: 20}
	peerToken := orchestrator.ProcessToken{
		PID: process.PID, StartTime: process.ProcessStartTime,
	}
	inspector := museBurstInspector{tree: orchestrator.ProcessTree{
		Root: rootToken,
		Descendants: []orchestrator.ProcessNode{
			{Token: harnessToken, Parent: rootToken},
			{Token: peerToken, Parent: harnessToken},
		},
	}}
	queue, err := nativeevents.NewQueue(nativeevents.Lineage{
		NodeID: "node-burst", SessionID: "session-burst", AgentID: "agent-burst",
		Harness: protocol.HarnessMuseCode, HarnessVersion: museHarnessVersion,
		AdapterType: protocol.AdapterTypeGenericPTY,
	})
	require.NoError(t, err)
	lifecycle := &museBurstLifecycle{}
	server, err := nativeevents.Listen(t.Context(), nativeevents.Options{
		RuntimeDir: t.TempDir(), Root: root, Inspector: inspector,
		Queue: queue, Lifecycle: lifecycle,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Close()) })

	drainCtx, cancelDrain := context.WithCancel(t.Context())
	t.Cleanup(cancelDrain)
	var stored atomic.Int64
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for stored.Load() < total {
			select {
			case <-drainCtx.Done():
				return
			case <-queue.HeadChanged():
			}
			for {
				head, ok := queue.Head()
				if !ok {
					break
				}
				queue.Settle(protocol.EmitEventResult{
					EventID: head.EventID, Status: protocol.EmitEventStatusStored,
				})
				stored.Add(1)
			}
		}
	}()

	hello := nativeevents.Hello{
		Operation: nativeevents.OperationHello,
		Harness:   protocol.HarnessMuseCode, HarnessVersion: museHarnessVersion,
		EmitterType: nativeevents.EmitterTypeHook, EmitterVersion: museEmitterVersion,
		SchemaVersion: nativeevents.SchemaVersion,
	}
	start := make(chan struct{})
	errorsByHook := make(chan error, total)
	var hooks sync.WaitGroup
	for index := range total {
		hooks.Add(1)
		go func(index int) {
			defer hooks.Done()
			<-start
			ctx, cancel := context.WithTimeout(t.Context(), EmitTimeout)
			defer cancel()
			conn, dialErr := (&net.Dialer{}).DialContext(ctx, "unix", server.Path())
			if dialErr != nil {
				errorsByHook <- dialErr
				return
			}
			defer func() { _ = conn.Close() }()
			event := nativeevents.Event{
				Operation: nativeevents.OperationEvent, Sequence: 1,
				EmitterEventID: protocol.OpaqueID(fmt.Sprintf("burst-event-%02d", index)),
				Type:           protocol.EventTypeTurnStarted, ObservedAt: time.Now().UnixMilli(),
				SessionRef: "muse-session", TurnRef: protocol.OpaqueID(fmt.Sprintf("turn-%02d", index)),
			}
			switch index % 4 {
			case 1:
				event.Type = protocol.EventTypeSubagentStarted
			case 2:
				event.Type = protocol.EventTypeToolStarted
				event.ToolRef = protocol.OpaqueID(fmt.Sprintf("tool-%02d", index))
			case 3:
				event.Type = protocol.EventTypeTurnStreaming
			}
			errorsByHook <- emitConnected(ctx, conn, hello, event, nil)
		}(index)
	}
	close(start)
	hooks.Wait()
	close(errorsByHook)
	for hookErr := range errorsByHook {
		require.NoError(t, hookErr)
	}

	select {
	case <-drained:
	case <-time.After(time.Second):
		t.Fatal("durable queue did not drain")
	}
	require.Equal(t, int64(total), stored.Load())
	require.Zero(t, queue.Dropped())
	require.Eventually(t, func() bool {
		return lifecycle.activated.Load() == total
	}, time.Second, time.Millisecond)
	require.Never(t, func() bool {
		return lifecycle.failed.Load() != 0
	}, 100*time.Millisecond, time.Millisecond)
}

func TestMuseBurstDropsValidEventsOnIncompleteInspection(t *testing.T) {
	t.Parallel()
	const (
		total    = 5
		accepted = total - 3
	)

	process, err := registry.CurrentProcess()
	require.NoError(t, err)
	root := protocol.ProcessIdentity{
		PID: process.PID + 1_000_000, ProcessStartTime: 10,
		UID: process.UID, GID: process.GID,
	}
	rootToken := orchestrator.ProcessToken{PID: root.PID, StartTime: root.ProcessStartTime}
	harnessToken := orchestrator.ProcessToken{PID: root.PID + 1, StartTime: 20}
	peerToken := orchestrator.ProcessToken{
		PID: process.PID, StartTime: process.ProcessStartTime,
	}
	inspector := &museBurstIncompleteInspector{
		total: total, started: make(chan struct{}),
		tree: orchestrator.ProcessTree{
			Root: rootToken,
			Descendants: []orchestrator.ProcessNode{
				{Token: harnessToken, Parent: rootToken},
				{Token: peerToken, Parent: harnessToken},
			},
		},
	}
	queue, err := nativeevents.NewQueue(nativeevents.Lineage{
		NodeID: "node-incomplete", SessionID: "session-incomplete", AgentID: "agent-incomplete",
		Harness: protocol.HarnessMuseCode, HarnessVersion: museHarnessVersion,
		AdapterType: protocol.AdapterTypeGenericPTY,
	})
	require.NoError(t, err)
	server, err := nativeevents.Listen(t.Context(), nativeevents.Options{
		RuntimeDir: t.TempDir(), Root: root, Inspector: inspector, Queue: queue,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Close()) })

	drainCtx, cancelDrain := context.WithCancel(t.Context())
	t.Cleanup(cancelDrain)
	var stored atomic.Int64
	go func() {
		for stored.Load() < accepted {
			select {
			case <-drainCtx.Done():
				return
			case <-queue.HeadChanged():
			}
			for {
				head, ok := queue.Head()
				if !ok {
					break
				}
				queue.Settle(protocol.EmitEventResult{
					EventID: head.EventID, Status: protocol.EmitEventStatusStored,
				})
				stored.Add(1)
			}
		}
	}()

	hello := nativeevents.Hello{
		Operation: nativeevents.OperationHello,
		Harness:   protocol.HarnessMuseCode, HarnessVersion: museHarnessVersion,
		EmitterType: nativeevents.EmitterTypeHook, EmitterVersion: museEmitterVersion,
		SchemaVersion: nativeevents.SchemaVersion,
	}
	start := make(chan struct{})
	errorsByHook := make(chan error, total)
	var hooks sync.WaitGroup
	for index := range total {
		hooks.Add(1)
		go func(index int) {
			defer hooks.Done()
			<-start
			ctx, cancel := context.WithTimeout(t.Context(), EmitTimeout)
			defer cancel()
			conn, dialErr := (&net.Dialer{}).DialContext(ctx, "unix", server.Path())
			if dialErr != nil {
				errorsByHook <- dialErr
				return
			}
			defer func() { _ = conn.Close() }()
			event := nativeevents.Event{
				Operation: nativeevents.OperationEvent, Sequence: 1,
				EmitterEventID: protocol.OpaqueID(fmt.Sprintf("incomplete-event-%02d", index)),
				Type:           protocol.EventTypeTurnStarted, ObservedAt: time.Now().UnixMilli(),
				SessionRef: "muse-session", TurnRef: protocol.OpaqueID(fmt.Sprintf("turn-%02d", index)),
			}
			if index == 2 {
				event.Type = protocol.EventTypeTurnStreaming
			} else if index >= 3 {
				event.Type = protocol.EventTypeSubagentStarted
			}
			errorsByHook <- emitConnected(ctx, conn, hello, event, nil)
		}(index)
	}
	close(start)
	hooks.Wait()
	close(errorsByHook)

	var rejected int
	for hookErr := range errorsByHook {
		if hookErr == nil {
			continue
		}
		require.EqualError(t, hookErr, "adapter emit rejected")
		rejected++
	}
	require.Equal(t, 3, rejected)
	require.Eventually(t, func() bool {
		return stored.Load() == accepted
	}, time.Second, time.Millisecond)
	require.Zero(t, queue.Dropped())
}
