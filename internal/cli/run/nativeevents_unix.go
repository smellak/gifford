//go:build unix

package run

import (
	"context"
	"fmt"
	"io"

	"github.com/smellak/gifford/internal/nativeevents"
	"github.com/smellak/gifford/internal/protocol"
)

type nativeEventRun struct {
	runtimeDir   string
	registration protocol.RegisterAgent
	session      *registeredAgentSession
	nodeID       protocol.NodeID
	capabilities *adapterRun
}

func openNativeEventEntry(
	ctx context.Context,
	notice io.Writer,
	run nativeEventRun,
) *nativeevents.Server {
	if run.session == nil || run.registration.Process == nil || !run.nodeID.Valid() {
		return nil
	}
	manifest := run.registration.Manifest
	queue, err := nativeevents.NewQueue(nativeevents.Lineage{
		NodeID: run.nodeID, SessionID: run.session.sessionID,
		AgentID: manifest.AgentID, Harness: manifest.Harness,
		HarnessVersion: manifest.HarnessVersion, AdapterType: manifest.AdapterType,
	})
	if err != nil {
		nativeEventNotice(notice, err)
		return nil
	}
	if run.capabilities != nil {
		run.capabilities.bind(queue)
	}
	server, err := nativeevents.Listen(ctx, nativeevents.Options{
		RuntimeDir: run.runtimeDir, Root: *run.registration.Process, Queue: queue,
		Lifecycle: run.capabilities,
	})
	if err != nil {
		nativeEventNotice(notice, err)
		return nil
	}
	return server
}

func nativeEventNotice(notice io.Writer, err error) {
	_, _ = fmt.Fprintf(notice, "gifford: no native event entry for this run: %v\n", err)
}

// nativeObservationSession overlays the durable native-event high-water on
// ordinary PTY observations. Embedding preserves the guard and delivery
// paths; only the observation source is extended.
type nativeObservationSession struct {
	agentSession
	queue   *nativeevents.Queue
	changed chan struct{}
}

func newNativeObservationSession(
	ctx context.Context,
	base agentSession,
	queue *nativeevents.Queue,
) *nativeObservationSession {
	source := &nativeObservationSession{
		agentSession: base, queue: queue, changed: make(chan struct{}, 1),
	}
	go source.forwardChanges(ctx)
	return source
}

func (s *nativeObservationSession) forwardChanges(ctx context.Context) {
	baseChanged := s.agentSession.ObservationChanged()
	storedChanged := s.queue.StoredChanged()
	for {
		select {
		case <-ctx.Done():
			return
		case <-baseChanged:
		case <-storedChanged:
		}
		select {
		case s.changed <- struct{}{}:
		default:
		}
	}
}

func (s *nativeObservationSession) Observe() (protocol.StateObservationReport, bool) {
	report, ok := s.agentSession.Observe()
	if !ok {
		return protocol.StateObservationReport{}, false
	}
	generation, events := s.queue.StoredSnapshot()
	// An emitter timestamp just ahead of the local observation is retained for
	// the next cut instead of producing an invalid report.
	visible := events[:0]
	for _, event := range events {
		if !event.At.After(report.ObservedAt) {
			visible = append(visible, event)
		}
	}
	report.EventGeneration = generation
	report.NativeEvents = append([]protocol.StateObservationEvent{}, visible...)
	return report, true
}

func (s *nativeObservationSession) ObservationChanged() <-chan struct{} { return s.changed }

var _ interface {
	Observe() (protocol.StateObservationReport, bool)
	ObservationChanged() <-chan struct{}
} = (*nativeObservationSession)(nil)
