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
	server, err := nativeevents.Listen(ctx, nativeevents.Options{
		RuntimeDir: run.runtimeDir, Root: *run.registration.Process, Queue: queue,
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
