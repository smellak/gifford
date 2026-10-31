//go:build linux

package agentlink

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

func TestReviewCoalescingPreservesObservedGenerationHighWater(t *testing.T) {
	for _, tc := range []struct {
		name    string
		advance func(*protocol.StateObservationReport)
	}{
		{"output", func(r *protocol.StateObservationReport) { r.OutputGeneration++ }},
		{"event", func(r *protocol.StateObservationReport) { r.EventGeneration++ }},
		{"guard", func(r *protocol.StateObservationReport) { r.InputGuardVersion++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sink := newObservationSink(true)
			link := startObservedLink(t, sink)
			t.Cleanup(sink.unblock)
			first := linkObservation(1)
			link.proxy.send(t, link.conn, protocol.AgentOpReportStateObservation, first)
			require.Equal(t, first, sink.next(t))

			changed := first
			changed.ObservationID = protocol.OpaqueID("changed-" + tc.name)
			changed.ObservationGeneration = 2
			changed.ObservedAt = first.ObservedAt.Add(time.Second)
			tc.advance(&changed)
			rollback := first
			rollback.ObservationID = protocol.OpaqueID("rollback-" + tc.name)
			rollback.ObservationGeneration = 3
			rollback.ObservedAt = first.ObservedAt.Add(2 * time.Second)
			require.NoError(t, changed.Validate())
			require.NoError(t, rollback.Validate())

			link.proxy.send(t, link.conn, protocol.AgentOpReportStateObservation, changed)
			link.proxy.send(t, link.conn, protocol.AgentOpReportStateObservation, rollback)
			link.waitForFirstHeartbeat(t)
			require.Empty(t, sink.submitted)
			sink.unblock()
			require.Equal(t, changed, sink.next(t),
				"a transport refresh must not erase stronger evidence before the producer sees it")
		})
	}
}
