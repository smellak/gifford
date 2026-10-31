//go:build conformance && linux

package conformance

import (
	"context"
	"errors"
	"testing"

	"github.com/smellak/gifford/internal/protocol"
)

type staticAgentListReader struct {
	result protocol.AgentListResult
	err    error
}

func (reader staticAgentListReader) Call(
	_ context.Context,
	method protocol.ControlMethod,
	_ any,
	result any,
) error {
	if reader.err != nil {
		return reader.err
	}
	if method != protocol.ControlMethodAgentList {
		return errors.New("unexpected control method")
	}
	target, ok := result.(*protocol.AgentListResult)
	if !ok {
		return errors.New("unexpected control result")
	}
	*target = reader.result
	return nil
}

func TestRegisteredEndpointReadFailsClosed(t *testing.T) {
	endpoint := &protocol.RegisteredEndpointProcess{PID: 102, ProcessStartTime: 20}
	row := func(agent protocol.AgentID, state protocol.SessionState, process *protocol.RegisteredEndpointProcess) protocol.AgentListItem {
		return protocol.AgentListItem{
			AgentID: agent, NodeID: "node-conformance", Harness: protocol.HarnessCodex,
			HarnessVersion: "1.0.0", Mode: protocol.AgentRunModeRun,
			SessionState: state, Deliverable: true, RegisteredEndpoint: process,
			Phase: protocol.AgentPhaseStatus{
				Value: protocol.AgentPhaseUnknown, Source: protocol.AgentPhaseSourceNone,
			},
			Channel: protocol.AgentEffectiveChannel{
				TX: true, RX: true, AutoDelivery: true, AutoContinue: true, Mode: protocol.AgentModeFull,
			},
			Queue: protocol.AgentQueueCounts{},
		}
	}
	tests := []struct {
		name   string
		reader agentListReader
		want   bool
	}{
		{
			name: "exact connected row",
			reader: staticAgentListReader{result: protocol.AgentListResult{Agents: []protocol.AgentListItem{
				row(conformanceRecipient, protocol.SessionStateConnected, endpoint),
			}}},
			want: true,
		},
		{
			name: "missing field",
			reader: staticAgentListReader{result: protocol.AgentListResult{Agents: []protocol.AgentListItem{
				row(conformanceRecipient, protocol.SessionStateConnected, nil),
			}}},
		},
		{
			name: "another agent",
			reader: staticAgentListReader{result: protocol.AgentListResult{Agents: []protocol.AgentListItem{
				row("another-agent", protocol.SessionStateConnected, endpoint),
				row(conformanceRecipient, protocol.SessionStateConnected, nil),
			}}},
		},
		{
			name: "disconnected row",
			reader: staticAgentListReader{result: protocol.AgentListResult{Agents: []protocol.AgentListItem{
				row(conformanceRecipient, protocol.SessionStateDisconnected, endpoint),
			}}},
		},
		{
			name: "ambiguous rows",
			reader: staticAgentListReader{result: protocol.AgentListResult{Agents: []protocol.AgentListItem{
				row(conformanceRecipient, protocol.SessionStateConnected, endpoint),
				row(conformanceRecipient, protocol.SessionStateConnected, endpoint),
			}}},
		},
		{
			name:   "failed query",
			reader: staticAgentListReader{err: errors.New("agent.list unavailable")},
		},
		{
			name: "invalid token",
			reader: staticAgentListReader{result: protocol.AgentListResult{Agents: []protocol.AgentListItem{
				row(conformanceRecipient, protocol.SessionStateConnected,
					&protocol.RegisteredEndpointProcess{PID: 0, ProcessStartTime: 20}),
			}}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			process, ok := readRegisteredEndpointProcess(t.Context(), test.reader, conformanceRecipient)
			if ok != test.want {
				t.Fatalf("registered endpoint available = %t, want %t: %+v", ok, test.want, process)
			}
			if ok && process != *endpoint {
				t.Fatalf("registered endpoint = %+v, want %+v", process, *endpoint)
			}
		})
	}
}

func TestRegisteredEndpointDiscountIsExactAndNonRecursive(t *testing.T) {
	const root = 100
	harness := processInfo{pid: 101, ppid: root, startTime: 10, comm: "harness"}
	endpoint := processInfo{pid: 102, ppid: harness.pid, startTime: 20, comm: "gifford"}
	child := processInfo{pid: 103, ppid: endpoint.pid, startTime: 30, comm: "tool"}
	token := protocol.RegisteredEndpointProcess{PID: int64(endpoint.pid), ProcessStartTime: int64(endpoint.startTime)}

	filtered, matches := withoutRegisteredEndpointProcess([]processInfo{harness, endpoint}, token)
	if matches != 1 || !exactHarnessProcesses(root, filtered) {
		t.Fatalf("legitimate endpoint was not discounted exactly once: matches=%d tree=%s",
			matches, processTreeEvidence(filtered))
	}

	tests := []struct {
		name      string
		processes []processInfo
		token     protocol.RegisteredEndpointProcess
		matches   int
	}{
		{
			name: "recycled pid", processes: []processInfo{harness, endpoint},
			token: protocol.RegisteredEndpointProcess{PID: int64(endpoint.pid), ProcessStartTime: 21},
		},
		{
			name: "endpoint child remains", processes: []processInfo{harness, endpoint, child},
			token: token, matches: 1,
		},
		{
			name: "ambiguous duplicate", processes: []processInfo{harness, endpoint, endpoint},
			token: token, matches: 2,
		},
		{
			name: "unrelated token", processes: []processInfo{harness, endpoint},
			token: protocol.RegisteredEndpointProcess{PID: 999, ProcessStartTime: 999},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			filtered, matches := withoutRegisteredEndpointProcess(test.processes, test.token)
			if matches != test.matches {
				t.Fatalf("matches = %d, want %d", matches, test.matches)
			}
			if matches == 1 && exactHarnessProcesses(root, filtered) {
				t.Fatalf("negative tree became exact after discount: %s", processTreeEvidence(filtered))
			}
		})
	}
}
