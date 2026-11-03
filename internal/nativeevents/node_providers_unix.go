//go:build unix

package nativeevents

import (
	"context"
	"fmt"
	"sync"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/tmuxinstance"
)

// RegisteredInstances is the narrow registry surface the node entry
// needs: the ACTIVE row of one agent. Both *tmuxinstance.Registry and
// the orchestrator's delivery interface satisfy it.
type RegisteredInstances interface {
	Lookup(context.Context, protocol.AgentID) (tmuxinstance.Instance, error)
}

// InstanceRegistry adapts a tmux instance registry to the node entry's
// InstanceLookup. Lookup returns the ACTIVE row or ErrNotRegistered, so
// both the unknown agent and the invalidated instance refuse the hello
// with the same closed reason.
type InstanceRegistry struct {
	Registry RegisteredInstances
}

// LookupNodeInstance implements InstanceLookup.
func (a InstanceRegistry) LookupNodeInstance(ctx context.Context, agent protocol.AgentID) (NodeInstance, error) {
	instance, err := a.Registry.Lookup(ctx, agent)
	if err != nil {
		return NodeInstance{}, fmt.Errorf("lookup node instance: %w", err)
	}
	return NodeInstance{
		Agent:   instance.AgentID,
		Session: instance.SessionID,
		Harness: protocol.ProcessIdentity{
			PID: instance.HarnessPID, ProcessStartTime: instance.HarnessStart,
			UID: int64(instance.AgentUID),
		},
		HarnessType:    instance.Harness,
		HarnessVersion: instance.HarnessVersion,
		Active:         instance.State == tmuxinstance.StateActive,
	}, nil
}

// SessionQueueProvider shares one durable queue per session so sequences
// and emitter dedup stay stable across the session's connections.
type SessionQueueProvider struct {
	NodeID protocol.NodeID

	mu     sync.Mutex
	queues map[protocol.OpaqueID]*Queue
}

// QueueForSession implements SessionQueues.
func (p *SessionQueueProvider) QueueForSession(_ context.Context, agent protocol.AgentID, session protocol.OpaqueID, instance NodeInstance) (*Queue, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if queue, ok := p.queues[session]; ok {
		return queue, nil
	}
	queue, err := NewQueue(Lineage{
		NodeID: p.NodeID, SessionID: session, AgentID: agent,
		Harness: instance.HarnessType, HarnessVersion: instance.HarnessVersion,
		AdapterType: protocol.AdapterTypeNativeHook,
	})
	if err != nil {
		return nil, err
	}
	if p.queues == nil {
		p.queues = make(map[protocol.OpaqueID]*Queue)
	}
	p.queues[session] = queue
	return queue, nil
}
