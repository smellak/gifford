package hub

import (
	"sync"

	"github.com/smellak/gifford/internal/protocol"
)

// EndpointProcess identifies one process without trusting its executable name
// or a client-supplied role. StartTime prevents a recycled PID from matching.
type EndpointProcess struct {
	PID       int64
	StartTime int64
}

// EndpointIdentity is the authenticated process evidence retained for one
// registered Gifford endpoint. Ancestors runs nearest-parent first through the
// registered session process; none of those ancestors inherits the exception.
type EndpointIdentity struct {
	AgentID   protocol.AgentID
	SessionID protocol.OpaqueID
	Process   EndpointProcess
	Ancestors []EndpointProcess
}

func (i EndpointIdentity) valid() bool {
	if !i.AgentID.Valid() || !i.SessionID.Valid() || i.Process.PID <= 0 || i.Process.StartTime < 0 {
		return false
	}
	for _, ancestor := range i.Ancestors {
		if ancestor.PID <= 0 || ancestor.StartTime < 0 {
			return false
		}
	}
	return true
}

func (i EndpointIdentity) clone() EndpointIdentity {
	i.Ancestors = append([]EndpointProcess(nil), i.Ancestors...)
	return i
}

type endpointKey struct {
	agent   protocol.AgentID
	session protocol.OpaqueID
}

type endpointEntry struct {
	lease    uint64
	identity EndpointIdentity
}

// endpointDirectory keeps at most one live endpoint per agent session. A
// lease makes a replaced connection's later Close harmless to its replacement.
type endpointDirectory struct {
	mu      sync.RWMutex
	next    uint64
	byKey   map[endpointKey]endpointEntry
	byLease map[uint64]endpointKey
}

func (d *endpointDirectory) bind(identity EndpointIdentity) uint64 {
	if !identity.valid() {
		return 0
	}
	key := endpointKey{agent: identity.AgentID, session: identity.SessionID}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.byKey == nil {
		d.byKey = make(map[endpointKey]endpointEntry)
		d.byLease = make(map[uint64]endpointKey)
	}
	if prior, ok := d.byKey[key]; ok {
		delete(d.byLease, prior.lease)
	}
	d.next++
	if d.next == 0 {
		d.next++
	}
	entry := endpointEntry{lease: d.next, identity: identity.clone()}
	d.byKey[key] = entry
	d.byLease[entry.lease] = key
	return entry.lease
}

func (d *endpointDirectory) release(lease uint64) {
	if lease == 0 {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	key, ok := d.byLease[lease]
	if !ok {
		return
	}
	delete(d.byLease, lease)
	if entry, exists := d.byKey[key]; exists && entry.lease == lease {
		delete(d.byKey, key)
	}
}

func (d *endpointDirectory) current(lease uint64) bool {
	if lease == 0 {
		return false
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	key, ok := d.byLease[lease]
	if !ok {
		return false
	}
	entry, ok := d.byKey[key]
	return ok && entry.lease == lease
}

func (d *endpointDirectory) revoke(agent protocol.AgentID, session protocol.OpaqueID) {
	key := endpointKey{agent: agent, session: session}
	d.mu.Lock()
	defer d.mu.Unlock()
	if entry, ok := d.byKey[key]; ok {
		delete(d.byLease, entry.lease)
		delete(d.byKey, key)
	}
}

func (d *endpointDirectory) revokeAgent(agent protocol.AgentID) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for key, entry := range d.byKey {
		if key.agent != agent {
			continue
		}
		delete(d.byLease, entry.lease)
		delete(d.byKey, key)
	}
}

func (d *endpointDirectory) lookup(
	agent protocol.AgentID,
	session protocol.OpaqueID,
) (EndpointIdentity, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	entry, ok := d.byKey[endpointKey{agent: agent, session: session}]
	if !ok {
		return EndpointIdentity{}, false
	}
	return entry.identity.clone(), true
}
