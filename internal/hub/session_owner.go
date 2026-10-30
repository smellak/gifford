package hub

import (
	"sync"

	"github.com/smellak/gifford/internal/protocol"
)

type sessionOwnerKey struct {
	agent   protocol.AgentID
	session protocol.OpaqueID
}

// sessionOwnerDirectory gives each successful register_agent response one
// connection lease. Re-registering the exact session replaces that lease, so
// a delayed close from the prior transport cannot detach the new owner.
type sessionOwnerDirectory struct {
	mu      sync.RWMutex
	next    uint64
	byKey   map[sessionOwnerKey]uint64
	byLease map[uint64]sessionOwnerKey
}

func (d *sessionOwnerDirectory) bind(agent protocol.AgentID, session protocol.OpaqueID) uint64 {
	if !agent.Valid() || !session.Valid() {
		return 0
	}
	key := sessionOwnerKey{agent: agent, session: session}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.byKey == nil {
		d.byKey = make(map[sessionOwnerKey]uint64)
		d.byLease = make(map[uint64]sessionOwnerKey)
	}
	if prior, ok := d.byKey[key]; ok {
		delete(d.byLease, prior)
	}
	d.next++
	if d.next == 0 {
		d.next++
	}
	d.byKey[key] = d.next
	d.byLease[d.next] = key
	return d.next
}

func (d *sessionOwnerDirectory) current(lease uint64) bool {
	if lease == 0 {
		return false
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	key, ok := d.byLease[lease]
	return ok && d.byKey[key] == lease
}

func (d *sessionOwnerDirectory) release(lease uint64) bool {
	if lease == 0 {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	key, ok := d.byLease[lease]
	if !ok || d.byKey[key] != lease {
		return false
	}
	delete(d.byLease, lease)
	delete(d.byKey, key)
	return true
}
