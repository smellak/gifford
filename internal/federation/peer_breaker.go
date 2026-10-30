package federation

import (
	"sync"
	"time"

	"github.com/smellak/gifford/internal/protocol"
)

const (
	peerFailureThreshold = 3
	peerBreakerOpenFor   = 30 * time.Second
	peerCircuitOpenError = "CIRCUIT_OPEN"
)

// peerCircuitBreaker isolates repeated delivery failures by destination. Its
// durable effect is recorded on federation_outbox by the replicator; this
// in-memory state only decides when one recovery probe may run.
type peerCircuitBreaker struct {
	mu    sync.Mutex
	peers map[protocol.NodeID]*peerCircuitState
}

type peerCircuitState struct {
	failures  int
	openUntil time.Time
	probing   bool
}

// allow reports whether a request may reach peer now. After the open interval,
// exactly one caller becomes the recovery probe.
func (b *peerCircuitBreaker) allow(
	peer protocol.NodeID,
	now time.Time,
) (allowed bool, probe bool, retryAt time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	state := b.peers[peer]
	if state == nil || state.failures < peerFailureThreshold {
		return true, false, time.Time{}
	}
	if now.Before(state.openUntil) || state.probing {
		return false, false, state.openUntil
	}
	state.probing = true
	return true, true, time.Time{}
}

func (b *peerCircuitBreaker) succeeded(peer protocol.NodeID) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.peers, peer)
}

// failed records one failed request and returns the end of the open interval.
// opened is true only when this failure opens or reopens the circuit.
func (b *peerCircuitBreaker) failed(
	peer protocol.NodeID,
	now time.Time,
	probe bool,
) (retryAt time.Time, opened bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.peers == nil {
		b.peers = make(map[protocol.NodeID]*peerCircuitState)
	}
	state := b.peers[peer]
	if state == nil {
		state = &peerCircuitState{}
		b.peers[peer] = state
	}
	wasOpen := now.Before(state.openUntil)
	if probe {
		state.probing = false
	}
	if state.failures < peerFailureThreshold {
		state.failures++
	}
	if state.failures < peerFailureThreshold {
		return time.Time{}, false
	}
	openUntil := now.Add(peerBreakerOpenFor)
	if openUntil.After(state.openUntil) {
		state.openUntil = openUntil
	}
	return state.openUntil, probe || !wasOpen
}

func (b *peerCircuitBreaker) neutral(peer protocol.NodeID, probe bool) {
	if !probe {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if state := b.peers[peer]; state != nil {
		state.probing = false
	}
}
