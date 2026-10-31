package orchestrator

import (
	"sync"

	"github.com/smellak/gifford/internal/protocol"
)

// PersistenceGate is the process-wide automatic-action latch for durable
// write failures. Recovery deliberately requires three facts from one failure
// epoch: a committed DEGRADED snapshot, a later committed HEALTHY snapshot,
// and a successful events-table commit. Their arrival order cannot let stale
// evidence reopen a newer failure.
type PersistenceGate struct {
	mu sync.RWMutex

	epoch             uint64
	blocked           bool
	degradedCommitted bool
	healthyCommitted  bool
	writerRecovered   bool
	changed           chan struct{}
}

type persistenceSnapshotPlan struct {
	epoch         uint64
	active        bool
	forceDegraded bool
}

// NewPersistenceGate returns an initially open latch.
func NewPersistenceGate() *PersistenceGate {
	return &PersistenceGate{changed: make(chan struct{})}
}

// Fail closes the latch and starts a new failure epoch. Callers must invoke it
// once for each transition into a failed durable-writer state, not for every
// retry of the same failure.
func (g *PersistenceGate) Fail() uint64 {
	if g == nil {
		return 0
	}
	g.mu.Lock()
	g.epoch++
	g.blocked = true
	g.degradedCommitted = false
	g.healthyCommitted = false
	g.writerRecovered = false
	epoch := g.epoch
	g.notifyLocked()
	g.mu.Unlock()
	return epoch
}

// Blocked reports whether automatic state consumption is latched closed.
func (g *PersistenceGate) Blocked() bool {
	if g == nil {
		return false
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.blocked
}

// Changed returns a channel closed by the next gate transition. It is a
// coalescing wake-up hint; authoritative state must be read again afterwards.
func (g *PersistenceGate) Changed() <-chan struct{} {
	if g == nil {
		return nil
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.changed
}

// WriterRecovered records the first successful events-table commit after the
// current failure. It cannot reopen the latch by itself: durable DEGRADED and
// later HEALTHY state snapshots from this same epoch are also required.
func (g *PersistenceGate) WriterRecovered() {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.blocked || g.writerRecovered {
		return
	}
	g.writerRecovered = true
	g.maybeOpenLocked()
	g.notifyLocked()
}

func (g *PersistenceGate) snapshotPlan() persistenceSnapshotPlan {
	if g == nil {
		return persistenceSnapshotPlan{}
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	return persistenceSnapshotPlan{
		epoch: g.epoch, active: g.blocked,
		forceDegraded: g.blocked && !g.degradedCommitted,
	}
}

func (g *PersistenceGate) snapshotCommitted(
	plan persistenceSnapshotPlan,
	health protocol.HealthState,
) {
	if g == nil || !plan.active {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.blocked || g.epoch != plan.epoch {
		return
	}
	if plan.forceDegraded {
		if health == protocol.HealthStateDegraded {
			g.degradedCommitted = true
			g.maybeOpenLocked()
			g.notifyLocked()
		}
		return
	}
	if g.degradedCommitted && health == protocol.HealthStateHealthy {
		g.healthyCommitted = true
		g.maybeOpenLocked()
		g.notifyLocked()
	}
}

func (g *PersistenceGate) maybeOpenLocked() {
	if g.degradedCommitted && g.healthyCommitted && g.writerRecovered {
		g.blocked = false
		g.degradedCommitted = false
		g.healthyCommitted = false
		g.writerRecovered = false
	}
}

func (g *PersistenceGate) notifyLocked() {
	close(g.changed)
	g.changed = make(chan struct{})
}
