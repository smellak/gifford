//go:build unix

package run

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/smellak/gifford/internal/adapters"
	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/nativeevents"
	"github.com/smellak/gifford/internal/protocol"
)

const adapterEventDigits = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// adapterRun connects the run-scoped native-event capabilities and an optional
// verified adapter to the run lifecycle. The PTY session remains the input
// guard and delivery path throughout. The first capability change occupies
// legacy sequence zero and is retained for every Core connection; later
// changes join the native durable queue.
type adapterRun struct {
	client  *adapters.Client
	clock   clock.Clock
	nodeID  protocol.NodeID
	session protocol.OpaqueID
	native  protocol.CapabilityManifest
	changed chan struct{}

	mu           sync.RWMutex
	queue        *nativeevents.Queue
	capabilities protocol.Capabilities
	event        protocol.EventEnvelope
	current      bool
}

func newAdapterRun(
	client *adapters.Client,
	nodeID protocol.NodeID,
	sessionID protocol.OpaqueID,
	native, initial protocol.CapabilityManifest,
	wallClock clock.Clock,
) *adapterRun {
	if wallClock == nil {
		wallClock = clock.System{}
	}
	return &adapterRun{
		client: client, clock: wallClock, nodeID: nodeID, session: sessionID,
		native: native, capabilities: initial.Capabilities, changed: make(chan struct{}, 1),
	}
}

func (a *adapterRun) bind(queue *nativeevents.Queue) { a.queue = queue }

func (a *adapterRun) start() <-chan struct{} {
	done := make(chan struct{})
	if a.client == nil {
		close(done)
		return done
	}
	go func() {
		defer close(done)
		for range a.client.Changes() {
			a.retainAdapterDegradation()
		}
	}()
	return done
}

func (a *adapterRun) retainAdapterDegradation() {
	a.mu.Lock()
	fallback := adapters.GenericManifest(
		a.native.AgentID, a.native.Harness, a.native.HarnessVersion,
	).Capabilities
	_ = a.transitionLocked(fallback)
	a.mu.Unlock()
}

// NativeEmitterActivated implements nativeevents.EmitterLifecycle. Only a
// first event on a freshly bound connection calls it.
func (a *adapterRun) NativeEmitterActivated(nativeevents.EmitterType) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	target := adapters.NativeEventCapabilities(a.capabilities, a.native.Capabilities)
	return a.transitionLocked(target)
}

// NativeEmitterFailed removes only lifecycle-event capabilities. Other
// abilities already lost with the adapter heartbeat are never resurrected.
func (a *adapterRun) NativeEmitterFailed(nativeevents.EmitterType) {
	a.mu.Lock()
	defer a.mu.Unlock()
	fallback := adapters.GenericManifest(
		a.native.AgentID, a.native.Harness, a.native.HarnessVersion,
	).Capabilities
	target := adapters.NativeEventCapabilities(a.capabilities, fallback)
	_ = a.transitionLocked(target)
}

func (a *adapterRun) transitionLocked(after protocol.Capabilities) error {
	if after == a.capabilities {
		return nil
	}
	event, err := newAdapterEvent(a.nodeID, a.session, a.native, a.capabilities, after, a.clock.Now())
	if err != nil {
		return err
	}
	if !a.current {
		a.event, a.current = event, true
		select {
		case a.changed <- struct{}{}:
		default:
		}
	} else {
		if a.queue == nil {
			return errCapabilityAuditUnavailable
		}
		receipt, enqueueErr := a.queue.EnqueueCapability(event)
		if enqueueErr != nil {
			return fmt.Errorf("enqueue capability transition: %w", enqueueErr)
		}
		if !receipt.Accepted() {
			return errCapabilityAuditUnavailable
		}
	}
	a.capabilities = after
	return nil
}

// CurrentEvent implements agentsock.EventSource. Payload is cloned so a
// connection cannot mutate the retained bytes another connection will replay.
func (a *adapterRun) CurrentEvent() (protocol.EventEnvelope, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	event := a.event
	event.Payload = append(protocol.Payload(nil), event.Payload...)
	return event, a.current
}

// EventChanged implements agentsock.EventSource.
func (a *adapterRun) EventChanged() <-chan struct{} { return a.changed }

func (a *adapterRun) stop(done <-chan struct{}) {
	if a.client != nil {
		_ = a.client.Close()
	}
	<-done
}

// newAdapterEvent constructs one immutable, canonical capability transition.
func newAdapterEvent(
	nodeID protocol.NodeID,
	sessionID protocol.OpaqueID,
	native protocol.CapabilityManifest,
	before, after protocol.Capabilities,
	now time.Time,
) (protocol.EventEnvelope, error) {
	payload, err := json.Marshal(adapters.CapabilitiesChangedEvent{
		EventType: protocol.EventTypeAdapterCapabilitiesChanged,
		Before:    before,
		After:     after,
	})
	if err != nil {
		return protocol.EventEnvelope{}, fmt.Errorf("marshal capability event: %w", err)
	}
	eventID, err := mintAdapterEventID(now)
	if err != nil {
		return protocol.EventEnvelope{}, err
	}
	return protocol.EventEnvelope{
		EventID:          eventID,
		SchemaVersion:    protocol.SchemaVersion,
		NodeID:           nodeID,
		SessionID:        sessionID,
		AgentID:          native.AgentID,
		Harness:          native.Harness,
		HarnessVersion:   native.HarnessVersion,
		AdapterType:      native.AdapterType,
		SourceQuality:    protocol.SourceQualityProcessMetrics,
		EventType:        protocol.EventTypeAdapterCapabilitiesChanged,
		Origin:           protocol.OriginSystem,
		WallTime:         now.UTC(),
		Payload:          protocol.Payload(payload),
		Sensitivity:      protocol.SensitivityNormal,
		RedactionApplied: false,
	}, nil
}

// mintAdapterEventID returns a time-ordered Crockford ULID with cryptographic
// entropy. The ID is minted once and retained unchanged for every replay.
func mintAdapterEventID(now time.Time) (protocol.ULID, error) {
	millis := now.UnixMilli()
	if millis < 0 || millis >= 1<<48 {
		return "", fmt.Errorf("adapter event time outside ULID range")
	}
	var raw [16]byte
	stamp := big.NewInt(millis).Bytes()
	copy(raw[6-len(stamp):6], stamp)
	if _, err := rand.Read(raw[6:]); err != nil {
		return "", fmt.Errorf("read adapter event entropy: %w", err)
	}

	value := new(big.Int).SetBytes(raw[:])
	base := big.NewInt(32)
	var remainder big.Int
	var encoded [26]byte
	for i := len(encoded) - 1; i >= 0; i-- {
		value.QuoRem(value, base, &remainder)
		encoded[i] = adapterEventDigits[remainder.Int64()]
	}
	return protocol.ULID(encoded[:]), nil
}
