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
	"github.com/smellak/gifford/internal/protocol"
)

const adapterEventDigits = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// adapterRun connects one verified native adapter to the run lifecycle. The
// PTY session remains the input guard and delivery path throughout. Its one
// native-to-generic degradation is retained as an immutable envelope so each
// authenticated Core connection can receive the same event.
type adapterRun struct {
	client   *adapters.Client
	clock    clock.Clock
	template protocol.EventEnvelope
	changed  chan struct{}

	mu      sync.RWMutex
	event   protocol.EventEnvelope
	current bool
}

func newAdapterRun(client *adapters.Client, template protocol.EventEnvelope, wallClock clock.Clock) *adapterRun {
	if wallClock == nil {
		wallClock = clock.System{}
	}
	return &adapterRun{
		client: client, clock: wallClock, template: template,
		changed: make(chan struct{}, 1),
	}
}

func (a *adapterRun) start() <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range a.client.Changes() {
			a.retainDegradation()
		}
	}()
	return done
}

func (a *adapterRun) retainDegradation() {
	a.mu.Lock()
	if a.current {
		a.mu.Unlock()
		return
	}
	event := a.template
	event.WallTime = a.clock.Now().UTC()
	a.event, a.current = event, true
	a.mu.Unlock()
	select {
	case a.changed <- struct{}{}:
	default:
	}
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
	_ = a.client.Close()
	<-done
}

// newAdapterEvent prepares every immutable field before native capabilities
// are registered. SessionID and WallTime are bound later to the admitted Hub
// session and the observed degradation, respectively.
func newAdapterEvent(
	nodeID protocol.NodeID,
	native, fallback protocol.CapabilityManifest,
	now time.Time,
) (protocol.EventEnvelope, error) {
	payload, err := json.Marshal(adapters.CapabilitiesChangedEvent{
		EventType: protocol.EventTypeAdapterCapabilitiesChanged,
		Before:    native.Capabilities,
		After:     fallback.Capabilities,
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
		AgentID:          native.AgentID,
		Harness:          native.Harness,
		HarnessVersion:   native.HarnessVersion,
		AdapterType:      native.AdapterType,
		SourceQuality:    protocol.SourceQualityProcessMetrics,
		EventType:        protocol.EventTypeAdapterCapabilitiesChanged,
		Origin:           protocol.OriginSystem,
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
