package nativeevents

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/smellak/gifford/internal/protocol"
)

// MaxQueuedEvents is the bounded native-event backlog of one run.
const MaxQueuedEvents = 256

var (
	// ErrInvalidLineage means a queue cannot construct valid session events.
	ErrInvalidLineage = errors.New("nativeevents: invalid durable lineage")
	// ErrConflict means an emitter id was reused for different content.
	ErrConflict = errors.New("nativeevents: conflicting event replay")
)

// Lineage is the authenticated run identity copied to every native event.
type Lineage struct {
	NodeID         protocol.NodeID
	SessionID      protocol.OpaqueID
	AgentID        protocol.AgentID
	Harness        protocol.Harness
	HarnessVersion string
	AdapterType    protocol.AdapterType
}

// Queue retains native events until the agent socket gets a terminal Core
// result. Its public methods implement agentsock.DurableEvents without an
// import dependency on that package.
type Queue struct {
	lineage Lineage
	changed chan struct{}

	mu           sync.Mutex
	queued       []*queuedEvent
	byEmitterID  map[protocol.OpaqueID]*queuedEvent
	nextSequence int64
	dropped      int64
}

type queuedEvent struct {
	envelope protocol.EventEnvelope
	content  Event
	emitter  EmitterType
	done     chan struct{}
	stored   bool
	closed   bool
}

// Receipt waits for the durable result of one accepted event. A zero receipt
// is a bounded-queue drop and never becomes an acknowledgement.
type Receipt struct{ event *queuedEvent }

// Accepted reports whether the event entered the bounded queue or matched an
// earlier session event.
func (r Receipt) Accepted() bool { return r.event != nil }

// Wait returns true only after the Core stored this event (or reported it had
// already stored the same immutable envelope).
func (r Receipt) Wait(ctx context.Context) bool {
	if r.event == nil {
		return false
	}
	select {
	case <-ctx.Done():
		return false
	case <-r.event.done:
		return r.event.stored
	}
}

// NewQueue constructs an empty native-event queue.
func NewQueue(lineage Lineage) (*Queue, error) {
	if !lineage.NodeID.Valid() || !lineage.SessionID.Valid() ||
		!lineage.AgentID.Valid() || !lineage.Harness.Valid() ||
		!validHarnessVersion(lineage.HarnessVersion) || !lineage.AdapterType.Valid() {
		return nil, ErrInvalidLineage
	}
	return &Queue{
		lineage: lineage, changed: make(chan struct{}, 1),
		byEmitterID: make(map[protocol.OpaqueID]*queuedEvent),
	}, nil
}

// Submit validates and queues one wire event without waiting for storage. An
// exact emitter-id replay shares the original receipt; different content is a
// conflict. A full queue consumes one session sequence (leaving a durable gap)
// and returns an unaccepted receipt.
func (q *Queue) Submit(emitter EmitterType, event Event) (Receipt, error) {
	if q == nil || !emitter.Valid() || event.Validate() != nil {
		return Receipt{}, ErrInvalidMessage
	}
	content := eventContent(event)
	q.mu.Lock()
	defer q.mu.Unlock()
	if existing := q.byEmitterID[event.EmitterEventID]; existing != nil {
		if existing.emitter != emitter || existing.content != content {
			return Receipt{}, ErrConflict
		}
		return Receipt{event: existing}, nil
	}
	sequence := q.nextSequence
	q.nextSequence++
	if len(q.queued) >= MaxQueuedEvents {
		q.dropped++
		return Receipt{}, nil
	}
	envelope, err := q.build(emitter, event, sequence)
	if err != nil {
		return Receipt{}, err
	}
	entry := &queuedEvent{
		envelope: envelope, content: content, emitter: emitter, done: make(chan struct{}),
	}
	q.queued = append(q.queued, entry)
	q.byEmitterID[event.EmitterEventID] = entry
	select {
	case q.changed <- struct{}{}:
	default:
	}
	return Receipt{event: entry}, nil
}

func eventContent(event Event) Event {
	event.Operation = ""
	event.Sequence = 0
	event.EmitterEventID = ""
	return event
}

func (q *Queue) build(
	emitter EmitterType,
	event Event,
	sequence int64,
) (protocol.EventEnvelope, error) {
	payload, err := json.Marshal(protocol.NativeDurableEventPayload{
		EmitterEventID: event.EmitterEventID,
		SessionRef:     event.SessionRef,
		Result:         event.Result,
		ToolKind:       event.ToolKind,
	})
	if err != nil {
		return protocol.EventEnvelope{}, fmt.Errorf("nativeevents: encode durable payload: %w", err)
	}
	eventID, err := mintEventID(time.UnixMilli(event.ObservedAt).UTC())
	if err != nil {
		return protocol.EventEnvelope{}, err
	}
	quality := protocol.SourceQualityNativeHook
	if emitter == EmitterTypePlugin {
		quality = protocol.SourceQualityNativeStructured
	}
	l := q.lineage
	envelope := protocol.EventEnvelope{
		EventID: eventID, SchemaVersion: protocol.SchemaVersion, NodeID: l.NodeID,
		SessionID: l.SessionID, AgentID: l.AgentID,
		Harness: l.Harness, HarnessVersion: l.HarnessVersion, AdapterType: l.AdapterType,
		SourceQuality: quality, EventType: event.Type,
		Stream: protocol.StreamNativeEvent, Origin: protocol.OriginHarness,
		WallTime: time.UnixMilli(event.ObservedAt).UTC(), Sequence: sequence,
		TurnID: event.TurnRef, ToolCallID: event.ToolRef, CorrelationID: event.RequestRef,
		Payload: payload, Sensitivity: protocol.SensitivityNormal,
	}
	if err := protocol.ValidateNativeDurableEvent(envelope); err != nil {
		return protocol.EventEnvelope{}, fmt.Errorf("nativeevents: build durable event: %w", err)
	}
	return envelope, nil
}

// Head returns a clone of the oldest unsettled envelope.
func (q *Queue) Head() (protocol.EventEnvelope, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.queued) == 0 {
		return protocol.EventEnvelope{}, false
	}
	event := q.queued[0].envelope
	event.Payload = append(protocol.Payload(nil), event.Payload...)
	return event, true
}

// HeadChanged signals a newly queued event.
func (q *Queue) HeadChanged() <-chan struct{} { return q.changed }

// Sent records no separate state: the immutable head remains until Settle.
func (q *Queue) Sent(protocol.ULID) {}

// Settle retires the head and releases its receipt. Only durable success
// produces an acknowledgement.
func (q *Queue) Settle(result protocol.EmitEventResult) {
	stored := result.Status == protocol.EmitEventStatusStored ||
		result.Status == protocol.EmitEventStatusAlreadyStored
	q.finish(result.EventID, stored)
}

// Refuse retires a head the agent socket could not write or validate.
func (q *Queue) Refuse(id protocol.ULID) { q.finish(id, false) }

func (q *Queue) finish(id protocol.ULID, stored bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.queued) == 0 || q.queued[0].envelope.EventID != id {
		return
	}
	entry := q.queued[0]
	q.queued[0] = nil
	q.queued = q.queued[1:]
	if !entry.closed {
		entry.stored = stored
		entry.closed = true
		close(entry.done)
	}
}

// Dropped reports the number of events that encountered a full queue.
func (q *Queue) Dropped() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.dropped
}

const eventIDDigits = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

func mintEventID(now time.Time) (protocol.ULID, error) {
	millis := now.UnixMilli()
	if millis < 0 || millis >= 1<<48 {
		return "", errors.New("nativeevents: event time outside ULID range")
	}
	var raw [16]byte
	stamp := big.NewInt(millis).Bytes()
	copy(raw[6-len(stamp):6], stamp)
	if _, err := rand.Read(raw[6:]); err != nil {
		return "", fmt.Errorf("nativeevents: read event entropy: %w", err)
	}
	value := new(big.Int).SetBytes(raw[:])
	base := big.NewInt(32)
	var remainder big.Int
	var encoded [26]byte
	for i := len(encoded) - 1; i >= 0; i-- {
		value.QuoRem(value, base, &remainder)
		encoded[i] = eventIDDigits[remainder.Int64()]
	}
	return protocol.ULID(encoded[:]), nil
}
