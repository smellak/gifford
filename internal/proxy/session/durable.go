package session

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/smellak/gifford/internal/protocol"
)

// MaxDurableBytes bounds the durable events waiting for the Core, counted as
// their encoded envelopes. Past it a new event is dropped and counted: the
// events queued stay, the one in flight included, and the dropped one leaves
// a gap in the sequence. Every durable envelope is small and bounded, so this
// also bounds how many wait.
const MaxDurableBytes = 4 << 20

// ErrInvalidLineage means a durable queue was asked for with a node, session,
// agent or harness that no durable event can carry.
var ErrInvalidLineage = errors.New("session: invalid durable event lineage")

// DurableLineage is what every durable event of a session carries besides its
// own fields: the node, and the session, agent and harness the Core
// registered.
type DurableLineage struct {
	NodeID         protocol.NodeID
	SessionID      protocol.OpaqueID
	AgentID        protocol.AgentID
	Harness        protocol.Harness
	HarnessVersion string
	AdapterType    protocol.AdapterType
}

// DurableReport counts what became of a session's durable events, and
// nothing of their content.
type DurableReport struct {
	// Stored counts the events the Core stored, or had stored already.
	Stored int64
	// Rejected counts the events the Core rejected: terminal, never stored.
	Rejected int64
	// Refused counts the events the agent socket refused unwritten.
	Refused int64
	// Dropped counts the events never queued because the queue was full.
	Dropped int64
	// Invalid counts the events of a durable type that did not build as one.
	Invalid int64
	// Pending counts the events queued and never written.
	Pending int64
	// Unconfirmed counts the events written that wait for their result.
	Unconfirmed int64
}

// DurableQueue retains a session's durable events for the agent socket. As
// an EventSink it builds each durable event once, when the session emits it,
// with its own id, and queues it without waiting. As the socket's
// agentsock.DurableEvents it hands out the oldest one until the Core settles
// it.
type DurableQueue struct {
	lineage DurableLineage
	changed chan struct{}

	mu     sync.Mutex
	queued []durableEvent
	bytes  int
	report DurableReport
}

// durableEvent is one queued durable event: its envelope as built, its
// encoded size and whether it was written.
type durableEvent struct {
	envelope protocol.EventEnvelope
	size     int
	sent     bool
}

// NewDurableQueue starts an empty queue for the events of lineage. It refuses
// a lineage with a field that is missing or not valid: then no event is built
// at all.
func NewDurableQueue(lineage DurableLineage) (*DurableQueue, error) {
	if !lineage.NodeID.Valid() || !lineage.SessionID.Valid() || !lineage.AgentID.Valid() ||
		!lineage.Harness.Valid() || !ValidHarnessVersion(lineage.HarnessVersion) ||
		!lineage.AdapterType.Valid() {
		return nil, ErrInvalidLineage
	}
	return &DurableQueue{lineage: lineage, changed: make(chan struct{}, 1)}, nil
}

// Emit builds the durable event of e and queues it. An event of a type that
// is not durable is not the queue's; one that does not build, or that the
// full queue has no room for, is counted and dropped. It never waits.
func (q *DurableQueue) Emit(e Event) {
	envelope, size, durable, err := q.build(e)
	if !durable {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	switch {
	case err != nil:
		q.report.Invalid++
		return
	case q.bytes+size > MaxDurableBytes:
		q.report.Dropped++
		return
	}
	q.queued = append(q.queued, durableEvent{envelope: envelope, size: size})
	q.bytes += size
	select {
	case q.changed <- struct{}{}:
	default:
	}
}

// Head is the oldest event not yet settled, as it was built.
func (q *DurableQueue) Head() (protocol.EventEnvelope, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.queued) == 0 {
		return protocol.EventEnvelope{}, false
	}
	head := q.queued[0].envelope
	head.Payload = append(protocol.Payload(nil), head.Payload...)
	return head, true
}

// HeadChanged signals a newly queued event.
func (q *DurableQueue) HeadChanged() <-chan struct{} { return q.changed }

// Sent marks the head written.
func (q *DurableQueue) Sent(id protocol.ULID) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.queued) > 0 && q.queued[0].envelope.EventID == id {
		q.queued[0].sent = true
	}
}

// Settle takes the head off with the Core's result for it.
func (q *DurableQueue) Settle(result protocol.EmitEventResult) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.takeLocked(result.EventID) {
		return
	}
	if result.Status == protocol.EmitEventStatusRejected {
		q.report.Rejected++
		return
	}
	q.report.Stored++
}

// Refuse takes the head off unwritten.
func (q *DurableQueue) Refuse(id protocol.ULID) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.takeLocked(id) {
		q.report.Refused++
	}
}

// takeLocked takes the head off when it is id. Callers hold mu.
func (q *DurableQueue) takeLocked(id protocol.ULID) bool {
	if len(q.queued) == 0 || q.queued[0].envelope.EventID != id {
		return false
	}
	q.bytes -= q.queued[0].size
	q.queued[0] = durableEvent{}
	q.queued = q.queued[1:]
	return true
}

// Report counts the events so far, the ones still queued included.
func (q *DurableQueue) Report() DurableReport {
	q.mu.Lock()
	defer q.mu.Unlock()
	r := q.report
	for _, e := range q.queued {
		if e.sent {
			r.Unconfirmed++
		} else {
			r.Pending++
		}
	}
	return r
}

// build makes the durable event of e: its closed payload, in an envelope of
// the queue's lineage with a new id, encoded once to size it. durable is
// false for an event that is not of a durable type, and err is set for one
// that is but does not build or validate as one.
func (q *DurableQueue) build(e Event) (protocol.EventEnvelope, int, bool, error) {
	spec, durable := durableSpecs[e.Type]
	if !durable {
		return protocol.EventEnvelope{}, 0, false, nil
	}
	payload, delivery, ok := durablePayload(e.Payload)
	if !ok {
		return protocol.EventEnvelope{}, 0, true, fmt.Errorf("session: no durable payload for %s", e.Type)
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return protocol.EventEnvelope{}, 0, true, fmt.Errorf("session: durable payload: %w", err)
	}
	wall := e.WallTime.UTC()
	id, err := mintDurableEventID(wall)
	if err != nil {
		return protocol.EventEnvelope{}, 0, true, err
	}
	l := q.lineage
	envelope := protocol.EventEnvelope{
		EventID: id, SchemaVersion: protocol.SchemaVersion, NodeID: l.NodeID,
		SessionID: l.SessionID, AgentID: l.AgentID,
		Harness: l.Harness, HarnessVersion: l.HarnessVersion, AdapterType: l.AdapterType,
		SourceQuality: protocol.SourceQualityPTYScreen, EventType: e.Type,
		Direction: spec.Direction, Stream: protocol.StreamPTY, Origin: spec.Origin,
		WallTime: wall, MonotonicNS: e.MonotonicNS, Sequence: e.Sequence,
		Payload: raw, Sensitivity: protocol.SensitivityNormal,
	}
	if spec.EnvelopeDeliveryID {
		envelope.DeliveryID = delivery
	}
	if err := protocol.ValidateDurableEventEnvelope(envelope); err != nil {
		return protocol.EventEnvelope{}, 0, true, fmt.Errorf("session: durable event: %w", err)
	}
	encoded, err := json.Marshal(envelope)
	if err != nil {
		return protocol.EventEnvelope{}, 0, true, fmt.Errorf("session: durable event: %w", err)
	}
	return envelope, len(encoded), true, nil
}

// durableSpecs is the durable event allowlist by type.
var durableSpecs = func() map[protocol.EventType]protocol.DurableEventSpec {
	specs := make(map[protocol.EventType]protocol.DurableEventSpec)
	for _, spec := range protocol.DurableEventSpecs() {
		specs[spec.Type] = spec
	}
	return specs
}()

// durablePayload is the closed durable payload of a session payload, and the
// delivery id the envelope repeats for the delivery steps. It carries no text,
// output, screen or content hash: an io.chunk keeps its two hashes in the
// local recording, since over a harness that echoes the keys they give the
// keys away. A delivery failure carries its fixed code, never the error text.
func durablePayload(payload any) (any, protocol.OpaqueID, bool) {
	switch p := payload.(type) {
	case PTYOpened:
		return protocol.DurablePTYOpened{
			Cols: int64(p.Cols), Rows: int64(p.Rows), Harness: p.Harness, HarnessVersion: p.HarnessVersion,
		}, "", true
	case PTYClosed:
		return protocol.DurablePTYClosed{}, "", true
	case IOChunkEvent:
		return protocol.DurableIOChunk{
			ByteCount: p.ByteCount, ANSIPresent: p.ANSIPresent, RecordingSequence: p.RecordingSequence,
		}, "", true
	case InputKeyChunk:
		return protocol.DurableInputKeyChunk{
			PrintableCount: p.PrintableCount, DraftGeneration: p.Generation, InputGuardVersion: p.GuardVersion,
		}, "", true
	case InputControlKey:
		return protocol.DurableInputControlKey{
			Key: protocol.DurableInputKeyCode(p.Key), Shift: p.Shift, Alt: p.Alt, Ctrl: p.Ctrl,
			DraftGeneration: p.Generation, InputGuardVersion: p.GuardVersion,
		}, "", true
	case InputPaste:
		return protocol.DurableInputPaste{
			SizeBytes: p.SizeBytes, DraftGeneration: p.Generation, InputGuardVersion: p.GuardVersion,
		}, "", true
	case InputSubmit:
		return protocol.DurableInputSubmit{DraftGeneration: p.Generation, InputGuardVersion: p.GuardVersion}, "", true
	case InputResize:
		return protocol.DurableInputResize{
			Cols: int64(p.Cols), Rows: int64(p.Rows), DraftGeneration: p.Generation, InputGuardVersion: p.GuardVersion,
		}, "", true
	case InputDraft:
		return protocol.DurableInputDraft{
			State: protocol.DurableDraftState(p.State), DraftGeneration: p.Generation, InputGuardVersion: p.GuardVersion,
		}, "", true
	case DeliveryPrepared:
		return durableDelivery(p.DeliveryID, p.Generation, p.Version), p.DeliveryID, true
	case DeliveryRevalidated:
		return durableDelivery(p.DeliveryID, p.Generation, p.Version), p.DeliveryID, true
	case DeliveryStarted:
		return durableDelivery(p.DeliveryID, p.Generation, p.Version), p.DeliveryID, true
	case DeliveryCommitted:
		return durableDelivery(p.DeliveryID, p.Generation, p.Version), p.DeliveryID, true
	case DeliveryChunk:
		return protocol.DurableDeliveryChunk{
			DeliveryID: p.DeliveryID, Generation: p.Generation, Version: p.Version, Bytes: p.Bytes,
		}, p.DeliveryID, true
	case DeliveryAborted:
		return protocol.DurableDeliveryAborted{
			DeliveryID: p.DeliveryID, Generation: p.Generation, Version: p.Version,
			Reason: protocol.DurableDeliveryAbortReason(p.Reason),
		}, p.DeliveryID, true
	case DeliveryCancelled:
		return protocol.DurableDeliveryCancelled{
			DeliveryID: p.DeliveryID, Reason: protocol.CancelReason(p.Reason),
		}, p.DeliveryID, true
	case DeliveryFailed:
		return protocol.DurableDeliveryFailed{DeliveryID: p.DeliveryID, Reason: p.Code}, p.DeliveryID, true
	case protocol.CaptureQuotaReached:
		return p, "", true
	case protocol.CaptureGapDetected:
		return p, "", true
	default:
		return nil, "", false
	}
}

func durableDelivery(id protocol.OpaqueID, generation uint64, version int64) protocol.DurableDelivery {
	return protocol.DurableDelivery{DeliveryID: id, Generation: generation, Version: version}
}

// ulidDigits is the Crockford base32 alphabet of a ULID.
const ulidDigits = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// mintDurableEventID returns a time-ordered ULID with cryptographic entropy.
// It is minted once per event, when the event is built, and the event keeps it
// for every write.
func mintDurableEventID(now time.Time) (protocol.ULID, error) {
	millis := now.UnixMilli()
	if millis < 0 || millis >= 1<<48 {
		return "", errors.New("session: durable event time outside ULID range")
	}
	var raw [16]byte
	stamp := big.NewInt(millis).Bytes()
	copy(raw[6-len(stamp):6], stamp)
	if _, err := rand.Read(raw[6:]); err != nil {
		return "", fmt.Errorf("session: durable event entropy: %w", err)
	}
	value := new(big.Int).SetBytes(raw[:])
	base := big.NewInt(32)
	var remainder big.Int
	var encoded [26]byte
	for i := len(encoded) - 1; i >= 0; i-- {
		value.QuoRem(value, base, &remainder)
		encoded[i] = ulidDigits[remainder.Int64()]
	}
	return protocol.ULID(encoded[:]), nil
}
