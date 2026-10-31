package proxies

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

// DurableEventStore turns one Buffer receipt into the per-event result used by
// the agent socket. Exact event-id replay is success; any different envelope
// colliding by id or session sequence is terminal conflict.
type DurableEventStore struct {
	store  *eventstore.Store
	buffer *eventstore.Buffer

	mu       sync.Mutex
	sessions map[protocol.OpaqueID]*sync.Mutex
}

// NewDurableEventStore binds receipts and exact-replay reads to one store.
func NewDurableEventStore(
	store *eventstore.Store,
	buffer *eventstore.Buffer,
) (*DurableEventStore, error) {
	if store == nil || store.DB() == nil || buffer == nil {
		return nil, ErrInvalidOptions
	}
	return &DurableEventStore{
		store: store, buffer: buffer, sessions: make(map[protocol.OpaqueID]*sync.Mutex),
	}, nil
}

// StoreDurableEvent waits for the result of this event only. Transient store
// failures leave the receipt pending while Buffer applies bounded pressure.
func (s *DurableEventStore) StoreDurableEvent(
	ctx context.Context,
	event protocol.EventEnvelope,
) (agentlink.DurableEventReceipt, error) {
	if s == nil || s.store == nil || s.buffer == nil || ctx == nil {
		return agentlink.DurableEventReceipt{}, ErrInvalidOptions
	}
	lock := s.sessionLock(event.SessionID)
	lock.Lock()
	defer lock.Unlock()
	if replay, found, err := s.replayReceipt(ctx, event); err != nil {
		return agentlink.DurableEventReceipt{}, err
	} else if found {
		return replay, nil
	}
	if allowed, err := s.captureEventAllowed(ctx, event); err != nil {
		return agentlink.DurableEventReceipt{}, err
	} else if !allowed {
		return agentlink.DurableEventReceipt{
			Status: protocol.EmitEventStatusRejected,
			Reason: protocol.EmitEventRejectReasonNotAllowed,
		}, nil
	}
	receipt, err := s.buffer.EnqueueWithReceipt(ctx, []protocol.EventEnvelope{event})
	if err != nil {
		return agentlink.DurableEventReceipt{}, fmt.Errorf("enqueue durable event: %w", err)
	}
	var result eventstore.BufferReceipt
	select {
	case <-ctx.Done():
		return agentlink.DurableEventReceipt{}, fmt.Errorf("wait durable event: %w", ctx.Err())
	case received, ok := <-receipt:
		if !ok {
			return agentlink.DurableEventReceipt{}, errors.New("durable event receipt closed without result")
		}
		result = received
	}
	if result.Stored {
		return agentlink.DurableEventReceipt{Status: protocol.EmitEventStatusStored}, nil
	}
	if !errors.Is(result.Err, eventstore.ErrDuplicateEvent) {
		return agentlink.DurableEventReceipt{}, fmt.Errorf("store durable event: %w", result.Err)
	}
	replay, found, err := s.replayReceipt(ctx, event)
	if err != nil {
		return agentlink.DurableEventReceipt{}, err
	}
	if found {
		return replay, nil
	}
	// The conflicting unique key was the session sequence, not this event ID.
	return agentlink.DurableEventReceipt{
		Status: protocol.EmitEventStatusRejected,
		Reason: protocol.EmitEventRejectReasonConflict,
	}, nil
}

type durableCaptureState struct {
	id       protocol.OpaqueID
	hasQuota bool
	hasGap   bool
}

type durableCaptureMarker struct {
	id    protocol.OpaqueID
	quota bool
	gap   bool
	cause protocol.CaptureGapCause
}

func (s *DurableEventStore) captureEventAllowed(
	ctx context.Context,
	event protocol.EventEnvelope,
) (bool, error) {
	marker, relevant, err := decodeDurableCaptureMarker(event)
	if err != nil {
		return false, fmt.Errorf("decode durable capture marker: %w", err)
	}
	if !relevant {
		return true, nil
	}
	state, err := s.readDurableCaptureState(ctx, event.SessionID)
	if err != nil {
		return false, err
	}
	return state.admits(marker), nil
}

func (s *DurableEventStore) readDurableCaptureState(
	ctx context.Context,
	session protocol.OpaqueID,
) (durableCaptureState, error) {
	var state durableCaptureState
	fromSequence := int64(0)
	for {
		events, err := s.store.SessionEvents(ctx, session, fromSequence, eventstore.MaxBatchEvents)
		if err != nil {
			return durableCaptureState{}, fmt.Errorf("read durable capture state: %w", err)
		}
		for i := range events {
			stored := events[i]
			if stored.EventType != protocol.EventTypeCaptureQuotaReached &&
				stored.EventType != protocol.EventTypeCaptureGapDetected {
				continue
			}
			// Bootstrap capture markers have their own lifecycle and limits. Only
			// the pty_screen profile belongs to the general-event source.
			if stored.SourceQuality != protocol.SourceQualityPTYScreen {
				continue
			}
			if err := protocol.ValidateDurableEventEnvelope(stored); err != nil {
				return durableCaptureState{}, fmt.Errorf(
					"validate stored durable capture marker %s: %w", stored.EventID, err,
				)
			}
			marker, relevant, err := decodeDurableCaptureMarker(stored)
			if err != nil || !relevant {
				return durableCaptureState{}, fmt.Errorf(
					"decode stored durable capture marker %s: %w", stored.EventID, err,
				)
			}
			if !state.admits(marker) {
				return durableCaptureState{}, fmt.Errorf(
					"stored durable capture marker %s violates session limits", stored.EventID,
				)
			}
			state.apply(marker)
		}
		if len(events) < eventstore.MaxBatchEvents {
			return state, nil
		}
		last := events[len(events)-1].Sequence
		if last == math.MaxInt64 {
			return state, nil
		}
		fromSequence = last + 1
	}
}

func decodeDurableCaptureMarker(
	event protocol.EventEnvelope,
) (durableCaptureMarker, bool, error) {
	switch event.EventType {
	case protocol.EventTypeCaptureQuotaReached:
		payload, err := protocol.DecodeDurableEventPayload(event.EventType, event.Payload)
		if err != nil {
			return durableCaptureMarker{}, true, fmt.Errorf("decode quota payload: %w", err)
		}
		quota := payload.(*protocol.CaptureQuotaReached)
		return durableCaptureMarker{id: quota.CaptureSessionID, quota: true}, true, nil
	case protocol.EventTypeCaptureGapDetected:
		payload, err := protocol.DecodeDurableEventPayload(event.EventType, event.Payload)
		if err != nil {
			return durableCaptureMarker{}, true, fmt.Errorf("decode gap payload: %w", err)
		}
		gap := payload.(*protocol.CaptureGapDetected)
		return durableCaptureMarker{
			id: gap.CaptureSessionID, gap: true, cause: gap.Cause,
		}, true, nil
	default:
		return durableCaptureMarker{}, false, nil
	}
}

func (s durableCaptureState) admits(marker durableCaptureMarker) bool {
	if s.id != "" && s.id != marker.id {
		return false
	}
	if marker.quota && s.hasQuota || marker.gap && s.hasGap {
		return false
	}
	return !marker.gap || marker.cause != protocol.CaptureGapCauseQuota || s.hasQuota
}

func (s *durableCaptureState) apply(marker durableCaptureMarker) {
	if s.id == "" {
		s.id = marker.id
	}
	s.hasQuota = s.hasQuota || marker.quota
	s.hasGap = s.hasGap || marker.gap
}

func (s *DurableEventStore) sessionLock(session protocol.OpaqueID) *sync.Mutex {
	s.mu.Lock()
	defer s.mu.Unlock()
	lock := s.sessions[session]
	if lock == nil {
		lock = &sync.Mutex{}
		s.sessions[session] = lock
	}
	return lock
}

// replayReceipt checks immutable identity before any new-event limits. An
// exact replay stays successful even after the session has reached a quota or
// other one-shot bound; a reused event id is always a terminal conflict.
func (s *DurableEventStore) replayReceipt(
	ctx context.Context,
	event protocol.EventEnvelope,
) (agentlink.DurableEventReceipt, bool, error) {
	stored, found, err := s.store.EventByID(ctx, event.EventID)
	if err != nil {
		return agentlink.DurableEventReceipt{}, false,
			fmt.Errorf("read durable event replay: %w", err)
	}
	if !found {
		return agentlink.DurableEventReceipt{}, false, nil
	}
	if protocol.EqualEventEnvelopes(stored, event) {
		return agentlink.DurableEventReceipt{
			Status: protocol.EmitEventStatusAlreadyStored,
		}, true, nil
	}
	return agentlink.DurableEventReceipt{
		Status: protocol.EmitEventStatusRejected,
		Reason: protocol.EmitEventRejectReasonConflict,
	}, true, nil
}
