package agentlink

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/smellak/gifford/internal/protocol"
)

type durableEventHooks struct {
	afterResultSend  func(protocol.ULID)
	beforeResultSend func(protocol.ULID)
	beforeOffer      func(protocol.ULID)
	afterOffer       func(protocol.ULID, int64)
	afterOfferReject func(protocol.ULID)
}

func validateDurableEnvelope(
	event protocol.EventEnvelope,
	frame protocol.AgentFrame,
	nodeID protocol.NodeID,
	sessionID protocol.OpaqueID,
	authenticated protocol.CapabilityManifest,
) (protocol.EventEnvelope, error) {
	if frame.ReplyTo != nil {
		return protocol.EventEnvelope{}, errors.New("durable event cannot be a reply")
	}
	if err := validateDurableProfile(event); err != nil {
		return protocol.EventEnvelope{}, fmt.Errorf("validate durable envelope: %w", err)
	}
	if event.NodeID != nodeID || event.SessionID != sessionID ||
		event.AgentID != authenticated.AgentID || event.Harness != authenticated.Harness ||
		event.HarnessVersion != authenticated.HarnessVersion ||
		event.AdapterType != authenticated.AdapterType {
		return protocol.EventEnvelope{}, errors.New("durable envelope does not match authenticated lineage")
	}
	canonical, err := canonicalDurablePayload(event)
	if err != nil {
		return protocol.EventEnvelope{}, fmt.Errorf("canonicalize durable event payload: %w", err)
	}
	event.Payload = canonical
	return event, nil
}

func validateDurableProfile(event protocol.EventEnvelope) error {
	if event.Stream == protocol.StreamNativeEvent {
		if err := protocol.ValidateNativeDurableEvent(event); err != nil {
			return fmt.Errorf("validate native durable event: %w", err)
		}
		return nil
	}
	if err := protocol.ValidateDurableEventEnvelope(event); err != nil {
		return fmt.Errorf("validate durable event: %w", err)
	}
	return nil
}

func canonicalDurablePayload(event protocol.EventEnvelope) (protocol.Payload, error) {
	if event.Stream == protocol.StreamNativeEvent {
		payload, err := protocol.CanonicalNativeDurableEventPayload(event.Payload)
		if err != nil {
			return nil, fmt.Errorf("canonicalize native durable payload: %w", err)
		}
		return payload, nil
	}
	payload, err := protocol.CanonicalDurableEventPayload(event.EventType, event.Payload)
	if err != nil {
		return nil, fmt.Errorf("canonicalize durable payload: %w", err)
	}
	return payload, nil
}

// offerDurableEvent admits one source event at a time. The same immutable
// event may retarget its pending result after reconnect. One successor may
// wait while the current receipt is being written or retired; all other
// in-flight events are wire errors.
func (c *Client) offerDurableEvent(job durableEventJob) error {
	eventID, frameID := job.event.EventID, job.frameID
	if hook := c.opts.durableHooks.beforeOffer; hook != nil {
		hook(eventID)
	}
	c.durableMu.Lock()
	current := c.durableEvent
	if current != nil {
		sameEvent := protocol.EqualEventEnvelopes(current.event, job.event)
		switch {
		case current.generation != job.generation && sameEvent:
			current.generation = job.generation
			current.frameID = job.frameID
			current.suspended = false
			current.receiptState = durableReceiptPending
			current.successor = nil
			if !c.durableRunning {
				select {
				case c.durableWake <- struct{}{}:
				default:
				}
			}
		case current.generation == job.generation && !sameEvent &&
			current.receiptState == durableReceiptWritten:
			// The Proxy can send its next event as soon as it observes the
			// receipt. Queue that event while the worker retires the old job.
			c.durableEvent = &job
		case current.generation == job.generation && !sameEvent &&
			current.receiptState == durableReceiptWriting && current.successor == nil:
			// A successful socket write becomes visible to the Proxy before the
			// writer goroutine can record its completion. Hold one successor in
			// that interval, but do not hand it to storage yet.
			current.successor = &job
		default:
			c.durableMu.Unlock()
			if hook := c.opts.durableHooks.afterOfferReject; hook != nil {
				hook(eventID)
			}
			return ErrUnexpectedFrame
		}
	} else {
		c.durableEvent = &job
		select {
		case c.durableWake <- struct{}{}:
		default:
		}
	}
	c.durableMu.Unlock()
	if hook := c.opts.durableHooks.afterOffer; hook != nil {
		hook(eventID, frameID)
	}
	return nil
}

func (c *Client) writeDurableEvents() {
	defer c.wg.Done()
	for {
		job, ok := c.nextDurableEvent()
		if !ok {
			return
		}
		var receipt DurableEventReceipt
		var err error
		if job.result != nil {
			receipt = *job.result
		} else {
			receipt, err = c.opts.DurableEvents.StoreDurableEvent(c.ctx, job.event)
		}
		if err != nil {
			generation := c.finishDurableEvent(job)
			c.reportDurableError(generation, fmt.Errorf("store durable event: %w", err))
			continue
		}
		result := protocol.EmitEventResult{
			EventID: job.event.EventID,
			Status:  receipt.Status,
			Reason:  receipt.Reason,
		}
		if err := result.Validate(); err != nil {
			generation := c.finishDurableEvent(job)
			c.reportDurableError(generation, fmt.Errorf("invalid durable receipt: %w", err))
			continue
		}
		c.retainDurableReceipt(job, receipt)
		for {
			generation, frameID, ok := c.durableTarget(job)
			if !ok {
				break
			}
			sendErr := c.sendDurableResult(job, generation, frameID, result)
			if sendErr == nil {
				if !c.completeDurableEvent(job, generation, frameID) {
					continue
				}
				break
			}
			if errors.Is(sendErr, ErrLinkDown) {
				if !c.suspendDurableEvent(job, generation, frameID) {
					continue
				}
				break
			}
			if !c.completeDurableEvent(job, generation, frameID) {
				continue
			}
			c.reportDurableError(generation, sendErr)
			break
		}
	}
}

func (c *Client) nextDurableEvent() (*durableEventJob, bool) {
	for {
		if job := c.takeDurableEvent(); job != nil {
			return job, true
		}
		select {
		case <-c.ctx.Done():
			return nil, false
		case <-c.durableWake:
		}
	}
}

func (c *Client) takeDurableEvent() *durableEventJob {
	c.durableMu.Lock()
	defer c.durableMu.Unlock()
	if c.durableEvent == nil || c.durableRunning || c.durableEvent.suspended {
		return nil
	}
	c.durableRunning = true
	return c.durableEvent
}

func (c *Client) finishDurableEvent(job *durableEventJob) uint64 {
	c.durableMu.Lock()
	defer c.durableMu.Unlock()
	if c.durableEvent != job {
		return job.generation
	}
	generation := job.generation
	c.durableEvent = nil
	c.durableRunning = false
	return generation
}

func (c *Client) durableTarget(job *durableEventJob) (uint64, int64, bool) {
	c.durableMu.Lock()
	defer c.durableMu.Unlock()
	if c.durableEvent != job {
		return job.generation, job.frameID, false
	}
	job.receiptState = durableReceiptPending
	job.successor = nil
	return job.generation, job.frameID, true
}

func (c *Client) retainDurableReceipt(job *durableEventJob, receipt DurableEventReceipt) {
	c.durableMu.Lock()
	defer c.durableMu.Unlock()
	if c.durableEvent == job {
		job.result = &receipt
	}
}

// suspendDurableEvent keeps a known terminal result when its connection dies
// before the Proxy can observe it. Only an exact replay on a later connection
// wakes the worker; a different event remains a wire error.
func (c *Client) suspendDurableEvent(
	job *durableEventJob,
	generation uint64,
	frameID int64,
) bool {
	c.durableMu.Lock()
	defer c.durableMu.Unlock()
	if c.durableEvent != job || job.generation != generation || job.frameID != frameID {
		return false
	}
	job.suspended = true
	c.durableRunning = false
	return true
}

func (c *Client) completeDurableEvent(
	job *durableEventJob,
	generation uint64,
	frameID int64,
) bool {
	c.durableMu.Lock()
	defer c.durableMu.Unlock()
	if c.durableEvent != job {
		// A next event replaced this already-acknowledged job. Hand the
		// single worker slot to it without disturbing the queued pointer.
		c.durableRunning = false
		if c.durableEvent != nil && !c.durableEvent.suspended {
			select {
			case c.durableWake <- struct{}{}:
			default:
			}
		}
		return true
	}
	if job.generation != generation || job.frameID != frameID {
		job.receiptState = durableReceiptPending
		job.successor = nil
		return false
	}
	c.durableEvent = nil
	c.durableRunning = false
	return true
}

func (c *Client) sendDurableResult(
	job *durableEventJob,
	generation uint64,
	replyTo int64,
	result protocol.EmitEventResult,
) error {
	if hook := c.opts.durableHooks.beforeResultSend; hook != nil {
		hook(job.event.EventID)
	}
	if err := c.acquireSend(c.ctx); err != nil {
		return err
	}
	defer c.releaseSend()
	raw, err := encodeBody(result)
	if err != nil {
		return err
	}
	c.mu.Lock()
	if c.generation != generation || c.snap.State != StateUp || c.conn == nil {
		c.mu.Unlock()
		return ErrLinkDown
	}
	conn := c.conn
	frameID := c.nextID
	c.nextID++
	c.mu.Unlock()
	if !c.beginDurableResultWrite(job, generation, replyTo) {
		return ErrLinkDown
	}
	if err := c.writeFrame(c.ctx, conn, protocol.AgentFrame{
		Op:      protocol.AgentOpEmitEventResult,
		FrameID: frameID,
		ReplyTo: &replyTo,
		SentAt:  c.opts.Clock.Now(),
		Body:    raw,
	}); err != nil {
		c.finishDurableResultWrite(job, generation, replyTo, false)
		return err
	}
	if hook := c.opts.durableHooks.afterResultSend; hook != nil {
		hook(job.event.EventID)
	}
	c.finishDurableResultWrite(job, generation, replyTo, true)
	return nil
}

func (c *Client) beginDurableResultWrite(
	job *durableEventJob,
	generation uint64,
	frameID int64,
) bool {
	c.durableMu.Lock()
	defer c.durableMu.Unlock()
	if c.durableEvent != job || job.generation != generation || job.frameID != frameID ||
		job.receiptState != durableReceiptPending {
		return false
	}
	job.receiptState = durableReceiptWriting
	return true
}

func (c *Client) finishDurableResultWrite(
	job *durableEventJob,
	generation uint64,
	frameID int64,
	written bool,
) {
	c.durableMu.Lock()
	defer c.durableMu.Unlock()
	if c.durableEvent != job || job.generation != generation || job.frameID != frameID {
		return
	}
	if !written {
		job.receiptState = durableReceiptPending
		job.successor = nil
		return
	}
	job.receiptState = durableReceiptWritten
	if job.successor != nil {
		c.durableEvent = job.successor
		job.successor = nil
	}
}

func encodeBody(body any) (protocol.Payload, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("encode durable result: %w", err)
	}
	return protocol.Payload(raw), nil
}

func (c *Client) reportDurableError(generation uint64, err error) {
	select {
	case c.durableErr <- taggedError{generation: generation, err: err}:
	case <-c.ctx.Done():
	}
}
