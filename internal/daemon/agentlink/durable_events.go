package agentlink

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/smellak/gifford/internal/protocol"
)

type durableEventHooks struct {
	afterResultSend func(protocol.ULID)
	beforeOffer     func(protocol.ULID)
	afterOffer      func(protocol.ULID, int64)
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
	if err := protocol.ValidateDurableEventEnvelope(event); err != nil {
		return protocol.EventEnvelope{}, fmt.Errorf("validate durable envelope: %w", err)
	}
	if event.NodeID != nodeID || event.SessionID != sessionID ||
		event.AgentID != authenticated.AgentID || event.Harness != authenticated.Harness ||
		event.HarnessVersion != authenticated.HarnessVersion ||
		event.AdapterType != authenticated.AdapterType {
		return protocol.EventEnvelope{}, errors.New("durable envelope does not match authenticated lineage")
	}
	canonical, err := protocol.CanonicalDurableEventPayload(event.EventType, event.Payload)
	if err != nil {
		return protocol.EventEnvelope{}, fmt.Errorf("canonicalize durable event payload: %w", err)
	}
	event.Payload = canonical
	return event, nil
}

// offerDurableEvent admits one source event at a time. The same immutable
// event may retarget its pending result after reconnect; a second event on the
// same connection or a different event while one is pending is a wire error.
func (c *Client) offerDurableEvent(job durableEventJob) error {
	eventID, frameID := job.event.EventID, job.frameID
	if hook := c.opts.durableHooks.beforeOffer; hook != nil {
		hook(eventID)
	}
	c.durableMu.Lock()
	current := c.durableEvent
	if current != nil {
		if current.generation == job.generation ||
			!protocol.EqualEventEnvelopes(current.event, job.event) {
			c.durableMu.Unlock()
			return ErrUnexpectedFrame
		}
		current.generation = job.generation
		current.frameID = job.frameID
		current.suspended = false
		if !c.durableRunning {
			select {
			case c.durableWake <- struct{}{}:
			default:
			}
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
		generation, ok, sendErr := c.sendAndFinishDurableResult(job, result)
		if !ok {
			continue
		}
		if sendErr != nil && !errors.Is(sendErr, ErrLinkDown) {
			c.reportDurableError(generation, sendErr)
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

// sendAndFinishDurableResult holds durableMu from target selection through a
// successful socket write and retirement. Once the result is visible, a
// following event therefore cannot observe the completed job still installed.
func (c *Client) sendAndFinishDurableResult(
	job *durableEventJob,
	result protocol.EmitEventResult,
) (uint64, bool, error) {
	c.durableMu.Lock()
	defer c.durableMu.Unlock()
	if c.durableEvent != job {
		return job.generation, false, nil
	}
	generation, frameID := job.generation, job.frameID
	sendErr := c.sendDurableResult(generation, frameID, result)
	if sendErr == nil && c.opts.durableHooks.afterResultSend != nil {
		c.opts.durableHooks.afterResultSend(job.event.EventID)
	}
	if errors.Is(sendErr, ErrLinkDown) {
		job.suspended = true
	} else {
		c.durableEvent = nil
	}
	c.durableRunning = false
	return generation, true, sendErr
}

func (c *Client) sendDurableResult(
	generation uint64,
	replyTo int64,
	result protocol.EmitEventResult,
) error {
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
	return c.writeFrame(c.ctx, conn, protocol.AgentFrame{
		Op:      protocol.AgentOpEmitEventResult,
		FrameID: frameID,
		ReplyTo: &replyTo,
		SentAt:  c.opts.Clock.Now(),
		Body:    raw,
	})
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
