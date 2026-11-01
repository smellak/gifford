package agentlink

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/smellak/gifford/internal/adapters"
	"github.com/smellak/gifford/internal/protocol"
)

func (c *Client) applyCapabilitiesChanged(
	authenticated protocol.CapabilityManifest,
	frame protocol.AgentFrame,
) error {
	if c.opts.CapabilityWriter == nil || frame.ReplyTo != nil {
		return c.capabilitiesConflict()
	}
	var event protocol.EventEnvelope
	if err := decodeStrictJSON(frame.Body, &event); err != nil {
		return c.capabilitiesConflict()
	}
	if err := validateCapabilitiesEnvelope(
		event, c.opts.NodeID, c.opts.SessionID, authenticated,
	); err != nil {
		return c.capabilitiesConflict()
	}
	if err := c.opts.CapabilityWriter.ApplyCapabilitiesChanged(c.ctx, event); err != nil {
		if errors.Is(err, ErrCapabilitiesConflict) {
			return c.capabilitiesConflict()
		}
		return fmt.Errorf("apply capabilities changed: %w", err)
	}
	return nil
}

func (c *Client) applyCapabilitiesEvent(
	authenticated protocol.CapabilityManifest,
	frame protocol.AgentFrame,
	event protocol.EventEnvelope,
	generation uint64,
) error {
	if event.Sequence > 0 && c.opts.DurableEvents == nil {
		return c.capabilitiesConflict()
	}
	if err := c.applyCapabilitiesChanged(authenticated, frame); err != nil {
		return err
	}
	if event.Sequence == 0 {
		return nil
	}
	return c.offerDurableEvent(durableEventJob{
		event: event, generation: generation, frameID: frame.FrameID,
		result: &DurableEventReceipt{Status: protocol.EmitEventStatusStored},
	})
}

func validateCapabilitiesEnvelope(
	event protocol.EventEnvelope,
	nodeID protocol.NodeID,
	sessionID protocol.OpaqueID,
	authenticated protocol.CapabilityManifest,
) error {
	if err := event.Validate(); err != nil {
		return fmt.Errorf("validate capability envelope: %w", err)
	}
	if event.NodeID != nodeID || event.SessionID != sessionID ||
		event.AgentID != authenticated.AgentID || event.Harness != authenticated.Harness ||
		event.HarnessVersion != authenticated.HarnessVersion ||
		event.AdapterType != authenticated.AdapterType ||
		event.SourceQuality != protocol.SourceQualityProcessMetrics ||
		event.EventType != protocol.EventTypeAdapterCapabilitiesChanged ||
		event.Direction != "" || event.Stream != "" || event.Origin != protocol.OriginSystem ||
		event.WallTime != event.WallTime.UTC() || event.WallTime.IsZero() ||
		event.MonotonicNS != 0 || event.Sequence < 0 || event.TurnID != "" ||
		event.ToolCallID != "" || event.MessageID != "" || event.DeliveryID != "" ||
		event.DecisionID != "" || event.ParentEventID != "" || event.CorrelationID != "" ||
		event.Sensitivity != protocol.SensitivityNormal || event.RedactionApplied ||
		authenticated.AdapterType == protocol.AdapterTypeNone {
		return errors.New("capability envelope does not match authenticated lineage")
	}
	var change adapters.CapabilitiesChangedEvent
	if err := decodeStrictJSON(event.Payload, &change); err != nil {
		return err
	}
	canonical, err := json.Marshal(change)
	if err != nil || !bytes.Equal(canonical, event.Payload) {
		return errors.New("capability payload is not canonical")
	}
	before := authenticated
	before.Capabilities = change.Before
	after := authenticated
	after.Capabilities = change.After
	if change.EventType != protocol.EventTypeAdapterCapabilitiesChanged ||
		!adapters.ValidCapabilitiesTransition(before, after) {
		return errors.New("capability payload does not describe the authenticated transition")
	}
	return nil
}

func decodeStrictJSON(raw []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("decode strict JSON: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON")
	}
	return nil
}

func (c *Client) capabilitiesConflict() error {
	return fmt.Errorf("%w: code=%s agent_id=%s session_id=%s",
		ErrCapabilitiesConflict, ReasonCapabilitiesConflict, c.opts.AgentID, c.opts.SessionID)
}
