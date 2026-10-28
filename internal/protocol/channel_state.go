package protocol

import (
	"time"
)

// This file holds the controls and views of §6.2: one compare-and-swap
// row per (scope_type, scope_key, plane), and the views that read them.

// linkArrow joins the two agents of a link scope key.
const linkArrow = "→"

// ChannelControl is one control row.
type ChannelControl struct {
	ControlID           ControlID        `json:"control_id"`
	ScopeType           ScopeType        `json:"scope_type"`
	ScopeKey            string           `json:"scope_key" jsonschema:"minLength=1,maxLength=200"`
	Plane               Plane            `json:"plane"`
	Mode                ControlMode      `json:"mode,omitempty"`
	TxEnabled           *bool            `json:"tx_enabled,omitempty"`
	RxEnabled           *bool            `json:"rx_enabled,omitempty"`
	AutoDeliveryEnabled *bool            `json:"auto_delivery_enabled,omitempty"`
	AutoContinueEnabled *bool            `json:"auto_continue_enabled,omitempty"`
	FromAgent           AgentID          `json:"from_agent,omitempty"`
	ToAgent             AgentID          `json:"to_agent,omitempty"`
	Reason              string           `json:"reason,omitempty" jsonschema:"maxLength=280"`
	ActorType           ActorType        `json:"actor_type"`
	CreatedBy           string           `json:"created_by" jsonschema:"minLength=1,maxLength=128"`
	ExpiresAt           time.Time        `json:"expires_at,omitzero"`
	Restore             *ControlSnapshot `json:"restore,omitempty"`
	ThenMode            ControlMode      `json:"then_mode,omitempty"`
	Version             int64            `json:"version" jsonschema:"minimum=1"`
	CreatedAt           time.Time        `json:"created_at"`
	UpdatedAt           time.Time        `json:"updated_at"`
}

// Validate checks the row, including the legal fields per scope.
func (c ChannelControl) Validate() error {
	if err := checkRoot(&c); err != nil {
		return err
	}
	if c.UpdatedAt.Before(c.CreatedAt) {
		return &ValidationError{Path: "updated_at", Reason: "never before created_at"}
	}
	if c.Restore != nil && c.ExpiresAt.IsZero() {
		return &ValidationError{Path: "restore", Reason: "needs expires_at"}
	}
	flags := c.TxEnabled != nil || c.RxEnabled != nil ||
		c.AutoDeliveryEnabled != nil || c.AutoContinueEnabled != nil
	endpoints := c.FromAgent != "" || c.ToAgent != ""
	switch c.ScopeType {
	case ScopeTypeGlobal:
		if c.ScopeKey != "*" {
			return &ValidationError{Path: pathScopeKey, Reason: "global uses *"}
		}
		if c.Plane != PlaneAll {
			return &ValidationError{Path: pathPlane, Reason: "global uses all"}
		}
		if !ChannelMode(c.Mode).Valid() {
			return &ValidationError{Path: pathMode, Reason: "global needs a channel mode"}
		}
		if flags || endpoints {
			return &ValidationError{Path: pathScopeType, Reason: "global carries no flags or endpoints"}
		}
		return checkThenMode(c)
	case ScopeTypeNode:
		if c.Plane != PlaneAll {
			return &ValidationError{Path: pathPlane, Reason: "node uses all"}
		}
		if !ChannelMode(c.Mode).Valid() {
			return &ValidationError{Path: pathMode, Reason: "node needs a channel mode"}
		}
		if flags || endpoints {
			return &ValidationError{Path: pathScopeType, Reason: "node carries no flags or endpoints"}
		}
		return checkThenMode(c)
	case ScopeTypeAgent:
		if c.Plane != PlaneAll {
			return &ValidationError{Path: pathPlane, Reason: "agent uses all"}
		}
		if c.Mode != "" {
			return &ValidationError{Path: pathMode, Reason: "agent carries no mode"}
		}
		if !flags {
			return &ValidationError{Path: pathScopeType, Reason: "agent needs one of the four flags"}
		}
		if endpoints || c.ThenMode != "" {
			return &ValidationError{Path: pathScopeType, Reason: "agent carries no endpoints or then_mode"}
		}
		return nil
	case ScopeTypeLink:
		if c.Plane != PlaneMessage && c.Plane != PlaneArtifact {
			return &ValidationError{Path: pathPlane, Reason: "link uses message or artifact"}
		}
		if !LinkMode(c.Mode).Valid() {
			return &ValidationError{Path: pathMode, Reason: "link needs a link mode"}
		}
		if c.FromAgent == "" || c.ToAgent == "" {
			return &ValidationError{Path: pathScopeType, Reason: "link needs both endpoints"}
		}
		if c.FromAgent == c.ToAgent {
			return &ValidationError{Path: pathFromAgent, Reason: "from differs from to"}
		}
		if c.ScopeKey != string(c.FromAgent)+linkArrow+string(c.ToAgent) {
			return &ValidationError{Path: pathScopeKey, Reason: "link key joins the endpoints"}
		}
		if flags || c.ThenMode != "" {
			return &ValidationError{Path: pathScopeType, Reason: "link carries no flags or then_mode"}
		}
		return nil
	case ScopeTypeThread:
		if c.Plane != PlaneAll {
			return &ValidationError{Path: pathPlane, Reason: "thread uses all"}
		}
		if !LinkMode(c.Mode).Valid() {
			return &ValidationError{Path: pathMode, Reason: "thread needs a link mode"}
		}
		if flags || endpoints || c.ThenMode != "" {
			return &ValidationError{Path: pathScopeType, Reason: "thread carries nothing else"}
		}
		return nil
	case ScopeTypeLoop:
		if c.Plane != PlaneAll {
			return &ValidationError{Path: pathPlane, Reason: "loop uses all"}
		}
		if c.Mode != ControlModePaused && c.Mode != ControlModeStopped {
			return &ValidationError{Path: pathMode, Reason: "loop override is PAUSED or STOPPED"}
		}
		if flags || endpoints || c.ThenMode != "" {
			return &ValidationError{Path: pathScopeType, Reason: "loop carries nothing else"}
		}
		return nil
	default:
		return &ValidationError{Path: pathScopeType, Reason: "unknown scope"}
	}
}

// checkThenMode enforces the --then off rule: only OFF, only on a
// DRAINING global or node row.
func checkThenMode(c ChannelControl) error {
	if c.ThenMode == "" {
		return nil
	}
	if c.ThenMode != ControlModeOff {
		return &ValidationError{Path: "then_mode", Reason: "only OFF"}
	}
	if ChannelMode(c.Mode) != ChannelModeDraining {
		return &ValidationError{Path: "then_mode", Reason: "only with mode DRAINING"}
	}
	return nil
}

// ControlSnapshot is the control restored at expires_at; absent restore
// deletes the override.
type ControlSnapshot struct {
	Mode                ControlMode `json:"mode,omitempty"`
	TxEnabled           *bool       `json:"tx_enabled,omitempty"`
	RxEnabled           *bool       `json:"rx_enabled,omitempty"`
	AutoDeliveryEnabled *bool       `json:"auto_delivery_enabled,omitempty"`
	AutoContinueEnabled *bool       `json:"auto_continue_enabled,omitempty"`
	Reason              string      `json:"reason,omitempty" jsonschema:"maxLength=280"`
	ActorType           ActorType   `json:"actor_type"`
	CreatedBy           string      `json:"created_by" jsonschema:"minLength=1,maxLength=128"`
	ExpiresAt           time.Time   `json:"expires_at,omitzero"`
	ThenMode            ControlMode `json:"then_mode,omitempty"`
	Version             int64       `json:"version" jsonschema:"minimum=1"`
}

// Validate checks the snapshot.
func (s ControlSnapshot) Validate() error {
	return checkRoot(&s)
}

// ChannelState is the global or node view.
type ChannelState struct {
	ScopeType     ScopeType       `json:"scope_type"`
	ScopeKey      string          `json:"scope_key" jsonschema:"minLength=1,maxLength=200"`
	Mode          ChannelMode     `json:"mode"`
	EffectiveMode ChannelMode     `json:"effective_mode"`
	Version       int64           `json:"version" jsonschema:"minimum=0"`
	Control       *ChannelControl `json:"control,omitempty"`
	Drained       bool            `json:"drained,omitempty"`
}

// Validate checks the view.
func (s ChannelState) Validate() error {
	if err := checkRoot(&s); err != nil {
		return err
	}
	if s.ScopeType != ScopeTypeGlobal && s.ScopeType != ScopeTypeNode {
		return &ValidationError{Path: pathScopeType, Reason: "global or node"}
	}
	if s.ScopeType == ScopeTypeGlobal {
		if s.ScopeKey != "*" {
			return &ValidationError{Path: pathScopeKey, Reason: "global uses *"}
		}
		if s.EffectiveMode != s.Mode {
			return &ValidationError{Path: "effective_mode", Reason: "global effective equals mode"}
		}
	}
	if (s.Control != nil) != (s.Version >= 1) {
		return &ValidationError{Path: pathControl, Reason: "present iff version >= 1"}
	}
	if s.Control != nil {
		if s.Control.ScopeType != s.ScopeType || s.Control.ScopeKey != s.ScopeKey {
			return &ValidationError{Path: pathControl, Reason: "matching scope"}
		}
		if ChannelMode(s.Control.Mode) != s.Mode {
			return &ValidationError{Path: pathControl, Reason: "matching mode"}
		}
	}
	return nil
}

// AgentChannel is one agent's channel view.
type AgentChannel struct {
	AgentID      AgentID          `json:"agent_id"`
	TX           bool             `json:"tx"`
	RX           bool             `json:"rx"`
	AutoDelivery bool             `json:"auto_delivery"`
	AutoContinue bool             `json:"auto_continue"`
	Mode         AgentMode        `json:"mode,omitempty"`
	Deliverable  bool             `json:"deliverable"`
	Version      int64            `json:"version" jsonschema:"minimum=0"`
	Control      *ChannelControl  `json:"control,omitempty"`
	Effective    ChannelEffective `json:"effective"`
}

// Validate checks the view.
func (a AgentChannel) Validate() error {
	if err := checkRoot(&a); err != nil {
		return err
	}
	if (a.Control != nil) != (a.Version >= 1) {
		return &ValidationError{Path: pathControl, Reason: "present iff version >= 1"}
	}
	if a.Mode != "" {
		matched, ok := agentMacro(a.TX, a.RX, a.AutoDelivery, a.AutoContinue)
		if !ok || matched != a.Mode {
			return &ValidationError{Path: pathMode, Reason: "the macro matching the flags"}
		}
	}
	return nil
}

// agentMacro returns the macro matching one agent's local flags, or false
// when none matches. SEND_ONLY leaves auto_delivery as it was, so any
// value matches there.
func agentMacro(tx, rx, autoDelivery, autoContinue bool) (AgentMode, bool) {
	switch {
	case tx && rx && autoDelivery && autoContinue:
		return AgentModeFull, true
	case tx && rx && !autoDelivery && !autoContinue:
		return AgentModeManual, true
	case !tx && rx && autoDelivery && !autoContinue:
		return AgentModeReceiveOnly, true
	case tx && !rx && !autoContinue:
		return AgentModeSendOnly, true
	case !tx && !rx && !autoDelivery && !autoContinue:
		return AgentModeOff, true
	default:
		return "", false
	}
}

// LinkState is one directional link view.
type LinkState struct {
	FromAgent       AgentID         `json:"from_agent"`
	ToAgent         AgentID         `json:"to_agent"`
	MessageMode     LinkMode        `json:"message_mode"`
	ArtifactMode    LinkMode        `json:"artifact_mode"`
	MessageVersion  int64           `json:"message_version" jsonschema:"minimum=0"`
	ArtifactVersion int64           `json:"artifact_version" jsonschema:"minimum=0"`
	MessageControl  *ChannelControl `json:"message_control,omitempty"`
	ArtifactControl *ChannelControl `json:"artifact_control,omitempty"`
}

// Validate checks the link view.
func (l LinkState) Validate() error {
	if err := checkRoot(&l); err != nil {
		return err
	}
	if l.FromAgent == l.ToAgent {
		return &ValidationError{Path: pathFromAgent, Reason: "from differs from to"}
	}
	if (l.MessageControl != nil) != (l.MessageVersion >= 1) {
		return &ValidationError{Path: pathMessageControl, Reason: "present iff message_version >= 1"}
	}
	if (l.ArtifactControl != nil) != (l.ArtifactVersion >= 1) {
		return &ValidationError{Path: pathArtifactControl, Reason: "present iff artifact_version >= 1"}
	}
	wantKey := string(l.FromAgent) + linkArrow + string(l.ToAgent)
	if l.MessageControl != nil {
		if l.MessageControl.ScopeType != ScopeTypeLink || l.MessageControl.ScopeKey != wantKey {
			return &ValidationError{Path: pathMessageControl, Reason: "matching link scope"}
		}
		if l.MessageControl.Plane != PlaneMessage {
			return &ValidationError{Path: pathMessageControl, Reason: "plane message"}
		}
	}
	if l.ArtifactControl != nil {
		if l.ArtifactControl.ScopeType != ScopeTypeLink || l.ArtifactControl.ScopeKey != wantKey {
			return &ValidationError{Path: pathArtifactControl, Reason: "matching link scope"}
		}
		if l.ArtifactControl.Plane != PlaneArtifact {
			return &ValidationError{Path: pathArtifactControl, Reason: "plane artifact"}
		}
	}
	return nil
}

// LinkList lists link states.
type LinkList struct {
	Links []LinkState `json:"links"`
}

// Validate checks the list.
func (l LinkList) Validate() error {
	return checkRoot(&l)
}
