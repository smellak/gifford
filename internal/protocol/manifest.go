package protocol

import (
	"fmt"
	"strings"
)

// CapabilityManifest is the agent handshake manifest: what the harness
// verified, not what it claims.
type CapabilityManifest struct {
	SchemaVersion      int64              `json:"schema_version" jsonschema:"enum=1"`
	AgentID            AgentID            `json:"agent_id"`
	Harness            Harness            `json:"harness"`
	HarnessVersion     string             `json:"harness_version" jsonschema:"minLength=1,maxLength=64"`
	AdapterType        AdapterType        `json:"adapter_type"`
	InputGuardProvider InputGuardProvider `json:"input_guard_provider"`
	CaptureMode        CaptureMode        `json:"capture_mode"`
	Deliverable        bool               `json:"deliverable"`
	Capabilities       Capabilities       `json:"capabilities"`
}

// Validate checks the manifest and its cross-field rules.
func (m CapabilityManifest) Validate() error {
	if m.SchemaVersion != SchemaVersion {
		return fmt.Errorf("schema_version %d: %w", m.SchemaVersion, ErrUnsupportedVersion)
	}
	if err := checkRoot(&m); err != nil {
		return err
	}
	if m.CaptureMode == CaptureModeBootstrapPipePane {
		if m.Deliverable {
			return &ValidationError{Path: "deliverable", Reason: "bootstrap agents are never deliverable"}
		}
		if m.InputGuardProvider != InputGuardProviderNone {
			return &ValidationError{Path: "input_guard_provider", Reason: "bootstrap agents guard nothing"}
		}
		if m.AdapterType != AdapterTypeNone {
			return &ValidationError{Path: pathAdapterType, Reason: "bootstrap agents have no adapter"}
		}
		if m.Capabilities.MCP || m.Capabilities.InternalMessageDelivery {
			return &ValidationError{Path: pathCapabilities, Reason: "bootstrap agents expose no endpoint"}
		}
	}
	if m.Deliverable {
		if m.CaptureMode != CaptureModePTYProxy {
			return &ValidationError{Path: "capture_mode", Reason: "deliverable agents run under the proxy"}
		}
		if m.InputGuardProvider == InputGuardProviderNone {
			return &ValidationError{Path: "input_guard_provider", Reason: "deliverable agents guard the draft"}
		}
	}
	if m.InputGuardProvider == InputGuardProviderNativeAdapter && !m.Capabilities.PromptBufferIntrospection {
		return &ValidationError{Path: pathCapabilities, Reason: "native guards introspect the prompt buffer"}
	}
	if m.Capabilities.StructuredEvents || m.Capabilities.InternalMessageDelivery ||
		m.Capabilities.NativeCancel || m.Capabilities.ToolEvents ||
		m.Capabilities.ApprovalEvents || m.Capabilities.SubagentEvents {
		if !strings.HasPrefix(string(m.AdapterType), "native_") {
			return &ValidationError{Path: pathAdapterType, Reason: "native capabilities need a native adapter"}
		}
	}
	if !m.Capabilities.PTYCapture {
		return &ValidationError{Path: pathCapabilities, Reason: "both capture modes capture the PTY output"}
	}
	return nil
}

// Capabilities are the verified harness capabilities. Every field is
// optional and absent means false; only verified abilities are declared.
type Capabilities struct {
	StructuredEvents          bool `json:"structured_events,omitempty"`
	PromptBufferIntrospection bool `json:"prompt_buffer_introspection,omitempty"`
	ExplicitSubmitEvents      bool `json:"explicit_submit_events,omitempty"`
	ExplicitCancelEvents      bool `json:"explicit_cancel_events,omitempty"`
	InternalMessageDelivery   bool `json:"internal_message_delivery,omitempty"`
	HumanNotificationUI       bool `json:"human_notification_ui,omitempty"`
	ToolEvents                bool `json:"tool_events,omitempty"`
	ApprovalEvents            bool `json:"approval_events,omitempty"`
	SubagentEvents            bool `json:"subagent_events,omitempty"`
	NativeCancel              bool `json:"native_cancel,omitempty"`
	PTYCapture                bool `json:"pty_capture,omitempty"`
	MCP                       bool `json:"mcp,omitempty"`
}

// Validate checks the capabilities.
func (c Capabilities) Validate() error {
	return checkRoot(&c)
}

// ProcessIdentity identifies the observed OS process.
type ProcessIdentity struct {
	PID              int64  `json:"pid" jsonschema:"minimum=1"`
	ProcessStartTime int64  `json:"process_start_time" jsonschema:"minimum=0"`
	UID              int64  `json:"uid" jsonschema:"minimum=0"`
	GID              int64  `json:"gid" jsonschema:"minimum=0"`
	Cgroup           string `json:"cgroup,omitempty" jsonschema:"maxLength=512"`
	ParentChainHash  Digest `json:"parent_chain_hash,omitempty"`
}

// Validate checks the process identity.
func (p ProcessIdentity) Validate() error {
	return checkRoot(&p)
}
