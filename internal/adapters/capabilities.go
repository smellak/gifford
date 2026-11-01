package adapters

import (
	"strings"

	"github.com/smellak/gifford/internal/protocol"
)

// NativeEventCapabilities returns capabilities with the three lifecycle
// signals copied from target. Other verified abilities are left unchanged.
func NativeEventCapabilities(
	current protocol.Capabilities,
	target protocol.Capabilities,
) protocol.Capabilities {
	current.StructuredEvents = target.StructuredEvents
	current.ToolEvents = target.ToolEvents
	current.ApprovalEvents = target.ApprovalEvents
	return current
}

// ValidCapabilitiesTransition admits the legacy complete degradation and the
// bidirectional lifecycle-capability transition. The latter may change only
// structured, tool and approval events, and every changed bit must move in
// the same direction.
func ValidCapabilitiesTransition(
	before protocol.CapabilityManifest,
	after protocol.CapabilityManifest,
) bool {
	if before.Validate() != nil || after.Validate() != nil ||
		before.Capabilities == after.Capabilities ||
		before.AdapterType != protocol.AdapterTypeGenericPTY &&
			!strings.HasPrefix(string(before.AdapterType), "native_") {
		return false
	}
	beforeIdentity := before
	afterIdentity := after
	beforeIdentity.Capabilities = protocol.Capabilities{}
	afterIdentity.Capabilities = protocol.Capabilities{}
	if beforeIdentity != afterIdentity {
		return false
	}
	generic := GenericManifest(before.AgentID, before.Harness, before.HarnessVersion)
	if after.Capabilities == generic.Capabilities {
		return true
	}

	beforeOther := NativeEventCapabilities(before.Capabilities, protocol.Capabilities{})
	afterOther := NativeEventCapabilities(after.Capabilities, protocol.Capabilities{})
	if beforeOther != afterOther {
		return false
	}
	direction := 0
	for _, pair := range [][2]bool{
		{before.Capabilities.StructuredEvents, after.Capabilities.StructuredEvents},
		{before.Capabilities.ToolEvents, after.Capabilities.ToolEvents},
		{before.Capabilities.ApprovalEvents, after.Capabilities.ApprovalEvents},
	} {
		if pair[0] == pair[1] {
			continue
		}
		next := 1
		if pair[0] {
			next = -1
		}
		if direction != 0 && direction != next {
			return false
		}
		direction = next
	}
	return direction != 0
}
