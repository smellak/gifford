package endpoint

// This file holds the served tool table: the ten MCP tools with their
// input schemas. Schemas are plain JSON Schema objects describing the
// arguments each handler decodes; no tool modifies a channel control —
// request_loop_extension only asks the Hub, per R48.

import (
	"encoding/json"
	"fmt"

	"github.com/smellak/gifford/internal/protocol"
)

// ToolDef is one served MCP tool.
type ToolDef struct {
	Name        string
	Description string
	InputSchema map[string]any
}

// fieldType names the JSON Schema kind member.
const fieldType = "type"

const pendingMessageInstructions = "A line in the agent input beginning with [GIFFORD] means messages are pending. " +
	"Call list_pending_messages, then read_message with its id and delivery_generation. " +
	"When the message is handled, call ack_message with status ACKNOWLEDGED. " +
	"Reply to its sender with send_message unless the message says no response is needed."

// objectSchema builds {"type":"object","properties":...,"required":...}.
func objectSchema(properties map[string]any, required ...string) map[string]any {
	schema := map[string]any{fieldType: "object", "properties": properties}
	if len(required) > 0 {
		names := make([]any, len(required))
		for i, name := range required {
			names[i] = name
		}
		schema["required"] = names
	}
	return schema
}

// str is {"type":"string"} with an optional description.
func str(description string) map[string]any {
	field := map[string]any{fieldType: "string"}
	if description != "" {
		field["description"] = description
	}
	return field
}

func integer(description string, minimum int) map[string]any {
	field := map[string]any{fieldType: "integer", "minimum": minimum}
	if description != "" {
		field["description"] = description
	}
	return field
}

// Tools is the served table in stable order.
func Tools() []ToolDef {
	return []ToolDef{
		{
			Name:        toolSendMessage,
			Description: "Send a message to an agent. " + pendingMessageInstructions,
			InputSchema: objectSchema(map[string]any{
				"to":                 str("Recipient agent."),
				"subject":            str("Message subject."),
				"body":               str("Message body plaintext."),
				"priority":           str("Delivery priority."),
				"thread_id":          str("Thread to continue."),
				"ttl":                str("Time to live."),
				"client_msg_id":      str("Client deduplication id."),
				"requested_autonomy": str("Requested autonomy grant."),
				"requested_budget":   map[string]any{fieldType: "object"},
			}, "to", "subject", "body"),
		},
		{
			Name:        "list_pending_messages",
			Description: "List messages awaiting this agent. " + pendingMessageInstructions,
			InputSchema: objectSchema(map[string]any{}),
		},
		{
			Name:        "read_message",
			Description: "Read one pending message. " + pendingMessageInstructions,
			InputSchema: objectSchema(map[string]any{
				"id":                  str("Message id."),
				"delivery_generation": integer("Exact delivery generation from list_pending_messages.", 1),
			}, "id", "delivery_generation"),
		},
		{
			Name: "ack_message",
			Description: "Acknowledge a message; a repeated ack has no second effect. " +
				pendingMessageInstructions,
			InputSchema: objectSchema(map[string]any{
				"id":                  str("Message id."),
				"delivery_generation": integer("Exact acknowledged delivery generation.", 1),
				"status": map[string]any{
					fieldType: "string",
					"enum":    protocol.AckStatus("").EnumValues(),
					"description": "Use ACKNOWLEDGED when the message has been handled; " +
						"use PROCESSING while work remains.",
				},
			}, "id", "delivery_generation", "status"),
		},
		{
			Name:        "reply_to_message",
			Description: "Reply to one message.",
			InputSchema: objectSchema(map[string]any{
				"id":                 str("Message id."),
				"body":               str("Reply body plaintext."),
				"client_msg_id":      str("Client deduplication id."),
				"requested_autonomy": str("Requested autonomy grant."),
			}, "id", "body"),
		},
		{
			Name:        "reject_message",
			Description: "Reject one message with a reason.",
			InputSchema: objectSchema(map[string]any{
				"id":     str("Message id."),
				"reason": str("Rejection reason."),
			}, "id", "reason"),
		},
		{
			Name:        "get_thread",
			Description: "Read one thread.",
			InputSchema: objectSchema(map[string]any{
				"thread_id": str("Thread id."),
			}, "thread_id"),
		},
		{
			Name:        "get_agent_presence",
			Description: "Check whether an agent is present.",
			InputSchema: objectSchema(map[string]any{
				"agent_id": str("Agent id."),
			}, "agent_id"),
		},
		{
			Name:        "get_delivery_status",
			Description: "Check delivery of one message.",
			InputSchema: objectSchema(map[string]any{
				"message_id": str("Message id."),
			}, "message_id"),
		},
		{
			Name:        "request_loop_extension",
			Description: "Ask for more loop turns. The only tool related to controls; it changes none.",
			InputSchema: objectSchema(map[string]any{
				"loop_id": str("Loop id."),
				"turns":   map[string]any{fieldType: "integer", "minimum": 1},
				"reason":  str("Why the extension is needed."),
			}, "loop_id", "turns", "reason"),
		},
	}
}

// toolNames returns the served names in order.
func toolNames() []string {
	defs := Tools()
	names := make([]string, len(defs))
	for i, def := range defs {
		names[i] = def.Name
	}
	return names
}

// toolsListJSON renders the tools/list payload.
func toolsListJSON(haveIdentity bool) (json.RawMessage, error) {
	type wireTool struct {
		Name        string         `json:"name"`
		Description string         `json:"description,omitempty"`
		InputSchema map[string]any `json:"inputSchema"`
	}
	type wireList struct {
		Tools []wireTool `json:"tools"`
	}
	list := wireList{Tools: []wireTool{}}
	if haveIdentity {
		for _, def := range Tools() {
			list.Tools = append(list.Tools, wireTool(def))
		}
	}
	raw, err := json.Marshal(list)
	if err != nil {
		return nil, fmt.Errorf("encode tool list: %w", err)
	}
	return raw, nil
}
