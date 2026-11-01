package adapter

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/nativeevents"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	claudeCodeHarnessVersion = "2.1.280"
	claudeCodeEmitterVersion = "1"
	maxClaudeCodeHookBytes   = 1 << 20
	claudeNonceBytes         = 16
)

const claudeQuestionTool = "AskUserQuestion"

// claudeCodeHookInput is the complete projection the emitter may retain from
// stdin. Content-bearing members are deliberately absent: encoding/json reads
// past them without placing their values in this object.
type claudeCodeHookInput struct {
	HookEventName string `json:"hook_event_name"`
	SessionID     string `json:"session_id"`
	PromptID      string `json:"prompt_id"`
	Source        string `json:"source"`
	Reason        string `json:"reason"`
	ToolName      string `json:"tool_name"`
	ToolUseID     string `json:"tool_use_id"`
	AgentID       string `json:"agent_id"`
}

func newHookCmd(send emitFunc, now func() time.Time) *cobra.Command {
	return deps.Group(
		"hook", "Emit native events from harness hooks",
		newClaudeCodeHookCmd(send, now, rand.Reader), newCodexHookCmd(send, now, rand.Reader),
	)
}

func newClaudeCodeHookCmd(send emitFunc, now func() time.Time, nonces io.Reader) *cobra.Command {
	return &cobra.Command{
		Use:                "claude-code",
		Short:              "Observe one Claude Code 2.1.280 hook (always exits 0)",
		Args:               cobra.ArbitraryArgs,
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, args []string) (result error) {
			// A hook must never change Claude Code's control flow, even after an
			// unexpected internal panic.
			defer func() {
				if recover() != nil {
					result = nil
				}
			}()
			if len(args) != 0 {
				_, _ = io.Copy(io.Discard, io.LimitReader(cmd.InOrStdin(), maxClaudeCodeHookBytes+1))
				return nil
			}
			input, ok := readClaudeCodeHook(cmd.InOrStdin())
			if !ok {
				return nil
			}
			event, ok := claudeCodeEvent(input, now().UnixMilli(), nonces)
			if !ok {
				return nil
			}
			hello := nativeevents.Hello{
				Operation: nativeevents.OperationHello,
				Harness:   protocol.HarnessClaudeCode, HarnessVersion: claudeCodeHarnessVersion,
				EmitterType: nativeevents.EmitterTypeHook, EmitterVersion: claudeCodeEmitterVersion,
				SchemaVersion: nativeevents.SchemaVersion,
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), EmitTimeout)
			defer cancel()
			_ = send(ctx, os.Getenv(SocketEnvironment), hello, event)
			return nil
		},
	}
}

func readClaudeCodeHook(r io.Reader) (claudeCodeHookInput, bool) {
	raw, err := io.ReadAll(io.LimitReader(r, maxClaudeCodeHookBytes+1))
	if err != nil || len(raw) > maxClaudeCodeHookBytes {
		return claudeCodeHookInput{}, false
	}
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return claudeCodeHookInput{}, false
	}
	var input claudeCodeHookInput
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if decoder.Decode(&input) != nil {
		return claudeCodeHookInput{}, false
	}
	var extra any
	if !errors.Is(decoder.Decode(&extra), io.EOF) {
		return claudeCodeHookInput{}, false
	}
	return input, true
}

func claudeCodeEvent(
	input claudeCodeHookInput,
	observedAt int64,
	nonces io.Reader,
) (nativeevents.Event, bool) {
	sessionRef, ok := claudeRef(input.SessionID)
	if !ok || observedAt < 1 {
		return nativeevents.Event{}, false
	}
	event := nativeevents.Event{
		Operation: nativeevents.OperationEvent, Sequence: 1, ObservedAt: observedAt,
		SessionRef: sessionRef,
	}
	switch input.HookEventName {
	case "SessionStart":
		if input.Source != "startup" && input.Source != "resume" {
			return nativeevents.Event{}, false
		}
		event.Type = protocol.EventTypeSessionStarted
	case "UserPromptSubmit":
		promptRef, valid := claudeRef(input.PromptID)
		if !valid {
			return nativeevents.Event{}, false
		}
		event.Type = protocol.EventTypePromptSubmitted
		event.TurnRef = promptRef
	case "PreToolUse":
		toolRef, valid := claudeRef(input.ToolUseID)
		if !valid {
			return nativeevents.Event{}, false
		}
		event.TurnRef, _ = claudeRef(input.PromptID)
		if input.ToolName == claudeQuestionTool {
			event.Type = protocol.EventTypeQuestionRequested
			event.RequestRef = toolRef
		} else {
			event.Type = protocol.EventTypeToolStarted
			event.ToolRef = toolRef
			event.ToolKind = claudeToolKind(input.ToolName)
		}
	case "PermissionRequest":
		promptRef, valid := claudeRef(input.PromptID)
		if !valid {
			return nativeevents.Event{}, false
		}
		requestRef, valid := newPermissionRef(nonces)
		if !valid {
			return nativeevents.Event{}, false
		}
		event.Type = protocol.EventTypeApprovalRequested
		event.TurnRef = promptRef
		event.RequestRef = requestRef
	case "PostToolUse":
		toolRef, valid := claudeRef(input.ToolUseID)
		if !valid {
			return nativeevents.Event{}, false
		}
		event.TurnRef, _ = claudeRef(input.PromptID)
		event.Result = protocol.NativeEventResultOK
		if input.ToolName == claudeQuestionTool {
			event.Type = protocol.EventTypeQuestionResolved
			event.RequestRef = toolRef
		} else {
			event.Type = protocol.EventTypeToolCompleted
			event.ToolRef = toolRef
			event.ToolKind = claudeToolKind(input.ToolName)
		}
	case "PostToolUseFailure":
		toolRef, valid := claudeRef(input.ToolUseID)
		if !valid {
			return nativeevents.Event{}, false
		}
		event.TurnRef, _ = claudeRef(input.PromptID)
		event.Result = protocol.NativeEventResultFailed
		if input.ToolName == claudeQuestionTool {
			event.Type = protocol.EventTypeQuestionResolved
			event.RequestRef = toolRef
		} else {
			event.Type = protocol.EventTypeToolFailed
			event.ToolRef = toolRef
			event.ToolKind = claudeToolKind(input.ToolName)
		}
	case "Notification", "PreCompact":
		return nativeevents.Event{}, false
	case "SubagentStart":
		_, valid := claudeRef(input.AgentID)
		if !valid {
			return nativeevents.Event{}, false
		}
		event.Type = protocol.EventTypeSubagentStarted
		event.TurnRef, _ = claudeRef(input.PromptID)
	case "Stop":
		promptRef, valid := claudeRef(input.PromptID)
		if !valid {
			return nativeevents.Event{}, false
		}
		event.Type = protocol.EventTypeTurnCompleted
		event.TurnRef = promptRef
		event.Result = protocol.NativeEventResultOK
	case "StopFailure":
		promptRef, valid := claudeRef(input.PromptID)
		if !valid {
			return nativeevents.Event{}, false
		}
		event.Type = protocol.EventTypeTurnCompleted
		event.TurnRef = promptRef
		event.Result = protocol.NativeEventResultFailed
	case "SessionEnd":
		if !claudeSessionEndReason(input.Reason) {
			return nativeevents.Event{}, false
		}
		event.Type = protocol.EventTypeSessionEnded
	default:
		return nativeevents.Event{}, false
	}
	eventID, valid := newClaudeEmitterEventID(nonces)
	if !valid {
		return nativeevents.Event{}, false
	}
	event.EmitterEventID = eventID
	if event.Validate() != nil {
		return nativeevents.Event{}, false
	}
	return event, true
}

func claudeRef(value string) (protocol.OpaqueID, bool) {
	ref := protocol.OpaqueID(value)
	return ref, len(ref) <= 64 && ref.Valid()
}

func newPermissionRef(r io.Reader) (protocol.OpaqueID, bool) {
	nonceHex, ok := claudeNonceHex(r)
	if !ok {
		return "", false
	}
	return protocol.OpaqueID("perm-" + nonceHex), true
}

func newClaudeEmitterEventID(r io.Reader) (protocol.OpaqueID, bool) {
	nonceHex, ok := claudeNonceHex(r)
	if !ok {
		return "", false
	}
	return protocol.OpaqueID("cc-" + nonceHex), true
}

func claudeNonceHex(r io.Reader) (string, bool) {
	var nonce [claudeNonceBytes]byte
	if _, err := io.ReadFull(r, nonce[:]); err != nil {
		return "", false
	}
	return hex.EncodeToString(nonce[:]), true
}

func claudeToolKind(toolName string) protocol.NativeToolKind {
	switch toolName {
	case "Bash", "PowerShell":
		return protocol.NativeToolKindShell
	case "Read", "Edit", "Write", "NotebookEdit":
		return protocol.NativeToolKindFile
	case "Glob", "Grep":
		return protocol.NativeToolKindSearch
	case "WebFetch", "WebSearch":
		return protocol.NativeToolKindWeb
	default:
		if strings.HasPrefix(toolName, "mcp__") {
			return protocol.NativeToolKindMCP
		}
		return protocol.NativeToolKindOther
	}
}

func claudeSessionEndReason(reason string) bool {
	switch reason {
	case "clear", "resume", "logout", "prompt_input_exit", "other":
		return true
	default:
		return false
	}
}
