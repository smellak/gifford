package adapter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/nativeevents"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	codexHarnessVersion = "0.154.0"
	codexEmitterVersion = "1"
	maxCodexHookBytes   = 1 << 20
	codexNonceBytes     = 16
	codexEventIDPrefix  = "cx-"
	codexRequestPrefix  = "perm-"

	codexHookSessionStart     = "SessionStart"
	codexHookSessionEnd       = "SessionEnd"
	codexHookUserPromptSubmit = "UserPromptSubmit"
	codexHookPreToolUse       = "PreToolUse"
	codexHookPermission       = "PermissionRequest"
	codexHookPostToolUse      = "PostToolUse"
	codexHookStop             = "Stop"
	codexHookSubagentStart    = "SubagentStart"
	codexHookSubagentStop     = "SubagentStop"
	codexHookPreCompact       = "PreCompact"
	codexHookPostCompact      = "PostCompact"
	codexHookInterrupt        = "Interrupt"

	codexSourceStartup = "startup"
	codexSourceResume  = "resume"
	codexSourceClear   = "clear"
	codexSourceCompact = "compact"
	codexReasonOther   = "other"
	codexTriggerManual = "manual"
	codexTriggerAuto   = "auto"
)

// codexHookInput is the complete projection the emitter may retain from stdin.
// Content-bearing fields are absent so encoding/json discards their values.
type codexHookInput struct {
	HookEventName string `json:"hook_event_name"`
	SessionID     string `json:"session_id"`
	TurnID        string `json:"turn_id"`
	Source        string `json:"source"`
	Reason        string `json:"reason"`
	ToolUseID     string `json:"tool_use_id"`
	AgentID       string `json:"agent_id"`
	Trigger       string `json:"trigger"`
}

func newCodexHookCmd(send emitFunc, now func() time.Time, nonces io.Reader) *cobra.Command {
	return &cobra.Command{
		Use:                "codex",
		Short:              "Observe one Codex 0.154.0 hook (always exits 0)",
		Args:               cobra.ArbitraryArgs,
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, args []string) (result error) {
			// Observation must never change Codex control flow, including after
			// an unexpected internal panic.
			defer func() {
				if recover() != nil {
					result = nil
				}
			}()
			if len(args) != 0 {
				_, _ = io.Copy(io.Discard, io.LimitReader(cmd.InOrStdin(), maxCodexHookBytes+1))
				return nil
			}
			input, ok := readCodexHook(cmd.InOrStdin())
			if !ok {
				return nil
			}
			event, ok := codexEvent(input, now().UnixMilli(), nonces)
			if !ok {
				return nil
			}
			hello := nativeevents.Hello{
				Operation: nativeevents.OperationHello,
				Harness:   protocol.HarnessCodex, HarnessVersion: codexHarnessVersion,
				EmitterType: nativeevents.EmitterTypeHook, EmitterVersion: codexEmitterVersion,
				SchemaVersion: nativeevents.SchemaVersion,
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), EmitTimeout)
			defer cancel()
			_ = send(ctx, os.Getenv(SocketEnvironment), hello, event)
			return nil
		},
	}
}

func readCodexHook(r io.Reader) (codexHookInput, bool) {
	raw, err := io.ReadAll(io.LimitReader(r, maxCodexHookBytes+1))
	if err != nil || len(raw) > maxCodexHookBytes {
		return codexHookInput{}, false
	}
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return codexHookInput{}, false
	}
	var input codexHookInput
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if decoder.Decode(&input) != nil {
		return codexHookInput{}, false
	}
	var extra any
	if !errors.Is(decoder.Decode(&extra), io.EOF) {
		return codexHookInput{}, false
	}
	return input, true
}

func codexEvent(
	input codexHookInput,
	observedAt int64,
	nonces io.Reader,
) (nativeevents.Event, bool) {
	sessionRef, ok := codexRef("session", input.SessionID)
	if !ok || observedAt < 1 {
		return nativeevents.Event{}, false
	}
	event := nativeevents.Event{
		Operation: nativeevents.OperationEvent, Sequence: 1, ObservedAt: observedAt,
		SessionRef: sessionRef,
	}
	switch input.HookEventName {
	case codexHookSessionStart:
		if input.Source != codexSourceStartup && input.Source != codexSourceResume && input.Source != codexSourceClear {
			return nativeevents.Event{}, false
		}
		event.Type = protocol.EventTypeSessionStarted
	case codexHookSessionEnd:
		if input.Reason != codexReasonOther {
			return nativeevents.Event{}, false
		}
		event.Type = protocol.EventTypeSessionEnded
	case codexHookUserPromptSubmit:
		turnRef, valid := codexRef("turn", input.TurnID)
		if !valid {
			return nativeevents.Event{}, false
		}
		event.Type = protocol.EventTypePromptSubmitted
		event.TurnRef = turnRef
	case codexHookPreToolUse:
		turnRef, turnValid := codexRef("turn", input.TurnID)
		toolRef, toolValid := codexRef("tool", input.ToolUseID)
		if !turnValid || !toolValid {
			return nativeevents.Event{}, false
		}
		event.Type = protocol.EventTypeToolStarted
		event.TurnRef = turnRef
		event.ToolRef = toolRef
	case codexHookPermission:
		requestRef, valid := newCodexRandomRef(codexRequestPrefix, nonces)
		if !valid {
			return nativeevents.Event{}, false
		}
		event.Type = protocol.EventTypeApprovalRequested
		if turnRef, turnValid := codexNativeRef(input.TurnID); turnValid {
			event.TurnRef = turnRef
		}
		event.RequestRef = requestRef
	case codexHookPostToolUse:
		turnRef, turnValid := codexRef("turn", input.TurnID)
		toolRef, toolValid := codexRef("tool", input.ToolUseID)
		if !turnValid || !toolValid {
			return nativeevents.Event{}, false
		}
		event.Type = protocol.EventTypeToolCompleted
		event.TurnRef = turnRef
		event.ToolRef = toolRef
		event.Result = protocol.NativeEventResultOK
	case codexHookSubagentStart:
		turnRef, turnValid := codexRef("turn", input.TurnID)
		_, agentValid := codexRef("agent", input.AgentID)
		if !turnValid || !agentValid {
			return nativeevents.Event{}, false
		}
		event.Type = protocol.EventTypeSubagentStarted
		event.TurnRef = turnRef
	case codexHookPostCompact:
		if input.Trigger != codexTriggerManual && input.Trigger != codexTriggerAuto {
			return nativeevents.Event{}, false
		}
		turnRef, valid := codexRef("turn", input.TurnID)
		if !valid {
			return nativeevents.Event{}, false
		}
		event.Type = protocol.EventTypeContextCompactionCompleted
		event.TurnRef = turnRef
	case codexHookInterrupt:
		turnRef, valid := codexRef("turn", input.TurnID)
		if !valid {
			return nativeevents.Event{}, false
		}
		event.Type = protocol.EventTypeTurnCompleted
		event.TurnRef = turnRef
		event.Result = protocol.NativeEventResultCancelled
	case codexHookStop, codexHookSubagentStop, codexHookPreCompact:
		return nativeevents.Event{}, false
	default:
		return nativeevents.Event{}, false
	}
	eventID, ok := newCodexRandomRef(codexEventIDPrefix, nonces)
	if !ok {
		return nativeevents.Event{}, false
	}
	event.EmitterEventID = eventID
	if event.Validate() != nil {
		return nativeevents.Event{}, false
	}
	return event, true
}

func codexRef(domain, value string) (protocol.OpaqueID, bool) {
	if value == "" {
		return "", false
	}
	if ref, ok := codexNativeRef(value); ok {
		return ref, true
	}
	digest := sha256.New()
	var length [8]byte
	for _, part := range []string{codexEmitterVersion, domain, value} {
		binary.BigEndian.PutUint64(length[:], uint64(len(part)))
		_, _ = digest.Write(length[:])
		_, _ = digest.Write([]byte(part))
	}
	return protocol.OpaqueID(hex.EncodeToString(digest.Sum(nil))), true
}

func codexNativeRef(value string) (protocol.OpaqueID, bool) {
	ref := protocol.OpaqueID(value)
	return ref, len(ref) <= 64 && ref.Valid()
}

func newCodexRandomRef(prefix string, r io.Reader) (protocol.OpaqueID, bool) {
	var nonce [codexNonceBytes]byte
	if _, err := io.ReadFull(r, nonce[:]); err != nil {
		return "", false
	}
	return protocol.OpaqueID(prefix + hex.EncodeToString(nonce[:])), true
}
