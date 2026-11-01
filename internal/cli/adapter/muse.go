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
	museHarnessVersion = "1.4.13-R6996.1"
	museEmitterVersion = "1"
	maxMuseHookBytes   = 1 << 20
	museNonceBytes     = 16
	museEventIDPrefix  = "mu-"
	museRequestPrefix  = "perm-"

	museHookSessionStart       = "SessionStart"
	museHookSessionEnd         = "SessionEnd"
	museHookUserPromptSubmit   = "UserPromptSubmit"
	museHookPreToolUse         = "PreToolUse"
	museHookPostToolUse        = "PostToolUse"
	museHookPostToolUseFailure = "PostToolUseFailure"
	museHookPostToolBatch      = "PostToolBatch"
	museHookToolUseStart       = "ToolUseStart"
	museHookPermission         = "PermissionRequest"
	museHookPreLLMCall         = "PreLLMCall"
	museHookPostLLMCall        = "PostLLMCall"
	museHookPreCompact         = "PreCompact"
	museHookPostCompact        = "PostCompact"
	museHookSubagentStart      = "SubagentStart"
	museHookSubagentStop       = "SubagentStop"
	museHookStop               = "Stop"
	museHookStopFailure        = "StopFailure"
	museHookInterrupt          = "Interrupt"
	museHookSessionFork        = "SessionFork"
	museHookNotification       = "Notification"

	museSourceStartup = "startup"
	museSourceResume  = "resume"
	museSourceClear   = "clear"
	museSourceCompact = "compact"
	museTriggerManual = "manual"
	museTriggerAuto   = "auto"
)

// museHookInput is the complete projection the emitter may retain from stdin.
// Content-bearing fields are absent so encoding/json discards their values.
type museHookInput struct {
	HookEventName  string `json:"hook_event_name"`
	SessionID      string `json:"session_id"`
	TurnID         string `json:"turn_id"`
	Source         string `json:"source"`
	ToolUseID      string `json:"tool_use_id"`
	ChildSessionID string `json:"child_session_id"`
	Trigger        string `json:"trigger"`
}

func newMuseHookCmd(send emitFunc, now func() time.Time, nonces io.Reader) *cobra.Command {
	return &cobra.Command{
		Use:                "muse",
		Short:              "Observe one Muse Code 1.4.13 hook (always exits 0)",
		Args:               cobra.ArbitraryArgs,
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, args []string) (result error) {
			// Observation must never change Muse control flow, including after
			// an unexpected internal panic.
			defer func() {
				if recover() != nil {
					result = nil
				}
			}()
			if len(args) != 0 {
				_, _ = io.Copy(io.Discard, io.LimitReader(cmd.InOrStdin(), maxMuseHookBytes+1))
				return nil
			}
			input, ok := readMuseHook(cmd.InOrStdin())
			if !ok {
				return nil
			}
			event, ok := museEvent(input, now().UnixMilli(), nonces)
			if !ok {
				return nil
			}
			hello := nativeevents.Hello{
				Operation: nativeevents.OperationHello,
				Harness:   protocol.HarnessMuseCode, HarnessVersion: museHarnessVersion,
				EmitterType: nativeevents.EmitterTypeHook, EmitterVersion: museEmitterVersion,
				SchemaVersion: nativeevents.SchemaVersion,
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), EmitTimeout)
			defer cancel()
			_ = send(ctx, os.Getenv(SocketEnvironment), hello, event)
			return nil
		},
	}
}

func readMuseHook(r io.Reader) (museHookInput, bool) {
	raw, err := io.ReadAll(io.LimitReader(r, maxMuseHookBytes+1))
	if err != nil || len(raw) > maxMuseHookBytes {
		return museHookInput{}, false
	}
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return museHookInput{}, false
	}
	var input museHookInput
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if decoder.Decode(&input) != nil {
		return museHookInput{}, false
	}
	var extra any
	if !errors.Is(decoder.Decode(&extra), io.EOF) {
		return museHookInput{}, false
	}
	return input, true
}

func museEvent(input museHookInput, observedAt int64, nonces io.Reader) (nativeevents.Event, bool) {
	sessionRef, ok := museRef("session", input.SessionID)
	if !ok || observedAt < 1 {
		return nativeevents.Event{}, false
	}
	event := nativeevents.Event{
		Operation: nativeevents.OperationEvent, Sequence: 1, ObservedAt: observedAt,
		SessionRef: sessionRef,
	}
	switch input.HookEventName {
	case museHookSessionStart:
		if input.Source != museSourceStartup && input.Source != museSourceResume && input.Source != museSourceClear {
			return nativeevents.Event{}, false
		}
		event.Type = protocol.EventTypeSessionStarted
	case museHookSessionEnd:
		event.Type = protocol.EventTypeSessionEnded
	case museHookUserPromptSubmit:
		turnRef, valid := museRef("turn", input.TurnID)
		if !valid {
			return nativeevents.Event{}, false
		}
		event.Type = protocol.EventTypePromptSubmitted
		event.TurnRef = turnRef
	case museHookPreLLMCall:
		turnRef, valid := museRef("turn", input.TurnID)
		if !valid {
			return nativeevents.Event{}, false
		}
		event.Type = protocol.EventTypeTurnStarted
		event.TurnRef = turnRef
	case museHookPostLLMCall:
		turnRef, valid := museRef("turn", input.TurnID)
		if !valid {
			return nativeevents.Event{}, false
		}
		event.Type = protocol.EventTypeTurnStreaming
		event.TurnRef = turnRef
	case museHookPreToolUse:
		turnRef, turnValid := museRef("turn", input.TurnID)
		toolRef, toolValid := museRef("tool", input.ToolUseID)
		if !turnValid || !toolValid {
			return nativeevents.Event{}, false
		}
		event.Type = protocol.EventTypeToolStarted
		event.TurnRef = turnRef
		event.ToolRef = toolRef
	case museHookPermission:
		requestRef, valid := newMuseRandomRef(museRequestPrefix, nonces)
		if !valid {
			return nativeevents.Event{}, false
		}
		event.Type = protocol.EventTypeApprovalRequested
		if turnRef, turnValid := museNativeRef(input.TurnID); turnValid {
			event.TurnRef = turnRef
		}
		event.RequestRef = requestRef
	case museHookPostToolUse, museHookPostToolUseFailure:
		turnRef, turnValid := museRef("turn", input.TurnID)
		toolRef, toolValid := museRef("tool", input.ToolUseID)
		if !turnValid || !toolValid {
			return nativeevents.Event{}, false
		}
		event.TurnRef = turnRef
		event.ToolRef = toolRef
		if input.HookEventName == museHookPostToolUse {
			event.Type = protocol.EventTypeToolCompleted
			event.Result = protocol.NativeEventResultOK
		} else {
			event.Type = protocol.EventTypeToolFailed
			event.Result = protocol.NativeEventResultFailed
		}
	case museHookSubagentStart:
		turnRef, turnValid := museRef("turn", input.TurnID)
		_, childValid := museRef("child_session", input.ChildSessionID)
		if !turnValid || !childValid {
			return nativeevents.Event{}, false
		}
		event.Type = protocol.EventTypeSubagentStarted
		event.TurnRef = turnRef
	case museHookPostCompact:
		if input.Trigger != museTriggerManual && input.Trigger != museTriggerAuto {
			return nativeevents.Event{}, false
		}
		turnRef, valid := museRef("turn", input.TurnID)
		if !valid {
			return nativeevents.Event{}, false
		}
		event.Type = protocol.EventTypeContextCompactionCompleted
		event.TurnRef = turnRef
	case museHookInterrupt:
		turnRef, valid := museRef("turn", input.TurnID)
		if !valid {
			return nativeevents.Event{}, false
		}
		event.Type = protocol.EventTypeTurnCompleted
		event.TurnRef = turnRef
		event.Result = protocol.NativeEventResultCancelled
	case museHookStop, museHookSubagentStop, museHookPreCompact, museHookNotification,
		museHookToolUseStart, museHookPostToolBatch, museHookStopFailure, museHookSessionFork:
		return nativeevents.Event{}, false
	default:
		return nativeevents.Event{}, false
	}
	eventID, ok := newMuseRandomRef(museEventIDPrefix, nonces)
	if !ok {
		return nativeevents.Event{}, false
	}
	event.EmitterEventID = eventID
	if event.Validate() != nil {
		return nativeevents.Event{}, false
	}
	return event, true
}

func museRef(domain, value string) (protocol.OpaqueID, bool) {
	if value == "" {
		return "", false
	}
	if ref, ok := museNativeRef(value); ok {
		return ref, true
	}
	digest := sha256.New()
	var length [8]byte
	for _, part := range []string{museEmitterVersion, domain, value} {
		binary.BigEndian.PutUint64(length[:], uint64(len(part)))
		_, _ = digest.Write(length[:])
		_, _ = digest.Write([]byte(part))
	}
	return protocol.OpaqueID(hex.EncodeToString(digest.Sum(nil))), true
}

func museNativeRef(value string) (protocol.OpaqueID, bool) {
	ref := protocol.OpaqueID(value)
	return ref, len(ref) <= 64 && ref.Valid()
}

func newMuseRandomRef(prefix string, r io.Reader) (protocol.OpaqueID, bool) {
	var nonce [museNonceBytes]byte
	if _, err := io.ReadFull(r, nonce[:]); err != nil {
		return "", false
	}
	return protocol.OpaqueID(prefix + hex.EncodeToString(nonce[:])), true
}
