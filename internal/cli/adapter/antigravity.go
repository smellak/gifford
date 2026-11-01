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
	"strconv"
	"time"

	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/nativeevents"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	antigravityHarnessVersion = "1.4.8"
	antigravityEmitterVersion = "1"
	maxAntigravityHookBytes   = 1 << 20
	antigravityNonceBytes     = 16
	antigravityEventIDPrefix  = "ag-"

	antigravityHookSessionStart   = "SessionStart"
	antigravityHookPreToolUse     = "PreToolUse"
	antigravityHookPostToolUse    = "PostToolUse"
	antigravityHookPreInvocation  = "PreInvocation"
	antigravityHookPostInvocation = "PostInvocation"
	antigravityHookStop           = "Stop"
)

// antigravityHookInput is the complete projection retained from stdin. Fields
// carrying prompts, paths, arguments, output, model names and error text are
// deliberately absent. The error decoder retains only empty versus non-empty.
type antigravityHookInput struct {
	ConversationID string                     `json:"conversationId"`
	InvocationNum  *int64                     `json:"invocationNum"`
	StepIdx        *int64                     `json:"stepIdx"`
	ExecutionNum   *int64                     `json:"executionNum"`
	FullyIdle      *bool                      `json:"fullyIdle"`
	Error          antigravityErrorProjection `json:"error"`
}

type antigravityErrorProjection struct {
	NonEmpty bool
}

func (p *antigravityErrorProjection) UnmarshalJSON(raw []byte) error {
	var value *string
	if err := json.Unmarshal(raw, &value); err != nil || value == nil {
		return errors.New("invalid antigravity error field")
	}
	p.NonEmpty = *value != ""
	return nil
}

func newAntigravityHookCmd(send emitFunc, now func() time.Time, nonces io.Reader) *cobra.Command {
	return &cobra.Command{
		Use:                "antigravity <hook>",
		Short:              "Observe one Antigravity 1.4.8 hook (always exits 0)",
		Args:               cobra.ArbitraryArgs,
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, args []string) (result error) {
			hook := ""
			if len(args) > 0 {
				hook = args[0]
			}
			// Antigravity consumes stdout as a control response. Emit the same
			// neutral object on success, error and panic paths.
			defer func() {
				if recover() != nil {
					result = nil
				}
				_, _ = io.WriteString(cmd.OutOrStdout(), antigravityNeutralOutput(hook))
			}()

			if len(args) != 1 || !antigravityHookKnown(hook) {
				_, _ = io.Copy(io.Discard, io.LimitReader(cmd.InOrStdin(), maxAntigravityHookBytes+1))
				return nil
			}
			input, ok := readAntigravityHook(cmd.InOrStdin())
			if !ok {
				return nil
			}
			event, ok := antigravityEvent(hook, input, now().UnixMilli(), nonces)
			if !ok {
				return nil
			}
			hello := nativeevents.Hello{
				Operation: nativeevents.OperationHello,
				Harness:   protocol.HarnessAntigravity, HarnessVersion: antigravityHarnessVersion,
				EmitterType: nativeevents.EmitterTypeHook, EmitterVersion: antigravityEmitterVersion,
				SchemaVersion: nativeevents.SchemaVersion,
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), EmitTimeout)
			defer cancel()
			_ = send(ctx, os.Getenv(SocketEnvironment), hello, event)
			return nil
		},
	}
}

func antigravityHookKnown(hook string) bool {
	switch hook {
	case antigravityHookSessionStart, antigravityHookPreToolUse, antigravityHookPostToolUse,
		antigravityHookPreInvocation, antigravityHookPostInvocation, antigravityHookStop:
		return true
	default:
		return false
	}
}

func antigravityNeutralOutput(hook string) string {
	switch hook {
	case antigravityHookSessionStart, antigravityHookPreInvocation:
		return "{\"injectSteps\":[]}\n"
	case antigravityHookPreToolUse, antigravityHookStop:
		return "{\"decision\":\"allow\"}\n"
	case antigravityHookPostToolUse:
		return "{}\n"
	case antigravityHookPostInvocation:
		return "{\"injectSteps\":[],\"terminationBehavior\":\"\"}\n"
	default:
		return "{}\n"
	}
}

func readAntigravityHook(r io.Reader) (antigravityHookInput, bool) {
	raw, err := io.ReadAll(io.LimitReader(r, maxAntigravityHookBytes+1))
	if err != nil || len(raw) > maxAntigravityHookBytes {
		return antigravityHookInput{}, false
	}
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return antigravityHookInput{}, false
	}
	var input antigravityHookInput
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if decoder.Decode(&input) != nil {
		return antigravityHookInput{}, false
	}
	var extra any
	if !errors.Is(decoder.Decode(&extra), io.EOF) {
		return antigravityHookInput{}, false
	}
	return input, true
}

func antigravityEvent(
	hook string,
	input antigravityHookInput,
	observedAt int64,
	nonces io.Reader,
) (nativeevents.Event, bool) {
	sessionRef, ok := antigravitySessionRef(input.ConversationID)
	if !ok || observedAt < 1 {
		return nativeevents.Event{}, false
	}
	event := nativeevents.Event{
		Operation: nativeevents.OperationEvent, Sequence: 1, ObservedAt: observedAt,
		SessionRef: sessionRef,
	}
	switch hook {
	case antigravityHookSessionStart:
		event.Type = protocol.EventTypeSessionStarted
	case antigravityHookPreInvocation, antigravityHookPostInvocation:
		turnRef, valid := antigravityNumberRef("invocation", input.ConversationID, input.InvocationNum)
		if !valid {
			return nativeevents.Event{}, false
		}
		event.TurnRef = turnRef
		if hook == antigravityHookPreInvocation {
			event.Type = protocol.EventTypeTurnStarted
		} else {
			event.Type = protocol.EventTypeTurnStreaming
		}
	case antigravityHookPreToolUse, antigravityHookPostToolUse:
		toolRef, valid := antigravityNumberRef("tool", input.ConversationID, input.StepIdx)
		if !valid {
			return nativeevents.Event{}, false
		}
		event.ToolRef = toolRef
		switch {
		case hook == antigravityHookPreToolUse:
			event.Type = protocol.EventTypeToolStarted
		case input.Error.NonEmpty:
			event.Type = protocol.EventTypeToolFailed
			event.Result = protocol.NativeEventResultFailed
		default:
			event.Type = protocol.EventTypeToolCompleted
			event.Result = protocol.NativeEventResultOK
		}
	case antigravityHookStop:
		if input.FullyIdle == nil || *input.FullyIdle {
			return nativeevents.Event{}, false
		}
		turnRef, valid := antigravityNumberRef("execution", input.ConversationID, input.ExecutionNum)
		if !valid {
			return nativeevents.Event{}, false
		}
		event.Type = protocol.EventTypeBackgroundTaskStarted
		event.TurnRef = turnRef
	default:
		return nativeevents.Event{}, false
	}
	eventID, valid := newAntigravityEventID(nonces)
	if !valid {
		return nativeevents.Event{}, false
	}
	event.EmitterEventID = eventID
	if event.Validate() != nil {
		return nativeevents.Event{}, false
	}
	return event, true
}

func antigravitySessionRef(value string) (protocol.OpaqueID, bool) {
	if value == "" {
		return "", false
	}
	ref := protocol.OpaqueID(value)
	if len(ref) <= 64 && ref.Valid() {
		return ref, true
	}
	return antigravityDigest(antigravityEmitterVersion, "session", value), true
}

func antigravityNumberRef(
	domain string,
	conversationID string,
	value *int64,
) (protocol.OpaqueID, bool) {
	if conversationID == "" || value == nil || *value < 0 {
		return "", false
	}
	return antigravityDigest(
		antigravityEmitterVersion, domain, conversationID, strconv.FormatInt(*value, 10),
	), true
}

func antigravityDigest(parts ...string) protocol.OpaqueID {
	digest := sha256.New()
	var length [8]byte
	for _, part := range parts {
		binary.BigEndian.PutUint64(length[:], uint64(len(part)))
		_, _ = digest.Write(length[:])
		_, _ = digest.Write([]byte(part))
	}
	return protocol.OpaqueID(hex.EncodeToString(digest.Sum(nil)))
}

func newAntigravityEventID(r io.Reader) (protocol.OpaqueID, bool) {
	var nonce [antigravityNonceBytes]byte
	if _, err := io.ReadFull(r, nonce[:]); err != nil {
		return "", false
	}
	return protocol.OpaqueID(antigravityEventIDPrefix + hex.EncodeToString(nonce[:])), true
}
