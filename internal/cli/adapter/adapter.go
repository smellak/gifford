// Package adapter implements the hook-safe `gifford adapter emit` client.
package adapter

import (
	"context"
	"errors"
	"net"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/nativeevents"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	// SocketEnvironment is set only inside a run with an active native entry.
	SocketEnvironment = nativeevents.SocketEnvironment
	// EmitTimeout bounds dial, hello, event and durable acknowledgement together.
	EmitTimeout = 250 * time.Millisecond
)

type emitFunc func(context.Context, string, nativeevents.Hello, nativeevents.Event) error

// NewCmd returns the gifford adapter command tree.
func NewCmd(_ *deps.Deps) *cobra.Command {
	return newCmd(emit, clock.System{}.Now)
}

func newCmd(send emitFunc, now func() time.Time) *cobra.Command {
	return deps.Group("adapter", "Native adapter event utilities", newEmitCmd(send, now))
}

func newEmitCmd(send emitFunc, now func() time.Time) *cobra.Command {
	var (
		harness        string
		harnessVersion string
		emitter        string
		schemaVersion  int64
		sequence       int64
		emitterEventID string
		eventType      string
		observedAt     int64
		sessionRef     string
		turnRef        string
		toolRef        string
		requestRef     string
		result         string
		toolKind       string
	)
	cmd := &cobra.Command{
		Use:   "emit",
		Short: "Emit one content-free native lifecycle event (always exits 0)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if observedAt == 0 {
				observedAt = now().UnixMilli()
			}
			hello := nativeevents.Hello{
				Operation: nativeevents.OperationHello, Harness: protocol.Harness(harness),
				HarnessVersion: harnessVersion, EmitterType: nativeevents.EmitterType(emitter),
				SchemaVersion: schemaVersion,
			}
			event := nativeevents.Event{
				Operation: nativeevents.OperationEvent, Sequence: sequence,
				EmitterEventID: protocol.OpaqueID(emitterEventID), Type: protocol.EventType(eventType),
				ObservedAt: observedAt, SessionRef: protocol.OpaqueID(sessionRef),
				TurnRef: protocol.OpaqueID(turnRef), ToolRef: protocol.OpaqueID(toolRef),
				RequestRef: protocol.OpaqueID(requestRef), Result: protocol.NativeEventResult(result),
				ToolKind: protocol.NativeToolKind(toolKind),
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), EmitTimeout)
			defer cancel()
			// This command is called from harness hooks. Any internal failure,
			// including malformed supplied metadata, is deliberately swallowed.
			_ = send(ctx, os.Getenv(SocketEnvironment), hello, event)
			return nil
		},
	}
	cmd.Flags().StringVar(&harness, "harness", "", "harness that emitted the event")
	cmd.Flags().StringVar(&harnessVersion, "harness-version", "", "measured harness version")
	cmd.Flags().StringVar(&emitter, "emitter-type", "", "emitter type: plugin, hook or mod")
	cmd.Flags().Int64Var(&schemaVersion, "schema-version", nativeevents.SchemaVersion, "native link schema version")
	cmd.Flags().Int64Var(&sequence, "seq", 1, "connection-local event sequence")
	cmd.Flags().StringVar(&emitterEventID, "emitter-event-id", "", "stable event id for retries")
	cmd.Flags().StringVar(&eventType, "type", "", "closed native event type")
	cmd.Flags().Int64Var(&observedAt, "observed-at", 0, "emitter clock in Unix milliseconds (now when omitted)")
	cmd.Flags().StringVar(&sessionRef, "session-ref", "", "optional emitter session reference")
	cmd.Flags().StringVar(&turnRef, "turn-ref", "", "optional emitter turn reference")
	cmd.Flags().StringVar(&toolRef, "tool-ref", "", "tool reference (required for tool events)")
	cmd.Flags().StringVar(&requestRef, "request-ref", "", "request reference (required for request events)")
	cmd.Flags().StringVar(&result, "result", "", "optional result: ok, failed, cancelled or denied")
	cmd.Flags().StringVar(&toolKind, "tool-kind", "", "optional tool kind: shell, file, search, web, mcp or other")
	return cmd
}

func emit(
	ctx context.Context,
	path string,
	hello nativeevents.Hello,
	event nativeevents.Event,
) error {
	if path == "" || hello.Validate() != nil || event.Validate() != nil {
		return errors.New("adapter emit unavailable")
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", path)
	if err != nil {
		return err //nolint:wrapcheck // the hook command does not expose this error
	}
	defer func() { _ = conn.Close() }()
	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			return err //nolint:wrapcheck // internal and intentionally swallowed
		}
	}
	if err := nativeevents.WriteMessage(conn, hello); err != nil {
		return err //nolint:wrapcheck
	}
	message, err := nativeevents.ReadMessage(conn)
	if err != nil {
		return err //nolint:wrapcheck
	}
	helloResult, ok := message.(nativeevents.HelloResult)
	if !ok || !helloResult.Accepted {
		return errors.New("adapter emit rejected")
	}
	if err := nativeevents.WriteMessage(conn, event); err != nil {
		return err //nolint:wrapcheck
	}
	message, err = nativeevents.ReadMessage(conn)
	if err != nil {
		return err //nolint:wrapcheck
	}
	ack, ok := message.(nativeevents.Ack)
	if !ok || ack.Sequence != event.Sequence || ack.EmitterEventID != event.EmitterEventID {
		return errors.New("adapter emit invalid acknowledgement")
	}
	return nil
}
