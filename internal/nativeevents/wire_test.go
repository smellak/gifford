package nativeevents

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

func wireHello() Hello {
	return Hello{
		Operation: OperationHello, Harness: protocol.HarnessCodex,
		HarnessVersion: "1.2.3", EmitterType: EmitterTypePlugin,
		SchemaVersion: SchemaVersion,
	}
}

func wireEvent() Event {
	return Event{
		Operation: OperationEvent, Sequence: 1, EmitterEventID: "event-1",
		Type: protocol.EventTypeToolCompleted, ObservedAt: 1793509200123,
		TurnRef: "turn-1", ToolRef: "tool-1", Result: protocol.NativeEventResultOK,
		ToolKind: protocol.NativeToolKindShell,
	}
}

func TestWireMessagesRoundTrip(t *testing.T) {
	t.Parallel()
	messages := []any{
		wireHello(),
		HelloResult{Operation: OperationHelloResult, Accepted: true},
		wireEvent(),
		Ack{Operation: OperationAck, Sequence: 1, EmitterEventID: "event-1"},
	}
	for _, want := range messages {
		var stream bytes.Buffer
		require.NoError(t, WriteMessage(&stream, want))
		body := stream.Bytes()[messageLengthBytes:]
		require.Equal(t, uint32(len(body)), binary.BigEndian.Uint32(stream.Bytes()[:messageLengthBytes]))
		got, err := ReadMessage(&stream)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
}

func TestWireRejectsUnknownFieldsAndOversize(t *testing.T) {
	t.Parallel()
	bad := []byte(`{"op":"event","seq":1,"emitter_event_id":"event-1","type":"turn.started","observed_at":1,"prompt":"secret"}`)
	framed := binary.BigEndian.AppendUint32(nil, uint32(len(bad)))
	framed = append(framed, bad...)
	_, err := ReadMessage(bytes.NewReader(framed))
	require.ErrorIs(t, err, ErrInvalidMessage)

	prefix := binary.BigEndian.AppendUint32(nil, MaxMessageBytes+1)
	_, err = ReadMessage(bytes.NewReader(prefix))
	require.ErrorIs(t, err, ErrMessageTooLarge)
}

func TestWireQuestionResolvedNeedsRequestAndResult(t *testing.T) {
	t.Parallel()
	event := wireEvent()
	event.Type = protocol.EventTypeQuestionResolved
	event.ToolRef = ""
	event.RequestRef = ""
	require.ErrorIs(t, event.Validate(), ErrInvalidMessage)
	event.RequestRef = "request-1"
	require.NoError(t, event.Validate())
	event.Result = ""
	require.ErrorIs(t, event.Validate(), ErrInvalidMessage)
}

func TestHelloResultReasonsAreClosed(t *testing.T) {
	t.Parallel()
	require.NoError(t, (HelloResult{
		Operation: OperationHelloResult, Reason: HelloRejectPeerNotInTree,
	}).Validate())
	require.Error(t, (HelloResult{
		Operation: OperationHelloResult, Accepted: true, Reason: HelloRejectPeerNotInTree,
	}).Validate())
	require.Error(t, (HelloResult{
		Operation: OperationHelloResult, Reason: "future_reason",
	}).Validate())
}
