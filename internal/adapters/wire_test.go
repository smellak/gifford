package adapters

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

func TestMessageRoundTrip(t *testing.T) {
	t.Parallel()

	manifest := nativeManifest()
	want := Message{Operation: OperationManifest, ID: 2, ReplyTo: 1, Manifest: &manifest}
	var stream bytes.Buffer
	require.NoError(t, WriteMessage(&stream, want))

	body := stream.Bytes()[messageLengthBytes:]
	require.Equal(t, uint32(len(body)), binary.BigEndian.Uint32(stream.Bytes()[:messageLengthBytes]))
	got, err := ReadMessage(&stream)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestMessageCodecRefusesBadInput(t *testing.T) {
	t.Parallel()

	var stream bytes.Buffer
	require.ErrorIs(t, WriteMessage(&stream, Message{Operation: OperationPing}), ErrInvalidMessage)
	require.Empty(t, stream.Bytes())

	body := `{"op":"ping","id":1,"extra":true}`
	var framed bytes.Buffer
	require.NoError(t, binary.Write(&framed, binary.BigEndian, uint32(len(body))))
	framed.WriteString(body)
	_, err := ReadMessage(&framed)
	require.ErrorIs(t, err, ErrInvalidMessage)

	framed.Reset()
	require.NoError(t, binary.Write(&framed, binary.BigEndian, uint32(MaxMessageBytes+1)))
	_, err = ReadMessage(&framed)
	require.ErrorIs(t, err, ErrMessageTooLarge)
}

func TestWriteMessageRefusesOversizeManifest(t *testing.T) {
	t.Parallel()

	manifest := nativeManifest()
	manifest.HarnessVersion = strings.Repeat("x", MaxMessageBytes)
	var stream bytes.Buffer
	err := WriteMessage(&stream, Message{Operation: OperationManifest, ID: 2, ReplyTo: 1, Manifest: &manifest})
	require.ErrorIs(t, err, ErrMessageTooLarge)
	require.Empty(t, stream.Bytes())
}

func nativeManifest() protocol.CapabilityManifest {
	return protocol.CapabilityManifest{
		SchemaVersion:      protocol.SchemaVersion,
		AgentID:            "agent-1",
		Harness:            protocol.HarnessCodex,
		HarnessVersion:     "1.2.3",
		AdapterType:        protocol.AdapterTypeNativeAPIThinPTY,
		InputGuardProvider: protocol.InputGuardProviderPTYProxy,
		CaptureMode:        protocol.CaptureModePTYProxy,
		Deliverable:        true,
		Capabilities: protocol.Capabilities{
			StructuredEvents: true,
			ToolEvents:       true,
			PTYCapture:       true,
			MCP:              true,
		},
	}
}
