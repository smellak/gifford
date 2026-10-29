package agentsock

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

// sentAt is the fixed send time of the test frames.
var sentAt = time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)

const (
	testAgent   protocol.AgentID  = "agent-1"
	testSession protocol.OpaqueID = "sess-1"
)

// testFrame is one frame of op with body, answering replyTo when it is
// not zero.
func testFrame(t *testing.T, op protocol.AgentOp, id, replyTo int64, body any) protocol.AgentFrame {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	f := protocol.AgentFrame{Op: op, FrameID: id, SentAt: sentAt, Body: raw}
	if replyTo > 0 {
		f.ReplyTo = &replyTo
	}
	return f
}

// rawFrame is a length prefix and json, whatever json holds.
func rawFrame(json string) []byte {
	out := binary.BigEndian.AppendUint32(nil, uint32(len(json)))
	return append(out, json...)
}

// countingWriter counts the Write calls.
type countingWriter struct {
	bytes.Buffer
	writes int
}

func (w *countingWriter) Write(p []byte) (int, error) {
	w.writes++
	return w.Buffer.Write(p)
}

func TestFrameRoundTrip(t *testing.T) {
	t.Parallel()
	frames := []protocol.AgentFrame{
		testFrame(t, protocol.AgentOpHeartbeat, 1, 0, protocol.AgentTarget{AgentID: testAgent}),
		testFrame(t, protocol.AgentOpRegisterAgentResult, 2, 1, protocol.RegisterAgentResult{AgentID: testAgent, SessionID: testSession}),
		testFrame(t, protocol.AgentOpError, 3, 9, protocol.FrameError{Code: protocol.FrameErrorCodeUnsupportedOp, Message: "op not supported"}),
	}
	var w countingWriter
	for _, f := range frames {
		require.NoError(t, WriteFrame(&w, f))
	}
	require.Equal(t, len(frames), w.writes, "one Write per frame")

	r := bytes.NewReader(w.Bytes())
	for _, want := range frames {
		got, err := ReadFrame(r)
		require.NoError(t, err)
		require.Equal(t, want.Op, got.Op)
		require.Equal(t, want.FrameID, got.FrameID)
		require.Equal(t, want.ReplyTo, got.ReplyTo)
		require.True(t, want.SentAt.Equal(got.SentAt))
		require.JSONEq(t, string(want.Body), string(got.Body))
	}
	_, err := ReadFrame(r)
	require.ErrorIs(t, err, io.EOF, "the end of the stream between frames")
}

func TestWriteFramePrefixesTheLength(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	f := testFrame(t, protocol.AgentOpHeartbeat, 1, 0, protocol.AgentTarget{AgentID: testAgent})
	require.NoError(t, WriteFrame(&buf, f))
	n := binary.BigEndian.Uint32(buf.Bytes()[:4])
	require.Equal(t, buf.Len()-4, int(n), "4 bytes big-endian, then the JSON")
	require.True(t, json.Valid(buf.Bytes()[4:]))
}

func TestWriteFrameRefusesAnInvalidFrame(t *testing.T) {
	t.Parallel()
	for name, f := range map[string]protocol.AgentFrame{
		"no frame id":              testFrame(t, protocol.AgentOpHeartbeat, 0, 0, protocol.AgentTarget{AgentID: testAgent}),
		"an unknown op":            testFrame(t, "bogus", 1, 0, protocol.AgentTarget{AgentID: testAgent}),
		"a result without a reply": testFrame(t, protocol.AgentOpRegisterAgentResult, 1, 0, protocol.RegisterAgentResult{AgentID: testAgent, SessionID: testSession}),
		"a body of another op":     testFrame(t, protocol.AgentOpHeartbeat, 1, 0, protocol.FrameError{Code: protocol.FrameErrorCodeInternal, Message: "x"}),
		"no send time":             {Op: protocol.AgentOpHeartbeat, FrameID: 1, Body: protocol.Payload(`{"agent_id":"agent-1"}`)},
	} {
		var buf bytes.Buffer
		require.ErrorIs(t, WriteFrame(&buf, f), ErrInvalidFrame, name)
		require.Zero(t, buf.Len(), "%s: nothing written", name)
	}
}

func TestReadFrameRefusesALengthOverTheCap(t *testing.T) {
	t.Parallel()
	prefix := binary.BigEndian.AppendUint32(nil, MaxFrameBytes+1)
	r := bytes.NewReader(append(prefix, strings.Repeat("x", 64)...))
	_, err := ReadFrame(r)
	require.ErrorIs(t, err, ErrFrameTooLarge)
	require.Equal(t, 64, r.Len(), "nothing past the length is read")

	// The cap itself is a length the codec reads whole.
	r = bytes.NewReader(append(binary.BigEndian.AppendUint32(nil, MaxFrameBytes), bytes.Repeat([]byte("x"), MaxFrameBytes)...))
	_, err = ReadFrame(r)
	require.ErrorIs(t, err, ErrInvalidFrame, "at the cap: read, then refused as JSON")
	require.Zero(t, r.Len())
}

func TestReadFrameRefusesAnInvalidFrame(t *testing.T) {
	t.Parallel()
	const canary = "canary-7c2v"
	for name, tc := range map[string]struct {
		json string
		id   int64
	}{
		"not JSON":                 {json: `{"op":` + canary, id: 0},
		"an unknown field":         {json: `{"op":"heartbeat","frame_id":7,"sent_at":"2026-10-29T12:00:00Z","body":{"agent_id":"agent-1"},"x":"` + canary + `"}`, id: 7},
		"trailing data":            {json: `{"op":"heartbeat","frame_id":7,"sent_at":"2026-10-29T12:00:00Z","body":{"agent_id":"agent-1"}} {}`, id: 0},
		"an unknown op":            {json: `{"op":"bogus","frame_id":8,"sent_at":"2026-10-29T12:00:00Z","body":{}}`, id: 8},
		"no frame id":              {json: `{"op":"heartbeat","frame_id":0,"sent_at":"2026-10-29T12:00:00Z","body":{"agent_id":"agent-1"}}`, id: 0},
		"a result without a reply": {json: `{"op":"register_agent_result","frame_id":9,"sent_at":"2026-10-29T12:00:00Z","body":{"agent_id":"agent-1","session_id":"sess-1"}}`, id: 9},
		"a body field it lacks":    {json: `{"op":"heartbeat","frame_id":10,"sent_at":"2026-10-29T12:00:00Z","body":{"agent_id":"agent-1","text":"` + canary + `"}}`, id: 10},
	} {
		good := testFrame(t, protocol.AgentOpHeartbeat, 11, 0, protocol.AgentTarget{AgentID: testAgent})
		var stream bytes.Buffer
		stream.Write(rawFrame(tc.json))
		require.NoError(t, WriteFrame(&stream, good))

		got, err := ReadFrame(&stream)
		require.ErrorIs(t, err, ErrInvalidFrame, name)
		require.Equal(t, tc.id, got.FrameID, "%s: the id to answer", name)
		require.NotContains(t, err.Error(), canary, "%s: the reason never quotes the frame", name)

		next, err := ReadFrame(&stream)
		require.NoError(t, err, "%s: the stream stays aligned", name)
		require.Equal(t, int64(11), next.FrameID)
	}
}

func TestReadFrameCutShort(t *testing.T) {
	t.Parallel()
	_, err := ReadFrame(bytes.NewReader(nil))
	require.ErrorIs(t, err, io.EOF)
	_, err = ReadFrame(bytes.NewReader([]byte{0, 0}))
	require.ErrorIs(t, err, io.ErrUnexpectedEOF, "inside the length")
	_, err = ReadFrame(bytes.NewReader(rawFrame(`{"op":"heartbeat"}`)[:10]))
	require.ErrorIs(t, err, io.ErrUnexpectedEOF, "inside the JSON")
}
