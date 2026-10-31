//go:build unix

package session

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/agentsock"
	"github.com/smellak/gifford/internal/protocol"
)

// sockLineage is the lineage of the agent socket the tests listen on: its
// agent, the session its Core greets with and its harness.
func sockLineage() DurableLineage {
	l := testLineage()
	l.AgentID, l.SessionID = sockAgent, sockSession
	return l
}

// durableRun is a live session whose events also go to a durable queue.
type durableRun struct {
	session *Session
	queue   *DurableQueue
	sink    *chanSink
	master  *os.File
	out     string
	done    chan runResult
}

// floodBytes is how much output a line holding "flood" makes the durable
// harness print.
const floodBytes = 12000

// writeDurableHarness drops a fake harness like writeHarnessScript's that
// also prints floodBytes of output, and no newline, for a line holding
// "flood".
func writeDurableHarness(t *testing.T) string {
	t.Helper()
	script := filepath.Join(t.TempDir(), "durableharness")
	body := "#!/bin/sh\nOUT=\"$1\"\nstty icrnl -echo\necho durable-ready\n" +
		"while IFS= read -r line; do\n" +
		"  printf '%s\\n' \"$line\" >> \"$OUT\"\n" +
		"  case \"$line\" in *__done__*) exit 0;;\n" +
		"    *flood*) head -c " + strconv.Itoa(floodBytes) + " /dev/zero | tr '\\0' x;;\n  esac\ndone\n"
	require.NoError(t, os.WriteFile(script, []byte(body), 0o700))
	return script
}

// startDurableSession runs the durable harness with every event going to
// the queue as well, and returns once it is ready. The run passes the
// registered harness version.
func startDurableSession(t *testing.T, id string, edit func(*Options)) *durableRun {
	t.Helper()
	q, err := NewDurableQueue(sockLineage())
	require.NoError(t, err)
	out := filepath.Join(t.TempDir(), "harness.out")
	master, slave := testPTY(t)
	sink := &chanSink{}
	opts := testOptions(t.TempDir(), id, slave, slave, fanoutSink{sinks: []EventSink{sink, q}},
		[]string{writeDurableHarness(t), out})
	opts.HarnessVersion = q.lineage.HarnessVersion
	if edit != nil {
		edit(&opts)
	}
	s, err := NewSession(opts)
	require.NoError(t, err)
	done := runAsync(t, s)
	waitRaw(t, slave)
	readUntil(t, master, "durable-ready")
	return &durableRun{session: s, queue: q, sink: sink, master: master, out: out, done: done}
}

// durableTypes counts the session's events of a durable type.
func durableTypes(events []Event) int {
	n := 0
	for _, e := range events {
		if protocol.IsDurableEventType(e.Type) {
			n++
		}
	}
	return n
}

// A real session's events reach the Core over the agent socket, one at a
// time, each settled before the next: every event of a durable type the
// session emitted, in its order, validating, with the registered lineage
// and its own id. No frame carries a typed byte, an output byte or a
// content hash.
func TestDurableEventsReachTheCore(t *testing.T) {
	t.Parallel()
	r := startDurableSession(t, "durable-core", nil)
	srv := listenAgentSocketWith(t, r.session, nil, func(o *agentsock.Options) { o.Durable = r.queue })
	k, _ := greetCore(t, srv.Path())
	k.send(protocol.AgentOpAnnounceSupport, 0, protocol.AgentSocketSupport{DurableEvents: true})

	const typed = "qz7canary"
	writeKeys(t, r.master, typed+"\r")
	require.Eventually(t, func() bool {
		return strings.Contains(harnessOut(t, r.out), typed)
	}, 10*time.Second, time.Millisecond, "the line never reached the harness")
	endDeliverySession(t, r.master, r.done)

	var got []protocol.EventEnvelope
	ids := make(map[protocol.ULID]bool)
	for len(got) == 0 || got[len(got)-1].EventType != protocol.EventTypePTYClosed {
		f := k.read()
		if f.Op != protocol.AgentOpEmitEvent {
			continue
		}
		var e protocol.EventEnvelope
		require.NoError(t, json.Unmarshal(f.Body, &e))
		require.NoError(t, protocol.ValidateDurableEventEnvelope(e), e.EventType)
		l := sockLineage()
		require.Equal(t, l.NodeID, e.NodeID)
		require.Equal(t, l.SessionID, e.SessionID)
		require.Equal(t, l.AgentID, e.AgentID)
		require.False(t, ids[e.EventID], "one id per event")
		ids[e.EventID] = true
		got = append(got, e)
		k.send(protocol.AgentOpEmitEventResult, f.FrameID,
			protocol.EmitEventResult{EventID: e.EventID, Status: protocol.EmitEventStatusStored})
	}

	var want []Event
	for _, e := range r.sink.all() {
		if protocol.IsDurableEventType(e.Type) {
			want = append(want, e)
		}
	}
	require.Len(t, got, len(want), "every durable event, none dropped")
	for i, e := range got {
		require.Equal(t, want[i].Type, e.EventType)
		require.Equal(t, want[i].Sequence, e.Sequence)
	}
	require.Equal(t, protocol.EventTypePTYOpened, got[0].EventType)
	seen := make(map[protocol.EventType]bool)
	for _, e := range got {
		seen[e.EventType] = true
	}
	for _, kind := range []protocol.EventType{
		protocol.EventTypeIOChunk, protocol.EventTypeInputKeyChunk, protocol.EventTypeInputDraftOpened,
	} {
		require.True(t, seen[kind], "output and input reach the Core: %s", kind)
	}
	raw := k.raw.String()
	for _, secret := range []string{typed, "chunk_digest", "screen_hash_after", "durable-ready"} {
		require.NotContains(t, raw, secret)
	}
	// The last result may still be on its way to the queue.
	require.Eventually(t, func() bool {
		return r.queue.Report() == DurableReport{Stored: int64(len(got))}
	}, 10*time.Second, time.Millisecond)
}

// A Core that takes the events but never settles one holds only the
// durable events: the first stays in flight, written once, the queue fills
// to its cap and drops the rest, counted, and a line typed meanwhile reaches
// the harness at once.
func TestDurableWaitNeverDelaysKeys(t *testing.T) {
	t.Parallel()
	// One byte a piece: each output byte is an io.chunk of its own.
	r := startDurableSession(t, "durable-jam", func(o *Options) {
		o.ChunkMinBytes, o.ChunkMaxBytes = 1, 1
	})
	srv := listenAgentSocketWith(t, r.session, nil, func(o *agentsock.Options) { o.Durable = r.queue })
	k, _ := greetCore(t, srv.Path())
	k.send(protocol.AgentOpAnnounceSupport, 0, protocol.AgentSocketSupport{DurableEvents: true})

	var mu sync.Mutex
	emitted := 0
	reading := make(chan struct{})
	go func() {
		defer close(reading)
		for {
			f, err := agentsock.ReadFrame(k.c)
			if err != nil {
				return
			}
			if f.Op == protocol.AgentOpEmitEvent {
				mu.Lock()
				emitted++
				mu.Unlock()
			}
		}
	}()
	go func() { _, _ = io.Copy(io.Discard, r.master) }()

	writeKeys(t, r.master, "flood\r")
	require.Eventually(t, func() bool {
		chunks := 0
		for _, e := range r.sink.all() {
			if e.Type == protocol.EventTypeIOChunk {
				chunks++
			}
		}
		return chunks >= floodBytes
	}, 60*time.Second, 10*time.Millisecond, "the flood never came through")
	require.Positive(t, r.queue.Report().Dropped, "the queue filled")

	start := time.Now()
	writeKeys(t, r.master, "marker\r")
	require.Eventually(t, func() bool {
		return strings.Contains(harnessOut(t, r.out), "marker")
	}, 10*time.Second, time.Millisecond, "the line never reached the harness")
	require.Less(t, time.Since(start), 2*time.Second, "the wait delayed the human")
	endDeliverySession(t, r.master, r.done)

	report := r.queue.Report()
	require.Equal(t, int64(1), report.Unconfirmed, "the first event waits, in flight")
	require.Zero(t, report.Stored)
	require.Zero(t, report.Invalid)
	require.Equal(t, int64(durableTypes(r.sink.all())), report.Unconfirmed+report.Pending+report.Dropped,
		"every durable event queued or counted as dropped")
	head, ok := r.queue.Head()
	require.True(t, ok)
	require.Equal(t, protocol.EventTypePTYOpened, head.EventType, "the head stays")
	r.queue.mu.Lock()
	require.LessOrEqual(t, r.queue.bytes, MaxDurableBytes)
	r.queue.mu.Unlock()

	require.NoError(t, srv.Close())
	<-reading
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 1, emitted, "written once, never again while it waits")
}

// A delivery the session refuses before writing anything gets its fixed
// code on the event: a bad line is LINE_INVALID and a mode the session does
// not write is MODE_INVALID. The events file keeps the error's text.
func TestDurableDeliveryFailureFromTheSession(t *testing.T) {
	t.Parallel()
	dr := startDeliverySession(t, "durable-fail", 0, false)
	for _, c := range []struct {
		line string
		mode protocol.InjectMode
		want protocol.DurableDeliveryFailureReason
	}{
		{line: "a\nb", mode: protocol.InjectModeTyped, want: protocol.DurableDeliveryFailureLineInvalid},
		{line: "hi", want: protocol.DurableDeliveryFailureModeInvalid},
	} {
		rep, err := dr.session.PrepareDelivery("d1", 0, 0)
		require.NoError(t, err)
		require.Equal(t, DeliveryPreparedOutcome, rep.Outcome)
		_, err = dr.session.CommitDelivery("d1", c.line, c.mode)
		require.Error(t, err)
		failed := sinkPayloads(dr.sink, protocol.EventTypeInputDeliveryFailed)
		last, ok := failed[len(failed)-1].(DeliveryFailed)
		require.True(t, ok)
		require.Equal(t, c.want, last.Code)
		require.Equal(t, err.Error(), last.Reason)
	}
	require.Empty(t, harnessOut(t, dr.out))
	endDeliverySession(t, dr.master, dr.done)
}
