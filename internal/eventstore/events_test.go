package eventstore

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

// testEvent returns a valid agent-scoped envelope; the id holds the writer
// number, so two writers never share an event_id.
func testEvent(writer int, session string, seq int64) protocol.EventEnvelope {
	return protocol.EventEnvelope{
		EventID:       protocol.ULID(fmt.Sprintf("01JB%01d%021d", writer, seq)),
		SchemaVersion: protocol.SchemaVersion,
		NodeID:        "node-a",
		SessionID:     protocol.OpaqueID(session),
		AgentID:       "agent-1",
		Harness:       protocol.HarnessGeneric,
		AdapterType:   protocol.AdapterTypeGenericPTY,
		SourceQuality: protocol.SourceQualitySynthetic,
		EventType:     protocol.EventTypeIOChunk,
		Direction:     protocol.DirectionOut,
		Stream:        protocol.StreamPTY,
		Origin:        protocol.OriginHarness,
		WallTime:      time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC).Add(time.Duration(seq) * time.Millisecond),
		MonotonicNS:   seq * 1_000_000,
		Sequence:      seq,
		Payload:       protocol.Payload(fmt.Sprintf(`{"n":%d}`, seq)),
		Sensitivity:   protocol.SensitivityNormal,
	}
}

// fullEnvelope sets every optional field; it passes Validate.
func fullEnvelope() protocol.EventEnvelope {
	return protocol.EventEnvelope{
		EventID:          "01JB0000000000000000000010",
		SchemaVersion:    protocol.SchemaVersion,
		NodeID:           "node-a",
		SessionID:        "sess-2",
		AgentID:          "agent-2",
		Harness:          protocol.HarnessClaudeCode,
		HarnessVersion:   "2.1.287",
		AdapterType:      protocol.AdapterTypeNativeMod,
		SourceQuality:    protocol.SourceQualityNativeStructured,
		EventType:        protocol.EventTypeToolStarted,
		Direction:        protocol.DirectionInternal,
		Stream:           protocol.StreamNativeEvent,
		Origin:           protocol.OriginHarness,
		WallTime:         time.Date(2026, 10, 26, 12, 41, 2, 123456789, time.FixedZone("+01:00", 3600)),
		MonotonicNS:      883425102931,
		Sequence:         7,
		TurnID:           "turn-712",
		ToolCallID:       "tool-99",
		MessageID:        "01JB0000000000000000000011",
		DeliveryID:       "dlv-1",
		DecisionID:       "chd_00000000000000000000000001",
		ParentEventID:    "01JB0000000000000000000012",
		CorrelationID:    "corr-1",
		Payload:          protocol.Payload("{ \"tool\" : \"go test\",\n  \"args\": [\"-race\"], \"note\": \"café ü €\" }"),
		Sensitivity:      protocol.SensitivitySensitive,
		RedactionApplied: true,
	}
}

// testFixtureEnvelope strictly decodes one protocol envelope fixture.
func testFixtureEnvelope(t *testing.T, name string) protocol.EventEnvelope {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "protocol", "testdata", "event_envelope", name))
	require.NoError(t, err)
	var envelope protocol.EventEnvelope
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	require.NoError(t, dec.Decode(&envelope))
	return envelope
}

// testEventCount returns the number of stored events.
func testEventCount(ctx context.Context, t *testing.T, store *Store) int {
	t.Helper()
	var count int
	require.NoError(t, store.DB().QueryRowContext(ctx, "SELECT count(*) FROM events").Scan(&count))
	return count
}

// TestAppendEventsRoundTrip stores the version fixtures and the full
// envelope, then reads each back unchanged.
func TestAppendEventsRoundTrip(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	ctx := t.Context()
	agent := testFixtureEnvelope(t, "valid_agent.json")
	gifford := testFixtureEnvelope(t, "valid_gifford.json")
	full := fullEnvelope()
	require.NoError(t, full.Validate())
	require.NoError(t, store.AppendEvents(ctx, []protocol.EventEnvelope{agent, gifford, full}))

	for _, original := range []protocol.EventEnvelope{agent, gifford, full} {
		got, err := store.SessionEvents(ctx, original.SessionID, original.Sequence, 1)
		require.NoError(t, err)
		require.Len(t, got, 1)
		want := original
		want.WallTime = original.WallTime.UTC()
		require.Equal(t, want, got[0])
	}

	var payloadType string
	require.NoError(t, store.DB().QueryRowContext(ctx,
		"SELECT typeof(payload_json) FROM events WHERE event_id = ?", string(full.EventID)).Scan(&payloadType))
	require.Equal(t, "text", payloadType)

	var sessionID, agentID, adapterType string
	var direction, stream sql.NullString
	require.NoError(t, store.DB().QueryRowContext(ctx,
		"SELECT session_id, agent_id, adapter_type, direction, stream FROM events WHERE event_id = ?",
		string(gifford.EventID)).Scan(&sessionID, &agentID, &adapterType, &direction, &stream))
	require.Empty(t, sessionID)
	require.Empty(t, agentID)
	require.Empty(t, adapterType)
	require.False(t, direction.Valid)
	require.False(t, stream.Valid)

	var wallTime string
	require.NoError(t, store.DB().QueryRowContext(ctx,
		"SELECT wall_time FROM events WHERE event_id = ?", string(agent.EventID)).Scan(&wallTime))
	require.Equal(t, "2026-10-26T11:41:02.314000000Z", wallTime)
}

// testDuplicateStore opens a store holding (s, 1) and (s, 2).
func testDuplicateStore(t *testing.T) *Store {
	t.Helper()
	store := testStore(t)
	require.NoError(t, store.AppendEvents(t.Context(), []protocol.EventEnvelope{
		testEvent(1, "s", 1),
		testEvent(1, "s", 2),
	}))
	return store
}

// TestAppendEventsDuplicate refuses batches that repeat an event_id or a
// session sequence, storing nothing: two rows remain.
func TestAppendEventsDuplicate(t *testing.T) {
	t.Parallel()

	t.Run("sequence twice", func(t *testing.T) {
		t.Parallel()
		store := testDuplicateStore(t)
		ctx := t.Context()
		refused := testEvent(2, "s", 2)
		err := store.AppendEvents(ctx, []protocol.EventEnvelope{testEvent(2, "s", 3), refused})
		require.ErrorIs(t, err, ErrDuplicateEvent)
		var dup *DuplicateEventError
		require.ErrorAs(t, err, &dup)
		require.Equal(t, 1, dup.Index)
		require.Equal(t, refused.EventID, dup.EventID)
		require.Equal(t, protocol.OpaqueID("s"), dup.SessionID)
		require.Equal(t, int64(2), dup.Sequence)
		got, queryErr := store.SessionEvents(ctx, "s", 3, 1)
		require.NoError(t, queryErr)
		require.Empty(t, got)
		require.Equal(t, 2, testEventCount(ctx, t, store))
	})

	t.Run("event id twice", func(t *testing.T) {
		t.Parallel()
		store := testDuplicateStore(t)
		ctx := t.Context()
		stored := testEvent(1, "s", 1)
		moved := stored
		moved.SessionID = "t"
		err := store.AppendEvents(ctx, []protocol.EventEnvelope{moved})
		require.ErrorIs(t, err, ErrDuplicateEvent)
		var dup *DuplicateEventError
		require.ErrorAs(t, err, &dup)
		require.Equal(t, 0, dup.Index)
		require.Equal(t, stored.EventID, dup.EventID)
		require.Equal(t, 2, testEventCount(ctx, t, store))
	})

	t.Run("one batch one key", func(t *testing.T) {
		t.Parallel()
		store := testDuplicateStore(t)
		ctx := t.Context()
		err := store.AppendEvents(ctx, []protocol.EventEnvelope{
			testEvent(1, "u", 9),
			testEvent(2, "u", 9),
		})
		require.ErrorIs(t, err, ErrDuplicateEvent)
		var dup *DuplicateEventError
		require.ErrorAs(t, err, &dup)
		require.Equal(t, 1, dup.Index)
		require.Equal(t, protocol.OpaqueID("u"), dup.SessionID)
		require.Equal(t, int64(9), dup.Sequence)
		require.Equal(t, 2, testEventCount(ctx, t, store))
	})
}

// TestAppendEventsInvalid stores nothing when validation, the batch size
// or the context fails.
func TestAppendEventsInvalid(t *testing.T) {
	t.Parallel()

	t.Run("invalid envelope", func(t *testing.T) {
		t.Parallel()
		store := testStore(t)
		ctx := t.Context()
		broken := testEvent(1, "s", 1)
		broken.NodeID = ""
		batch := []protocol.EventEnvelope{testEvent(1, "s", 0), testEvent(1, "s", 9), broken}
		err := store.AppendEvents(ctx, batch)
		require.ErrorIs(t, err, protocol.ErrInvalid)
		require.True(t, strings.HasPrefix(err.Error(), "event 2:"), err.Error())
		require.Equal(t, 0, testEventCount(ctx, t, store))
	})

	t.Run("unsupported version", func(t *testing.T) {
		t.Parallel()
		store := testStore(t)
		ctx := t.Context()
		broken := testEvent(1, "s", 1)
		broken.SchemaVersion = 2
		err := store.AppendEvents(ctx, []protocol.EventEnvelope{broken})
		require.ErrorIs(t, err, protocol.ErrUnsupportedVersion)
		require.Equal(t, 0, testEventCount(ctx, t, store))
	})

	t.Run("batch too large", func(t *testing.T) {
		t.Parallel()
		store := testStore(t)
		ctx := t.Context()
		err := store.AppendEvents(ctx, make([]protocol.EventEnvelope, MaxBatchEvents+1))
		require.ErrorIs(t, err, ErrInvalidArgument)
		require.Equal(t, 0, testEventCount(ctx, t, store))
	})

	t.Run("canceled context", func(t *testing.T) {
		t.Parallel()
		store := testStore(t)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		err := store.AppendEvents(ctx, []protocol.EventEnvelope{testEvent(1, "s", 1)})
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 0, testEventCount(context.Background(), t, store))
	})

	t.Run("empty batch", func(t *testing.T) {
		t.Parallel()
		store := testStore(t)
		ctx := t.Context()
		require.NoError(t, store.AppendEvents(ctx, nil))
		require.NoError(t, store.AppendEvents(ctx, []protocol.EventEnvelope{}))
		require.Equal(t, 0, testEventCount(ctx, t, store))
	})
}

// TestAppendEventsTx proves the caller-owned transaction is the only atomic
// boundary: the helper neither commits nor rolls back and it never needs a
// second pooled connection.
func TestAppendEventsTx(t *testing.T) {
	t.Parallel()

	t.Run("caller commits", func(t *testing.T) {
		t.Parallel()
		store := testStore(t)
		store.DB().SetMaxOpenConns(1)
		tx, err := store.DB().BeginTx(t.Context(), nil)
		require.NoError(t, err)
		t.Cleanup(func() { _ = tx.Rollback() })
		require.NoError(t, store.AppendEventsTx(t.Context(), tx,
			[]protocol.EventEnvelope{testEvent(1, "tx-commit", 1)}))
		var inTransaction int
		require.NoError(t, tx.QueryRowContext(t.Context(), "SELECT count(*) FROM events").Scan(&inTransaction))
		require.Equal(t, 1, inTransaction)
		require.NoError(t, tx.Commit())
		require.Equal(t, 1, testEventCount(t.Context(), t, store))
	})

	t.Run("caller rolls back", func(t *testing.T) {
		t.Parallel()
		store := testStore(t)
		tx, err := store.DB().BeginTx(t.Context(), nil)
		require.NoError(t, err)
		require.NoError(t, store.AppendEventsTx(t.Context(), tx,
			[]protocol.EventEnvelope{testEvent(1, "tx-rollback", 1)}))
		require.NoError(t, tx.Rollback())
		require.Equal(t, 0, testEventCount(t.Context(), t, store))
	})

	t.Run("validation leaves transaction usable", func(t *testing.T) {
		t.Parallel()
		store := testStore(t)
		tx, err := store.DB().BeginTx(t.Context(), nil)
		require.NoError(t, err)
		t.Cleanup(func() { _ = tx.Rollback() })
		broken := testEvent(1, "tx-invalid", 1)
		broken.NodeID = ""
		err = store.AppendEventsTx(t.Context(), tx, []protocol.EventEnvelope{broken})
		require.ErrorIs(t, err, protocol.ErrInvalid)
		var count int
		require.NoError(t, tx.QueryRowContext(t.Context(), "SELECT count(*) FROM events").Scan(&count))
		require.Zero(t, count)
	})

	t.Run("arguments", func(t *testing.T) {
		t.Parallel()
		store := testStore(t)
		require.ErrorIs(t, store.AppendEventsTx(t.Context(), nil,
			[]protocol.EventEnvelope{testEvent(1, "tx-nil", 1)}), ErrInvalidArgument)
		require.NoError(t, store.AppendEventsTx(t.Context(), nil, nil))
	})
}

func TestNextEventSequenceTxUsesCallerTransaction(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	store.DB().SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	tx, err := store.DB().BeginTx(ctx, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	next, err := store.NextEventSequenceTx(ctx, tx, "sequence-session")
	require.NoError(t, err)
	require.Zero(t, next)
	require.NoError(t, store.AppendEventsTx(ctx, tx,
		[]protocol.EventEnvelope{testEvent(1, "sequence-session", next)}))
	next, err = store.NextEventSequenceTx(ctx, tx, "sequence-session")
	require.NoError(t, err)
	require.Equal(t, int64(1), next)

	_, err = store.NextEventSequenceTx(ctx, nil, "sequence-session")
	require.ErrorIs(t, err, ErrInvalidArgument)
}

// TestSessionEvents reads one window of one session in sequence order.
func TestSessionEvents(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	ctx := t.Context()
	batch := make([]protocol.EventEnvelope, 0, 12)
	for seq := range 10 {
		batch = append(batch, testEvent(1, "a", int64(seq)))
	}
	batch = append(batch, testEvent(2, "b", 0), testEvent(2, "b", 1))
	require.NoError(t, store.AppendEvents(ctx, batch))

	got, err := store.SessionEvents(ctx, "a", 3, 4)
	require.NoError(t, err)
	require.Len(t, got, 4)
	for i, event := range got {
		require.Equal(t, int64(3+i), event.Sequence)
		require.Equal(t, protocol.OpaqueID("a"), event.SessionID)
	}

	for _, limit := range []int{0, MaxBatchEvents + 1} {
		_, err := store.SessionEvents(ctx, "a", 0, limit)
		require.ErrorIs(t, err, ErrInvalidArgument)
	}

	_, err = store.DB().ExecContext(ctx, `INSERT INTO events
		(event_id, schema_version, node_id, session_id, agent_id, adapter_type,
		 source_quality, event_type, wall_time, monotonic_ns, sequence,
		 payload_json, sensitivity, redaction_applied)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"01JB0000000000000000000099", 1, "node-a", "bad", "agent-1", "generic_pty",
		"synthetic", "io.chunk", "not-a-time", 0, 0, `{}`, "normal", 0)
	require.NoError(t, err)
	events, err := store.SessionEvents(ctx, "bad", 0, 10)
	require.Error(t, err)
	require.Nil(t, events)
}

// TestAppendEventsGoroutines writes from four goroutines through one
// store without losing or doubling an event.
func TestAppendEventsGoroutines(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	ctx := t.Context()
	const writers = 4
	const perWriter = 1000
	const batchSize = 250
	errs := make(chan error, writers)
	var wg sync.WaitGroup
	for writer := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			session := fmt.Sprintf("g%d", writer)
			for start := 0; start < perWriter; start += batchSize {
				batch := make([]protocol.EventEnvelope, 0, batchSize)
				for seq := start; seq < start+batchSize; seq++ {
					batch = append(batch, testEvent(writer, session, int64(seq)))
				}
				if err := store.AppendEvents(ctx, batch); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	for writer := range writers {
		session := fmt.Sprintf("g%d", writer)
		var count, distinct int64
		var minSeq, maxSeq int64
		require.NoError(t, store.DB().QueryRowContext(ctx,
			"SELECT count(*), count(DISTINCT sequence), min(sequence), max(sequence) FROM events WHERE session_id = ?",
			session).Scan(&count, &distinct, &minSeq, &maxSeq))
		require.Equal(t, int64(perWriter), count)
		require.Equal(t, int64(perWriter), distinct)
		require.Equal(t, int64(0), minSeq)
		require.Equal(t, int64(perWriter-1), maxSeq)
	}
}

// TestHelperWriter is the child of TestTwoProcessWriters: it writes 5,000
// events in batches of 250 to the database its environment names.
func TestHelperWriter(t *testing.T) {
	if os.Getenv("GIFFORD_EVENTSTORE_HELPER") != "1" {
		t.Skip("event store writer helper")
	}
	writer, err := strconv.Atoi(os.Getenv("GIFFORD_EVENTSTORE_WRITER"))
	require.NoError(t, err)
	path := os.Getenv("GIFFORD_EVENTSTORE_DB")
	require.NotEmpty(t, path)
	store, err := Open(t.Context(), path)
	require.NoError(t, err)
	ctx := t.Context()
	session := fmt.Sprintf("proc-%d", writer)
	for start := 0; start < 5000; start += 250 {
		batch := make([]protocol.EventEnvelope, 0, 250)
		for seq := start; seq < start+250; seq++ {
			batch = append(batch, testEvent(writer, session, int64(seq)))
		}
		require.NoError(t, store.AppendEvents(ctx, batch))
	}
	require.NoError(t, store.Close())
}

// TestTwoProcessWriters stores 10,000 events from two processes in
// batches of 250 with contiguous sequences.
func TestTwoProcessWriters(t *testing.T) {
	t.Parallel()

	path := testStorePath(t)
	store, err := Open(t.Context(), path)
	require.NoError(t, err)
	require.NoError(t, store.Close())

	ctx := t.Context()
	type child struct {
		cmd *exec.Cmd
		out *bytes.Buffer
	}
	children := make([]child, 0, 2)
	for _, writer := range []string{"1", "2"} {
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestHelperWriter$")
		cmd.Env = append(os.Environ(),
			"GIFFORD_EVENTSTORE_HELPER=1",
			"GIFFORD_EVENTSTORE_DB="+path,
			"GIFFORD_EVENTSTORE_WRITER="+writer)
		var out bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &out
		require.NoError(t, cmd.Start())
		children = append(children, child{cmd: cmd, out: &out})
	}
	for _, c := range children {
		require.NoError(t, c.cmd.Wait(), "writer child failed: %s", c.out.String())
	}

	store, err = Open(ctx, path)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	var count int64
	require.NoError(t, store.DB().QueryRowContext(ctx, "SELECT count(*) FROM events").Scan(&count))
	require.Equal(t, int64(10000), count)
	for _, session := range []string{"proc-1", "proc-2"} {
		var perSession, distinct, minSeq, maxSeq int64
		require.NoError(t, store.DB().QueryRowContext(ctx,
			"SELECT count(*), count(DISTINCT sequence), min(sequence), max(sequence) FROM events WHERE session_id = ?",
			session).Scan(&perSession, &distinct, &minSeq, &maxSeq))
		require.Equal(t, int64(5000), perSession)
		require.Equal(t, int64(5000), distinct)
		require.Equal(t, int64(0), minSeq)
		require.Equal(t, int64(4999), maxSeq)
	}
	var integrity string
	require.NoError(t, store.DB().QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&integrity))
	require.Equal(t, "ok", integrity)
}
