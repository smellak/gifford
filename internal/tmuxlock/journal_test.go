package tmuxlock

import (
	"bytes"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// memoryState is an in-memory StateFile. Scripted faults replace individual
// pwrite or fdatasync calls; synced keeps the record after every successful
// fdatasync, the sequence of durable records.
type memoryState struct {
	raw       []byte
	writeCall int
	syncCall  int
	writes    map[int]func(p []byte, offset int64) (int, bool, error)
	syncErrs  map[int]error
	synced    [][]byte
	closed    bool
}

func newMemoryState(t *testing.T, record stateRecord) *memoryState {
	t.Helper()
	// Plant the raw bytes without policy validation: the file may hold
	// anything and refusal is BeginAttempt's job, which still validates.
	raw, err := encodeRecord(stateMagic, record)
	require.NoError(t, err)
	return &memoryState{
		raw: raw, writes: map[int]func([]byte, int64) (int, bool, error){},
		syncErrs: map[int]error{},
	}
}

func (m *memoryState) ReadAt(p []byte, offset int64) (int, error) {
	return copy(p, m.raw[offset:]), nil
}

// WriteAt applies the scripted fault for this call, if any: it returns how
// many bytes to copy, whether to copy them, and the error.
func (m *memoryState) WriteAt(p []byte, offset int64) (int, error) {
	call := m.writeCall
	m.writeCall++
	if fault, ok := m.writes[call]; ok {
		n, apply, err := fault(p, offset)
		if apply {
			copy(m.raw[offset:], p[:n])
		}
		return n, err
	}
	return copy(m.raw[offset:], p), nil
}

func (m *memoryState) Datasync() error {
	call := m.syncCall
	m.syncCall++
	if err := m.syncErrs[call]; err != nil {
		return err
	}
	m.synced = append(m.synced, append([]byte(nil), m.raw...))
	return nil
}

func (m *memoryState) Close() error {
	m.closed = true
	return nil
}

func (m *memoryState) durable(t *testing.T) []stateRecord {
	t.Helper()
	records := make([]stateRecord, 0, len(m.synced))
	for _, raw := range m.synced {
		record, err := decodeStateRecord(neutralRecordPolicy(t), raw)
		require.NoError(t, err)
		records = append(records, record)
	}
	return records
}

const (
	journalAgent  = "agent-one"
	journalBootID = "6a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d"
	journalSchema = "example-op/1"
)

var (
	journalLive = LiveBinding{
		PanelBinding: PanelBinding{OwnerUID: 1113, Socket: "/tmp/tmux-1113/default", PaneID: "%21"},
		Host:         "node-a", BootID: journalBootID,
		PanePID: 424242, PaneStart: 987654,
	}
	journalOperation = Operation{
		Schema: journalSchema, Agent: journalAgent, Inbox: Inbox{Unread: 2, NewestID: 77},
	}
	journalWriter = Writer{PID: 3333, Start: 555555}
	journalClock  = func() time.Time { return time.Date(2026, 11, 3, 9, 30, 0, 123456789, time.UTC) }
)

func journalPolicy() RecordPolicy {
	return RecordPolicy{PeerOwner: testPeerOwner, PeerWriters: []string{"legacy_writer"}}
}

func journalRecord(t *testing.T) stateRecord {
	t.Helper()
	binding, err := liveRecordBinding(journalLive)
	require.NoError(t, err)
	record := initialState(binding, nodeOwner, journalClock())
	return record
}

func TestJournalWritesWholeAttemptTrace(t *testing.T) {
	t.Parallel()

	file := newMemoryState(t, journalRecord(t))
	journal, err := BeginAttempt(file, journalLive, journalOperation, journalWriter, journalPolicy(), journalClock)
	require.NoError(t, err)
	require.NoError(t, journal.Step(1))
	require.NoError(t, journal.Child(0, 0))
	require.NoError(t, journal.Step(2))
	require.NoError(t, journal.Step(3))
	require.NoError(t, journal.Child(4242, 777))
	require.NoError(t, journal.Step(4))
	require.NoError(t, journal.Finish(true))
	require.NoError(t, journal.Close())
	require.True(t, file.closed)

	records := file.durable(t)
	require.Len(t, records, 8, "EN_CURSO, step 1, child, step 2, step 3, child, step 4, HECHO")
	wantPhases := []string{
		phaseRunning, phaseRunning, phaseRunning, phaseRunning,
		phaseRunning, phaseRunning, phaseRunning, phaseDone,
	}
	wantSteps := []int{0, 1, 1, 2, 3, 3, 4, 4}
	op, err := operationDigest(journalOperation, records[0].binding())
	require.NoError(t, err)
	for index, record := range records {
		require.Equal(t, uint64(index+1), record.Gen, "gen rises by one per durable write")
		require.Equal(t, wantPhases[index], record.Phase)
		require.Equal(t, wantSteps[index], record.Step)
		require.Equal(t, records[0].Attempt, record.Attempt, "one attempt id for the whole attempt")
		require.True(t, validHex(record.Attempt, 32))
		require.Equal(t, op, record.Operation)
		require.Equal(t, "-", record.Result)
		require.Equal(t, "-", record.TxID)
		require.Equal(t, nodeWriter, record.Writer)
		require.Equal(t, int64(3333), record.WriterPID)
		require.Equal(t, "555555", record.WriterStart)
		require.Equal(t, nodeOwner, record.Owner)
		require.Equal(t, records[0].binding(), record.binding())
	}
	require.Equal(t, int64(0), records[2].ChildPID, "a zero identity records child 0")
	require.Equal(t, "0", records[2].ChildStart)
	require.Equal(t, int64(4242), records[5].ChildPID)
	require.Equal(t, "777", records[5].ChildStart)
}

func TestJournalFinishWithoutSuccessIsUncertain(t *testing.T) {
	t.Parallel()

	file := newMemoryState(t, journalRecord(t))
	journal, err := BeginAttempt(file, journalLive, journalOperation, journalWriter, journalPolicy(), journalClock)
	require.NoError(t, err)
	require.NoError(t, journal.Step(1))
	require.NoError(t, journal.Finish(false))
	records := file.durable(t)
	last := records[len(records)-1]
	require.Equal(t, phaseUncertain, last.Phase)
	require.Equal(t, 1, last.Step)
	require.ErrorIs(t, journal.Finish(true), ErrJournalRefused, "a terminal record is never rewritten")
	require.ErrorIs(t, journal.Step(2), ErrJournalRefused)
	require.ErrorIs(t, journal.Child(1, 1), ErrJournalRefused)
	require.Len(t, file.synced, len(records), "refused updates write nothing")
}

func TestJournalFinishNotWrittenAtStepZeroIsDeferredAndRetryable(t *testing.T) {
	t.Parallel()

	file := newMemoryState(t, journalRecord(t))
	journal, err := BeginAttempt(file, journalLive, journalOperation, journalWriter, journalPolicy(), journalClock)
	require.NoError(t, err)
	require.NoError(t, journal.FinishNotWritten())
	records := file.durable(t)
	last := records[len(records)-1]
	require.Equal(t, phaseDeferred, last.Phase)
	require.Zero(t, last.Step)
	require.Zero(t, last.ChildPID)
	require.Equal(t, "0", last.ChildStart)

	_, err = BeginAttempt(file, journalLive, journalOperation, journalWriter, journalPolicy(), journalClock)
	require.NoError(t, err, "APLAZADO admits the next attempt")
}

func TestJournalFinishNotWrittenAfterStepIsUncertain(t *testing.T) {
	t.Parallel()

	file := newMemoryState(t, journalRecord(t))
	journal, err := BeginAttempt(file, journalLive, journalOperation, journalWriter, journalPolicy(), journalClock)
	require.NoError(t, err)
	require.NoError(t, journal.Step(1))
	require.NoError(t, journal.FinishNotWritten())
	records := file.durable(t)
	last := records[len(records)-1]
	require.Equal(t, phaseUncertain, last.Phase)
	require.Equal(t, 1, last.Step)

	_, err = BeginAttempt(file, journalLive, journalOperation, journalWriter, journalPolicy(), journalClock)
	require.ErrorIs(t, err, ErrAttemptUncertain)
}

func TestJournalBeginRefusesAttemptInFlightOrUncertain(t *testing.T) {
	t.Parallel()

	for _, phase := range []string{phaseRunning, phaseUncertain} {
		record := journalRecord(t)
		record.Phase, record.Attempt, record.Operation, record.Step = phase, "11111111111111111111111111111111",
			"2222222222222222222222222222222222222222222222222222222222222222", 1
		file := newMemoryState(t, record)
		_, err := BeginAttempt(file, journalLive, journalOperation, journalWriter, journalPolicy(), journalClock)
		require.ErrorIs(t, err, ErrAttemptUncertain, phase)
		require.Zero(t, file.writeCall, "%s: nothing is written", phase)
	}
	for _, phase := range []string{phaseIdle, phaseDone, phaseDeferred} {
		record := journalRecord(t)
		record.Phase = phase
		file := newMemoryState(t, record)
		_, err := BeginAttempt(file, journalLive, journalOperation, journalWriter, journalPolicy(), journalClock)
		require.NoError(t, err, phase)
	}
}

func TestJournalBeginRefusesForeignRecordAndBadInputs(t *testing.T) {
	t.Parallel()

	changed := func(edit func(*LiveBinding)) LiveBinding {
		live := journalLive
		edit(&live)
		return live
	}
	lives := map[string]LiveBinding{
		"pane pid":   changed(func(live *LiveBinding) { live.PanePID++ }),
		"pane start": changed(func(live *LiveBinding) { live.PaneStart++ }),
		"boot id":    changed(func(live *LiveBinding) { live.BootID = "1f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0" }),
		"host":       changed(func(live *LiveBinding) { live.Host = "node-b" }),
		"socket":     changed(func(live *LiveBinding) { live.Socket = "/tmp/tmux-1113/other" }),
		"pane":       changed(func(live *LiveBinding) { live.PaneID = "%22" }),
		"owner uid":  changed(func(live *LiveBinding) { live.OwnerUID = 1114 }),
	}
	for name, live := range lives {
		file := newMemoryState(t, journalRecord(t))
		_, err := BeginAttempt(file, live, journalOperation, journalWriter, journalPolicy(), journalClock)
		require.ErrorIs(t, err, ErrJournalRefused, name)
		require.Zero(t, file.writeCall, name)
	}

	operations := map[string]Operation{
		"no schema":       {Agent: journalAgent, Inbox: Inbox{Unread: 1, NewestID: 1}},
		"schema case":     {Schema: "Example-op/1", Agent: journalAgent, Inbox: Inbox{Unread: 1, NewestID: 1}},
		"agent grammar":   {Schema: journalSchema, Agent: "Agent_One", Inbox: Inbox{Unread: 1, NewestID: 1}},
		"agent too long":  {Schema: journalSchema, Agent: string(make([]byte, 65)), Inbox: Inbox{Unread: 1, NewestID: 1}},
		"no unread":       {Schema: journalSchema, Agent: journalAgent, Inbox: Inbox{Unread: 0, NewestID: 1}},
		"unread too many": {Schema: journalSchema, Agent: journalAgent, Inbox: Inbox{Unread: 1_000_001, NewestID: 1}},
		"no newest":       {Schema: journalSchema, Agent: journalAgent, Inbox: Inbox{Unread: 1, NewestID: 0}},
		"newest unsafe":   {Schema: journalSchema, Agent: journalAgent, Inbox: Inbox{Unread: 1, NewestID: 1 << 53}},
	}
	for name, operation := range operations {
		file := newMemoryState(t, journalRecord(t))
		_, err := BeginAttempt(file, journalLive, operation, journalWriter, journalPolicy(), journalClock)
		require.ErrorIs(t, err, ErrJournalRefused, name)
		require.Zero(t, file.writeCall, name)
	}

	records := map[string]func(*stateRecord){
		"another owner": func(record *stateRecord) { record.Owner = testOtherOwner },
		"gen exhausted": func(record *stateRecord) {
			record.Gen = maxSafeInteger - 1 - generationHeadroom - 6
		},
	}
	for name, edit := range records {
		record := journalRecord(t)
		edit(&record)
		file := newMemoryState(t, record)
		_, err := BeginAttempt(file, journalLive, journalOperation, journalWriter, journalPolicy(), journalClock)
		require.ErrorIs(t, err, ErrJournalRefused, name)
		require.Zero(t, file.writeCall, name)
	}

	corrupt := newMemoryState(t, journalRecord(t))
	corrupt.raw[952] ^= 1
	_, err := BeginAttempt(corrupt, journalLive, journalOperation, journalWriter, journalPolicy(), journalClock)
	require.ErrorIs(t, err, ErrJournalRefused, "a record failing its checksum")

	for name, writer := range map[string]Writer{
		"no pid": {PID: 0, Start: 1}, "pid range": {PID: maxRecordPID + 1, Start: 1}, "no start": {PID: 1},
	} {
		file := newMemoryState(t, journalRecord(t))
		_, err := BeginAttempt(file, journalLive, journalOperation, writer, journalPolicy(), journalClock)
		require.ErrorIs(t, err, ErrJournalRefused, name)
	}
}

func TestJournalOperationDigestVector(t *testing.T) {
	t.Parallel()

	binding, err := liveRecordBinding(journalLive)
	require.NoError(t, err)
	// The canonical preimage for these inputs: sorted keys, no spaces,
	// ASCII only. Its digest was computed by an independent encoder.
	const preimage = `{"agent":"agent-one","binding":{"boot_id":"6a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d",` +
		`"host":"node-a","pane":21,"pane_pid":424242,"pane_start":"987654",` +
		`"socket_sha":"eb9d236c7fa256c1c63896fd6cfbf929c61c02342bf003a3bc5de304cdf24fcf","uid":1113},` +
		`"inbox":{"kind":"unread","n":2,"newest_id":77},"schema":"example-op/1",` +
		`"template":"gifford-pending-v1"}`
	raw, err := operationPreimageBytes(journalOperation, binding)
	require.NoError(t, err)
	require.True(t, bytes.Equal([]byte(preimage), raw), "canonical preimage: %s", raw)
	op, err := operationDigest(journalOperation, binding)
	require.NoError(t, err)
	require.Equal(t, "7f6b5349937ecedb0aeb9f99a4b9aa2881d3ff46dbc7134c8fc999bdc6cc561b", op)
}

func TestJournalDurableWriteContinuesShortAndInterruptedWrites(t *testing.T) {
	t.Parallel()

	file := newMemoryState(t, journalRecord(t))
	short := func(p []byte, _ int64) (int, bool, error) { return min(len(p), 100), true, nil }
	interrupted := func([]byte, int64) (int, bool, error) { return 0, false, syscall.EINTR }
	file.writes[0], file.writes[1], file.writes[2] = interrupted, short, interrupted
	file.writes[3], file.writes[4] = short, short
	journal, err := BeginAttempt(file, journalLive, journalOperation, journalWriter, journalPolicy(), journalClock)
	require.NoError(t, err)
	records := file.durable(t)
	require.Len(t, records, 1)
	require.Equal(t, phaseRunning, records[0].Phase)
	require.Greater(t, file.writeCall, 5, "the loop kept writing until the record was whole")
	require.NoError(t, journal.Finish(false))
}

func TestJournalUpdateNotDurableNeverAdvances(t *testing.T) {
	t.Parallel()

	cases := map[string]func(*memoryState, int){
		"pwrite ENOSPC": func(file *memoryState, call int) {
			file.writes[call] = func([]byte, int64) (int, bool, error) { return 0, false, syscall.ENOSPC }
		},
		"pwrite EIO after a short write": func(file *memoryState, call int) {
			file.writes[call] = func([]byte, int64) (int, bool, error) { return 10, true, nil }
			file.writes[call+1] = func([]byte, int64) (int, bool, error) { return 0, false, syscall.EIO }
		},
		"no progress": func(file *memoryState, call int) {
			for index := range maxDurableStalls + 1 {
				file.writes[call+index] = func([]byte, int64) (int, bool, error) { return 0, false, nil }
			}
		},
		"interrupted forever": func(file *memoryState, call int) {
			for index := range maxDurableStalls + 1 {
				file.writes[call+index] = func([]byte, int64) (int, bool, error) { return 0, false, syscall.EINTR }
			}
		},
	}
	for name, fault := range cases {
		// Before the effect: Begin fails and nothing may be typed.
		file := newMemoryState(t, journalRecord(t))
		fault(file, 0)
		_, err := BeginAttempt(file, journalLive, journalOperation, journalWriter, journalPolicy(), journalClock)
		require.ErrorIs(t, err, ErrJournalNotDurable, name)
		require.Empty(t, file.synced, name)

		// After it: the failed step leaves the last durable record in
		// memory, so the terminal write follows it by exactly one.
		file = newMemoryState(t, journalRecord(t))
		journal, err := BeginAttempt(file, journalLive, journalOperation, journalWriter, journalPolicy(), journalClock)
		require.NoError(t, err, name)
		fault(file, file.writeCall)
		require.ErrorIs(t, journal.Step(1), ErrJournalNotDurable, name)
		require.NoError(t, journal.Finish(false), name)
		records := file.durable(t)
		last := records[len(records)-1]
		require.Equal(t, phaseUncertain, last.Phase, name)
		require.Equal(t, uint64(2), last.Gen, name)
		require.Equal(t, 0, last.Step, name)
	}

	file := newMemoryState(t, journalRecord(t))
	file.syncErrs[0] = syscall.EIO
	_, err := BeginAttempt(file, journalLive, journalOperation, journalWriter, journalPolicy(), journalClock)
	require.ErrorIs(t, err, ErrJournalNotDurable, "fdatasync failure before the effect")
	require.ErrorIs(t, err, syscall.EIO)

	file = newMemoryState(t, journalRecord(t))
	journal, err := BeginAttempt(file, journalLive, journalOperation, journalWriter, journalPolicy(), journalClock)
	require.NoError(t, err)
	require.NoError(t, journal.Step(1))
	file.syncErrs[file.syncCall] = syscall.EIO
	require.ErrorIs(t, journal.Child(0, 0), ErrJournalNotDurable, "fdatasync failure after the effect")
	require.NoError(t, journal.Finish(false))
	records := file.durable(t)
	require.Equal(t, uint64(3), records[len(records)-1].Gen)
	require.Equal(t, phaseUncertain, records[len(records)-1].Phase)
}

func TestJournalStepsOnlyMoveForward(t *testing.T) {
	t.Parallel()

	file := newMemoryState(t, journalRecord(t))
	journal, err := BeginAttempt(file, journalLive, journalOperation, journalWriter, journalPolicy(), journalClock)
	require.NoError(t, err)
	require.ErrorIs(t, journal.Child(1, 1), ErrJournalRefused, "no child before a launched step")
	for _, step := range []int{0, 5, -1} {
		require.ErrorIs(t, journal.Step(step), ErrJournalRefused, "step %d", step)
	}
	require.NoError(t, journal.Step(2))
	require.ErrorIs(t, journal.Step(2), ErrJournalRefused, "a step never repeats")
	require.ErrorIs(t, journal.Step(1), ErrJournalRefused, "a step never goes back")
	require.Len(t, file.synced, 2)
}
