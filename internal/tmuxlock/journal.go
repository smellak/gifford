package tmuxlock

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"syscall"
	"time"
)

// This file is the durable attempt journal of one pane's .state record. The
// caller holds the pane flock for the whole attempt: the journal reads the
// record once, then every update raises gen by one and reaches the disk
// (pwrite loop, then fdatasync) before the caller may take its next physical
// step. A failed update never advances the in-memory record, so the next
// write still follows the last durable one.

// PendingTemplate is the closed template id of the node's pending-message
// notice in the operation digest preimage.
const PendingTemplate = "gifford-pending-v1"

const (
	nodeWriter = "gifford"
	// maxDurableStalls bounds interrupted or zero-progress pwrite calls
	// before an update counts as not durable.
	maxDurableStalls = 8
	maxInboxUnread   = int64(1_000_000)
	maxInboxNewestID = int64(1<<53 - 1)
)

var (
	// ErrAttemptUncertain means the record holds an attempt that may have
	// typed already: EN_CURSO from a writer that never finished, or
	// INCIERTO. Only an authenticated reconcile clears it; no new effect
	// may start.
	ErrAttemptUncertain = errors.New("pane attempt uncertain")
	// ErrJournalNotDurable means an update did not reach the disk. Before
	// the first effect it forbids the effect; after it, the attempt is
	// uncertain.
	ErrJournalNotDurable = errors.New("pane journal update not durable")
	// ErrJournalRefused means the record or the attempt inputs do not
	// allow a new attempt: wrong binding, no generation headroom, or an
	// invalid operation preimage.
	ErrJournalRefused = errors.New("pane journal refused")

	agentPattern  = regexp.MustCompile(`^[a-z0-9-]{1,64}$`)
	schemaPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9./-]{0,63}$`)
)

// StateFile is the open .state descriptor the journal updates in place. Its
// methods map to pread, pwrite and fdatasync on one descriptor; Close
// releases it.
type StateFile interface {
	ReadAt(p []byte, offset int64) (int, error)
	WriteAt(p []byte, offset int64) (int, error)
	Datasync() error
	Close() error
}

// Writer is the identity of the process that journals the attempt: its pid
// and the start time from field 22 of /proc/<pid>/stat.
type Writer struct {
	PID   int64
	Start uint64
}

// Inbox is the unread inbox the notice announces, as the operation digest
// names it: how many messages wait and the newest one's id.
type Inbox struct {
	Unread   int64
	NewestID int64
}

// Operation is the preimage of one attempt's op digest: the schema id, the
// agent the notice is for and its inbox, with the record's binding and the
// template added by the journal. Schema comes from configuration; the
// template is always PendingTemplate.
type Operation struct {
	Schema string
	Agent  string
	Inbox  Inbox
}

type operationInbox struct {
	Kind     string `json:"kind"`
	Unread   int64  `json:"n"`
	NewestID int64  `json:"newest_id"`
}

type operationPreimage struct {
	Agent    string         `json:"agent"`
	Binding  recordBinding  `json:"binding"`
	Inbox    operationInbox `json:"inbox"`
	Schema   string         `json:"schema"`
	Template string         `json:"template"`
}

// operationPreimage returns the canonical preimage bytes. encoding/json
// writes struct fields in declaration order, which is the alphabetical
// order the canonical form needs; every value is restricted to ASCII
// digits, lowercase letters and the few punctuation marks that encode the
// same under ensure_ascii, so no escaping can differ.
func operationPreimageBytes(operation Operation, binding recordBinding) ([]byte, error) {
	if !schemaPattern.MatchString(operation.Schema) || !agentPattern.MatchString(operation.Agent) ||
		operation.Inbox.Unread < 1 || operation.Inbox.Unread > maxInboxUnread ||
		operation.Inbox.NewestID < 1 || operation.Inbox.NewestID > maxInboxNewestID ||
		!validRecordBinding(binding) {
		return nil, fmt.Errorf("operation preimage: %w", ErrJournalRefused)
	}
	raw, err := json.Marshal(operationPreimage{
		Agent: operation.Agent, Binding: binding,
		Inbox: operationInbox{
			Kind: "unread", Unread: operation.Inbox.Unread, NewestID: operation.Inbox.NewestID,
		},
		Schema: operation.Schema, Template: PendingTemplate,
	})
	if err != nil {
		return nil, fmt.Errorf("encode operation preimage: %w", err)
	}
	return raw, nil
}

// operationDigest returns the lowercase sha256 of the canonical preimage.
func operationDigest(operation Operation, binding recordBinding) (string, error) {
	raw, err := operationPreimageBytes(operation, binding)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

// Journal is one attempt's open journal. It is not safe for concurrent use:
// the pane flock already serializes every writer of the record.
type Journal struct {
	file   StateFile
	state  stateRecord
	policy validatedRecordPolicy
	now    func() time.Time
}

// Attempt returns the durable operation id created by BeginAttempt. It is the
// identifier shared with any common writer interval for this attempt.
func (j *Journal) Attempt() string {
	if j == nil {
		return ""
	}
	return j.state.Attempt
}

// BeginAttempt reads the record under the caller's pane flock and makes the
// attempt durable as EN_CURSO before any effect: gen+1, a fresh 32-hex
// attempt, the op digest, result "-", this writer's identity, no child and
// step 0. It refuses an attempt already in flight or uncertain, a record of
// another pane or process, and a generation without room for the attempt.
// On any error no effect may start; the caller still closes file.
func BeginAttempt(
	file StateFile,
	live LiveBinding,
	operation Operation,
	writer Writer,
	policy RecordPolicy,
	now func() time.Time,
) (*Journal, error) {
	if file == nil || now == nil || writer.PID < 1 || writer.PID > maxRecordPID || writer.Start == 0 {
		return nil, fmt.Errorf("begin pane attempt: %w", ErrJournalRefused)
	}
	validated, err := validateRecordPolicy(policy)
	if err != nil {
		return nil, fmt.Errorf("begin pane attempt: %w", ErrJournalRefused)
	}
	state, err := readStateRecord(validated, file)
	if err != nil {
		return nil, err
	}
	expected, err := liveRecordBinding(live)
	if err != nil {
		return nil, err
	}
	if state.binding() != expected {
		return nil, fmt.Errorf("pane record binding: %w", ErrJournalRefused)
	}
	if state.Owner != nodeOwner {
		return nil, fmt.Errorf("pane record owner: %w", ErrJournalRefused)
	}
	switch state.Phase {
	case phaseIdle, phaseDone, phaseDeferred:
	case phaseRunning, phaseUncertain:
		return nil, fmt.Errorf("pane record phase %s: %w", state.Phase, ErrAttemptUncertain)
	default:
		return nil, fmt.Errorf("pane record phase: %w", ErrJournalRefused)
	}
	// The attempt writes at most seven generations (EN_CURSO, four steps,
	// two children, terminal) and must leave the headroom intact.
	if state.Gen > maxSafeInteger-1-generationHeadroom-7 {
		return nil, fmt.Errorf("pane record generation exhausted: %w", ErrJournalRefused)
	}
	op, err := operationDigest(operation, expected)
	if err != nil {
		return nil, err
	}
	attempt, err := newAttemptID()
	if err != nil {
		return nil, err
	}
	journal := &Journal{file: file, state: state, policy: validated, now: now}
	next := state
	next.Phase, next.Attempt, next.Operation, next.Result = phaseRunning, attempt, op, "-"
	next.Writer, next.WriterPID = nodeWriter, writer.PID
	next.WriterStart = strconv.FormatUint(writer.Start, 10)
	next.ChildPID, next.ChildStart, next.Step = 0, "0", 0
	if err := journal.write(next); err != nil {
		return nil, err
	}
	return journal, nil
}

// Step makes the next sub-step durable before its process starts.
func (j *Journal) Step(step int) error {
	if step < 1 || step > 4 || step <= j.state.Step || j.state.Phase != phaseRunning {
		return fmt.Errorf("pane attempt step %d: %w", step, ErrJournalRefused)
	}
	next := j.state
	next.Step = step
	return j.write(next)
}

// Child makes the identity of the effect process just started durable. A
// zero or unrepresentable identity records child 0, which with step >= 1
// means unknown, never "no child".
func (j *Journal) Child(pid int64, start uint64) error {
	if j.state.Phase != phaseRunning || j.state.Step < 1 {
		return fmt.Errorf("pane attempt child: %w", ErrJournalRefused)
	}
	next := j.state
	next.ChildPID, next.ChildStart = 0, "0"
	if pid >= 1 && pid <= maxRecordPID && start > 0 {
		next.ChildPID, next.ChildStart = pid, strconv.FormatUint(start, 10)
	}
	return j.write(next)
}

// Finish makes the terminal phase durable: HECHO only when done, INCIERTO
// otherwise. It never retries and never resets the attempt.
func (j *Journal) Finish(done bool) error {
	if j.state.Phase != phaseRunning {
		return fmt.Errorf("pane attempt finish: %w", ErrJournalRefused)
	}
	next := j.state
	next.Phase = phaseUncertain
	if done {
		next.Phase = phaseDone
	}
	return j.write(next)
}

// FinishNotWritten makes a proven no-effect end durable. An attempt that
// never advanced past step 0 and has no child is safe to retry, so it ends
// APLAZADO; any later boundary remains INCIERTO because an effect may have
// started. It never retries and never resets the attempt.
func (j *Journal) FinishNotWritten() error {
	if j.state.Phase != phaseRunning {
		return fmt.Errorf("pane attempt finish not written: %w", ErrJournalRefused)
	}
	next := j.state
	next.Phase = phaseUncertain
	if j.state.Step == 0 && j.state.ChildPID == 0 && j.state.ChildStart == "0" {
		next.Phase = phaseDeferred
	}
	return j.write(next)
}

// Close releases the record descriptor.
func (j *Journal) Close() error {
	if err := j.file.Close(); err != nil {
		return fmt.Errorf("close pane record: %w", err)
	}
	return nil
}

// write raises gen by one, stamps ts, validates the whole record and makes
// it durable. The in-memory record advances only after fdatasync returns.
func (j *Journal) write(next stateRecord) error {
	next.Gen = j.state.Gen + 1
	next.Timestamp = formatRecordTime(j.now())
	raw, err := encodeStateRecord(j.policy, next)
	if err != nil {
		return fmt.Errorf("encode pane record: %w: %w", ErrJournalRefused, err)
	}
	if err := durableWriteState(j.file, raw); err != nil {
		return err
	}
	j.state = next
	return nil
}

// durableWriteState writes the whole record at offset zero, continuing short
// writes and retrying a bounded number of interrupted or zero-progress
// calls, then syncs the data. Any other failure is not durable.
func durableWriteState(file StateFile, raw []byte) error {
	offset, stalls := 0, 0
	for offset < len(raw) {
		written, err := file.WriteAt(raw[offset:], int64(offset))
		if written > 0 {
			offset += written
		}
		switch {
		case err != nil && !isInterrupted(err):
			return fmt.Errorf("write pane record: %w: %w", ErrJournalNotDurable, err)
		case written <= 0:
			stalls++
			if stalls > maxDurableStalls {
				return fmt.Errorf("write pane record: no progress: %w", ErrJournalNotDurable)
			}
		}
	}
	if err := file.Datasync(); err != nil {
		return fmt.Errorf("sync pane record: %w: %w", ErrJournalNotDurable, err)
	}
	return nil
}

func isInterrupted(err error) bool {
	return errors.Is(err, syscall.EINTR)
}

func readStateRecord(policy validatedRecordPolicy, file StateFile) (stateRecord, error) {
	raw := make([]byte, recordSize)
	read, err := file.ReadAt(raw, 0)
	if read != recordSize || err != nil && !errors.Is(err, io.EOF) {
		return stateRecord{}, fmt.Errorf("read pane record: %w", ErrJournalRefused)
	}
	state, err := decodeStateRecord(policy, raw)
	if err != nil {
		return stateRecord{}, fmt.Errorf("decode pane record: %w: %w", ErrJournalRefused, err)
	}
	return state, nil
}

func liveRecordBinding(live LiveBinding) (recordBinding, error) {
	if err := validateLiveBinding(live); err != nil {
		return recordBinding{}, fmt.Errorf("%w: %w", ErrJournalRefused, err)
	}
	parts, err := deriveParts(live.PanelBinding)
	if err != nil {
		return recordBinding{}, fmt.Errorf("%w: %w", ErrJournalRefused, err)
	}
	pane, err := strconv.ParseUint(parts.file[:len(parts.file)-len(".lock")], 10, 32)
	if err != nil {
		return recordBinding{}, fmt.Errorf("pane number: %w", ErrJournalRefused)
	}
	return recordBinding{
		BootID: live.BootID, Host: live.Host,
		Pane:      uint32(pane),
		PanePID:   live.PanePID,
		PaneStart: strconv.FormatInt(live.PaneStart, 10), SocketSHA: parts.hash,
		UID: live.OwnerUID,
	}, nil
}

func newAttemptID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", fmt.Errorf("pane attempt id: %w", err)
	}
	return hex.EncodeToString(id[:]), nil
}
