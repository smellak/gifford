package triage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/gateway"
	"github.com/smellak/gifford/internal/protocol"
)

// maxTextRunes bounds the text sent for one message.
const maxTextRunes = 8192

// ErrInvalidConfig means New was given a missing collaborator.
var ErrInvalidConfig = errors.New("invalid triage config")

// errBadVerdict is a stored verdict that does not read back.
var errBadVerdict = errors.New("stored triage verdict is invalid")

// Delivery names one queued delivery: a message for one recipient, at one
// generation of its queue row.
type Delivery struct {
	MessageID          protocol.ULID
	AgentID            protocol.AgentID
	DeliveryGeneration int64
}

// Channel evaluates the channel for one queued delivery. AllowsNow must
// judge the current controls through tx alone, publish no Decision,
// compare and swap nothing and consume no budget.
type Channel interface {
	AllowsNow(ctx context.Context, tx *sql.Tx, d Delivery) (bool, error)
}

// Inferrer runs one model contract. *gateway.Gateway satisfies it.
type Inferrer interface {
	Infer(ctx context.Context, req protocol.InferRequest) (protocol.InferResult, error)
}

// Outcome is how a verdict came about.
type Outcome string

// Outcome values, as message_triage stores them.
const (
	// OutcomeClassified is a call that gave a label.
	OutcomeClassified Outcome = "classified"
	// OutcomeTimeout, OutcomeInvalid, OutcomeError, OutcomeCircuitOpen and
	// OutcomeRedactionFailed are failed calls: the Gateway's ErrTimeout,
	// ErrInvalidOutput, any other failure, ErrCircuitOpen and
	// ErrRedactionFailed.
	OutcomeTimeout         Outcome = "timeout"
	OutcomeInvalid         Outcome = "invalid"
	OutcomeError           Outcome = "error"
	OutcomeCircuitOpen     Outcome = "circuit_open"
	OutcomeRedactionFailed Outcome = "redaction_failed"
	// OutcomeEncrypted, OutcomeConfidential and OutcomeUnreadable are
	// messages that are never sent: end-to-end encrypted, CONFIDENTIAL, or
	// an envelope that cannot be read as a profile-none message.
	OutcomeEncrypted    Outcome = "encrypted"
	OutcomeConfidential Outcome = "confidential"
	OutcomeUnreadable   Outcome = "unreadable"
)

// Verdict is the triage of one message.
type Verdict struct {
	MessageID protocol.ULID
	// Label is set only when Outcome is OutcomeClassified.
	Label   protocol.TriageLabel
	Outcome Outcome
	// GlimmerCallID is the call, when one was made and recorded.
	GlimmerCallID protocol.OpaqueID
	CreatedAt     time.Time
}

// Classified reports whether the verdict has a label.
func (v Verdict) Classified() bool { return v.Outcome == OutcomeClassified }

// Triager classifies queued messages, one call per message.
type Triager struct {
	db    *sql.DB
	model Inferrer
	ch    Channel
	clock clock.Clock

	mu      sync.Mutex
	flights map[protocol.ULID]*flight
}

// flight is one message's classification in progress, which concurrent
// triages of the message wait for.
type flight struct {
	done    chan struct{}
	verdict Verdict
	err     error
	// waiters counts the triages waiting for this one.
	waiters int
}

// New returns a Triager over the node database.
func New(db *sql.DB, model Inferrer, ch Channel, clk clock.Clock) (*Triager, error) {
	if db == nil || model == nil || ch == nil || clk == nil {
		return nil, ErrInvalidConfig
	}
	return &Triager{db: db, model: model, ch: ch, clock: clk, flights: make(map[protocol.ULID]*flight)}, nil
}

// Triage returns the verdict of d's message. A message with none is
// classified first if d could be delivered now; otherwise ok is false and
// nothing was called or written.
func (t *Triager) Triage(ctx context.Context, d Delivery) (Verdict, bool, error) {
	if v, ok, err := Lookup(ctx, t.db, d.MessageID); err != nil || ok {
		return v, ok, err
	}
	raw, ok, err := t.deliverable(ctx, d)
	if err != nil || !ok {
		return Verdict{}, false, err
	}
	v, err := t.once(ctx, d.MessageID, raw)
	if err != nil {
		return Verdict{}, false, err
	}
	return v, true, nil
}

// deliverableSQL reads the envelope of a delivery that is QUEUED at its
// generation, has no pending orchestrator approval and whose agent has no
// unresolved UNCERTAIN attempt. Expiry and the channel are judged after.
const deliverableSQL = `SELECT dq.expires_at, me.encryption_profile, me.envelope
	FROM delivery_queue AS dq
	JOIN message_envelopes AS me ON me.message_id = dq.message_id
	WHERE dq.message_id = ? AND dq.agent_id = ? AND dq.delivery_generation = ?
		AND dq.state = 'QUEUED'
		AND NOT EXISTS (SELECT 1 FROM approval_requests AS ar
			WHERE ar.source = 'orchestrator' AND ar.object_type = 'message'
			AND ar.object_id = dq.message_id AND ar.agent_id = dq.agent_id
			AND ar.state = 'PENDING')
		AND NOT EXISTS (SELECT 1 FROM delivery_attempts AS da
			WHERE da.agent_id = dq.agent_id AND da.state = 'UNCERTAIN'
			AND da.resolution_approval_id IS NULL)`

// envelope is a message's stored envelope and its encryption profile.
type envelope struct {
	profile protocol.EncryptionProfile
	raw     []byte
}

// deliverable checks, in one read-only transaction that is closed before
// it returns, that d could be delivered now, and reads its envelope.
func (t *Triager) deliverable(ctx context.Context, d Delivery) (envelope, bool, error) {
	tx, err := t.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return envelope{}, false, fmt.Errorf("begin triage check: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var expires, profile string
	var raw []byte
	err = tx.QueryRowContext(ctx, deliverableSQL, string(d.MessageID), string(d.AgentID), d.DeliveryGeneration).
		Scan(&expires, &profile, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return envelope{}, false, nil
	}
	if err != nil {
		return envelope{}, false, fmt.Errorf("read triage delivery: %w", err)
	}
	expiresAt, err := eventstore.ParseTime(expires)
	if err != nil {
		return envelope{}, false, fmt.Errorf("read triage delivery expiry: %w", err)
	}
	if !expiresAt.After(t.clock.Now()) {
		return envelope{}, false, nil
	}
	allowed, err := t.ch.AllowsNow(ctx, tx, d)
	if err != nil {
		return envelope{}, false, fmt.Errorf("evaluate the channel for triage: %w", err)
	}
	if !allowed {
		return envelope{}, false, nil
	}
	if err := tx.Commit(); err != nil {
		return envelope{}, false, fmt.Errorf("end triage check: %w", err)
	}
	return envelope{profile: protocol.EncryptionProfile(profile), raw: raw}, true, nil
}

// once classifies a message unless another triage of it is already doing
// so, in which case it waits for that one's verdict.
func (t *Triager) once(ctx context.Context, id protocol.ULID, env envelope) (Verdict, error) {
	t.mu.Lock()
	if f, ok := t.flights[id]; ok {
		f.waiters++
		t.mu.Unlock()
		select {
		case <-f.done:
			return f.verdict, f.err
		case <-ctx.Done():
			return Verdict{}, fmt.Errorf("wait for triage: %w", ctx.Err())
		}
	}
	f := &flight{done: make(chan struct{})}
	t.flights[id] = f
	t.mu.Unlock()

	f.verdict, f.err = t.classify(ctx, id, env)
	t.mu.Lock()
	delete(t.flights, id)
	t.mu.Unlock()
	close(f.done)
	return f.verdict, f.err
}

// classify writes the verdict of one message. A flight that finished just
// before this one began has already stored its verdict, so it is read
// first.
func (t *Triager) classify(ctx context.Context, id protocol.ULID, env envelope) (Verdict, error) {
	if v, ok, err := Lookup(ctx, t.db, id); err != nil || ok {
		return v, err
	}
	v := Verdict{MessageID: id}
	text, sealed := readText(id, env)
	if sealed != "" {
		v.Outcome = sealed
	} else {
		payload, err := json.Marshal(struct {
			Text string `json:"text"`
		}{text})
		if err != nil {
			return Verdict{}, fmt.Errorf("encode triage payload: %w", err)
		}
		res, err := t.model.Infer(ctx, protocol.InferRequest{
			Contract: protocol.ModelContractTriageMessage, Payload: protocol.Payload(payload),
		})
		if err != nil && ctx.Err() != nil {
			// The caller gave up: no verdict, so a later triage asks again.
			return Verdict{}, fmt.Errorf("triage call: %w", ctx.Err())
		}
		v.Label, v.Outcome, v.GlimmerCallID = readResult(res, err)
	}
	v.CreatedAt = t.clock.Now()
	// A verdict is stored even if the caller gives up now.
	return t.store(context.WithoutCancel(ctx), v)
}

// readText reads the text of a message from its envelope, or the outcome
// of a message that is never sent.
func readText(id protocol.ULID, env envelope) (string, Outcome) {
	if env.profile != protocol.EncryptionProfileNone {
		return "", OutcomeEncrypted
	}
	var wire protocol.WireEnvelope
	if json.Unmarshal(env.raw, &wire) != nil || wire.Validate() != nil || wire.MessageID != id ||
		wire.EncryptionProfile != protocol.EncryptionProfileNone {
		return "", OutcomeUnreadable
	}
	var msg protocol.LogicalMessage
	if json.Unmarshal(wire.Ciphertext, &msg) != nil || msg.Validate() != nil || msg.MessageID != id {
		return "", OutcomeUnreadable
	}
	if msg.Sensitivity == protocol.MessageSensitivityConfidential {
		return "", OutcomeConfidential
	}
	return cutRunes("Subject: "+msg.Subject+"\n\n"+msg.Body, maxTextRunes), ""
}

// cutRunes keeps the first n runes of s.
func cutRunes(s string, n int) string {
	for i := range s {
		if n == 0 {
			return s[:i]
		}
		n--
	}
	return s
}

// readResult turns a call into a label, an outcome and the call id. A
// result without a valid label or a recorded call is invalid.
func readResult(res protocol.InferResult, err error) (protocol.TriageLabel, Outcome, protocol.OpaqueID) {
	call := res.CallRecord.CallID
	switch {
	case err == nil:
		if label := protocol.TriageLabel(res.Label); label.Valid() && call != "" {
			return label, OutcomeClassified, call
		}
		return "", OutcomeInvalid, call
	case errors.Is(err, gateway.ErrTimeout):
		return "", OutcomeTimeout, call
	case errors.Is(err, gateway.ErrInvalidOutput):
		return "", OutcomeInvalid, call
	case errors.Is(err, gateway.ErrCircuitOpen):
		return "", OutcomeCircuitOpen, call
	case errors.Is(err, gateway.ErrRedactionFailed):
		return "", OutcomeRedactionFailed, call
	default:
		return "", OutcomeError, call
	}
}

// insertVerdictSQL writes a verdict unless the message has one already.
const insertVerdictSQL = `INSERT INTO message_triage
	(message_id, label, outcome, glimmer_call_id, created_at) VALUES (?, ?, ?, ?, ?)
	ON CONFLICT (message_id) DO NOTHING`

// store writes v unless the message already has a verdict, and returns
// the one stored.
func (t *Triager) store(ctx context.Context, v Verdict) (Verdict, error) {
	_, err := t.db.ExecContext(ctx, insertVerdictSQL, string(v.MessageID), orNull(string(v.Label)),
		string(v.Outcome), orNull(string(v.GlimmerCallID)), eventstore.FormatTime(v.CreatedAt))
	if err != nil {
		return Verdict{}, fmt.Errorf("store triage verdict: %w", err)
	}
	stored, ok, err := Lookup(ctx, t.db, v.MessageID)
	if err != nil {
		return Verdict{}, err
	}
	if !ok {
		return Verdict{}, fmt.Errorf("store triage verdict: %w", errBadVerdict)
	}
	return stored, nil
}

// Querier is what Lookup needs: a database or a transaction.
type Querier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Lookup returns the stored verdict of a message; ok is false when it has
// none.
func Lookup(ctx context.Context, q Querier, id protocol.ULID) (Verdict, bool, error) {
	var label, call sql.NullString
	var outcome, created string
	err := q.QueryRowContext(ctx, `SELECT label, outcome, glimmer_call_id, created_at
		FROM message_triage WHERE message_id = ?`, string(id)).Scan(&label, &outcome, &call, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return Verdict{}, false, nil
	}
	if err != nil {
		return Verdict{}, false, fmt.Errorf("read triage verdict: %w", err)
	}
	at, err := eventstore.ParseTime(created)
	if err != nil {
		return Verdict{}, false, fmt.Errorf("read triage verdict: %w", errBadVerdict)
	}
	return Verdict{
		MessageID: id, Label: protocol.TriageLabel(label.String), Outcome: Outcome(outcome),
		GlimmerCallID: protocol.OpaqueID(call.String), CreatedAt: at,
	}, true, nil
}

// orNull is s, or NULL when s is empty.
func orNull(s string) sql.NullString { return sql.NullString{String: s, Valid: s != ""} }
