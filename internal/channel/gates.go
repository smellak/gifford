package channel

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"math/big"
	"strings"
	"time"

	"github.com/smellak/gifford/internal/protocol"
)

// PolicyVerdict is the Security Policy Engine's answer for one recipient.
// Its zero value is deliberately invalid.
type PolicyVerdict uint8

// Policy verdicts.
const (
	PolicyUnset PolicyVerdict = iota
	PolicyAllowed
	PolicyDenied
)

// Recipient is one receiver of a message, as the Hub's registry and policy
// know it.
type Recipient struct {
	Agent       protocol.AgentID
	Deliverable bool
	Policy      PolicyVerdict
}

// Message is the authenticated message metadata needed by a gate.
type Message struct {
	ID       protocol.ULID
	From     protocol.AgentID
	FromNode protocol.NodeID
	To       []Recipient
	ToNode   protocol.NodeID
	ParentID protocol.ULID
	ThreadID protocol.ThreadID
	HopCount int64
}

// validate checks all message metadata before a gate reads its clock.
func (m Message) validate() error {
	switch {
	case !m.ID.Valid():
		return fmt.Errorf("%w: id", ErrInvalidQuery)
	case !m.From.Valid():
		return fmt.Errorf("%w: from", ErrInvalidQuery)
	case !m.FromNode.Valid():
		return fmt.Errorf("%w: from_node", ErrInvalidQuery)
	case !m.ToNode.Valid():
		return fmt.Errorf("%w: to_node", ErrInvalidQuery)
	case !m.ThreadID.Valid():
		return fmt.Errorf("%w: thread_id", ErrInvalidQuery)
	case m.ParentID != "" && !m.ParentID.Valid():
		return fmt.Errorf("%w: parent_id", ErrInvalidQuery)
	case m.HopCount < 0:
		return fmt.Errorf("%w: hop_count", ErrInvalidQuery)
	case len(m.To) < 1 || len(m.To) > 32:
		return fmt.Errorf("%w: to", ErrInvalidQuery)
	}
	seen := make(map[protocol.AgentID]bool, len(m.To))
	for i, recipient := range m.To {
		if !recipient.Agent.Valid() || recipient.Agent == m.From || seen[recipient.Agent] {
			return fmt.Errorf("%w: to[%d]", ErrInvalidQuery, i)
		}
		if recipient.Policy != PolicyAllowed && recipient.Policy != PolicyDenied {
			return fmt.Errorf("%w: to[%d].policy", ErrInvalidQuery, i)
		}
		seen[recipient.Agent] = true
	}
	return nil
}

// DenyError is a gate's own DENY. It is the only error after which the caller
// commits the transaction.
type DenyError struct {
	decisionID protocol.ChannelDecisionID
	reason     protocol.DecisionReason
}

func (e *DenyError) Error() string { return "denied: " + string(e.reason) }

// Unwrap makes every DenyError match ErrDenied.
func (e *DenyError) Unwrap() error { return ErrDenied }

// DecisionID returns the governing decision id.
func (e *DenyError) DecisionID() protocol.ChannelDecisionID { return e.decisionID }

// Reason returns the governing denial reason.
func (e *DenyError) Reason() protocol.DecisionReason { return e.reason }

// Code returns the protocol error code with the same name as the reason.
func (e *DenyError) Code() protocol.ErrorCode { return protocol.ErrorCode(e.reason) }

type triple struct {
	state   protocol.ControlState
	reason  protocol.DecisionReason
	version int64
}

// Acceptance describes the receiver's recorded evaluation and the final
// message state without exposing a Decision.
type Acceptance struct {
	evaluation Decision
	final      triple
}

// EvaluationID returns the receiver's governing decision id.
func (a Acceptance) EvaluationID() protocol.ChannelDecisionID { return a.evaluation.ID() }

// State returns the final state written on the message and envelope.
func (a Acceptance) State() protocol.ControlState { return a.final.state }

// Reason returns the final reason written on the message and envelope.
func (a Acceptance) Reason() protocol.DecisionReason { return a.final.reason }

// Version returns the current node epoch written on the final triple.
func (a Acceptance) Version() int64 { return a.final.version }

type gateEvaluation struct {
	query   Query
	verdict verdict
	eff     Effective
	vers    protocol.ControlVersions
	decided Decision
}

// Admit evaluates and records the sender-side admission in the caller's
// transaction. The callback stores the message row between the decisions and
// the final triple.
func (e *Engine) Admit(
	ctx context.Context,
	tx *sql.Tx,
	m Message,
	store func(context.Context, protocol.AdmissionResult) error,
) (Decision, protocol.AdmissionResult, error) {
	if tx == nil || store == nil {
		return Decision{}, protocol.AdmissionResult{}, fmt.Errorf("%w: transaction or store", ErrInvalidQuery)
	}
	if err := m.validate(); err != nil {
		return Decision{}, protocol.AdmissionResult{}, err
	}
	if m.ParentID != "" {
		return Decision{}, protocol.AdmissionResult{}, ErrUnsupported
	}
	now := e.clock.Now()
	if err := ensureNewAdmission(ctx, tx, m.ID); err != nil {
		return Decision{}, protocol.AdmissionResult{}, err
	}
	v, epoch, err := readGateView(ctx, tx)
	if err != nil {
		return Decision{}, protocol.AdmissionResult{}, err
	}
	evaluated, err := evaluateRecipients(v, m, protocol.DecisionStageAdmission, now)
	if err != nil {
		return Decision{}, protocol.AdmissionResult{}, err
	}
	governing, err := e.prepareDecisions(evaluated, now)
	if err != nil {
		return Decision{}, protocol.AdmissionResult{}, err
	}
	if err := insertDecisions(ctx, tx, evaluated); err != nil {
		return Decision{}, protocol.AdmissionResult{}, err
	}
	if governing.Outcome() == Deny {
		return Decision{}, protocol.AdmissionResult{}, &DenyError{
			decisionID: governing.ID(),
			reason:     governing.Reason(),
		}
	}
	result := admissionResult(governing, m, epoch)
	if err := result.Validate(); err != nil {
		return Decision{}, protocol.AdmissionResult{}, fmt.Errorf("admission result: %w", err)
	}
	if err := store(ctx, result); err != nil {
		return Decision{}, protocol.AdmissionResult{}, fmt.Errorf("%w: %s", ErrStore, err.Error())
	}
	written := tripleOf(governing, epoch)
	if err := setMessageControl(ctx, tx, m.ID, written); err != nil {
		return Decision{}, protocol.AdmissionResult{}, err
	}
	return governing, result, nil
}

// Accept evaluates and records receiver-side acceptance in the caller's
// transaction. The callback stores the immutable envelope between the
// decisions and the final triples.
func (e *Engine) Accept(
	ctx context.Context,
	tx *sql.Tx,
	m Message,
	store func(context.Context) error,
) (Acceptance, error) {
	if tx == nil || store == nil {
		return Acceptance{}, fmt.Errorf("%w: transaction or store", ErrInvalidQuery)
	}
	if err := m.validate(); err != nil {
		return Acceptance{}, err
	}
	if m.ParentID != "" {
		return Acceptance{}, ErrUnsupported
	}
	now := e.clock.Now()
	stored, found, err := selectMessageControl(ctx, tx, m.ID)
	if err != nil {
		return Acceptance{}, err
	}
	if !found {
		return Acceptance{}, fmt.Errorf("%w: no local admission", ErrInvalidQuery)
	}
	if err := validateTriple(stored); err != nil {
		return Acceptance{}, err
	}
	if stored.state == protocol.ControlStateDenied {
		return Acceptance{}, fmt.Errorf("%w: message denied", ErrInvalidQuery)
	}
	if err := validateAntecedent(ctx, tx, m); err != nil {
		return Acceptance{}, err
	}
	count, err := envelopeCount(ctx, tx, m.ID)
	if err != nil {
		return Acceptance{}, err
	}
	if count != 0 {
		return Acceptance{}, fmt.Errorf("%w: envelope already stored", ErrInvalidQuery)
	}
	v, epoch, err := readGateView(ctx, tx)
	if err != nil {
		return Acceptance{}, err
	}
	evaluated, err := evaluateRecipients(v, m, protocol.DecisionStageAcceptance, now)
	if err != nil {
		return Acceptance{}, err
	}
	if epoch < stored.version {
		return Acceptance{}, fmt.Errorf("%w: epoch %d is behind stored %d", ErrInvalidControl, epoch, stored.version)
	}
	governing, err := e.prepareDecisions(evaluated, now)
	if err != nil {
		return Acceptance{}, err
	}
	if err := insertDecisions(ctx, tx, evaluated); err != nil {
		return Acceptance{}, err
	}
	accepted := tripleOf(governing, epoch)
	if governing.Outcome() == Deny {
		denied := triple{state: protocol.ControlStateDenied, reason: governing.Reason(), version: epoch}
		if err := compareAndSetMessageControl(ctx, tx, m.ID, denied, stored); err != nil {
			return Acceptance{}, err
		}
		return Acceptance{}, &DenyError{decisionID: governing.ID(), reason: governing.Reason()}
	}
	if err := store(ctx); err != nil {
		return Acceptance{}, fmt.Errorf("%w: %s", ErrStore, err.Error())
	}
	final := strongerTriple(stored, accepted)
	final.version = epoch
	if err := compareAndSetMessageControl(ctx, tx, m.ID, final, stored); err != nil {
		return Acceptance{}, err
	}
	if err := setEnvelopeControl(ctx, tx, m.ID, final); err != nil {
		return Acceptance{}, err
	}
	return Acceptance{evaluation: governing, final: final}, nil
}

func ensureNewAdmission(ctx context.Context, tx *sql.Tx, id protocol.ULID) error {
	stored, found, err := selectMessageControl(ctx, tx, id)
	if err != nil {
		return err
	}
	if found {
		if err := validateTriple(stored); err != nil {
			return err
		}
		return fmt.Errorf("%w: message already stored", ErrInvalidQuery)
	}
	records, err := selectDecisionRecords(ctx, tx, id)
	if err != nil {
		return err
	}
	for _, rec := range records {
		if rec.Stage == protocol.DecisionStageAdmission {
			return fmt.Errorf("%w: already admitted", ErrInvalidQuery)
		}
	}
	return nil
}

func readGateView(ctx context.Context, tx *sql.Tx) (view, int64, error) {
	selectAll, err := queryText("controls_select_all.sql")
	if err != nil {
		return view{}, 0, err
	}
	rows, err := tx.QueryContext(ctx, selectAll)
	if err != nil {
		return view{}, 0, fmt.Errorf("select channel controls: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var found []protocol.ChannelControl
	var dataErr error
	for rows.Next() {
		root, scanErr := scanControl(rows)
		if scanErr != nil {
			dataErr = scanErr
			break
		}
		found = append(found, root)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		_ = rows.Close()
		return view{}, 0, fmt.Errorf("read channel controls: %w", rowsErr)
	}
	if closeErr := rows.Close(); closeErr != nil {
		return view{}, 0, fmt.Errorf("close channel controls: %w", closeErr)
	}
	epoch, err := selectEpoch(ctx, tx)
	if err != nil {
		return view{}, 0, err
	}
	entries := make(map[cacheKey]cacheEntry)
	loaded := dataErr == nil
	if loaded {
		built, buildErr := buildEntries(found)
		if buildErr != nil {
			loaded = false
		} else {
			entries = built
		}
	}
	return view{loaded: loaded, entries: entries, stale: map[cacheKey]int64{}}, epoch, nil
}

func selectEpoch(ctx context.Context, tx *sql.Tx) (int64, error) {
	query, err := queryText("epoch_select.sql")
	if err != nil {
		return 0, err
	}
	var raw any
	if err := tx.QueryRowContext(ctx, query).Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, ErrUnavailable
		}
		return 0, fmt.Errorf("select control epoch: %w", err)
	}
	epoch, ok := raw.(int64)
	if !ok || epoch < 0 {
		return 0, ErrUnavailable
	}
	return epoch, nil
}

func evaluateRecipients(v view, m Message, stage protocol.DecisionStage, now time.Time) ([]gateEvaluation, error) {
	evaluated := make([]gateEvaluation, 0, len(m.To))
	for _, recipient := range m.To {
		q := Query{
			Stage:         stage,
			Plane:         protocol.PlaneMessage,
			From:          m.From,
			FromNode:      m.FromNode,
			To:            recipient.Agent,
			ToNode:        m.ToNode,
			ToDeliverable: recipient.Deliverable,
			Kind:          KindRoot,
			ThreadID:      m.ThreadID,
			Object: &Object{
				Type: protocol.DecisionObjectTypeMessage,
				ID:   protocol.OpaqueID(m.ID),
			},
			PolicyDenied: recipient.Policy == PolicyDenied,
			HopCount:     m.HopCount,
		}
		if err := q.Validate(); err != nil {
			return nil, err
		}
		decided, eff, versions := v.evaluate(q, now)
		evaluated = append(evaluated, gateEvaluation{query: q, verdict: decided, eff: eff, vers: versions})
	}
	return evaluated, nil
}

func (e *Engine) prepareDecisions(evaluated []gateEvaluation, now time.Time) (Decision, error) {
	hasDeny := false
	hasApproval := false
	for i := range evaluated {
		hasDeny = hasDeny || evaluated[i].verdict.outcome == Deny
		hasApproval = hasApproval || evaluated[i].verdict.outcome == Approval
	}
	if !hasDeny && hasApproval && evaluated[0].query.Stage != protocol.DecisionStageDelivery {
		return Decision{}, ErrApprovalUnavailable
	}
	for i := range evaluated {
		id, err := e.mintDecisionID(now)
		if err != nil {
			return Decision{}, fmt.Errorf("mint decision id: %w", err)
		}
		x := e.explainResult(evaluated[i].query, evaluated[i].verdict, evaluated[i].eff)
		rec, err := recordOf(id, evaluated[i].query, evaluated[i].verdict, evaluated[i].eff, evaluated[i].vers, x, now)
		if err != nil {
			return Decision{}, err
		}
		evaluated[i].decided, err = newDecision(rec, evaluated[i].verdict.class, evaluated[i].verdict.line)
		if err != nil {
			return Decision{}, err
		}
	}
	governing := evaluated[0].decided
	for i := 1; i < len(evaluated); i++ {
		if evaluated[i].decided.d.class > governing.d.class {
			governing = evaluated[i].decided
		}
	}
	return governing, nil
}

func (e *Engine) mintDecisionID(now time.Time) (protocol.ChannelDecisionID, error) {
	millis := now.UnixMilli()
	if millis < 0 || millis >= 1<<48 {
		return "", fmt.Errorf("time outside ULID range")
	}
	var raw [16]byte
	stamp := big.NewInt(millis).Bytes()
	copy(raw[6-len(stamp):6], stamp)
	if _, err := io.ReadFull(e.entropy, raw[6:]); err != nil {
		return "", fmt.Errorf("read entropy: %w", err)
	}
	encoded := new(big.Int).SetBytes(raw[:]).Text(32)
	const digits = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	var out strings.Builder
	for out.Len()+len(encoded) < 26 {
		out.WriteByte('0')
	}
	for i := range len(encoded) {
		c := encoded[i]
		index := int(c - '0')
		if c >= 'a' {
			index = int(c-'a') + 10
		}
		out.WriteByte(digits[index])
	}
	return protocol.ChannelDecisionID("chd_" + out.String()), nil
}

func insertDecisions(ctx context.Context, tx *sql.Tx, evaluated []gateEvaluation) error {
	query, err := queryText("decisions_insert.sql")
	if err != nil {
		return err
	}
	for _, item := range evaluated {
		params, err := decisionRecord(item.decided)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, query, params...); err != nil {
			return fmt.Errorf("insert channel decision: %w", err)
		}
	}
	return nil
}

func admissionResult(decided Decision, m Message, epoch int64) protocol.AdmissionResult {
	result := protocol.AdmissionResult{
		DecisionID:     decided.ID(),
		MessageID:      m.ID,
		ThreadID:       m.ThreadID,
		ControlVersion: epoch,
	}
	if decided.Outcome() == Allow {
		result.Result = protocol.AdmissionStatusAccepted
	} else {
		result.Result = protocol.AdmissionStatusAcceptedHeld
		result.ControlReason = decided.Reason()
	}
	return result
}

func tripleOf(decided Decision, epoch int64) triple {
	state := protocol.ControlStateAllowed
	switch decided.Outcome() {
	case Allow:
	case Hold:
		state = protocol.ControlStateHeld
	case Approval:
		state = protocol.ControlStateApprovalRequired
	case Deny:
		state = protocol.ControlStateDenied
	}
	return triple{state: state, reason: decided.Reason(), version: epoch}
}

func selectMessageControl(ctx context.Context, tx *sql.Tx, id protocol.ULID) (triple, bool, error) {
	query, err := queryText("messages_select_control.sql")
	if err != nil {
		return triple{}, false, err
	}
	var state, reason string
	var rawVersion any
	err = tx.QueryRowContext(ctx, query, string(id)).Scan(&state, &reason, &rawVersion)
	if errors.Is(err, sql.ErrNoRows) {
		return triple{}, false, nil
	}
	if err != nil {
		return triple{}, false, fmt.Errorf("select message control: %w", err)
	}
	version, ok := rawVersion.(int64)
	if !ok {
		return triple{}, false, fmt.Errorf("%w: message control version is not an integer", ErrInvalidControl)
	}
	return triple{state: protocol.ControlState(state), reason: protocol.DecisionReason(reason), version: version}, true, nil
}

func validateTriple(value triple) error {
	if !value.state.Valid() || !value.reason.Valid() || value.version < 0 {
		return fmt.Errorf("%w: invalid stored triple", ErrInvalidControl)
	}
	switch value.state {
	case protocol.ControlStateAllowed:
		if !allowReason(value.reason) {
			return fmt.Errorf("%w: ALLOWED with %s", ErrInvalidControl, value.reason)
		}
	case protocol.ControlStateApprovalRequired:
		if !approvalReason(value.reason) {
			return fmt.Errorf("%w: APPROVAL_REQUIRED with %s", ErrInvalidControl, value.reason)
		}
	case protocol.ControlStateDenied:
		if queuedReason(value.reason) {
			return fmt.Errorf("%w: DENIED with %s", ErrInvalidControl, value.reason)
		}
	case protocol.ControlStateHeld:
	}
	return nil
}

func allowReason(reason protocol.DecisionReason) bool {
	switch reason {
	case protocol.DecisionReasonAllowed, protocol.DecisionReasonApprovedOnce,
		protocol.DecisionReasonApprovedThread, protocol.DecisionReasonHoldReleased,
		protocol.DecisionReasonStepGranted, protocol.DecisionReasonBudgetExtended:
		return true
	default:
		return false
	}
}

func approvalReason(reason protocol.DecisionReason) bool {
	switch reason {
	case protocol.DecisionReasonLinkApproval, protocol.DecisionReasonLoopStepTokenRequired,
		protocol.DecisionReasonAgentAutoContinueOff, protocol.DecisionReasonObjectApprovalPending:
		return true
	default:
		return false
	}
}

func queuedReason(reason protocol.DecisionReason) bool {
	switch reason {
	case protocol.DecisionReasonObjectHeld, protocol.DecisionReasonObjectApprovalPending,
		protocol.DecisionReasonControlUnavailable, protocol.DecisionReasonControlChanged:
		return true
	default:
		return false
	}
}

func setMessageControl(ctx context.Context, tx *sql.Tx, id protocol.ULID, value triple) error {
	query, err := queryText("messages_set_control.sql")
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, query, value.state, value.reason, value.version, string(id))
	if err != nil {
		return fmt.Errorf("set message control: %w", err)
	}
	return requireOneRow(result, "set message control")
}

func compareAndSetMessageControl(ctx context.Context, tx *sql.Tx, id protocol.ULID, next, old triple) error {
	query, err := queryText("messages_cas_control.sql")
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, query,
		next.state, next.reason, next.version, string(id), old.state, old.reason, old.version)
	if err != nil {
		return fmt.Errorf("compare and set message control: %w", err)
	}
	return requireOneRow(result, "compare and set message control")
}

func setEnvelopeControl(ctx context.Context, tx *sql.Tx, id protocol.ULID, value triple) error {
	query, err := queryText("envelopes_set_control.sql")
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, query, value.state, value.reason, value.version, string(id))
	if err != nil {
		return fmt.Errorf("set envelope control: %w", err)
	}
	return requireOneRow(result, "set envelope control")
}

func requireOneRow(result sql.Result, operation string) error {
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s rows affected: %w", operation, err)
	}
	if count != 1 {
		return fmt.Errorf("%w: %s updated %d rows", ErrInvalidQuery, operation, count)
	}
	return nil
}

func envelopeCount(ctx context.Context, tx *sql.Tx, id protocol.ULID) (int64, error) {
	query, err := queryText("envelopes_count.sql")
	if err != nil {
		return 0, err
	}
	var count int64
	if err := tx.QueryRowContext(ctx, query, string(id)).Scan(&count); err != nil {
		return 0, fmt.Errorf("count message envelopes: %w", err)
	}
	return count, nil
}

func selectDecisionRecords(ctx context.Context, tx *sql.Tx, id protocol.ULID) ([]protocol.DecisionRecord, error) {
	return selectDecisionRecordsForObject(ctx, tx, protocol.DecisionObjectTypeMessage, protocol.OpaqueID(id))
}

func selectDecisionRecordsForObject(
	ctx context.Context,
	q Queryer,
	objectType protocol.DecisionObjectType,
	objectID protocol.OpaqueID,
) ([]protocol.DecisionRecord, error) {
	query, err := queryText("decisions_select_by_object.sql")
	if err != nil {
		return nil, err
	}
	rows, err := q.QueryContext(ctx, query, string(objectType), string(objectID))
	if err != nil {
		return nil, fmt.Errorf("select channel decisions: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var records []protocol.DecisionRecord
	for rows.Next() {
		rec, err := scanDecisionRecord(rows)
		if err != nil {
			_ = rows.Close()
			return nil, err
		}
		records = append(records, rec)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("read channel decisions: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close channel decisions: %w", err)
	}
	return records, nil
}

func scanDecisionRecord(rows *sql.Rows) (protocol.DecisionRecord, error) {
	var rec protocol.DecisionRecord
	var from, to, actor sql.NullString
	var matchedJSON, effectiveJSON, versionsJSON, explanationJSON, createdAt string
	if err := rows.Scan(
		&rec.DecisionID, &rec.ObjectType, &rec.ObjectID, &rec.Stage, &rec.Plane,
		&from, &to, &rec.Outcome, &rec.Reason, &matchedJSON, &effectiveJSON,
		&versionsJSON, &explanationJSON, &actor, &createdAt,
	); err != nil {
		return protocol.DecisionRecord{}, fmt.Errorf("scan channel decision: %w", err)
	}
	rec.FromAgent = protocol.AgentID(from.String)
	rec.ToAgent = protocol.AgentID(to.String)
	rec.ActorID = actor.String
	for name, item := range map[string]struct {
		raw  string
		into any
	}{
		"matched_controls_json":  {matchedJSON, &rec.MatchedControlIDs},
		"effective_control_json": {effectiveJSON, &rec.EffectiveControl},
		"control_versions_json":  {versionsJSON, &rec.ControlVersions},
		"explanation_json":       {explanationJSON, &rec.Explanation},
	} {
		if err := decodeJSON(item.raw, item.into); err != nil {
			return protocol.DecisionRecord{}, fmt.Errorf("%w: decision %s %s does not decode: %w", ErrInvalidControl, rec.DecisionID, name, err)
		}
	}
	parsed, err := parseChannelTime(createdAt)
	if err != nil {
		return protocol.DecisionRecord{}, fmt.Errorf("%w: decision %s created_at: %w", ErrInvalidControl, rec.DecisionID, err)
	}
	rec.CreatedAt = parsed
	if err := rec.Validate(); err != nil {
		return protocol.DecisionRecord{}, fmt.Errorf("%w: decision %s: %w", ErrInvalidControl, rec.DecisionID, err)
	}
	return rec, nil
}

func validateAntecedent(ctx context.Context, tx *sql.Tx, m Message) error {
	records, err := selectDecisionRecords(ctx, tx, m.ID)
	if err != nil {
		return err
	}
	want := make(map[protocol.AgentID]bool, len(m.To))
	for _, recipient := range m.To {
		want[recipient.Agent] = true
	}
	got := make(map[protocol.AgentID]bool, len(m.To))
	admissions := 0
	for _, rec := range records {
		if rec.ObjectType != protocol.DecisionObjectTypeMessage || rec.ObjectID != protocol.OpaqueID(m.ID) {
			return fmt.Errorf("%w: decision names another object", ErrInvalidControl)
		}
		if rec.Stage != protocol.DecisionStageAdmission {
			continue
		}
		admissions++
		if rec.Outcome != Allow && rec.Outcome != Hold || rec.Plane != protocol.PlaneMessage {
			return fmt.Errorf("%w: admission decision did not admit", ErrInvalidControl)
		}
		if rec.FromAgent != m.From || !want[rec.ToAgent] {
			return fmt.Errorf("%w: request does not match admission", ErrInvalidQuery)
		}
		if got[rec.ToAgent] {
			return fmt.Errorf("%w: duplicate admission recipient", ErrInvalidControl)
		}
		got[rec.ToAgent] = true
	}
	if admissions == 0 || len(got) != len(want) {
		return fmt.Errorf("%w: no complete local admission", ErrInvalidControl)
	}
	return nil
}

func strongerTriple(stored, evaluated triple) triple {
	if tripleClass(stored.state) > tripleClass(evaluated.state) {
		return stored
	}
	return evaluated
}

func tripleClass(state protocol.ControlState) int {
	switch state {
	case protocol.ControlStateApprovalRequired:
		return 1
	case protocol.ControlStateHeld:
		return 2
	case protocol.ControlStateDenied:
		return 3
	default:
		return 0
	}
}
