package channel

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/smellak/gifford/internal/protocol"
)

// Artifact is the authenticated, ciphertext-only metadata needed by the
// artifact admission gate.
type Artifact struct {
	ID              protocol.OpaqueID
	From            protocol.AgentID
	FromNode        protocol.NodeID
	To              Recipient
	ToNode          protocol.NodeID
	CiphertextBytes int64
}

func (a Artifact) validate() error {
	switch {
	case !a.ID.Valid():
		return fmt.Errorf("%w: id", ErrInvalidQuery)
	case !a.From.Valid():
		return fmt.Errorf("%w: from", ErrInvalidQuery)
	case !a.FromNode.Valid() || !a.ToNode.Valid() || a.FromNode != a.ToNode:
		return fmt.Errorf("%w: node", ErrInvalidQuery)
	case !a.To.Agent.Valid() || a.To.Agent == a.From:
		return fmt.Errorf("%w: to", ErrInvalidQuery)
	case a.To.Policy != PolicyAllowed && a.To.Policy != PolicyDenied:
		return fmt.Errorf("%w: policy", ErrInvalidQuery)
	case a.CiphertextBytes < 16:
		return fmt.Errorf("%w: artifact bytes", ErrInvalidQuery)
	default:
		return nil
	}
}

// ArtifactGateError is any non-ALLOW artifact verdict. H24 deliberately does
// not queue HOLD or approval artifacts, so callers commit only its decision
// audit and store no transfer.
type ArtifactGateError struct {
	decisionID protocol.ChannelDecisionID
	outcome    protocol.DecisionOutcome
	reason     protocol.DecisionReason
	version    int64
}

func (e *ArtifactGateError) Error() string { return "artifact channel not allowed" }

// DecisionID returns the governing channel decision.
func (e *ArtifactGateError) DecisionID() protocol.ChannelDecisionID { return e.decisionID }

// Outcome returns the non-ALLOW verdict.
func (e *ArtifactGateError) Outcome() protocol.DecisionOutcome { return e.outcome }

// Reason returns the governing channel reason.
func (e *ArtifactGateError) Reason() protocol.DecisionReason { return e.reason }

// Version returns the node control epoch used by the gate.
func (e *ArtifactGateError) Version() int64 { return e.version }

// AdmitArtifact evaluates the artifact plane and stores the transfer only for
// ALLOW. It returns Acceptance so no Decision escapes the channel package.
func (e *Engine) AdmitArtifact(
	ctx context.Context,
	tx *sql.Tx,
	a Artifact,
	store func(context.Context) error,
) (Acceptance, error) {
	if tx == nil || store == nil {
		return Acceptance{}, fmt.Errorf("%w: transaction or store", ErrInvalidQuery)
	}
	if err := a.validate(); err != nil {
		return Acceptance{}, err
	}
	if err := ensureNewArtifactAdmission(ctx, tx, a.ID); err != nil {
		return Acceptance{}, err
	}
	now := e.clock.Now()
	v, epoch, err := readGateView(ctx, tx)
	if err != nil {
		return Acceptance{}, err
	}
	evaluated, err := evaluateArtifact(v, a, now)
	if err != nil {
		return Acceptance{}, err
	}
	governing, err := e.prepareDecisionsWithApproval(evaluated, now, true)
	if err != nil {
		return Acceptance{}, err
	}
	if err := insertDecisions(ctx, tx, evaluated); err != nil {
		return Acceptance{}, err
	}
	if governing.Outcome() != Allow {
		return Acceptance{}, &ArtifactGateError{
			decisionID: governing.ID(), outcome: governing.Outcome(),
			reason: governing.Reason(), version: epoch,
		}
	}
	accepted := Acceptance{evaluation: governing, final: tripleOf(governing, epoch)}
	if err := store(ctx); err != nil {
		return Acceptance{}, fmt.Errorf("%w: %s", ErrStore, err.Error())
	}
	if err := setArtifactControl(ctx, tx, a.ID, accepted.final); err != nil {
		return Acceptance{}, err
	}
	return accepted, nil
}

func evaluateArtifact(v view, a Artifact, now time.Time) ([]gateEvaluation, error) {
	query := Query{
		Stage: protocol.DecisionStageAdmission, Plane: protocol.PlaneArtifact,
		From: a.From, FromNode: a.FromNode, To: a.To.Agent, ToNode: a.ToNode,
		ToDeliverable: a.To.Deliverable, Kind: KindRoot,
		Object:       &Object{Type: protocol.DecisionObjectTypeArtifact, ID: a.ID},
		PolicyDenied: a.To.Policy == PolicyDenied, ArtifactBytes: a.CiphertextBytes,
	}
	if err := query.Validate(); err != nil {
		return nil, err
	}
	decided, effective, versions := v.evaluate(query, now)
	return []gateEvaluation{{query: query, verdict: decided, eff: effective, vers: versions}}, nil
}

func ensureNewArtifactAdmission(ctx context.Context, tx *sql.Tx, id protocol.OpaqueID) error {
	if _, found, err := selectArtifactControl(ctx, tx, id); err != nil {
		return err
	} else if found {
		return fmt.Errorf("%w: artifact already stored", ErrInvalidQuery)
	}
	return nil
}

func selectArtifactControl(
	ctx context.Context,
	tx *sql.Tx,
	id protocol.OpaqueID,
) (triple, bool, error) {
	query, err := queryText("artifacts_select_control.sql")
	if err != nil {
		return triple{}, false, err
	}
	var state, reason string
	var version any
	err = tx.QueryRowContext(ctx, query, string(id)).Scan(&state, &reason, &version)
	if errors.Is(err, sql.ErrNoRows) {
		return triple{}, false, nil
	}
	if err != nil {
		return triple{}, false, fmt.Errorf("select artifact control: %w", err)
	}
	parsed, ok := version.(int64)
	if !ok {
		return triple{}, false, fmt.Errorf("%w: artifact control version is not an integer", ErrInvalidControl)
	}
	return triple{state: protocol.ControlState(state), reason: protocol.DecisionReason(reason), version: parsed}, true, nil
}

func setArtifactControl(ctx context.Context, tx *sql.Tx, id protocol.OpaqueID, value triple) error {
	query, err := queryText("artifacts_set_control.sql")
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, query, value.state, value.reason, value.version, string(id))
	if err != nil {
		return fmt.Errorf("set artifact control: %w", err)
	}
	return requireOneRow(result, "set artifact control")
}
