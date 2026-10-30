package agentlink

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

// InputGuardStore is the node's single writer for input_guards.
type InputGuardStore struct {
	db    *sql.DB
	clock clock.Clock
}

// NewInputGuardStore constructs a monotonic input-guard writer.
func NewInputGuardStore(db *sql.DB, clk clock.Clock) (*InputGuardStore, error) {
	if db == nil {
		return nil, ErrInvalidOptions
	}
	if clk == nil {
		clk = clock.System{}
	}
	return &InputGuardStore{db: db, clock: clk}, nil
}

// WriteInputGuard stores report only when its version is greater than the
// version already held for the Hub session. The report does not carry a
// submit-detector profile, so the canonical profile id comes from the durable
// session manifest as profiles/<harness>.yaml. Draft-opened, submit and cancel
// timestamps stay NULL because the report has no source for them.
func (s *InputGuardStore) WriteInputGuard(
	ctx context.Context,
	report protocol.InputGuardReport,
) (bool, error) {
	if err := report.Validate(); err != nil {
		return false, fmt.Errorf("write input guard: %w", err)
	}
	session, err := loadSession(ctx, s.db, report.SessionID, report.AgentID)
	if err != nil {
		return false, err
	}
	if session.capture != protocol.CaptureModePTYProxy || !session.deliverable ||
		session.manifest.CaptureMode != protocol.CaptureModePTYProxy || !session.manifest.Deliverable {
		return false, ErrSessionUnavailable
	}
	profileID := "profiles/" + string(session.manifest.Harness) + ".yaml"
	updatedAt := eventstore.FormatTime(s.clock.Now())
	var humanKeyAt any
	if !report.LastHumanKeyAt.IsZero() {
		humanKeyAt = eventstore.FormatTime(report.LastHumanKeyAt)
	}
	result, err := s.db.ExecContext(ctx, `INSERT INTO input_guards (
		session_id, agent_id, draft_generation, draft_state, activity_state,
		draft_opened_at, last_human_key_at, last_submit_at, last_cancel_at,
		detection_source, submit_detector_profile, confidence, version, updated_at
	) VALUES (?, ?, ?, ?, ?, NULL, ?, NULL, NULL, ?, ?, ?, ?, ?)
	ON CONFLICT(session_id) DO UPDATE SET
		agent_id = excluded.agent_id,
		draft_generation = excluded.draft_generation,
		draft_state = excluded.draft_state,
		activity_state = excluded.activity_state,
		draft_opened_at = NULL,
		last_human_key_at = excluded.last_human_key_at,
		last_submit_at = NULL,
		last_cancel_at = NULL,
		detection_source = excluded.detection_source,
		submit_detector_profile = excluded.submit_detector_profile,
		confidence = excluded.confidence,
		version = excluded.version,
		updated_at = excluded.updated_at
	WHERE excluded.version > input_guards.version`,
		report.SessionID, report.AgentID, report.DraftGeneration, report.DraftState,
		report.ActivityState, humanKeyAt, report.DetectionSource, profileID,
		report.Confidence, report.InputGuardVersion, updatedAt)
	if err != nil {
		return false, fmt.Errorf("upsert input guard: %w", err)
	}
	written, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read input guard result: %w", err)
	}
	return written == 1, nil
}
