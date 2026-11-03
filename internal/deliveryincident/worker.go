package deliveryincident

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	minimumObservations = int64(3)
	minimumAge          = 5 * time.Minute
	alertCooldown       = 4 * time.Hour
)

// Notifier files one content-free incident notice to an operator or original
// sender through the normal signed message path. The hub service implements it.
type Notifier interface {
	FileDeliveryIncidentNotice(context.Context, protocol.AgentID, Notice) error
}

// Options binds the worker to the register it alone owns.
type Options struct {
	DB       *sql.DB
	Clock    clock.Clock
	Operator protocol.AgentID
	Notifier Notifier
}

// Worker derives incidents from delivery_attempts without ever changing the
// attempt ledger. Durable pending records make filing restart-idempotent.
type Worker struct {
	db       *sql.DB
	clock    clock.Clock
	operator protocol.AgentID
	notifier Notifier
}

// NewWorker validates the worker dependencies. An empty operator is allowed:
// projection continues while notice filing stays disabled.
func NewWorker(opts Options) (*Worker, error) {
	if opts.DB == nil || opts.Clock == nil || opts.Notifier == nil ||
		opts.Operator != "" && !opts.Operator.Valid() {
		return nil, errors.New("delivery incident: invalid worker options")
	}
	return &Worker{db: opts.DB, clock: opts.Clock, operator: opts.Operator, notifier: opts.Notifier}, nil
}

// RunOnce refreshes the durable projection and files at most one notice. The
// return value is one only after a pending notice was filed and marked.
func (w *Worker) RunOnce(ctx context.Context) (int, error) {
	now := w.clock.Now().UTC()
	if w.operator.Valid() {
		pending, found, err := w.loadPending(ctx)
		if err != nil {
			return 0, err
		}
		if found {
			return w.filePending(ctx, pending, now)
		}
	}

	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("delivery incident: begin projection: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	episodes, err := loadEpisodes(ctx, tx)
	if err != nil {
		return 0, err
	}
	if err := projectEpisodes(ctx, tx, episodes, now); err != nil {
		return 0, err
	}
	var pending Notice
	var queued bool
	if w.operator.Valid() {
		pending, queued, err = queueDueNotice(ctx, tx, now)
		if err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("delivery incident: commit projection: %w", err)
	}
	if queued {
		return w.filePending(ctx, pending, now)
	}
	senderPending, found, err := w.loadPendingSender(ctx)
	if err != nil {
		return 0, err
	}
	if found {
		return w.filePendingSender(ctx, senderPending, now)
	}
	return 0, nil
}

type observation struct {
	deliveryID string
	messageID  string
	observedAt time.Time
}

type incidentKey struct {
	recipient protocol.AgentID
	reason    string
}

type episode struct {
	key         incidentKey
	observed    []observation
	firstSeen   time.Time
	lastSeen    time.Time
	recoveredAt time.Time
}

type episodeProjection struct {
	current   map[incidentKey]*episode
	recovered map[incidentKey][]*episode
}

func loadEpisodes(ctx context.Context, tx *sql.Tx) (episodeProjection, error) {
	rows, err := tx.QueryContext(ctx, `SELECT delivery_id, message_id, agent_id, state,
		COALESCE(terminal_reason, ''), finished_at
		FROM delivery_attempts
		WHERE state IN ('COMMITTED', 'COMMIT_NOT_CONSUMED', 'ABORTED_HUMAN_PRIORITY',
			'CANCELLED', 'FAILED', 'UNCERTAIN')
		AND finished_at IS NOT NULL
		ORDER BY agent_id, finished_at, delivery_id`)
	if err != nil {
		return episodeProjection{}, fmt.Errorf("delivery incident: read attempts: %w", err)
	}
	defer func() { _ = rows.Close() }()

	projection := episodeProjection{
		current:   make(map[incidentKey]*episode),
		recovered: make(map[incidentKey][]*episode),
	}
	active := make(map[protocol.AgentID]map[string]*episode)
	for rows.Next() {
		var deliveryID, messageID, recipientText, state, reasonText, finishedText string
		if err := rows.Scan(&deliveryID, &messageID, &recipientText, &state, &reasonText, &finishedText); err != nil {
			return episodeProjection{}, fmt.Errorf("delivery incident: scan attempt: %w", err)
		}
		finishedAt, err := eventstore.ParseTime(finishedText)
		if err != nil {
			return episodeProjection{}, fmt.Errorf("delivery incident: parse attempt finish: %w", err)
		}
		recipient := protocol.AgentID(recipientText)
		if !recipient.Valid() {
			return episodeProjection{}, fmt.Errorf("delivery incident: invalid attempt recipient %q", recipientText)
		}
		if state == "COMMITTED" {
			for _, current := range active[recipient] {
				if current.recoveredAt.IsZero() {
					current.recoveredAt = finishedAt
					projection.recovered[current.key] = append(
						projection.recovered[current.key], current,
					)
					delete(projection.current, current.key)
				}
			}
			delete(active, recipient)
			continue
		}
		if state == "ABORTED_HUMAN_PRIORITY" {
			continue
		}
		reason := normalizeReason(reasonText)
		if reason == "" {
			reason = normalizeReason(state)
		}
		if workingReason(reason) {
			continue
		}
		key := incidentKey{recipient: recipient, reason: reason}
		if active[recipient] == nil {
			active[recipient] = make(map[string]*episode)
		}
		current := active[recipient][reason]
		if current == nil {
			current = &episode{key: key, firstSeen: finishedAt}
			active[recipient][reason] = current
			projection.current[key] = current
		}
		current.lastSeen = finishedAt
		current.observed = append(current.observed, observation{
			deliveryID: deliveryID, messageID: messageID, observedAt: finishedAt,
		})
	}
	if err := rows.Err(); err != nil {
		return episodeProjection{}, fmt.Errorf("delivery incident: iterate attempts: %w", err)
	}
	return projection, nil
}

func normalizeReason(raw string) string {
	var normalized strings.Builder
	underscore := false
	for _, char := range strings.ToUpper(strings.TrimSpace(raw)) {
		if char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' {
			normalized.WriteRune(char)
			underscore = false
			continue
		}
		if normalized.Len() > 0 && !underscore {
			normalized.WriteByte('_')
			underscore = true
		}
	}
	return strings.Trim(normalized.String(), "_")
}

func workingReason(reason string) bool {
	return reason == "HUMAN_PRIORITY" || reason == "WORKING" || reason == "BUSY" ||
		strings.Contains(reason, "_WORKING") || strings.Contains(reason, "WORKING_") ||
		strings.Contains(reason, "_BUSY") || strings.Contains(reason, "BUSY_")
}

type storedIncident struct {
	firstSeen          string
	alertedAt          sql.NullString
	lastAlertedAt      sql.NullString
	recoveryNotifiedAt sql.NullString
	pendingKind        sql.NullString
	pendingAt          sql.NullString
	pendingMinutes     sql.NullInt64
	pendingAttempts    sql.NullInt64
	createdAt          string
}

func projectEpisodes(ctx context.Context, tx *sql.Tx, episodes episodeProjection, now time.Time) error {
	nowText := eventstore.FormatTime(now)
	keys := make(map[incidentKey]struct{}, len(episodes.current)+len(episodes.recovered))
	for key := range episodes.current {
		keys[key] = struct{}{}
	}
	for key := range episodes.recovered {
		keys[key] = struct{}{}
	}
	for key := range keys {
		stored, found, err := readStoredIncident(ctx, tx, key)
		if err != nil {
			return err
		}
		projected := episodeToProject(stored, found, episodes.current[key], episodes.recovered[key])
		if projected == nil {
			continue
		}
		firstText := eventstore.FormatTime(projected.firstSeen)
		if found && stored.firstSeen != firstText && stored.pendingKind.Valid {
			// A durable notice for the previous episode is an outbox item. Keep
			// it intact; the next turn projects the newer episode after filing.
			continue
		}
		createdAt := nowText
		var alertedAt, lastAlertedAt, recoveryNotifiedAt, pendingKind, pendingAt any
		var pendingMinutes, pendingAttempts any
		if found {
			lastAlertedAt = nullString(stored.lastAlertedAt)
		}
		if found && stored.firstSeen == firstText {
			createdAt = stored.createdAt
			alertedAt = nullString(stored.alertedAt)
			recoveryNotifiedAt = nullString(stored.recoveryNotifiedAt)
			pendingKind = nullString(stored.pendingKind)
			pendingAt = nullString(stored.pendingAt)
			pendingMinutes = nullInt64(stored.pendingMinutes)
			pendingAttempts = nullInt64(stored.pendingAttempts)
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO delivery_incidents
			(recipient_agent, normalized_reason, attempt_count, first_seen, last_seen,
			 alerted_at, last_alerted_at, recovered_at, recovery_notified_at,
			 pending_kind, pending_at, pending_minutes, pending_attempt_count,
			 created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT (recipient_agent, normalized_reason) DO UPDATE SET
			 attempt_count = excluded.attempt_count,
			 first_seen = excluded.first_seen,
			 last_seen = excluded.last_seen,
			 alerted_at = excluded.alerted_at,
			 last_alerted_at = excluded.last_alerted_at,
			 recovered_at = excluded.recovered_at,
			 recovery_notified_at = excluded.recovery_notified_at,
			 pending_kind = excluded.pending_kind,
			 pending_at = excluded.pending_at,
			 pending_minutes = excluded.pending_minutes,
			 pending_attempt_count = excluded.pending_attempt_count,
			 created_at = excluded.created_at,
			 updated_at = excluded.updated_at`,
			key.recipient, key.reason, len(projected.observed), firstText,
			eventstore.FormatTime(projected.lastSeen), alertedAt, lastAlertedAt,
			nullTime(projected.recoveredAt),
			recoveryNotifiedAt, pendingKind, pendingAt, pendingMinutes, pendingAttempts,
			createdAt, nowText)
		if err != nil {
			return fmt.Errorf("delivery incident: write projection: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM delivery_incident_messages
			WHERE recipient_agent = ? AND normalized_reason = ?`, key.recipient, key.reason); err != nil {
			return fmt.Errorf("delivery incident: replace message links: %w", err)
		}
		for _, observed := range projected.observed {
			if _, err := tx.ExecContext(ctx, `INSERT INTO delivery_incident_messages
				(recipient_agent, normalized_reason, delivery_id, message_id, observed_at)
				VALUES (?, ?, ?, ?, ?)`, key.recipient, key.reason, observed.deliveryID,
				observed.messageID, eventstore.FormatTime(observed.observedAt)); err != nil {
				return fmt.Errorf("delivery incident: link message: %w", err)
			}
		}
	}
	return nil
}

// episodeToProject keeps an alerted recovery in front of a same-key relapse.
// Once that recovery is durably notified, the current episode may replace it.
func episodeToProject(
	stored storedIncident,
	found bool,
	current *episode,
	recovered []*episode,
) *episode {
	if found {
		for _, candidate := range recovered {
			if eventstore.FormatTime(candidate.firstSeen) != stored.firstSeen {
				continue
			}
			if stored.alertedAt.Valid && !stored.recoveryNotifiedAt.Valid {
				return candidate
			}
			if current == nil {
				return candidate
			}
			break
		}
	}
	if current != nil {
		return current
	}
	if !found && len(recovered) > 0 {
		return recovered[len(recovered)-1]
	}
	return nil
}

func readStoredIncident(ctx context.Context, tx *sql.Tx, key incidentKey) (storedIncident, bool, error) {
	var stored storedIncident
	err := tx.QueryRowContext(ctx, `SELECT first_seen, alerted_at, last_alerted_at, recovery_notified_at,
		pending_kind, pending_at, pending_minutes, pending_attempt_count, created_at
		FROM delivery_incidents WHERE recipient_agent = ? AND normalized_reason = ?`,
		key.recipient, key.reason).Scan(&stored.firstSeen, &stored.alertedAt,
		&stored.lastAlertedAt, &stored.recoveryNotifiedAt, &stored.pendingKind, &stored.pendingAt,
		&stored.pendingMinutes, &stored.pendingAttempts, &stored.createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return stored, false, nil
	}
	if err != nil {
		return stored, false, fmt.Errorf("delivery incident: read projection: %w", err)
	}
	return stored, true, nil
}

func queueDueNotice(ctx context.Context, tx *sql.Tx, now time.Time) (Notice, bool, error) {
	rows, err := tx.QueryContext(ctx, `SELECT recipient_agent, normalized_reason,
		attempt_count, first_seen, alerted_at, recovered_at, recovery_notified_at
		FROM delivery_incidents WHERE pending_kind IS NULL
		ORDER BY first_seen, recipient_agent, normalized_reason`)
	if err != nil {
		return Notice{}, false, fmt.Errorf("delivery incident: read due incidents: %w", err)
	}
	defer func() { _ = rows.Close() }()
	type candidate struct {
		recipient                          protocol.AgentID
		reason, firstSeen                  string
		attempts                           int64
		alerted, recovered, recoveryNotice sql.NullString
	}
	var candidates []candidate
	for rows.Next() {
		var item candidate
		if err := rows.Scan(&item.recipient, &item.reason, &item.attempts, &item.firstSeen,
			&item.alerted, &item.recovered, &item.recoveryNotice); err != nil {
			return Notice{}, false, fmt.Errorf("delivery incident: scan due incident: %w", err)
		}
		candidates = append(candidates, item)
	}
	if err := rows.Err(); err != nil {
		return Notice{}, false, fmt.Errorf("delivery incident: iterate due incidents: %w", err)
	}
	if err := rows.Close(); err != nil {
		return Notice{}, false, fmt.Errorf("delivery incident: close due incidents: %w", err)
	}

	// Close an already-alerted episode before considering another alert.
	for _, item := range candidates {
		if !item.recovered.Valid || !item.alerted.Valid || item.recoveryNotice.Valid {
			continue
		}
		firstSeen, recoveredAt, err := parseIncidentTimes(item.firstSeen, item.recovered.String)
		if err != nil {
			return Notice{}, false, err
		}
		notice := Notice{
			Kind: NoticeRecovery, Recipient: item.recipient,
			NormalizedReason: item.reason, Minutes: elapsedMinutes(firstSeen, recoveredAt),
			Attempts: item.attempts, QueuedAt: now,
		}
		return storePending(ctx, tx, notice)
	}
	for _, item := range candidates {
		if item.recovered.Valid || item.attempts < minimumObservations {
			continue
		}
		firstSeen, err := eventstore.ParseTime(item.firstSeen)
		if err != nil {
			return Notice{}, false, fmt.Errorf("delivery incident: parse first seen: %w", err)
		}
		if now.Sub(firstSeen) < minimumAge {
			continue
		}
		if item.alerted.Valid {
			alertedAt, err := eventstore.ParseTime(item.alerted.String)
			if err != nil {
				return Notice{}, false, fmt.Errorf("delivery incident: parse alerted at: %w", err)
			}
			if now.Sub(alertedAt) < alertCooldown {
				continue
			}
		}
		coolingDown, err := recipientCoolingDown(ctx, tx, item.recipient, now)
		if err != nil {
			return Notice{}, false, err
		}
		if coolingDown {
			continue
		}
		busy, err := recipientBusy(ctx, tx, item.recipient, now)
		if err != nil {
			return Notice{}, false, err
		}
		if busy {
			continue
		}
		notice := Notice{
			Kind: NoticeAlert, Recipient: item.recipient,
			NormalizedReason: item.reason, Minutes: elapsedMinutes(firstSeen, now),
			Attempts: item.attempts, QueuedAt: now,
		}
		return storePending(ctx, tx, notice)
	}
	return Notice{}, false, nil
}

func recipientCoolingDown(
	ctx context.Context,
	q queryer,
	recipient protocol.AgentID,
	now time.Time,
) (bool, error) {
	var latest sql.NullString
	if err := q.QueryRowContext(ctx, `SELECT MAX(last_alerted_at) FROM delivery_incidents
		WHERE recipient_agent = ?`, recipient).Scan(&latest); err != nil {
		return false, fmt.Errorf("delivery incident: read recipient cooldown: %w", err)
	}
	if !latest.Valid {
		return false, nil
	}
	alertedAt, err := eventstore.ParseTime(latest.String)
	if err != nil {
		return false, fmt.Errorf("delivery incident: parse recipient cooldown: %w", err)
	}
	return now.Sub(alertedAt) < alertCooldown, nil
}

func storePending(ctx context.Context, tx *sql.Tx, notice Notice) (Notice, bool, error) {
	if !notice.Valid() {
		return Notice{}, false, errors.New("delivery incident: invalid pending notice")
	}
	result, err := tx.ExecContext(ctx, `UPDATE delivery_incidents SET
		pending_kind = ?, pending_at = ?, pending_minutes = ?, pending_attempt_count = ?, updated_at = ?
		WHERE recipient_agent = ? AND normalized_reason = ? AND pending_kind IS NULL`,
		notice.Kind, eventstore.FormatTime(notice.QueuedAt), notice.Minutes, notice.Attempts,
		eventstore.FormatTime(notice.QueuedAt), notice.Recipient, notice.NormalizedReason)
	if err != nil {
		return Notice{}, false, fmt.Errorf("delivery incident: queue notice: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return Notice{}, false, fmt.Errorf("delivery incident: queue notice count: %w", err)
	}
	return notice, changed == 1, nil
}

func (w *Worker) loadPending(ctx context.Context) (Notice, bool, error) {
	var notice Notice
	var kind, queuedAt string
	err := w.db.QueryRowContext(ctx, `SELECT pending_kind, recipient_agent, normalized_reason,
		pending_minutes, pending_attempt_count, pending_at
		FROM delivery_incidents WHERE pending_kind IS NOT NULL
		ORDER BY pending_at, recipient_agent, normalized_reason LIMIT 1`).Scan(
		&kind, &notice.Recipient, &notice.NormalizedReason, &notice.Minutes, &notice.Attempts, &queuedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Notice{}, false, nil
	}
	if err != nil {
		return Notice{}, false, fmt.Errorf("delivery incident: read pending notice: %w", err)
	}
	notice.Kind = NoticeKind(kind)
	notice.QueuedAt, err = eventstore.ParseTime(queuedAt)
	if err != nil {
		return Notice{}, false, fmt.Errorf("delivery incident: parse pending notice: %w", err)
	}
	if !notice.Valid() {
		return Notice{}, false, errors.New("delivery incident: stored pending notice is invalid")
	}
	return notice, true, nil
}

type pendingSenderNotice struct {
	destination protocol.AgentID
	notice      Notice
}

func (w *Worker) loadPendingSender(ctx context.Context) (pendingSenderNotice, bool, error) {
	var pending pendingSenderNotice
	var kind, queuedAt string
	err := w.db.QueryRowContext(ctx, `SELECT sender_agent, kind, recipient_agent,
		normalized_reason, minutes, attempt_count, queued_at
		FROM delivery_incident_sender_notices WHERE filed_at IS NULL
		ORDER BY queued_at, recipient_agent, normalized_reason, sender_agent LIMIT 1`).Scan(
		&pending.destination, &kind, &pending.notice.Recipient,
		&pending.notice.NormalizedReason, &pending.notice.Minutes,
		&pending.notice.Attempts, &queuedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return pendingSenderNotice{}, false, nil
	}
	if err != nil {
		return pendingSenderNotice{}, false, fmt.Errorf("delivery incident: read pending sender notice: %w", err)
	}
	pending.notice.Kind = NoticeKind(kind)
	pending.notice.QueuedAt, err = eventstore.ParseTime(queuedAt)
	if err != nil {
		return pendingSenderNotice{}, false, fmt.Errorf("delivery incident: parse pending sender notice: %w", err)
	}
	if !pending.destination.Valid() || !pending.notice.Valid() {
		return pendingSenderNotice{}, false, errors.New("delivery incident: stored pending sender notice is invalid")
	}
	return pending, true, nil
}

func (w *Worker) filePending(ctx context.Context, notice Notice, now time.Time) (int, error) {
	if notice.Kind == NoticeAlert {
		busy, err := recipientBusy(ctx, w.db, notice.Recipient, now)
		if err != nil {
			return 0, err
		}
		if busy {
			return 0, nil
		}
	}
	if err := w.notifier.FileDeliveryIncidentNotice(ctx, w.operator, notice); err != nil {
		return 0, fmt.Errorf("delivery incident: file notice: %w", err)
	}
	stamp := eventstore.FormatTime(now)
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("delivery incident: begin notice mark: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	args := []any{
		stamp, stamp, notice.Recipient, notice.NormalizedReason,
		notice.Kind, eventstore.FormatTime(notice.QueuedAt),
	}
	var result sql.Result
	if notice.Kind == NoticeAlert {
		result, err = tx.ExecContext(ctx, `UPDATE delivery_incidents SET alerted_at = ?, last_alerted_at = ?,
			pending_kind = NULL, pending_at = NULL, pending_minutes = NULL,
			pending_attempt_count = NULL, updated_at = ?
			WHERE recipient_agent = ? AND normalized_reason = ?
			AND pending_kind = ? AND pending_at = ?`, stamp, stamp, stamp,
			notice.Recipient, notice.NormalizedReason, notice.Kind,
			eventstore.FormatTime(notice.QueuedAt))
	} else {
		result, err = tx.ExecContext(ctx, `UPDATE delivery_incidents SET recovery_notified_at = ?,
			pending_kind = NULL, pending_at = NULL, pending_minutes = NULL,
			pending_attempt_count = NULL, updated_at = ?
			WHERE recipient_agent = ? AND normalized_reason = ?
			AND pending_kind = ? AND pending_at = ?`, args...)
	}
	if err != nil {
		return 0, fmt.Errorf("delivery incident: mark notice: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("delivery incident: mark notice count: %w", err)
	}
	if changed == 0 {
		return 0, nil
	}
	senderNotice := notice
	senderNotice.QueuedAt = now
	if err := queueSenderNotices(ctx, tx, senderNotice); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("delivery incident: commit notice mark: %w", err)
	}
	return 1, nil
}

func queueSenderNotices(ctx context.Context, tx *sql.Tx, notice Notice) error {
	rows, err := tx.QueryContext(ctx, `SELECT DISTINCT m.from_agent
		FROM delivery_incident_messages AS links
		JOIN messages AS m ON m.message_id = links.message_id
		WHERE links.recipient_agent = ? AND links.normalized_reason = ?
		ORDER BY m.from_agent`, notice.Recipient, notice.NormalizedReason)
	if err != nil {
		return fmt.Errorf("delivery incident: read incident senders: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var senders []protocol.AgentID
	for rows.Next() {
		var sender protocol.AgentID
		if err := rows.Scan(&sender); err != nil {
			return fmt.Errorf("delivery incident: scan incident sender: %w", err)
		}
		if !sender.Valid() {
			return fmt.Errorf("delivery incident: invalid incident sender %q", sender)
		}
		senders = append(senders, sender)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("delivery incident: iterate incident senders: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("delivery incident: close incident senders: %w", err)
	}

	stamp := eventstore.FormatTime(notice.QueuedAt)
	for _, sender := range senders {
		if notice.Kind == NoticeAlert {
			coolingDown, err := senderCoolingDown(ctx, tx, sender, notice.Recipient, notice.QueuedAt)
			if err != nil {
				return err
			}
			if coolingDown {
				continue
			}
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO delivery_incident_sender_notices
			(recipient_agent, normalized_reason, sender_agent, kind, queued_at,
			 minutes, attempt_count, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT (recipient_agent, normalized_reason, sender_agent, kind, queued_at)
			DO NOTHING`, notice.Recipient, notice.NormalizedReason, sender, notice.Kind,
			stamp, notice.Minutes, notice.Attempts, stamp, stamp); err != nil {
			return fmt.Errorf("delivery incident: queue sender notice: %w", err)
		}
	}
	return nil
}

func senderCoolingDown(
	ctx context.Context,
	q queryer,
	sender, recipient protocol.AgentID,
	now time.Time,
) (bool, error) {
	var latest sql.NullString
	var pending int64
	if err := q.QueryRowContext(ctx, `SELECT MAX(filed_at),
		COALESCE(SUM(CASE WHEN filed_at IS NULL THEN 1 ELSE 0 END), 0)
		FROM delivery_incident_sender_notices
		WHERE sender_agent = ? AND recipient_agent = ? AND kind = ?`,
		sender, recipient, NoticeAlert).Scan(&latest, &pending); err != nil {
		return false, fmt.Errorf("delivery incident: read sender cooldown: %w", err)
	}
	if pending > 0 {
		return true, nil
	}
	if !latest.Valid {
		return false, nil
	}
	filedAt, err := eventstore.ParseTime(latest.String)
	if err != nil {
		return false, fmt.Errorf("delivery incident: parse sender cooldown: %w", err)
	}
	return now.Sub(filedAt) < alertCooldown, nil
}

func (w *Worker) filePendingSender(
	ctx context.Context,
	pending pendingSenderNotice,
	now time.Time,
) (int, error) {
	if err := w.notifier.FileDeliveryIncidentNotice(
		ctx,
		pending.destination,
		pending.notice,
	); err != nil {
		return 0, fmt.Errorf("delivery incident: file sender notice: %w", err)
	}
	stamp := eventstore.FormatTime(now)
	result, err := w.db.ExecContext(ctx, `UPDATE delivery_incident_sender_notices
		SET filed_at = ?, updated_at = ?
		WHERE recipient_agent = ? AND normalized_reason = ? AND sender_agent = ?
		AND kind = ? AND queued_at = ? AND filed_at IS NULL`,
		stamp, stamp, pending.notice.Recipient, pending.notice.NormalizedReason,
		pending.destination, pending.notice.Kind, eventstore.FormatTime(pending.notice.QueuedAt))
	if err != nil {
		return 0, fmt.Errorf("delivery incident: mark sender notice: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("delivery incident: mark sender notice count: %w", err)
	}
	if changed == 0 {
		return 0, nil
	}
	return 1, nil
}

type queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func recipientBusy(ctx context.Context, q queryer, recipient protocol.AgentID, now time.Time) (bool, error) {
	var phase string
	err := q.QueryRowContext(ctx, `SELECT agent_phase FROM state_snapshots
		WHERE agent_id = ? AND stale = 0 AND expires_at > ?
		ORDER BY observed_at DESC, state_version DESC, state_id DESC LIMIT 1`,
		recipient, eventstore.FormatTime(now)).Scan(&phase)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("delivery incident: read recipient state: %w", err)
	}
	switch phase {
	case "THINKING_STREAMING", "RUNNING_TOOL", "BACKGROUND_WORK":
		return true, nil
	default:
		return false, nil
	}
}

func parseIncidentTimes(first, second string) (time.Time, time.Time, error) {
	firstTime, err := eventstore.ParseTime(first)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("delivery incident: parse first seen: %w", err)
	}
	secondTime, err := eventstore.ParseTime(second)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("delivery incident: parse recovery: %w", err)
	}
	return firstTime, secondTime, nil
}

func elapsedMinutes(start, end time.Time) int64 {
	if !end.After(start) {
		return 0
	}
	return int64(end.Sub(start).Truncate(time.Minute).Minutes())
}

func nullTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return eventstore.FormatTime(value)
}

func nullString(value sql.NullString) any {
	if !value.Valid {
		return nil
	}
	return value.String
}

func nullInt64(value sql.NullInt64) any {
	if !value.Valid {
		return nil
	}
	return value.Int64
}
