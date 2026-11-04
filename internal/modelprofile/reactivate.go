package modelprofile

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	// fallbackActorID is the actor of an automatic fallback to base. Only a
	// selection it wrote is ever reactivated automatically: an operator
	// rollback stays where the operator put it.
	fallbackActorID = "model-gateway"
	// reactivationActorID is the actor of an automatic reactivation.
	reactivationActorID = "model-reactivation"
	// reactivationReason is the reason recorded on the reactivated generation.
	reactivationReason = "automatic reactivation after fallback"
	// reactivationFirstDelay is the wait before the first probe. It matches
	// the adapter breaker's open window, so the probe is not refused by it.
	reactivationFirstDelay = 30 * time.Second
	// reactivationMaxDelay caps the doubling backoff between probes.
	reactivationMaxDelay = 5 * time.Minute
)

// NoticeKind names one model transition an operator is told about.
type NoticeKind string

const (
	// NoticeFallback says the adapter was replaced by the base model.
	NoticeFallback NoticeKind = "fallback"
	// NoticeReactivated says the adapter is selected again.
	NoticeReactivated NoticeKind = "reactivated"
)

// Notice is the metadata-only operator notice for one automatic model
// transition. Reason is a fixed gateway phrase, never model output.
type Notice struct {
	Kind            NoticeKind
	Generation      int64
	AdapterName     string
	AdapterRevision string
	Reason          string
}

// Notifier tells the operators about one automatic model transition. It
// must not block the selection change: errors are the notifier's own.
type Notifier func(context.Context, Notice)

// demotedAdapter is the adapter an automatic fallback replaced, held until
// it is reactivated or an operator changes the selection.
type demotedAdapter struct {
	name        string
	revision    string
	baseGen     int64
	attempts    int
	nextAttempt time.Time
}

// SetNotifier installs the operator notifier for automatic transitions.
func (s *Service) SetNotifier(notify Notifier) {
	s.mu.Lock()
	s.notify = notify
	s.mu.Unlock()
}

// noteFallback remembers the adapter an automatic fallback replaced; the
// first probe waits delay. The caller holds s.mu.
func (s *Service) noteFallback(before, after protocol.ModelSelection, delay time.Duration) {
	s.demoted = &demotedAdapter{
		name:        before.Profile.AdapterName,
		revision:    before.Profile.AdapterRevision,
		baseGen:     after.Generation,
		nextAttempt: s.clock.Now().Add(delay),
	}
}

// recoverDemoted rebuilds the pending reactivation after a restart: the
// current selection is a DEGRADED base written by the automatic fallback,
// and its event names the adapter it replaced. Anything else leaves
// nothing pending, and a fallback this process saw already holds its own.
func (s *Service) recoverDemoted(ctx context.Context) error {
	current := s.publisher.CurrentModelSelection()
	if s.demoted != nil || current.Profile.InferenceProfile != protocol.InferenceProfileBase ||
		current.OperationalState != "DEGRADED" || current.EventID == "" {
		return nil
	}
	var raw string
	err := s.db.QueryRowContext(ctx, `SELECT payload_json FROM events WHERE event_id = ? AND event_type = ?`,
		current.EventID, protocol.EventTypeControlModelProfileChanged).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read model fallback event: %w", err)
	}
	// An unreadable event names no adapter: nothing is reactivated.
	var change changePayload
	decoded := json.Unmarshal([]byte(raw), &change) == nil
	if !decoded || change.ActorID != fallbackActorID ||
		change.Generation != current.Generation || change.Before == nil ||
		change.Before.Profile.InferenceProfile != protocol.InferenceProfileAnalystLoRA {
		return nil
	}
	// A restarted gateway's breakers start closed: probe at once.
	s.noteFallback(*change.Before, current, 0)
	return nil
}

// ReactivateOnce probes the adapter an automatic fallback replaced once its
// backoff has elapsed, and selects it again when the serving boundary
// accepts it. It reports whether the adapter was reactivated. A failed
// probe doubles the wait, up to reactivationMaxDelay. Any selection change
// since the fallback, such as an operator activation or rollback, cancels
// the reactivation.
func (s *Service) ReactivateOnce(ctx context.Context) (bool, error) {
	s.mu.Lock()
	if !s.recovered {
		if err := s.recoverDemoted(ctx); err != nil {
			s.mu.Unlock()
			return false, err
		}
		s.recovered = true
	}
	pending := s.demoted
	if pending == nil || s.clock.Now().Before(pending.nextAttempt) {
		s.mu.Unlock()
		return false, nil
	}
	current := s.publisher.CurrentModelSelection()
	if current.Generation != pending.baseGen ||
		current.Profile.InferenceProfile != protocol.InferenceProfileBase {
		s.demoted = nil
		s.mu.Unlock()
		return false, nil
	}
	next, err := s.reactivationCandidate(ctx, pending, current)
	if err == nil {
		s.verifyMu.RLock()
		verifier := s.verifier
		s.verifyMu.RUnlock()
		if verifier == nil || verifier.VerifyModelSelection(ctx, next) != nil {
			err = ErrProfileNotServed
		}
	}
	if err == nil {
		err = s.persistChange(ctx, current, next, channel.Actor{Type: protocol.ActorTypeSystem, ID: reactivationActorID})
	}
	if err != nil {
		pending.attempts++
		pending.nextAttempt = s.clock.Now().Add(reactivationBackoff(pending.attempts))
		s.mu.Unlock()
		if errors.Is(err, ErrProfileNotServed) {
			return false, nil
		}
		return false, err
	}
	s.demoted = nil
	notify := s.notify
	s.mu.Unlock()
	if notify != nil {
		notify(context.WithoutCancel(ctx), Notice{
			Kind: NoticeReactivated, Generation: next.Generation,
			AdapterName: next.Profile.AdapterName, AdapterRevision: next.Profile.AdapterRevision,
			Reason: reactivationReason,
		})
	}
	return true, nil
}

func (s *Service) reactivationCandidate(
	ctx context.Context,
	pending *demotedAdapter,
	current protocol.ModelSelection,
) (protocol.ModelSelection, error) {
	metadata, err := readRegistration(ctx, s.db, pending.name, pending.revision)
	if err != nil {
		return protocol.ModelSelection{}, err
	}
	return s.adapterSelection(metadata, current.Generation+1, reactivationReason, "READY")
}

// reactivationBackoff is the wait after the n-th failed probe.
func reactivationBackoff(failures int) time.Duration {
	delay := reactivationFirstDelay
	for range failures {
		delay *= 2
		if delay >= reactivationMaxDelay {
			return reactivationMaxDelay
		}
	}
	return delay
}
