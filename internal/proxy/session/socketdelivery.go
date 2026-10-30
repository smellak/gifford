//go:build unix

package session

import (
	"errors"
	"fmt"

	"github.com/smellak/gifford/internal/agentsock"
	"github.com/smellak/gifford/internal/protocol"
)

// SocketDelivery is the session's side of the deliveries the agent
// socket runs.
func (s *Session) SocketDelivery() agentsock.Delivery { return socketDelivery{s: s} }

// socketDelivery runs the socket's delivery orders on the session.
type socketDelivery struct{ s *Session }

// socketResults maps the session outcomes to the wire results.
var socketResults = map[DeliveryOutcome]protocol.DeliveryResultKind{
	DeliveryPreparedOutcome:             protocol.DeliveryResultKindPrepared,
	DeliveryCommittedOutcome:            protocol.DeliveryResultKindCommitted,
	DeliveryAbortedHumanPriorityOutcome: protocol.DeliveryResultKindAbortedHumanPriority,
	DeliveryCancelledOutcome:            protocol.DeliveryResultKindCancelled,
}

// CheckLine is the check the commit runs on its line.
func (d socketDelivery) CheckLine(line string) error { return validateDeliveryLine(line) }

func (d socketDelivery) Prepare(id protocol.OpaqueID, generation uint64, version int64) (agentsock.DeliveryReport, error) {
	return socketReport(d.s.PrepareDelivery(id, generation, version))
}

func (d socketDelivery) Commit(id protocol.OpaqueID, line string, mode protocol.InjectMode) (agentsock.DeliveryReport, error) {
	return socketReport(d.s.CommitDelivery(id, line, mode))
}

// Cancel records the reason as the session records any: never
// interpreted.
func (d socketDelivery) Cancel(id protocol.OpaqueID, reason protocol.CancelReason) (agentsock.DeliveryReport, error) {
	return socketReport(d.s.CancelDelivery(id, string(reason)))
}

// DefaultMode is the profile's: bracketed_paste where it pastes, typed
// otherwise.
func (d socketDelivery) DefaultMode() protocol.InjectMode {
	if d.s.opts.Profile.BracketedPaste {
		return protocol.InjectModeBracketedPaste
	}
	return protocol.InjectModeTyped
}

// InputHolds reads the emulator's last snapshot for positive evidence
// that the input line holds exactly line: a live session, a measured
// profile with an input line pattern, nothing held back, a visible
// cursor in range, and the draft of the cursor row equal to line, not
// merely containing it.
func (d socketDelivery) InputHolds(line string) bool {
	s := d.s
	s.mu.Lock()
	snap, ok := s.lastSnap, s.hasSnap && s.running
	s.mu.Unlock()
	if !ok {
		return false
	}
	got, ok := cursorDraft(s.opts.Profile, snap)
	return ok && got == line
}

// socketReport maps one session answer for the socket. A delivery
// already started is the socket's ErrDeliveryActive.
func socketReport(rep DeliveryReport, err error) (agentsock.DeliveryReport, error) {
	if errors.Is(err, ErrDeliveryActive) {
		return agentsock.DeliveryReport{}, fmt.Errorf("%w: %w", agentsock.ErrDeliveryActive, err)
	}
	if err != nil {
		return agentsock.DeliveryReport{}, err
	}
	return agentsock.DeliveryReport{Result: socketResults[rep.Outcome], Generation: rep.Generation, Version: rep.Version}, nil
}
