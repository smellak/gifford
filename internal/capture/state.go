package capture

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/smellak/gifford/internal/protocol"
)

// Life is the derived §6.2 lifecycle of a session. The events are the
// truth; the row's state is their cache.
type Life int

const (
	// LifeNoRow means no session row exists: nothing may start.
	LifeNoRow Life = iota
	// LifeIncomplete means the row exists but no capture.attached
	// sits at sequence 0.
	LifeIncomplete
	// LifeLive means the session may write: attached, not terminal.
	LifeLive
	// LifeTerminal means a capture.stopped event exists.
	LifeTerminal
)

// HasAttachedAt0 reports a capture.attached at sequence 0.
func HasAttachedAt0(events []protocol.EventEnvelope) bool {
	for _, ev := range events {
		if ev.Sequence == 0 && ev.EventType == protocol.EventTypeCaptureAttached {
			return true
		}
	}
	return false
}

// FindStopped returns the last capture.stopped event, if any.
func FindStopped(events []protocol.EventEnvelope) *protocol.EventEnvelope {
	var found *protocol.EventEnvelope
	for i := range events {
		if events[i].EventType == protocol.EventTypeCaptureStopped {
			found = &events[i]
		}
	}
	return found
}

// DeriveLife derives the lifecycle: terminal iff a capture.stopped
// event exists; incomplete iff the row exists without an attached at
// zero; live otherwise. Without a row nothing may start.
func DeriveLife(hasRow bool, events []protocol.EventEnvelope) Life {
	if FindStopped(events) != nil {
		return LifeTerminal
	}
	if !hasRow {
		return LifeNoRow
	}
	if !HasAttachedAt0(events) {
		return LifeIncomplete
	}
	return LifeLive
}

// Row states and stop reasons shared by the state derivation, the
// repair and the ingest terminal paths.
const (
	stateAttached = "ATTACHED"
	stateError    = "ERROR"
	reasonError   = "error"
)

// StoppedRowState maps a stop reason to its cached row state: quota
// goes QUOTA, error goes ERROR, anything else goes STOPPED.
func StoppedRowState(reason protocol.CaptureStopReason) string {
	switch reason {
	case protocol.CaptureStopReasonQuota:
		return "QUOTA"
	case protocol.CaptureStopReasonError:
		return stateError
	default:
		return "STOPPED"
	}
}

// payloadValidator is a capture payload with strict validation.
type payloadValidator interface {
	Validate() error
}

// decodePayload unmarshals one envelope's raw payload into v,
// validating the capture payload strictly.
func decodePayload(ev *protocol.EventEnvelope, v payloadValidator) error {
	if err := json.Unmarshal([]byte(ev.Payload), v); err != nil {
		return fmt.Errorf("%w: bad %s payload: %w", ErrUnsafePath, ev.EventType, err)
	}
	if err := v.Validate(); err != nil {
		return fmt.Errorf("%w: bad %s payload: %w", ErrUnsafePath, ev.EventType, err)
	}
	return nil
}

// StoppedReason decodes the reason of a capture.stopped envelope.
func StoppedReason(ev *protocol.EventEnvelope) (protocol.CaptureStopReason, error) {
	var stopped protocol.CaptureStopped
	if err := decodePayload(ev, &stopped); err != nil {
		return "", err
	}
	return stopped.Reason, nil
}

// Slot is the §4.5 logical slot: the server instance plus the tmux
// session, window and pane indexes. The current %N is not part of it.
type Slot struct {
	Socket          string
	ServerPID       int64
	ServerStartTime int64
	SessionID       string
	WindowIndex     int64
	PaneIndex       int64
}

// SlotOf projects the session metadata onto its logical slot.
func SlotOf(tmux SessionTmux) Slot {
	return Slot{
		Socket:          tmux.Socket,
		ServerPID:       int64(tmux.ServerPID),
		ServerStartTime: tmux.ServerStartTime,
		SessionID:       tmux.SessionID,
		WindowIndex:     int64(tmux.WindowIndex),
		PaneIndex:       int64(tmux.PaneIndex),
	}
}

// LiveClaim is one live session in the store, for the claims check:
// its agent, its current %N on its server, and its logical slot.
type LiveClaim struct {
	AgentID          string
	CaptureSessionID string
	Server           string
	Pane             string
	Slot             Slot
}

// ClaimWant is the attach that asks: its agent, target %N on its
// server and logical slot, whether the pane facts show a pipe, and
// whether the operator passed --replace.
type ClaimWant struct {
	AgentID string
	Server  string
	Pane    string
	Slot    Slot
	HasPipe bool
	Replace bool
}

// CheckClaims evaluates §6.3 over every live session in the store: one
// live capture per agent, one live capture per pane (%N or logical
// slot on one server instance), and the pipe rule. A pane conflict
// names the other agent, even with --replace: a Gifford capture is
// never replaced. --replace only excuses a pipe no live session
// claims.
func CheckClaims(live []LiveClaim, want ClaimWant) error {
	for _, other := range live {
		if other.AgentID == want.AgentID {
			return fmt.Errorf("%w: agent %s already has capture %s",
				ErrAlreadyCaptured, want.AgentID, other.CaptureSessionID)
		}
		if other.Server == want.Server && (other.Pane == want.Pane || other.Slot == want.Slot) {
			return fmt.Errorf("%w: pane claimed by agent %s capture %s",
				ErrAlreadyCaptured, other.AgentID, other.CaptureSessionID)
		}
	}
	if want.HasPipe && !want.Replace {
		return fmt.Errorf("%w: pane has a pipe", ErrPipeExists)
	}
	return nil
}

// readStopRequest reads the session's stop.request strictly. A missing
// file is no request, not an error.
func readStopRequest(root *os.Root, agent, session string) (*stopRequest, error) {
	var r stopRequest
	err := readControlAt(root, agent+"/"+session+"/stop.request", 4096, &r)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	return &r, nil
}
