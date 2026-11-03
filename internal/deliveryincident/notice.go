package deliveryincident

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

// NoticeKind distinguishes an incident alert from its closing observation.
type NoticeKind string

// Notice kinds written through the normal signed message path.
const (
	NoticeAlert    NoticeKind = "ALERT"
	NoticeRecovery NoticeKind = "RECOVERY"
)

// Notice is the fixed metadata an incident notice may contain. It deliberately
// has no message body, subject, message id or arbitrary-text field.
type Notice struct {
	Kind             NoticeKind
	Recipient        protocol.AgentID
	NormalizedReason string
	Minutes          int64
	Attempts         int64
	QueuedAt         time.Time
}

// Valid reports whether a notice can be rendered without accepting free text.
func (n Notice) Valid() bool {
	if n.Kind != NoticeAlert && n.Kind != NoticeRecovery || !n.Recipient.Valid() ||
		n.Minutes < 0 || n.Attempts < 1 || n.QueuedAt.IsZero() ||
		len(n.NormalizedReason) < 1 || len(n.NormalizedReason) > 128 {
		return false
	}
	for _, char := range n.NormalizedReason {
		if char != '_' && (char < 'A' || char > 'Z') && (char < '0' || char > '9') {
			return false
		}
	}
	return true
}

// ClientMsgID is the stable idempotency key for one queued notification to
// one destination. An incident alert can fan out to the operator and several
// original senders without their filings colliding in the system sender's key
// space.
func (n Notice) ClientMsgID(destination protocol.AgentID) protocol.OpaqueID {
	digest := noticeDigest(n, destination, true)
	return protocol.OpaqueID("delivery-incident:" + digest)
}

// ThreadID groups every episode for one recipient and reason without exposing
// either value through an unbounded identifier.
func (n Notice) ThreadID() protocol.ThreadID {
	digest := noticeDigest(n, "", false)
	return protocol.ThreadID("delivery-incident-" + digest[:32])
}

func noticeDigest(n Notice, destination protocol.AgentID, includeOccurrence bool) string {
	parts := []string{string(n.Recipient), n.NormalizedReason}
	if includeOccurrence {
		parts = append(parts, string(destination), string(n.Kind), eventstore.FormatTime(n.QueuedAt))
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])
}

// Text renders the only allowed subject and body shape. All values are typed,
// normalized metadata produced by the worker; original message content and ids
// are never read for this notice.
func (n Notice) Text() (subject, body string) {
	status := "alert"
	if n.Kind == NoticeRecovery {
		status = "recovery"
	}
	subject = fmt.Sprintf("Delivery incident %s for %s", status, n.Recipient)
	body = fmt.Sprintf("Delivery incident %s.\nRecipient: %s\nReason: %s\nMinutes: %d\nAttempts: %d\n"+
		"This operator notice carries no message content. No response is needed.",
		status, n.Recipient, n.NormalizedReason, n.Minutes, n.Attempts)
	return subject, body
}
