package hub

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/smellak/gifford/internal/protocol"
)

const crockfordDigits = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// mintULID returns a fresh time-ordered identifier using cryptographic
// entropy. No caller-supplied value is needed for a new local admission.
func mintULID(now time.Time) (protocol.ULID, error) {
	millis := now.UnixMilli()
	if millis < 0 || millis >= 1<<48 {
		return "", fmt.Errorf("time outside ULID range")
	}
	var raw [16]byte
	stamp := big.NewInt(millis).Bytes()
	copy(raw[6-len(stamp):6], stamp)
	if _, err := rand.Read(raw[6:]); err != nil {
		return "", fmt.Errorf("read ULID entropy: %w", err)
	}
	encoded := new(big.Int).SetBytes(raw[:]).Text(32)
	var out strings.Builder
	for out.Len()+len(encoded) < 26 {
		out.WriteByte('0')
	}
	for i := range len(encoded) {
		char := encoded[i]
		index := int(char - '0')
		if char >= 'a' {
			index = int(char-'a') + 10
		}
		out.WriteByte(crockfordDigits[index])
	}
	return protocol.ULID(out.String()), nil
}

// localRouteDigest binds the generated message, local sender and recipient
// set without reading content.
func localRouteDigest(
	messageID protocol.ULID,
	node protocol.NodeID,
	sender protocol.AgentID,
	recipients []MessageRecipient,
) protocol.Digest {
	names := make([]string, len(recipients))
	for i, recipient := range recipients {
		names[i] = string(recipient.Agent)
	}
	sort.Strings(names)
	sum := sha256.Sum256([]byte(string(messageID) + "|" + string(node) + "|" + string(sender) + "|" +
		strings.Join(names, ",")))
	return protocol.Digest(fmt.Sprintf("sha256:%x", sum))
}

// localRouteDigestV1 binds one local hop to its predecessor using the frozen
// compact-JSON vector. Recipient order is significant. Federated roots keep
// using localRouteDigest until the federation protocol changes in 8.3.
func localRouteDigestV1(
	previous protocol.Digest,
	fromNode protocol.NodeID,
	fromAgent protocol.AgentID,
	toNode protocol.NodeID,
	recipients []MessageRecipient,
) (protocol.Digest, error) {
	names := make([]string, len(recipients))
	for i, recipient := range recipients {
		names[i] = string(recipient.Agent)
	}
	raw, err := json.Marshal([]any{
		"gifford-route-v1", string(previous), string(fromNode), string(fromAgent), string(toNode), names,
	})
	if err != nil {
		return "", fmt.Errorf("marshal route digest vector: %w", err)
	}
	sum := sha256.Sum256(raw)
	return protocol.Digest(fmt.Sprintf("sha256:%x", sum)), nil
}
