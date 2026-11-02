package tmuxinstance

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/smellak/gifford/internal/protocol"
)

const crockfordDigits = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// mintULID returns a fresh time-ordered identifier using cryptographic
// entropy.
func mintULID(now time.Time) (protocol.ULID, error) {
	millis := now.UnixMilli()
	if millis < 0 || millis >= 1<<48 {
		return "", fmt.Errorf("time outside ULID range: %w", ErrInvalidArgument)
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
