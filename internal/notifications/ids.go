package notifications

import (
	"bytes"
	"fmt"
	"io"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/smellak/gifford/internal/protocol"
)

type idGenerator struct {
	entropy io.Reader
	mu      sync.Mutex
	lastMS  int64
	last    [10]byte
}

func (g *idGenerator) ulid(now time.Time) (protocol.ULID, error) {
	millis := now.UnixMilli()
	if millis < 0 || millis >= 1<<48 {
		return "", fmt.Errorf("notification id time: %w", ErrInvalidOptions)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	var entropy [10]byte
	if _, err := io.ReadFull(g.entropy, entropy[:]); err != nil {
		return "", fmt.Errorf("read notification id entropy: %w", err)
	}
	if millis == g.lastMS && bytes.Compare(entropy[:], g.last[:]) <= 0 {
		entropy = g.last
		for index := len(entropy) - 1; index >= 0; index-- {
			entropy[index]++
			if entropy[index] != 0 {
				break
			}
		}
	}
	g.lastMS = millis
	g.last = entropy
	var raw [16]byte
	stamp := big.NewInt(millis).Bytes()
	copy(raw[6-len(stamp):6], stamp)
	copy(raw[6:], entropy[:])
	encoded := new(big.Int).SetBytes(raw[:]).Text(32)
	const digits = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	var out strings.Builder
	for out.Len()+len(encoded) < 26 {
		out.WriteByte('0')
	}
	for index := range len(encoded) {
		char := encoded[index]
		value := int(char - '0')
		if char >= 'a' {
			value = int(char-'a') + 10
		}
		out.WriteByte(digits[value])
	}
	return protocol.ULID(out.String()), nil
}

func (g *idGenerator) notificationID(now time.Time) (protocol.OpaqueID, error) {
	id, err := g.ulid(now)
	if err != nil {
		return "", err
	}
	return protocol.OpaqueID("ntf_" + id), nil
}
