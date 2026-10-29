package hub

import (
	"crypto/rand"
	"fmt"
	"sync"
	"time"

	"github.com/smellak/gifford/internal/protocol"
)

// This file mints identifiers: message ids and thread references. A ULID
// carries 48 bits of millisecond time and 80 bits of cryptographic
// randomness, so ids sort by creation and never repeat.

// crockford is the ULID alphabet.
const crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// ulidGuard keeps mints in one process monotonic within a millisecond.
var ulidGuard struct {
	sync.Mutex
	lastMillis int64
	lastRand   [10]byte
}

// mintULID returns a fresh ULID for now, monotonic within one process:
// clock steps back or repeated milliseconds walk the random part up.
func mintULID(now time.Time) (protocol.ULID, error) {
	millis := now.UnixMilli()
	ulidGuard.Lock()
	defer ulidGuard.Unlock()
	entropy := make([]byte, 10)
	if millis <= ulidGuard.lastMillis {
		millis = ulidGuard.lastMillis
		copy(entropy, ulidGuard.lastRand[:])
		increment(entropy)
	} else if _, err := rand.Read(entropy); err != nil {
		return "", fmt.Errorf("mint ulid: %w", err)
	}
	ulidGuard.lastMillis = millis
	copy(ulidGuard.lastRand[:], entropy)
	return encodeULID(millis, entropy), nil
}

// increment adds one to the big-endian random part.
func increment(entropy []byte) {
	for i := len(entropy) - 1; i >= 0; i-- {
		entropy[i]++
		if entropy[i] != 0 {
			return
		}
	}
}

// encodeULID renders 6 time bytes and 10 random bytes in Crockford
// base32: 10 time characters and 16 random characters.
func encodeULID(millis int64, entropy []byte) protocol.ULID {
	var out [26]byte
	value := uint64(millis)
	for i := 9; i >= 0; i-- {
		out[i] = crockford[value&0x1f]
		value >>= 5
	}
	bits := 0
	accumulator := 0
	pos := 10
	for _, octet := range entropy {
		accumulator = (accumulator << 8) | int(octet)
		bits += 8
		for bits >= 5 {
			bits -= 5
			out[pos] = crockford[(accumulator>>bits)&0x1f]
			pos++
		}
	}
	return protocol.ULID(out[:])
}
