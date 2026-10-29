//go:build linux

package hub

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/smellak/gifford/internal/protocol"
)

func verifyPeerProcess(peerPID int, claimed protocol.ProcessIdentity) error {
	if peerPID <= 0 || claimed.PID != int64(peerPID) {
		return fmt.Errorf("peer process id mismatch: %w", errInvalidArgument)
	}
	raw, err := os.ReadFile("/proc/" + strconv.Itoa(peerPID) + "/stat")
	if err != nil {
		return fmt.Errorf("read peer process: %w", errInvalidArgument)
	}
	closeParen := strings.LastIndexByte(string(raw), ')')
	if closeParen < 0 {
		return fmt.Errorf("parse peer process: %w", errInvalidArgument)
	}
	fields := strings.Fields(string(raw[closeParen+1:]))
	if len(fields) < 20 {
		return fmt.Errorf("parse peer process: %w", errInvalidArgument)
	}
	start, err := strconv.ParseInt(fields[19], 10, 64)
	if err != nil || start != claimed.ProcessStartTime {
		return fmt.Errorf("peer process start mismatch: %w", errInvalidArgument)
	}
	return nil
}
