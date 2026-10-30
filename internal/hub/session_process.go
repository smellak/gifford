package hub

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/smellak/gifford/internal/protocol"
)

// sessionProcessRuntime supplies the two pieces of kernel evidence that make
// a detached session resumable. Implementations must fail closed when either
// the current boot or the exact process identity cannot be observed.
type sessionProcessRuntime interface {
	bootID() (string, error)
	alive(protocol.ProcessIdentity, string) error
}

func validateBootID(bootID string) error {
	if bootID == "" || len(bootID) > 128 || strings.IndexFunc(bootID, func(r rune) bool {
		return unicode.IsControl(r) || unicode.IsSpace(r)
	}) >= 0 {
		return fmt.Errorf("invalid boot identity: %w", errInvalidArgument)
	}
	return nil
}
