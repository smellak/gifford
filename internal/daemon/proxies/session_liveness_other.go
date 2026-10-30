//go:build !linux

package proxies

import (
	"fmt"

	"github.com/smellak/gifford/internal/protocol"
)

type unsupportedSessionLiveness struct{}

func newSessionLiveness() sessionLiveness { return unsupportedSessionLiveness{} }

func (unsupportedSessionLiveness) alive(protocol.ProcessIdentity, string) error {
	return fmt.Errorf("detached process verification unavailable")
}
