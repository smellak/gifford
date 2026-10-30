package proxies

import "github.com/smellak/gifford/internal/protocol"

type sessionLiveness interface {
	alive(protocol.ProcessIdentity, string) error
}
