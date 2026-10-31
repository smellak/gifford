//go:build unix

package session

import "github.com/smellak/gifford/internal/agentsock"

// The agent socket drains the queue: it writes the head and settles it.
var _ agentsock.DurableEvents = (*DurableQueue)(nil)
