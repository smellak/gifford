package ctl

import (
	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
)

// newChannelCmd returns the `gifford ctl channel` group: operate the global
// and per-node channel switch.
func newChannelCmd() *cobra.Command {
	set := deps.Leaf("set <active|paused|draining|off>", "Set the global channel state (off asks for confirmation; --then off only applies to draining)")
	set.Flags().String("reason", "", "why the channel state is changed")
	set.Flags().Duration("ttl", 0, "how long the state applies")
	set.Flags().String("then", "", "state to move to when the TTL expires")

	setNode := deps.Leaf("set-node <node> <active|paused|draining|off>", "Set one node's channel state")
	setNode.Flags().String("reason", "", "why the node channel state is changed")
	setNode.Flags().Duration("ttl", 0, "how long the state applies")

	explain := deps.Leaf("explain", "Explain why the channel is or is not delivering, for a link, a message or an artifact")
	explain.Flags().String("from", "", "link source agent")
	explain.Flags().String("to", "", "link destination agent")
	explain.Flags().String("plane", "", "plane to explain")
	explain.Flags().String("thread", "", "thread to explain")
	explain.Flags().String("loop", "", "loop to explain")
	explain.Flags().String("message", "", "message to explain")
	explain.Flags().String("artifact", "", "artifact to explain")

	return deps.Group("channel", "Operate the global and per-node channel switch",
		deps.Leaf("status", "Show the channel's global and per-node state"),
		set,
		setNode,
		explain,
	)
}
