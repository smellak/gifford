package ctl

import (
	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
)

// NewCmd returns the gifford ctl command.
func NewCmd(_ *deps.Deps) *cobra.Command {
	approvals := deps.Leaf("approvals", "List approval requests, optionally only the pending ones")
	approvals.Flags().Bool("pending", false, "only list pending approval requests")

	approve := deps.Leaf("approve <approval_id>", "Approve a pending approval request")
	approve.Flags().Bool("once", false, "approve a single delivery only")
	approve.Flags().String("thread", "", "approve for a thread TTL")
	approve.Flags().Int("steps", 0, "approve a number of steps")
	approve.Flags().Int("add-turns", 0, "approve additional turns")
	approve.Flags().String("mode", "", "mode to approve")
	approve.Flags().Bool("deliver-now", false, "deliver immediately")

	reject := deps.Leaf("reject <approval_id>", "Reject a pending approval request")
	reject.Flags().String("reason", "", "why the request is rejected")

	approveMessage := deps.Leaf("approve-message <message_id>", "Approve the pending request tied to a message")
	approveMessage.Flags().Bool("once", false, "approve a single delivery only")
	approveMessage.Flags().String("thread", "", "approve for a thread TTL")

	rejectMessage := deps.Leaf("reject-message <message_id>", "Reject the pending request tied to a message")
	rejectMessage.Flags().String("reason", "", "why the request is rejected")

	return deps.Group("ctl", "Manual control and automation",
		deps.Leaf("pause <agent>", "Stop an agent's automated delivery; notifications keep working"),
		deps.Leaf("resume <agent>", "Restore an agent's delivery state from before the last pause"),
		deps.Leaf("hold-message <message_id>", "Hold a specific message"),
		deps.Leaf("deliver-next <agent>", "Force immediate evaluation of an agent's next item, still subject to the Draft Guard"),
		deps.Leaf("inspect <id>", "Show metadata, state and decisions for an object; the body only if the caller is authorized"),
		deps.Leaf("retry-artifact <artifact_id>", "Retry delivery of a stuck artifact"),
		deps.Leaf("unlock <agent>", "Clear an agent's Draft Guard from the UNKNOWN state (asks for interactive confirmation)"),
		deps.Leaf("revoke-node <node>", "Revoke a federation node"),
		deps.Leaf("replay <id>", "Replay recorded events for an object to reproduce its past decisions (exact arguments are not fixed yet — accept anything)"),
		newModelCmd(),
		newChannelCmd(),
		newAgentCmd(),
		newLinkCmd(),
		newLoopCmd(),
		approvals,
		approve,
		reject,
		approveMessage,
		rejectMessage,
		deps.Leaf("release-hold <message_id|artifact_id>", "Release a manual hold on a message or artifact"),
	)
}
