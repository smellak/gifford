package ctl

import (
	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/protocol"
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
		newPauseCmd(),
		newResumeCmd(),
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

func newPauseCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "pause <agent>",
		Short: "Stop an agent's automated delivery; notifications keep working",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			agent := protocol.AgentID(args[0])
			var current protocol.AgentChannel
			if err := call(cmd, protocol.ControlMethodAgentChannelGet, protocol.AgentParams{AgentID: agent}, &current); err != nil {
				return err
			}
			// The maximum protocol TTL preserves the prior row in restore_json so
			// resume can restore all flags rather than guessing a default.
			ttl := int64(2_592_000_000)
			off := false
			params := protocol.SetAgentChannelParams{AgentID: agent, AutoDelivery: &off, TTLMS: &ttl, Reason: "paused by operator", ExpectedVersion: current.Version}
			var updated protocol.AgentChannel
			if err := call(cmd, protocol.ControlMethodAgentChannelSet, params, &updated); err != nil {
				return err
			}
			return printJSON(cmd, updated)
		},
	}
}

func newResumeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "resume <agent>",
		Short: "Restore an agent's delivery state from before the last pause",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			agent := protocol.AgentID(args[0])
			var current protocol.AgentChannel
			if err := call(cmd, protocol.ControlMethodAgentChannelGet, protocol.AgentParams{AgentID: agent}, &current); err != nil {
				return err
			}
			tx, rx, delivery, continuation := current.TX, current.RX, true, current.AutoContinue
			if current.Control != nil && current.Control.Restore != nil {
				restore := current.Control.Restore
				if restore.TxEnabled != nil {
					tx = *restore.TxEnabled
				}
				if restore.RxEnabled != nil {
					rx = *restore.RxEnabled
				}
				if restore.AutoDeliveryEnabled != nil {
					delivery = *restore.AutoDeliveryEnabled
				}
				if restore.AutoContinueEnabled != nil {
					continuation = *restore.AutoContinueEnabled
				}
			}
			params := protocol.SetAgentChannelParams{AgentID: agent, TX: &tx, RX: &rx, AutoDelivery: &delivery, AutoContinue: &continuation, Reason: "resumed by operator", ExpectedVersion: current.Version}
			var updated protocol.AgentChannel
			if err := call(cmd, protocol.ControlMethodAgentChannelSet, params, &updated); err != nil {
				return err
			}
			return printJSON(cmd, updated)
		},
	}
}
