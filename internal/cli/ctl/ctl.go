package ctl

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/protocol"
)

// NewCmd returns the gifford ctl command.
func NewCmd(_ *deps.Deps) *cobra.Command {
	approvals := newApprovalsCmd()
	approve := newApproveCmd()
	reject := newRejectCmd()

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

func newApprovalsCmd() *cobra.Command {
	var pending bool
	cmd := &cobra.Command{
		Use:   "approvals",
		Short: "List approval requests, optionally only the pending ones",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			params := protocol.ListApprovalsParams{}
			if pending {
				params.State = protocol.ApprovalStatePending
			}
			var result protocol.ApprovalList
			if err := call(cmd, protocol.ControlMethodApprovalList, params, &result); err != nil {
				return err
			}
			return printJSON(cmd, result)
		},
	}
	cmd.Flags().BoolVar(&pending, "pending", false, "only list pending approval requests")
	return cmd
}

func newApproveCmd() *cobra.Command {
	var deliverNow bool
	cmd := &cobra.Command{
		Use:   "approve <approval_id>",
		Short: "Approve a pending approval request",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			for _, name := range []string{"once", "thread", "steps", "add-turns", "mode"} {
				if cmd.Flags().Changed(name) {
					return ctlNotImplemented(cmd)
				}
			}
			if !deliverNow {
				return ctlNotImplemented(cmd)
			}
			params := protocol.ResolveApprovalParams{
				ApprovalID: protocol.ApprovalID(args[0]),
				Option:     protocol.ApprovalOption("deliver_now"),
			}
			var result protocol.ApprovalRequest
			if err := call(cmd, protocol.ControlMethodApprovalResolve, params, &result); err != nil {
				return err
			}
			return printJSON(cmd, result)
		},
	}
	cmd.Flags().Bool("once", false, "approve a single delivery only")
	cmd.Flags().String("thread", "", "approve for a thread TTL")
	cmd.Flags().Int("steps", 0, "approve a number of steps")
	cmd.Flags().Int("add-turns", 0, "approve additional turns")
	cmd.Flags().String("mode", "", "mode to approve")
	cmd.Flags().BoolVar(&deliverNow, "deliver-now", false, "deliver immediately")
	return cmd
}

func newRejectCmd() *cobra.Command {
	var reason string
	cmd := &cobra.Command{
		Use:   "reject <approval_id>",
		Short: "Reject a pending approval request",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			reason = strings.TrimSpace(reason)
			if reason == "" {
				return cliError(exitcode.Usage, fmt.Errorf("reject requires --reason"))
			}
			params := protocol.ResolveApprovalParams{
				ApprovalID: protocol.ApprovalID(args[0]),
				Option:     protocol.ApprovalOption("reject"),
				Reason:     reason,
			}
			var result protocol.ApprovalRequest
			if err := call(cmd, protocol.ControlMethodApprovalResolve, params, &result); err != nil {
				return err
			}
			return printJSON(cmd, result)
		},
	}
	cmd.Flags().StringVar(&reason, "reason", "", "why the request is rejected")
	return cmd
}

func ctlNotImplemented(cmd *cobra.Command) error {
	return &deps.ExitError{
		Code: exitcode.NotImplemented,
		Err:  fmt.Errorf("%s: not implemented yet", cmd.CommandPath()),
	}
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
