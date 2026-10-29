package ctl

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/protocol"
)

// Cobra cannot dispatch through a dynamic agent id, so this command parses
// the fixed tail: agent <id> channel status|set|mode.
func newAgentCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "agent <id> channel <status|set|mode>", Short: "Operate one agent's channel", Args: cobra.ArbitraryArgs, RunE: runAgent}
	cmd.Flags().Bool("tx", false, "enable or disable transmission")
	cmd.Flags().Bool("rx", false, "enable or disable reception")
	cmd.Flags().Bool("auto-delivery", false, "enable or disable automated delivery")
	cmd.Flags().Bool("auto-continue", false, "enable or disable automated continuation")
	cmd.Flags().Duration("ttl", 0, "how long the state applies")
	cmd.Flags().String("reason", "", "why the agent channel is changed")
	return cmd
}

func runAgent(cmd *cobra.Command, args []string) error {
	if len(args) < 3 || args[1] != "channel" {
		return cliError(exitcode.Usage, fmt.Errorf("usage: gifford ctl agent <id> channel <status|set|mode>"))
	}
	agent := protocol.AgentID(args[0])
	switch args[2] {
	case "status":
		if len(args) != 3 {
			return cliError(exitcode.Usage, fmt.Errorf("agent channel status takes no arguments"))
		}
		var state protocol.AgentChannel
		if err := call(cmd, protocol.ControlMethodAgentChannelGet, protocol.AgentParams{AgentID: agent}, &state); err != nil {
			return err
		}
		return printJSON(cmd, state)
	case "set":
		if len(args) != 3 {
			return cliError(exitcode.Usage, fmt.Errorf("agent channel set takes flags only"))
		}
		return runAgentSet(cmd, agent)
	case "mode":
		if len(args) != 4 {
			return cliError(exitcode.Usage, fmt.Errorf("agent channel mode requires one mode"))
		}
		return runAgentMode(cmd, agent, args[3])
	default:
		return cliError(exitcode.Usage, fmt.Errorf("unknown agent channel command %q", args[2]))
	}
}

func runAgentSet(cmd *cobra.Command, agent protocol.AgentID) error {
	var current protocol.AgentChannel
	if err := call(cmd, protocol.ControlMethodAgentChannelGet, protocol.AgentParams{AgentID: agent}, &current); err != nil {
		return err
	}
	params := protocol.SetAgentChannelParams{AgentID: agent, ExpectedVersion: current.Version}
	flags := []struct {
		name   string
		target **bool
	}{{"tx", &params.TX}, {"rx", &params.RX}, {"auto-delivery", &params.AutoDelivery}, {"auto-continue", &params.AutoContinue}}
	for _, flag := range flags {
		if cmd.Flags().Changed(flag.name) {
			value, _ := cmd.Flags().GetBool(flag.name)
			*flag.target = &value
		}
	}
	ttl, _ := cmd.Flags().GetDuration("ttl")
	params.TTLMS = durationMS(ttl)
	params.Reason, _ = cmd.Flags().GetString("reason")
	if params.TX == nil && params.RX == nil && params.AutoDelivery == nil && params.AutoContinue == nil {
		return cliError(exitcode.Usage, fmt.Errorf("agent channel set needs at least one flag"))
	}
	var updated protocol.AgentChannel
	if err := call(cmd, protocol.ControlMethodAgentChannelSet, params, &updated); err != nil {
		return err
	}
	return printJSON(cmd, updated)
}

func runAgentMode(cmd *cobra.Command, agent protocol.AgentID, raw string) error {
	mode := protocol.AgentMode(strings.ToUpper(strings.ReplaceAll(raw, "-", "_")))
	if !mode.Valid() {
		return cliError(exitcode.Usage, fmt.Errorf("invalid agent mode %q", raw))
	}
	var current protocol.AgentChannel
	if err := call(cmd, protocol.ControlMethodAgentChannelGet, protocol.AgentParams{AgentID: agent}, &current); err != nil {
		return err
	}
	ttl, _ := cmd.Flags().GetDuration("ttl")
	reason, _ := cmd.Flags().GetString("reason")
	params := protocol.SetAgentModeParams{AgentID: agent, Mode: mode, TTLMS: durationMS(ttl), Reason: reason, ExpectedVersion: current.Version}
	var updated protocol.AgentChannel
	if err := call(cmd, protocol.ControlMethodAgentChannelSetMode, params, &updated); err != nil {
		return err
	}
	return printJSON(cmd, updated)
}
