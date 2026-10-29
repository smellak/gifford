package ctl

import (
	"bufio"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/protocol"
)

func newChannelCmd() *cobra.Command {
	set := &cobra.Command{Use: "set <active|paused|draining|off>", Short: "Set the global channel state", Args: cobra.ExactArgs(1), RunE: runSetGlobal}
	set.Flags().String("reason", "", "why the channel state is changed")
	set.Flags().Duration("ttl", 0, "how long the state applies")
	set.Flags().String("then", "", "state to move to when the TTL expires")
	set.Flags().Bool("yes", false, "confirm setting the channel off")

	setNode := &cobra.Command{Use: "set-node <node> <active|paused|draining|off>", Short: "Set one node's channel state", Args: cobra.ExactArgs(2), RunE: runSetNode}
	setNode.Flags().String("reason", "", "why the node channel state is changed")
	setNode.Flags().Duration("ttl", 0, "how long the state applies")
	setNode.Flags().String("then", "", "state to move to when the TTL expires")
	setNode.Flags().Bool("yes", false, "confirm setting the node off")

	status := &cobra.Command{Use: "status", Short: "Show the global channel state", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		var state protocol.ChannelState
		if err := call(cmd, protocol.ControlMethodChannelGetGlobal, protocol.EmptyParams{}, &state); err != nil {
			return err
		}
		return printJSON(cmd, state)
	}}
	return group("channel", "Operate the global and per-node channel switch", status, set, setNode, newExplainCmd())
}

func runSetGlobal(cmd *cobra.Command, args []string) error {
	mode, err := channelMode(args[0])
	if err != nil {
		return err
	}
	yes, _ := cmd.Flags().GetBool("yes")
	if mode == protocol.ChannelModeOff && !yes {
		if err := confirmOff(cmd); err != nil {
			return err
		}
	}
	reason, _ := cmd.Flags().GetString("reason")
	ttl, _ := cmd.Flags().GetDuration("ttl")
	thenRaw, _ := cmd.Flags().GetString("then")
	var then protocol.ChannelMode
	if thenRaw != "" {
		then, err = channelMode(thenRaw)
		if err != nil {
			return err
		}
	}
	var current protocol.ChannelState
	if err := call(cmd, protocol.ControlMethodChannelGetGlobal, protocol.EmptyParams{}, &current); err != nil {
		return err
	}
	params := protocol.SetGlobalParams{Mode: mode, Reason: reason, TTLMS: durationMS(ttl), ThenMode: then, ExpectedVersion: current.Version}
	var updated protocol.ChannelState
	if err := call(cmd, protocol.ControlMethodChannelSetGlobal, params, &updated); err != nil {
		return err
	}
	return printJSON(cmd, updated)
}

func runSetNode(cmd *cobra.Command, args []string) error {
	mode, err := channelMode(args[1])
	if err != nil {
		return err
	}
	yes, _ := cmd.Flags().GetBool("yes")
	if mode == protocol.ChannelModeOff && !yes {
		if err := confirmOff(cmd); err != nil {
			return err
		}
	}
	node := protocol.NodeID(args[0])
	var current protocol.ChannelState
	if err := call(cmd, protocol.ControlMethodChannelGetNode, protocol.NodeParams{NodeID: node}, &current); err != nil {
		return err
	}
	reason, _ := cmd.Flags().GetString("reason")
	ttl, _ := cmd.Flags().GetDuration("ttl")
	thenRaw, _ := cmd.Flags().GetString("then")
	var then protocol.ChannelMode
	if thenRaw != "" {
		then, err = channelMode(thenRaw)
		if err != nil {
			return err
		}
	}
	params := protocol.SetNodeParams{NodeID: node, Mode: mode, Reason: reason, TTLMS: durationMS(ttl), ThenMode: then, ExpectedVersion: current.Version}
	var updated protocol.ChannelState
	if err := call(cmd, protocol.ControlMethodChannelSetNode, params, &updated); err != nil {
		return err
	}
	return printJSON(cmd, updated)
}

func newExplainCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "explain", Short: "Explain why the channel is or is not delivering", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		from, _ := cmd.Flags().GetString("from")
		to, _ := cmd.Flags().GetString("to")
		planeRaw, _ := cmd.Flags().GetString("plane")
		thread, _ := cmd.Flags().GetString("thread")
		loop, _ := cmd.Flags().GetString("loop")
		message, _ := cmd.Flags().GetString("message")
		artifact, _ := cmd.Flags().GetString("artifact")
		params := protocol.ExplainParams{FromAgent: protocol.AgentID(from), ToAgent: protocol.AgentID(to), ThreadID: protocol.ThreadID(thread), LoopID: protocol.LoopID(loop), MessageID: protocol.ULID(message), ArtifactID: protocol.OpaqueID(artifact)}
		if planeRaw != "" {
			value, err := parsePlane(planeRaw, false)
			if err != nil {
				return err
			}
			params.Plane = value
		}
		var result protocol.Explanation
		if err := call(cmd, protocol.ControlMethodChannelExplain, params, &result); err != nil {
			return err
		}
		return printJSON(cmd, result)
	}}
	cmd.Flags().String("from", "", "link source agent")
	cmd.Flags().String("to", "", "link destination agent")
	cmd.Flags().String("plane", "", "message or artifact plane")
	cmd.Flags().String("thread", "", "thread to explain")
	cmd.Flags().String("loop", "", "loop to explain")
	cmd.Flags().String("message", "", "message to explain")
	cmd.Flags().String("artifact", "", "artifact to explain")
	return cmd
}

func group(use, short string, children ...*cobra.Command) *cobra.Command {
	cmd := &cobra.Command{Use: use, Short: short}
	cmd.AddCommand(children...)
	return cmd
}

func channelMode(raw string) (protocol.ChannelMode, error) {
	mode := protocol.ChannelMode(strings.ToUpper(raw))
	if !mode.Valid() {
		return "", cliError(exitcode.Usage, fmt.Errorf("invalid channel mode %q", raw))
	}
	return mode, nil
}

func parsePlane(raw string, allowAll bool) (protocol.Plane, error) {
	value := protocol.Plane(strings.ToLower(raw))
	if value == protocol.PlaneMessage || value == protocol.PlaneArtifact || allowAll && value == protocol.PlaneAll {
		return value, nil
	}
	return "", cliError(exitcode.Usage, fmt.Errorf("invalid plane %q", raw))
}

func durationMS(value time.Duration) *int64 {
	if value <= 0 {
		return nil
	}
	ms := value.Milliseconds()
	return &ms
}

func confirmOff(cmd *cobra.Command) error {
	_, _ = fmt.Fprint(cmd.ErrOrStderr(), "Set channel OFF? type yes: ")
	scanner := bufio.NewScanner(cmd.InOrStdin())
	if !scanner.Scan() || strings.TrimSpace(strings.ToLower(scanner.Text())) != "yes" {
		return cliError(exitcode.Refused, fmt.Errorf("channel OFF not confirmed"))
	}
	return nil
}
