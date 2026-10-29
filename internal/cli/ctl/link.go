package ctl

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/protocol"
)

func newLinkCmd() *cobra.Command {
	set := &cobra.Command{Use: "set <from> <to> <allow|hold|require-approval|deny>", Short: "Set a directional link mode", Args: cobra.ExactArgs(3), RunE: runLinkSet}
	set.Flags().String("plane", "all", "message, artifact or all")
	set.Flags().Bool("bidirectional", false, "apply the mode in both directions")
	set.Flags().Duration("ttl", 0, "how long the override applies")
	set.Flags().String("reason", "", "why the link mode is set")
	status := &cobra.Command{Use: "status <from> <to>", Short: "Show the directional link state", Args: cobra.ExactArgs(2), RunE: func(cmd *cobra.Command, args []string) error {
		var state protocol.LinkState
		params := protocol.LinkParams{FromAgent: protocol.AgentID(args[0]), ToAgent: protocol.AgentID(args[1])}
		if err := call(cmd, protocol.ControlMethodLinkGet, params, &state); err != nil {
			return err
		}
		return printJSON(cmd, state)
	}}
	list := &cobra.Command{Use: "list", Short: "List directional links", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		agent, _ := cmd.Flags().GetString("agent")
		var result protocol.LinkList
		if err := call(cmd, protocol.ControlMethodLinkList, protocol.ListLinksParams{AgentID: protocol.AgentID(agent)}, &result); err != nil {
			return err
		}
		return printJSON(cmd, result)
	}}
	list.Flags().String("agent", "", "only list links involving this agent")
	clearCmd := &cobra.Command{Use: "clear <from> <to>", Short: "Remove a link override", Args: cobra.ExactArgs(2), RunE: runLinkClear}
	clearCmd.Flags().String("plane", "all", "message, artifact or all")
	return group("link", "Operate directional links between agents", set, status, list, clearCmd)
}

func runLinkSet(cmd *cobra.Command, args []string) error {
	from, to := protocol.AgentID(args[0]), protocol.AgentID(args[1])
	mode := protocol.LinkMode(strings.ToUpper(strings.ReplaceAll(args[2], "-", "_")))
	if !mode.Valid() {
		return cliError(exitcode.Usage, fmt.Errorf("invalid link mode %q", args[2]))
	}
	planeRaw, _ := cmd.Flags().GetString("plane")
	selected, err := parsePlane(planeRaw, true)
	if err != nil {
		return err
	}
	bidirectional, _ := cmd.Flags().GetBool("bidirectional")
	versions, err := linkVersions(cmd, from, to, selected, bidirectional)
	if err != nil {
		return err
	}
	ttl, _ := cmd.Flags().GetDuration("ttl")
	reason, _ := cmd.Flags().GetString("reason")
	params := protocol.SetLinkParams{FromAgent: from, ToAgent: to, Mode: mode, Plane: selected, Bidirectional: bidirectional, TTLMS: durationMS(ttl), Reason: reason, ExpectedVersions: versions}
	var result protocol.LinkList
	if err := call(cmd, protocol.ControlMethodLinkSet, params, &result); err != nil {
		return err
	}
	return printJSON(cmd, result)
}

func runLinkClear(cmd *cobra.Command, args []string) error {
	from, to := protocol.AgentID(args[0]), protocol.AgentID(args[1])
	planeRaw, _ := cmd.Flags().GetString("plane")
	selected, err := parsePlane(planeRaw, true)
	if err != nil {
		return err
	}
	versions, err := linkVersions(cmd, from, to, selected, false)
	if err != nil {
		return err
	}
	params := protocol.DeleteLinkOverrideParams{FromAgent: from, ToAgent: to, Plane: selected, ExpectedVersions: versions}
	var result protocol.LinkState
	if err := call(cmd, protocol.ControlMethodLinkDeleteOverride, params, &result); err != nil {
		return err
	}
	return printJSON(cmd, result)
}

func linkVersions(cmd *cobra.Command, from, to protocol.AgentID, selected protocol.Plane, bidirectional bool) (protocol.ControlVersions, error) {
	result := protocol.ControlVersions{}
	directions := [][2]protocol.AgentID{{from, to}}
	if bidirectional {
		directions = append(directions, [2]protocol.AgentID{to, from})
	}
	planes := []protocol.Plane{selected}
	if selected == protocol.PlaneAll {
		planes = []protocol.Plane{protocol.PlaneMessage, protocol.PlaneArtifact}
	}
	for _, direction := range directions {
		var state protocol.LinkState
		if err := call(cmd, protocol.ControlMethodLinkGet, protocol.LinkParams{FromAgent: direction[0], ToAgent: direction[1]}, &state); err != nil {
			return nil, err
		}
		for _, item := range planes {
			version := state.MessageVersion
			if item == protocol.PlaneArtifact {
				version = state.ArtifactVersion
			}
			result["link:"+string(direction[0])+"→"+string(direction[1])+":"+string(item)] = version
		}
	}
	return result, nil
}
