package ctl

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/protocol"
)

// newLoopCmd returns the `gifford ctl loop` group: operate governed
// agent-to-agent loops.
func newLoopCmd() *cobra.Command {
	create := deps.Leaf("create", "Create a new governed loop between participants on a thread")
	create.Flags().String("thread", "", "thread to run the loop on")
	create.Flags().String("participants", "", "comma-separated loop participants")
	create.Flags().String("mode", "", "loop mode")
	create.Flags().Int("max-turns", 12, "maximum loop turns")
	create.Flags().Int("max-minutes", 10, "maximum loop minutes")
	create.Flags().String("profile", "", "governance profile for the loop")

	status := &cobra.Command{
		Use: "status <loop_id>", Short: "Show a loop's state and budget usage", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			loop, err := getLoop(cmd, protocol.LoopID(args[0]))
			if err != nil {
				return err
			}
			return printJSON(cmd, loop)
		},
	}

	list := &cobra.Command{
		Use: "list", Short: "List loops, optionally only the active ones", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			active, _ := cmd.Flags().GetBool("active")
			var result protocol.LoopList
			if err := call(cmd, protocol.ControlMethodLoopList, protocol.ListLoopsParams{Active: active}, &result); err != nil {
				return err
			}
			return printJSON(cmd, result)
		},
	}
	list.Flags().Bool("active", false, "only list active loops")

	setMode := &cobra.Command{
		Use: "set-mode <loop_id> <auto|step|paused|draining>", Short: "Change a loop's mode",
		Args: cobra.ExactArgs(2), RunE: runLoopSetMode,
	}
	setMode.Flags().String("reason", "", "why the loop mode is changed")

	next := deps.Leaf("next <loop_id>", "Advance a stepped loop by one or more steps")
	next.Flags().Int("steps", 1, "steps to advance")

	budget := &cobra.Command{
		Use: "budget <loop_id>", Short: "Extend a loop's budget and resume a budget-paused loop",
		Args: cobra.ExactArgs(1), RunE: runLoopBudget,
	}
	budget.Flags().Int64("add-turns", 0, "turns to add to the budget")
	budget.Flags().Int64("add-minutes", 0, "minutes to add to the budget")
	budget.Flags().Int64("add-messages", 0, "messages to add to the budget")
	budget.Flags().String("reason", "", "why the loop budget is extended")

	stop := &cobra.Command{
		Use: "stop <loop_id>", Short: "Stop a loop",
		Args: cobra.ExactArgs(1), RunE: runLoopStop,
	}
	stop.Flags().String("reason", "", "why the loop is stopped")
	stop.Flags().Bool("cancel-turn", false, "also cancel the turn in flight (not implemented yet)")

	return deps.Group("loop", "Operate governed agent-to-agent loops",
		create,
		status,
		list,
		setMode,
		next,
		budget,
		stop,
		deps.Leaf("complete <loop_id>", "Mark a loop as complete"),
	)
}

func getLoop(cmd *cobra.Command, id protocol.LoopID) (protocol.Loop, error) {
	var loop protocol.Loop
	if err := call(cmd, protocol.ControlMethodLoopGet, protocol.LoopParams{LoopID: id}, &loop); err != nil {
		return protocol.Loop{}, err
	}
	return loop, nil
}

func runLoopSetMode(cmd *cobra.Command, args []string) error {
	mode := protocol.LoopMode(strings.ToUpper(args[1]))
	switch mode {
	case protocol.LoopModeAuto, protocol.LoopModeStep, protocol.LoopModePaused, protocol.LoopModeDraining:
	default:
		return cliError(exitcode.Usage, fmt.Errorf("invalid loop mode %q", args[1]))
	}
	id := protocol.LoopID(args[0])
	current, err := getLoop(cmd, id)
	if err != nil {
		return err
	}
	reason, _ := cmd.Flags().GetString("reason")
	params := protocol.SetLoopModeParams{
		LoopID: id, Mode: mode, Reason: reason, ExpectedVersion: current.ControlVersion,
	}
	var result protocol.Loop
	if err := call(cmd, protocol.ControlMethodLoopSetMode, params, &result); err != nil {
		return err
	}
	return printJSON(cmd, result)
}

func runLoopBudget(cmd *cobra.Command, args []string) error {
	turns, _ := cmd.Flags().GetInt64("add-turns")
	minutes, _ := cmd.Flags().GetInt64("add-minutes")
	messages, _ := cmd.Flags().GetInt64("add-messages")
	if turns < 0 || minutes < 0 || messages < 0 || turns == 0 && minutes == 0 && messages == 0 {
		return cliError(exitcode.Usage, fmt.Errorf("at least one positive --add-* value is required"))
	}
	if turns > 1000 || messages > 1000 || minutes > 24*60 {
		return cliError(exitcode.Usage, fmt.Errorf("loop budget extension exceeds the per-command limit"))
	}
	id := protocol.LoopID(args[0])
	current, err := getLoop(cmd, id)
	if err != nil {
		return err
	}
	reason, _ := cmd.Flags().GetString("reason")
	params := protocol.ExtendLoopBudgetParams{
		LoopID: id, AddTurns: positiveInt64(turns), AddMessages: positiveInt64(messages),
		AddDurationMS: positiveInt64(minutes * int64(time.Minute/time.Millisecond)),
		Reason:        reason, ExpectedVersion: current.ControlVersion,
	}
	var result protocol.Loop
	if err := call(cmd, protocol.ControlMethodLoopExtendBudget, params, &result); err != nil {
		return err
	}
	return printJSON(cmd, result)
}

func runLoopStop(cmd *cobra.Command, args []string) error {
	cancelTurn, _ := cmd.Flags().GetBool("cancel-turn")
	if cancelTurn {
		return ctlNotImplemented(cmd)
	}
	reason, _ := cmd.Flags().GetString("reason")
	if strings.TrimSpace(reason) == "" {
		return cliError(exitcode.Usage, fmt.Errorf("--reason is required"))
	}
	id := protocol.LoopID(args[0])
	current, err := getLoop(cmd, id)
	if err != nil {
		return err
	}
	params := protocol.StopLoopParams{
		LoopID: id, Reason: reason, ExpectedVersion: current.ControlVersion,
	}
	var result protocol.Loop
	if err := call(cmd, protocol.ControlMethodLoopStop, params, &result); err != nil {
		return err
	}
	return printJSON(cmd, result)
}

func positiveInt64(value int64) *int64 {
	if value <= 0 {
		return nil
	}
	return &value
}
