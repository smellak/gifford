package ctl

import (
	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
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

	setMode := deps.Leaf("set-mode <loop_id> <auto|step|paused|draining>", "Change a loop's mode")
	setMode.Flags().String("reason", "", "why the loop mode is changed")

	next := deps.Leaf("next <loop_id>", "Advance a stepped loop by one or more steps")
	next.Flags().Int("steps", 1, "steps to advance")

	budget := deps.Leaf("budget <loop_id>", "Extend a loop's budget")
	budget.Flags().Int("add-turns", 0, "turns to add to the budget")
	budget.Flags().Int("add-minutes", 0, "minutes to add to the budget")
	budget.Flags().Int("add-messages", 0, "messages to add to the budget")

	stop := deps.Leaf("stop <loop_id>", "Stop a loop (--cancel-turn only applies with a verified native_cancel)")
	stop.Flags().String("reason", "", "why the loop is stopped")
	stop.Flags().Bool("cancel-turn", false, "also cancel the turn in flight")

	list := deps.Leaf("list", "List loops, optionally only the active ones")
	list.Flags().Bool("active", false, "only list active loops")

	return deps.Group("loop", "Operate governed agent-to-agent loops",
		create,
		deps.Leaf("status <loop_id>", "Show a loop's state and budget usage"),
		list,
		setMode,
		next,
		budget,
		stop,
		deps.Leaf("complete <loop_id>", "Mark a loop as complete"),
	)
}
