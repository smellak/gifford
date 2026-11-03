package ctl

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/config"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/tmuxlock"
	"github.com/smellak/gifford/internal/xdg"
)

const (
	paneOpProvision = "provision"
	paneOpTake      = "take"
	paneOpReturn    = "return"
	paneOpReconcile = "reconcile"
	paneOpRepair    = "repair"
	paneStatusUse   = "status"
)

// paneLockRoot is the panel-lock tree the pane commands observe. The node
// always uses tmuxlock.DefaultRoot; the variable only exists so tests can
// point the observer at a synthetic tree.
var paneLockRoot = tmuxlock.DefaultRoot

// pane result words for the single JSON line.
const (
	paneResultChanged = "changed"
	paneResultNoop    = "noop"
	paneResultError   = "error"
)

// paneOutcome is the single JSON line every `gifford ctl pane` command
// prints on stdout: exactly one compact line, no free text around it.
type paneOutcome struct {
	Op          string  `json:"op"`
	Result      string  `json:"result"`
	TxID        string  `json:"txid,omitempty"`
	OwnerBefore string  `json:"owner_before,omitempty"`
	OwnerAfter  string  `json:"owner_after,omitempty"`
	OwnerGen    *uint32 `json:"owner_gen,omitempty"`
	Phase       string  `json:"phase,omitempty"`
	Code        string  `json:"code,omitempty"`
	Message     string  `json:"message,omitempty"`
}

func printPaneOutcome(cmd *cobra.Command, outcome paneOutcome) error {
	line, err := json.Marshal(outcome)
	if err != nil {
		return cliError(exitcode.Error, err)
	}
	line = append(line, '\n')
	if _, err := cmd.OutOrStdout().Write(line); err != nil {
		return cliError(exitcode.Error, err)
	}
	return nil
}

// newPaneCmd returns the `gifford ctl pane` group: operate and observe the
// panel pane locks through the typed Control API methods. The CLI only
// forwards the operator-supplied target: it validates with Validate before
// calling and never derives or measures pane state on its own.
func newPaneCmd() *cobra.Command {
	return deps.Group("pane", "Provision, take, return, reconcile, repair and observe panel pane locks",
		newPaneProvisionCmd(),
		newPaneTakeCmd(),
		newPaneReturnCmd(),
		newPaneReconcileCmd(),
		newPaneRepairCmd(),
		newPaneStatusCmd(),
	)
}

type paneTargetFlags struct {
	ownerUID  uint32
	socket    string
	paneID    string
	host      string
	bootID    string
	panePID   int64
	paneStart int64
}

func (flags *paneTargetFlags) add(cmd *cobra.Command) {
	cmd.Flags().Uint32Var(&flags.ownerUID, "owner-uid", 0, "owner user id of the pane")
	cmd.Flags().StringVar(&flags.socket, "socket", "", "control socket path of the pane owner")
	cmd.Flags().StringVar(&flags.paneID, "pane-id", "", "tmux pane id of the pane (for example %3)")
	cmd.Flags().StringVar(&flags.host, "host", "", "host holding the pane")
	cmd.Flags().StringVar(&flags.bootID, "boot-id", "", "boot id of the host holding the pane")
	cmd.Flags().Int64Var(&flags.panePID, "pane-pid", 0, "process id of the pane")
	cmd.Flags().Int64Var(&flags.paneStart, "pane-start", 0, "start time of the pane process")
}

func (flags *paneTargetFlags) binding() tmuxlock.LiveBinding {
	return tmuxlock.LiveBinding{
		PanelBinding: tmuxlock.PanelBinding{
			OwnerUID: flags.ownerUID, Socket: flags.socket, PaneID: flags.paneID,
		},
		Host: flags.host, BootID: flags.bootID,
		PanePID: flags.panePID, PaneStart: flags.paneStart,
	}
}

func (flags *paneTargetFlags) target() protocol.PaneOperationTarget {
	return protocol.PaneOperationTarget{
		OwnerUID:  flags.ownerUID,
		Socket:    flags.socket,
		PaneID:    protocol.TmuxPaneID(flags.paneID),
		Host:      flags.host,
		BootID:    flags.bootID,
		PanePID:   flags.panePID,
		PaneStart: flags.paneStart,
	}
}

// readPaneReceipt returns the current lock receipt for the target, or nil
// when the tree has no readable receipt yet. A missing receipt is not an
// error: the first provision runs before any receipt exists.
func readPaneReceipt(binding tmuxlock.LiveBinding) *tmuxlock.PanelReceipt {
	policy, err := paneRecordPolicy()
	if err != nil {
		return nil
	}
	receipt, err := tmuxlock.ReadPanelReceipt(paneLockRoot, binding, policy)
	if err != nil {
		return nil
	}
	return &receipt
}

// paneRecordPolicy builds the deployment record policy from the node
// configuration, the same source the node builds it from. The CLI runs as
// the node user, so it reads the same file.
func paneRecordPolicy() (tmuxlock.RecordPolicy, error) {
	dirs, err := xdg.Resolve()
	if err != nil {
		return tmuxlock.RecordPolicy{}, fmt.Errorf("resolve node directories: %w", err)
	}
	cfg, err := config.Load(config.DefaultPath(dirs.Config))
	if err != nil {
		return tmuxlock.RecordPolicy{}, fmt.Errorf("load node configuration: %w", err)
	}
	if !cfg.TmuxLocks.Enabled {
		return tmuxlock.RecordPolicy{}, fmt.Errorf("pane locks are not configured")
	}
	return tmuxlock.RecordPolicy{
		PeerOwner: cfg.TmuxLocks.PeerOwner, PeerWriters: cfg.TmuxLocks.PeerWriters,
	}, nil
}

// paneAPICode extracts the API error code for the JSON line: the domain
// error code when the API sent one, the JSON-RPC code otherwise, and a
// stable local code when the call never reached the API.
func paneAPICode(err error) string {
	var rpcErr *rpcCallError
	if errors.As(err, &rpcErr) {
		if rpcErr.rpc.Data != nil {
			return string(rpcErr.rpc.Data.ErrorCode)
		}
		return strconv.FormatInt(rpcErr.rpc.Code, 10)
	}
	return "CALL_FAILED"
}

func paneAPIMessage(err error) string {
	var rpcErr *rpcCallError
	if errors.As(err, &rpcErr) {
		return rpcErr.rpc.Message
	}
	var exit *deps.ExitError
	if errors.As(err, &exit) && exit.Err != nil {
		return exit.Err.Error()
	}
	return err.Error()
}

func emitPaneError(cmd *cobra.Command, op, code string, err error, exit int) error {
	outcome := paneOutcome{Op: op, Result: paneResultError, Code: code, Message: err.Error()}
	if printErr := printPaneOutcome(cmd, outcome); printErr != nil {
		return printErr
	}
	return cliError(exit, err)
}

// runPaneCall sends one validated mutating call and prints the single JSON
// line: the API verdict (changed or noop) plus the lock receipt read before
// and after, so the operator sees owner_before and owner_after.
func runPaneCall(
	cmd *cobra.Command,
	op string,
	method protocol.ControlMethod,
	params any,
	binding tmuxlock.LiveBinding,
) error {
	var before *tmuxlock.PanelReceipt
	if receipt := readPaneReceipt(binding); receipt != nil {
		before = receipt
	}
	var result protocol.PaneOperationResult
	if err := call(cmd, method, params, &result); err != nil {
		return emitPaneError(cmd, op, paneAPICode(err), errors.New(paneAPIMessage(err)), paneCallExit(err))
	}
	outcome := paneOutcome{Op: op, Result: paneResultNoop}
	if result.Changed {
		outcome.Result = paneResultChanged
	}
	if before != nil {
		outcome.OwnerBefore = before.Owner
	}
	if after := readPaneReceipt(binding); after != nil {
		outcome.TxID = after.TxID
		outcome.OwnerAfter = after.Owner
		ownerGen := after.OwnerGen
		outcome.OwnerGen = &ownerGen
		outcome.Phase = after.Phase
	}
	return printPaneOutcome(cmd, outcome)
}

// paneCallExit keeps the exit mapping call assigns to API errors: a policy
// refusal stays a refusal, anything else is a generic error.
func paneCallExit(err error) int {
	var exit *deps.ExitError
	if errors.As(err, &exit) {
		return exit.Code
	}
	return exitcode.Error
}

func newPaneProvisionCmd() *cobra.Command {
	var flags paneTargetFlags
	cmd := &cobra.Command{
		SilenceUsage: true,
		Use:          "provision", Short: "Provision a panel pane lock for the target pane", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			params := protocol.PaneOperationParams{Target: flags.target()}
			if err := params.Validate(); err != nil {
				return emitPaneError(cmd, paneOpProvision, "INVALID_TARGET",
					fmt.Errorf("pane "+paneOpProvision+": invalid pane target: %w", err), exitcode.Usage)
			}
			return runPaneCall(cmd, paneOpProvision, protocol.ControlMethodPaneProvision, params, flags.binding())
		},
	}
	flags.add(cmd)
	return cmd
}

func newPaneTakeCmd() *cobra.Command {
	var flags paneTargetFlags
	cmd := &cobra.Command{
		SilenceUsage: true,
		Use:          "take", Short: "Take the panel pane lock for the target pane", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			params := protocol.PaneOperationParams{Target: flags.target()}
			if err := params.Validate(); err != nil {
				return emitPaneError(cmd, paneOpTake, "INVALID_TARGET",
					fmt.Errorf("pane "+paneOpTake+": invalid pane target: %w", err), exitcode.Usage)
			}
			return runPaneCall(cmd, paneOpTake, protocol.ControlMethodPaneTake, params, flags.binding())
		},
	}
	flags.add(cmd)
	return cmd
}

func newPaneReturnCmd() *cobra.Command {
	var flags paneTargetFlags
	cmd := &cobra.Command{
		SilenceUsage: true,
		Use:          "return", Short: "Return the panel pane lock for the target pane", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			params := protocol.PaneOperationParams{Target: flags.target()}
			if err := params.Validate(); err != nil {
				return emitPaneError(cmd, paneOpReturn, "INVALID_TARGET",
					fmt.Errorf("pane "+paneOpReturn+": invalid pane target: %w", err), exitcode.Usage)
			}
			return runPaneCall(cmd, paneOpReturn, protocol.ControlMethodPaneReturn, params, flags.binding())
		},
	}
	flags.add(cmd)
	return cmd
}

func newPaneReconcileCmd() *cobra.Command {
	var flags paneTargetFlags
	var expectedAttempt string
	cmd := &cobra.Command{
		SilenceUsage: true,
		Use:          "reconcile", Short: "Reconcile the panel pane lock when it shows the expected attempt", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			params := protocol.PaneAttemptParams{
				Target:          flags.target(),
				ExpectedAttempt: protocol.PaneAttemptID(expectedAttempt),
			}
			if err := params.Validate(); err != nil {
				code := "INVALID_TARGET"
				if params.Target.Validate() == nil {
					code = "INVALID_ATTEMPT"
				}
				return emitPaneError(cmd, paneOpReconcile, code,
					fmt.Errorf("pane "+paneOpReconcile+": invalid pane target or attempt: %w", err), exitcode.Usage)
			}
			return runPaneCall(cmd, paneOpReconcile, protocol.ControlMethodPaneReconcile, params, flags.binding())
		},
	}
	flags.add(cmd)
	cmd.Flags().StringVar(&expectedAttempt, "expected-attempt", "", "attempt identity the operator inspected (32 hex chars, or -)")
	return cmd
}

func newPaneRepairCmd() *cobra.Command {
	var flags paneTargetFlags
	cmd := &cobra.Command{
		SilenceUsage: true,
		Use:          "repair", Short: "Repair the panel pane lock for the target pane", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			params := protocol.PaneOperationParams{Target: flags.target()}
			if err := params.Validate(); err != nil {
				return emitPaneError(cmd, paneOpRepair, "INVALID_TARGET",
					fmt.Errorf("pane "+paneOpRepair+": invalid pane target: %w", err), exitcode.Usage)
			}
			return runPaneCall(cmd, paneOpRepair, protocol.ControlMethodPaneRepair, params, flags.binding())
		},
	}
	flags.add(cmd)
	return cmd
}

func newPaneStatusCmd() *cobra.Command {
	var flags paneTargetFlags
	cmd := &cobra.Command{
		SilenceUsage: true,
		Use:          paneStatusUse, Short: "Show the register and state receipt of the target pane lock", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := flags.target().Validate(); err != nil {
				outcome := paneOutcome{Op: paneStatusUse, Result: paneResultError, Code: "INVALID_TARGET", Message: err.Error()}
				if printErr := printPaneOutcome(cmd, outcome); printErr != nil {
					return printErr
				}
				return cliError(exitcode.Usage, fmt.Errorf("pane "+paneStatusUse+": invalid pane target: %w", err))
			}
			policy, err := paneRecordPolicy()
			if err != nil {
				outcome := paneOutcome{Op: paneStatusUse, Result: paneResultError, Code: "CONFIG_ERROR", Message: err.Error()}
				if printErr := printPaneOutcome(cmd, outcome); printErr != nil {
					return printErr
				}
				return cliError(exitcode.Error, fmt.Errorf("pane "+paneStatusUse+": %w", err))
			}
			receipt, err := tmuxlock.ReadPanelReceipt(paneLockRoot, flags.binding(), policy)
			if err != nil {
				outcome := paneOutcome{Op: paneStatusUse, Result: paneResultError, Code: "UNREADABLE", Message: err.Error()}
				if printErr := printPaneOutcome(cmd, outcome); printErr != nil {
					return printErr
				}
				return cliError(exitcode.Error, fmt.Errorf("pane "+paneStatusUse+": %w", err))
			}
			line, err := json.Marshal(receipt)
			if err != nil {
				return cliError(exitcode.Error, err)
			}
			line = append(line, '\n')
			if _, err := cmd.OutOrStdout().Write(line); err != nil {
				return cliError(exitcode.Error, err)
			}
			return nil
		},
	}
	flags.add(cmd)
	return cmd
}
