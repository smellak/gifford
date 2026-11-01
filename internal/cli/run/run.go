// Package run implements the `gifford run` subcommand: it runs the command
// after -- on a child PTY through the session wrapper and exits with its
// status.
package run

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/profiles"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy"
	"github.com/smellak/gifford/internal/xdg"
)

type runProfileLookup func(string, string) (profiles.Profile, error)

// sessionParams carries what runSession needs on every platform: the harness
// command and whole child environment, the profile name, the outer terminal
// and the notice writer.
type sessionParams struct {
	Command []string
	Env     []string
	Harness string
	Agent   string
	In      *os.File
	Out     *os.File
	Notice  io.Writer
	// profileLookup is an instance-local test seam. Nil loads the embedded
	// profiles exactly as production did before the seam existed.
	profileLookup runProfileLookup
}

// runSession runs the harness wrapped in the session on unix, or straight
// through the proxy core where no PTY wrapper exists. The platform files
// own it; run.go only defines its parameters above.

// daemonSocketName is the daemon's socket in the runtime directory. A
// placeholder: the subphase that builds the daemon owns the name.
const daemonSocketName = "daemon.sock"

const (
	genericHarness              = "generic"
	captureSessionEnv           = "GIFFORD_CAPTURE_SESSION_ID"
	claudePromptSuggestionEnv   = "CLAUDE_CODE_ENABLE_PROMPT_SUGGESTION"
	claudePromptSuggestionValue = "false"
)

// NewCmd returns the gifford run command.
func NewCmd(d *deps.Deps) *cobra.Command {
	return newCmdWithProfileLookup(d, nil)
}

// newCmdWithProfileLookup builds one command with an instance-local profile
// dependency. Tests use it for measured fixtures; production always passes nil.
func newCmdWithProfileLookup(_ *deps.Deps, profileLookup runProfileLookup) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run [flags] -- <command> [args...]",
		Short: "PTY proxy for one agent",
		Long: `Run <command> on a child pseudo-terminal and copy every byte between it and
this terminal unchanged. Everything after -- is the command line.

If the gifford daemon is not running, one line on stderr says so before the
command starts. So does one line when <command> is a Node.js launcher script:
if it starts the harness as a child process, the process guard defers every
automatic delivery; "Node.js launchers" in README.md has the recipe.
--agent registers the run with the hub under that agent id and holds the
session while the command lives; without a hub, or when registration fails,
one stderr line says so and the command runs unchanged. On Unix, --agent
also listens on the agent socket, <runtime>/agents/<agent>.sock, the Core's
link to this session; when it cannot open, one stderr line says so and the
command runs unchanged. The socket sends the session's state observations
only when config.yaml loads with state_analysis, and only to a Core that
takes them.
Before launching an admitted --agent run, Gifford also creates the private
native-event entry <runtime>/native-events/<agent>.sock and passes it to the
child as GIFFORD_ADAPTER_SOCKET. A failure leaves the PTY screen path active
and never prevents the command from starting.
--harness selects the harness profile that parses the keys.

With --agent and --harness claude-code, the child receives
CLAUDE_CODE_ENABLE_PROMPT_SUGGESTION=false unless that variable is already
present; any explicit value is preserved. Every child receives the local
GIFFORD_CAPTURE_SESSION_ID of its recording, replacing an inherited value;
the parent and the --version probe do not receive it. Every other environment
entry is preserved. Dim Claude suggestion text is not evidence that the input
is empty.

Exit status: the command's own status, or 128+N when signal N killed it.
gifford's own failures exit 1 (error), 2 (usage) or 3 (not implemented on
this platform).`,
		Args: cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if cmd.ArgsLenAtDash() != 0 || len(args) == 0 {
				return &deps.ExitError{
					Code: exitcode.Usage,
					Err:  errors.New("gifford run: expected -- <command> [args...]"),
				}
			}
			in, ok := cmd.InOrStdin().(*os.File)
			if !ok || in == nil {
				return &deps.ExitError{
					Code: exitcode.Error,
					Err:  fmt.Errorf("stdin: %w", proxy.ErrNotTerminal),
				}
			}
			out, ok := cmd.OutOrStdout().(*os.File)
			if !ok || out == nil {
				return &deps.ExitError{
					Code: exitcode.Error,
					Err:  fmt.Errorf("stdout: %w", proxy.ErrNotTerminal),
				}
			}
			harness, err := cmd.Flags().GetString("harness")
			if err != nil {
				return &deps.ExitError{
					Code: exitcode.Usage,
					Err:  fmt.Errorf("gifford run: %w", err),
				}
			}
			agent, err := cmd.Flags().GetString("agent")
			if err != nil {
				return &deps.ExitError{
					Code: exitcode.Usage,
					Err:  fmt.Errorf("gifford run: %w", err),
				}
			}
			if agent != "" && !protocol.AgentID(agent).Valid() {
				return &deps.ExitError{
					Code: exitcode.Usage,
					Err:  fmt.Errorf("gifford run: invalid agent id %q", agent),
				}
			}
			if nodeLauncher(args[0]) {
				_, _ = fmt.Fprintln(cmd.ErrOrStderr(), nodeLauncherNotice(args[0]))
			}
			exit, err := runSession(cmd.Context(), sessionParams{
				Command:       args,
				Env:           runChildEnvironment(harness, agent),
				Harness:       harness,
				Agent:         agent,
				In:            in,
				Out:           out,
				Notice:        cmd.ErrOrStderr(),
				profileLookup: profileLookup,
			})
			return exitError(exit, err)
		},
	}
	cmd.Flags().String("agent", "", "agent id to run the proxy for")
	cmd.Flags().String("harness", "", "harness profile to parse the keys with")
	cmd.SetFlagErrorFunc(func(_ *cobra.Command, err error) error {
		return &deps.ExitError{
			Code: exitcode.Usage,
			Err:  fmt.Errorf("gifford run: %w", err),
		}
	})
	return cmd
}

// runChildEnvironment returns nil on every transparent path, leaving os/exec
// to inherit the process environment. A Claude agent run needs a whole
// environment snapshot to add one default without changing the process or an
// explicit user value.
func runChildEnvironment(harness, agent string) []string {
	if agent == "" || harness != string(protocol.HarnessClaudeCode) {
		return nil
	}
	env := os.Environ()
	if _, exists := os.LookupEnv(claudePromptSuggestionEnv); exists {
		return env
	}
	return append(env, claudePromptSuggestionEnv+"="+claudePromptSuggestionValue)
}

func setEnvironmentVariable(env []string, name, value string) []string {
	if env == nil {
		env = os.Environ()
	}
	out := make([]string, 0, len(env)+1)
	for _, entry := range env {
		key, _, _ := strings.Cut(entry, "=")
		if key != name {
			out = append(out, entry)
		}
	}
	return append(out, name+"="+value)
}

func withoutEnvironmentVariable(env []string, name string) []string {
	if env == nil {
		env = os.Environ()
	}
	out := make([]string, 0, len(env))
	for _, entry := range env {
		key, _, _ := strings.Cut(entry, "=")
		if key != name {
			out = append(out, entry)
		}
	}
	return out
}

func daemonSocket() string {
	dirs, err := xdg.Resolve()
	if err != nil {
		return ""
	}
	return filepath.Join(dirs.Runtime, daemonSocketName)
}

func exitError(exit proxy.Exit, err error) error {
	if err != nil {
		if errors.Is(err, errors.ErrUnsupported) {
			return &deps.ExitError{
				Code: exitcode.NotImplemented,
				Err:  fmt.Errorf("gifford run: %w", err),
			}
		}
		return &deps.ExitError{
			Code: exitcode.Error,
			Err:  fmt.Errorf("gifford run: %w", err),
		}
	}
	if exit.Status() != 0 {
		return &deps.ExitError{
			Code: exit.Status(),
			Err:  fmt.Errorf("gifford run: %s", exit.String()),
		}
	}
	return nil
}
