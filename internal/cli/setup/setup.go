// Package setup implements `gifford setup`: it connects the installed
// coding-agent harnesses to Gifford, shows what it would change first, and
// can undo its last run.
package setup

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/profiles"
	core "github.com/smellak/gifford/internal/setup"
)

type options struct {
	all, dryRun, apply, undo, check bool
	approveTools, noApproveTools    bool
}

// NewCmd returns the gifford setup command.
func NewCmd(_ *deps.Deps) *cobra.Command {
	var opts options
	cmd := &cobra.Command{
		Use:   "setup --all (--dry-run | --apply | --undo | --check)",
		Short: "Connect the installed harnesses to Gifford",
		Long: `Detect Claude Code, Codex, OpenCode and Muse Code on PATH with their exact
versions, and show for each whether Gifford has a measured terminal profile
(MEASURED) and so may deliver a notice automatically, or not (UNMEASURED: every
delivery waits for an approval).

--dry-run lists every change without making it. --apply backs up each file it
touches, merges into it without removing anything, registers the gifford MCP
server and the observe-only adapters, and checks that the MCP server answers.
Pre-approving Gifford's four message tools always asks first; --approve-tools
or --no-approve-tools answers in advance. --undo restores the files of the last
run and removes what it registered. --check only checks the MCP server.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return run(cmd.Context(), cmd.InOrStdin(), cmd.OutOrStdout(), opts)
		},
	}
	flags := cmd.Flags()
	flags.BoolVar(&opts.all, "all", false, "set up every detected harness")
	flags.BoolVar(&opts.dryRun, "dry-run", false, "show the changes without making them")
	flags.BoolVar(&opts.apply, "apply", false, "make the changes")
	flags.BoolVar(&opts.undo, "undo", false, "undo the last setup run")
	flags.BoolVar(&opts.check, "check", false, "only check that the MCP server answers")
	flags.BoolVar(&opts.approveTools, "approve-tools", false, "pre-approve the four message tools without asking")
	flags.BoolVar(&opts.noApproveTools, "no-approve-tools", false, "never pre-approve the message tools")
	return cmd
}

func usage(err error) error { return &deps.ExitError{Code: exitcode.Usage, Err: err} }

func failure(err error) error { return &deps.ExitError{Code: exitcode.Error, Err: err} }

func run(ctx context.Context, in io.Reader, out io.Writer, opts options) error {
	modes := 0
	for _, set := range []bool{opts.dryRun, opts.apply, opts.undo, opts.check} {
		if set {
			modes++
		}
	}
	if modes != 1 {
		return usage(errors.New("choose exactly one of --dry-run, --apply, --undo or --check"))
	}
	if (opts.dryRun || opts.apply) && !opts.all {
		return usage(errors.New("--all is required: setup connects every detected harness"))
	}
	if opts.approveTools && opts.noApproveTools {
		return usage(errors.New("--approve-tools and --no-approve-tools exclude each other"))
	}
	env, err := systemEnv()
	if err != nil {
		return failure(err)
	}
	switch {
	case opts.undo:
		dir, err := core.Undo(ctx, env, out)
		if err != nil {
			return failure(fmt.Errorf("undo: %w", err))
		}
		_, _ = fmt.Fprintf(out, "undone: setup run %s\n", filepath.Base(dir))
		return nil
	case opts.check:
		return checkMCP(ctx, out, env.Gifford)
	}
	plan, err := core.NewPlan(ctx, env)
	if err != nil {
		return failure(err)
	}
	core.WriteReport(out, plan, opts.apply)
	warnPath(out, env)
	if opts.dryRun {
		return nil
	}
	_, _ = fmt.Fprintln(out)
	reader := bufio.NewReader(in)
	consent := func(step core.Step) bool {
		switch {
		case opts.approveTools:
			return true
		case opts.noApproveTools:
			return false
		}
		file, ok := in.(*os.File)
		if !ok || !term.IsTerminal(int(file.Fd())) {
			return false
		}
		_, _ = fmt.Fprintf(out, "%s: %s? [y/N] ", step.Harness, step.Description)
		answer, _ := reader.ReadString('\n')
		answer = strings.ToLower(strings.TrimSpace(answer))
		return answer == "y" || answer == "yes"
	}
	dir, err := core.Apply(ctx, env, plan, consent, out)
	if dir != "" {
		_, _ = fmt.Fprintf(out, "backup: %s (gifford setup --undo restores it)\n", dir)
	}
	if err != nil {
		return failure(err)
	}
	return checkMCP(ctx, out, env.Gifford)
}

func checkMCP(ctx context.Context, out io.Writer, gifford string) error {
	err := core.CheckMCP(ctx, gifford)
	switch {
	case err == nil:
		_, _ = fmt.Fprintln(out, "mcp: gifford mcp answers and lists its four message tools")
		return nil
	case errors.Is(err, core.ErrHubUnavailable):
		_, _ = fmt.Fprintf(out, "mcp: NOT CHECKED: %v\n", err)
		return nil
	default:
		return failure(fmt.Errorf("mcp: %w", err))
	}
}

// warnPath warns when the observer hooks, which run `gifford` from PATH,
// would start another binary than this one.
func warnPath(out io.Writer, env core.Env) {
	onPath, err := exec.LookPath("gifford")
	if err == nil {
		onPath, err = filepath.EvalSymlinks(onPath)
	}
	if err != nil || onPath != env.Gifford {
		_, _ = fmt.Fprintf(out, "warning: the observer hooks run `gifford` from PATH, which is not %s\n", env.Gifford)
	}
}

func systemEnv() (core.Env, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return core.Env{}, fmt.Errorf("find home directory: %w", err)
	}
	self, err := os.Executable()
	if err == nil {
		self, err = filepath.EvalSymlinks(self)
	}
	if err != nil {
		return core.Env{}, fmt.Errorf("find the gifford binary: %w", err)
	}
	loaded, err := profiles.Load()
	if err != nil {
		return core.Env{}, fmt.Errorf("load terminal profiles: %w", err)
	}
	return core.Env{
		Home:       home,
		ConfigHome: xdgDir("XDG_CONFIG_HOME", filepath.Join(home, ".config")),
		DataHome:   xdgDir("XDG_DATA_HOME", filepath.Join(home, ".local", "share")),
		CodexHome:  os.Getenv("CODEX_HOME"),
		Gifford:    self,
		LookPath:   exec.LookPath,
		Run: func(ctx context.Context, name string, args ...string) ([]byte, error) {
			ctx, cancel := context.WithTimeout(ctx, time.Minute)
			defer cancel()
			out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
			if err != nil {
				return out, fmt.Errorf("run %s: %w", filepath.Base(name), err)
			}
			return out, nil
		},
		Now:      clock.System{}.Now,
		Profiles: loaded,
	}, nil
}

func xdgDir(name, fallback string) string {
	if value := os.Getenv(name); filepath.IsAbs(value) {
		return value
	}
	return fallback
}
